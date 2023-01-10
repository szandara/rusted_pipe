use crossbeam::channel::unbounded;
use crossbeam::channel::Receiver;
use crossbeam::channel::RecvTimeoutError;
use crossbeam::channel::Sender;

use super::OrderedBuffer;
use super::PacketBufferAddress;
use crate::packet::ChannelID;

use crate::packet::PacketSet;
use crate::packet::WorkQueue;
use std::sync::{Arc, Mutex};
use std::thread;
use std::thread::JoinHandle;
use std::time::Duration;

pub trait PacketSynchronizer: Send {
    // fn start(
    //     &mut self,
    //     buffer: Arc<Mutex<dyn OrderedBuffer>>,
    //     work_queue: Arc<WorkQueue>,
    //     node_id: usize,
    //     available_channels: &Vec<ChannelID>,
    // ) -> ();

    // fn packet_event(&self, packet_address: PacketBufferAddress);

    // fn stop(&mut self) -> ();
    fn synchronize(
        &self,
        ordered_buffer: &Arc<Mutex<dyn OrderedBuffer>>,
        available_channels: &Vec<ChannelID>,
        work_queue: Arc<WorkQueue>,
    );
}

#[derive(Debug)]
pub struct TimestampSynchronizer {
    thread_handler: Option<JoinHandle<()>>,
    send_event: Sender<Option<PacketBufferAddress>>,
    receive_event: Receiver<Option<PacketBufferAddress>>,
}

impl TimestampSynchronizer {
    pub fn default() -> Self {
        let (send_event, receive_event) = unbounded::<Option<PacketBufferAddress>>();

        TimestampSynchronizer {
            thread_handler: None,
            send_event,
            receive_event,
        }
    }
}

fn synchronize(
    ordered_buffer: &mut Arc<Mutex<dyn OrderedBuffer>>,
    available_channels: &Vec<ChannelID>,
) -> Option<Vec<PacketBufferAddress>> {
    let buffer = ordered_buffer.lock().unwrap();
    let mut min_version = None;
    for channel in available_channels {
        let min_channel_version = buffer.peek(channel);
        if min_channel_version.is_none() {
            return None;
        }
        match min_version {
            Some(existing_min_message) => {
                if min_channel_version.unwrap() != existing_min_message {
                    return None;
                }
            }
            None => min_version = min_channel_version,
        }
    }
    if min_version.is_some() {
        return Some(
            available_channels
                .iter()
                .map(|channel| (channel.clone(), min_version.unwrap().clone()))
                .collect(),
        );
    }
    None
}

fn get_packets_for_version(
    data_versions: &Vec<PacketBufferAddress>,
    buffer: &mut Arc<Mutex<dyn OrderedBuffer>>,
) -> Option<PacketSet> {
    let mut buffer_locked = buffer.lock().unwrap();
    let mut has_none = false;
    let packet_set = data_versions
        .iter()
        .map(|(channel_id, data_version)| {
            let removed_packet = buffer_locked.pop(channel_id);
            println!("Adding {:?}", removed_packet);
            if removed_packet.is_err() {
                eprintln!(
                    "Error while reading data {}",
                    removed_packet.as_ref().err().unwrap()
                )
            }
            match removed_packet.unwrap() {
                Some(entry) => {
                    if entry.version != *data_version {
                        has_none = true;
                    }
                    return (
                        channel_id.clone(),
                        Some(((channel_id.clone(), data_version.clone()), entry)),
                    );
                }
                None => {
                    has_none = true;
                    return (channel_id.clone(), None);
                }
            }
        })
        .collect();
    if has_none {
        eprintln!(
            "Found wrong or none entries when generating packet set for {:?}. Skipping.",
            data_versions
        );
        return None;
    }
    Some(PacketSet::new(packet_set))
}

impl PacketSynchronizer for TimestampSynchronizer {
    fn synchronize(
        &self,
        ordered_buffer: &Arc<Mutex<dyn OrderedBuffer>>,
        available_channels: &Vec<ChannelID>,
        work_queue: Arc<WorkQueue>,
    ) {
        loop {
            if let Some(data_versions) =
                synchronize(&mut ordered_buffer.clone(), &available_channels)
            {
                if let Some(packet_set) =
                    get_packets_for_version(&data_versions, &mut ordered_buffer.clone())
                {
                    work_queue.push(packet_set)
                }
            } else {
                break;
            }
        }
    }
    // fn start(
    //     &mut self,
    //     buffer: Arc<Mutex<dyn OrderedBuffer>>,
    //     work_queue: Arc<WorkQueue>,
    //     node_id: usize,
    //     available_channels: &Vec<ChannelID>,
    // ) -> () {
    //     let mut buffer_thread = buffer.clone();
    //     let available_channels = available_channels.clone();

    //     let receive_thread = self.receive_event.clone();
    //     let handler = thread::spawn(move || loop {
    //         let result = receive_thread.recv_timeout(Duration::from_millis(100));
    //         if let Err(RecvTimeoutError::Timeout) = result {
    //             continue;
    //         }

    //         let data = result.unwrap();
    //         if data.is_none() {
    //             return ();
    //         }

    //         loop {
    //             if let Some(data_versions) = synchronize(&mut buffer_thread, &available_channels) {
    //                 if let Some(packet_set) =
    //                     get_packets_for_version(&data_versions, &mut buffer_thread)
    //                 {
    //                     work_queue.push(node_id, packet_set)
    //                 }
    //             } else {
    //                 break;
    //             }
    //         }
    //     });
    //     self.thread_handler = Some(handler);
    // }

    // fn stop(&mut self) -> () {
    //     self.send_event.send(None).unwrap();
    //     if self.thread_handler.is_some() {
    //         self.thread_handler.take().unwrap().join().unwrap();
    //     }
    //     ()
    // }

    // fn packet_event(&self, packet_address: PacketBufferAddress) {
    //     self.send_event.send(Some(packet_address)).unwrap();
    // }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffers::channel_buffers::BoundedBufferedData;
    use crate::buffers::single_buffers::FixedSizeBTree;
    use crate::buffers::{DataBuffer, PacketBufferAddress};
    use crate::channels::{ChannelID, Packet};
    use crate::DataVersion;

    fn create_test_buffer() -> BoundedBufferedData<FixedSizeBTree> {
        let mut buffer = BoundedBufferedData::<FixedSizeBTree>::new(100);

        buffer
            .create_channel(&ChannelID::new("test1".to_string()))
            .unwrap();
        buffer
            .create_channel(&ChannelID::new("test2".to_string()))
            .unwrap();
        return buffer;
    }

    #[test]
    fn test_read_channel_fails_if_channel_not_added() {
        let buffer = BoundedBufferedData::<FixedSizeBTree>::new(100);
        let safe_buffer: Arc<Mutex<dyn OrderedBuffer>> = Arc::new(Mutex::new(buffer));

        let packet = Packet::<String> {
            data: Box::new("data".to_string()),
            version: DataVersion { timestamp: 0 },
        };

        safe_buffer
            .lock()
            .unwrap()
            .create_channel(&ChannelID::new("test1".to_string()))
            .unwrap();

        assert!(safe_buffer
            .lock()
            .unwrap()
            .insert(
                &ChannelID::new("test1".to_string()),
                packet.clone().to_untyped(),
            )
            .is_ok());

        assert!(safe_buffer
            .lock()
            .unwrap()
            .insert(
                &ChannelID::new("test3".to_string()),
                packet.clone().to_untyped(),
            )
            .is_err());
    }

    #[test]
    fn test_read_channel_get_packets_packetset_has_all_channels_and_data() {
        let mut data: Vec<PacketBufferAddress> = vec![];

        data.push((
            ChannelID::new("test1".to_string()),
            DataVersion { timestamp: 0 },
        ));
        data.push((
            ChannelID::new("test2".to_string()),
            DataVersion { timestamp: 0 },
        ));
        let buffer = create_test_buffer();

        let mut safe_buffer: Arc<Mutex<dyn OrderedBuffer>> = Arc::new(Mutex::new(buffer));
        add_data(&safe_buffer, "test1".to_string(), 0);
        add_data(&safe_buffer, "test2".to_string(), 0);
        let packetset = get_packets_for_version(&data, &mut safe_buffer).unwrap();

        assert_eq!(packetset.channels(), 2);
        assert!(packetset
            .get_channel::<String>(&ChannelID::from("test1"))
            .is_ok());
        assert!(packetset
            .get_channel::<String>(&ChannelID::from("test2"))
            .is_ok());
    }

    fn add_data(
        buffer: &Arc<Mutex<dyn OrderedBuffer>>,
        channel_id: String,
        version_timestamp: u64,
    ) {
        let packet = Packet::<String> {
            data: Box::new("data".to_string()),
            version: DataVersion {
                timestamp: version_timestamp,
            },
        };

        buffer
            .lock()
            .unwrap()
            .insert(&ChannelID::new(channel_id), packet.clone().to_untyped())
            .unwrap();
    }

    fn check_packet(packet_set: PacketSet, expected_timestamp: u64) {
        for i in 0..2 {
            assert_eq!(
                packet_set.get::<String>(i).unwrap().version.timestamp,
                expected_timestamp
            );
        }
    }

    fn check_data(work_queue: &Arc<WorkQueue>, expected_timestamp: u64) {
        let work = work_queue.steal().success().unwrap();
        check_packet(work.packet_data, expected_timestamp);
    }

    #[test]
    fn test_timestamp_synchronize_retains_order() {
        let buffer = create_test_buffer();
        let mut safe_buffer: Arc<Mutex<dyn OrderedBuffer>> = Arc::new(Mutex::new(buffer));
        let channels = vec![
            ChannelID {
                id: "test1".to_string(),
            },
            ChannelID {
                id: "test2".to_string(),
            },
        ];
        add_data(&safe_buffer, "test1".to_string(), 1);
        add_data(&safe_buffer, "test2".to_string(), 1);

        add_data(&safe_buffer, "test1".to_string(), 2);
        add_data(&safe_buffer, "test2".to_string(), 2);

        let packet_set = synchronize(&mut safe_buffer, &channels).unwrap();
        assert_eq!(packet_set[0].1.timestamp, 1);
        assert_eq!(packet_set[1].1.timestamp, 1);
    }

    #[test]
    fn test_timestamp_synchronize_is_none_if_no_data_on_channel() {
        let buffer = create_test_buffer();
        let mut safe_buffer: Arc<Mutex<dyn OrderedBuffer>> = Arc::new(Mutex::new(buffer));
        let channels = vec![
            ChannelID {
                id: "test1".to_string(),
            },
            ChannelID {
                id: "test2".to_string(),
            },
        ];

        add_data(&safe_buffer, "test1".to_string(), 2);
        add_data(&safe_buffer, "test1".to_string(), 3);

        let packet_set = synchronize(&mut safe_buffer, &channels);
        assert!(packet_set.is_none());
    }
}
