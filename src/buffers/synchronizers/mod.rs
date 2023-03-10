pub mod first_sync;
pub mod timestamp;

use crate::buffers::OrderedBuffer;
use crate::packet::ChannelID;
use crate::DataVersion;

use crate::packet::PacketSet;
use crate::packet::WorkQueue;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

pub trait PacketSynchronizer: Send {
    fn synchronize(
        &mut self,
        ordered_buffer: Arc<Mutex<dyn OrderedBuffer>>,
        work_queue: Arc<WorkQueue>,
    );
}

fn synchronize(
    ordered_buffer: &mut Arc<Mutex<dyn OrderedBuffer>>,
) -> Option<HashMap<String, Option<DataVersion>>> {
    let min_version = get_min_versions(ordered_buffer);
    let version = min_version.values().next().unwrap();
    if min_version.values().all(|v| v.is_some()) && min_version.values().all(|v| v == version) {
        return Some(min_version);
    }
    None
}

fn get_min_versions(
    buffer: &mut Arc<Mutex<dyn OrderedBuffer>>,
) -> HashMap<String, Option<DataVersion>> {
    let buffer = buffer.lock().unwrap();
    let mut out_map = HashMap::<String, Option<DataVersion>>::default();

    for channel in buffer.available_channels().iter() {
        out_map.insert(channel.to_string(), buffer.peek(&channel).cloned());
    }
    return out_map;
}

fn get_packets_for_version(
    data_versions: &HashMap<String, Option<DataVersion>>,
    buffer: &mut Arc<Mutex<dyn OrderedBuffer>>,
    exact_match: bool,
) -> Option<PacketSet> {
    let mut buffer_locked = buffer.lock().unwrap();
    let mut valid_counter = 0;

    let packet_set = data_versions
        .iter()
        .map(|(channel_id, data_version)| {
            loop {
                let removed_packet = buffer_locked.pop(channel_id);
                if removed_packet.is_err() {
                    eprintln!(
                        "Error while reading data {}",
                        removed_packet.as_ref().err().unwrap()
                    );
                    break;
                }
                if let Some(entry) = removed_packet.unwrap() {
                    if let Some(data_version) = data_version {
                        if entry.version == *data_version {
                            valid_counter += 1;
                            return (
                                ChannelID::from(channel_id.as_str()),
                                Some((
                                    (ChannelID::from(channel_id.as_str()), data_version.clone()),
                                    entry,
                                )),
                            );
                        } else {
                            if exact_match {
                                break;
                            }
                        }
                    }
                    if exact_match {
                        break;
                    }
                } else {
                    break;
                }
            }
            return (ChannelID::from(channel_id.as_str()), None);
        })
        .collect();

    if valid_counter != buffer_locked.available_channels().len() {
        eprintln!(
            "Found mismatched entries when generating packet set for {:?}. Skipping.",
            data_versions
        );
        return None;
    }
    Some(PacketSet::new(packet_set))
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::buffers::single_buffers::FixedSizeBTree;
    use crate::channels::{ChannelID, Packet};
    use crate::DataVersion;

    // pub fn create_test_buffer() -> BoundedBufferedData<FixedSizeBTree<String>> {
    //     let mut buffer = BoundedBufferedData::<FixedSizeBTree>::new(100, false);

    //     buffer
    //         .create_channel(&ChannelID::new("test1".to_string()))
    //         .unwrap();
    //     buffer
    //         .create_channel(&ChannelID::new("test2".to_string()))
    //         .unwrap();
    //     return buffer;
    // }

    // pub fn add_data(
    //     buffer: &Arc<Mutex<dyn OrderedBuffer>>,
    //     channel_id: String,
    //     version_timestamp: u128,
    // ) {
    //     let packet = Packet::<String> {
    //         data: Box::new("data".to_string()),
    //         version: DataVersion {
    //             timestamp: version_timestamp,
    //         },
    //     };

    //     buffer
    //         .lock()
    //         .unwrap()
    //         .insert(&ChannelID::new(channel_id), packet.clone().to_untyped())
    //         .unwrap();
    // }

    // #[test]
    // fn test_timestamp_synchronize_is_none_if_no_data_on_channel() {
    //     let buffer = create_test_buffer();
    //     let mut safe_buffer: Arc<Mutex<dyn OrderedBuffer>> = Arc::new(Mutex::new(buffer));

    //     add_data(&safe_buffer, "test1".to_string(), 2);
    //     add_data(&safe_buffer, "test1".to_string(), 3);

    //     let packet_set = synchronize(&mut safe_buffer);
    //     assert!(packet_set.is_none());
    // }
}
