use crossbeam::channel::unbounded;
use crossbeam::channel::Receiver;
use crossbeam::channel::RecvTimeoutError;
use crossbeam::channel::Sender;

use super::OrderedBuffer;
use super::PacketBufferAddress;
use crate::packet::ChannelID;
use crate::DataVersion;

use crate::packet::PacketSet;
use crate::packet::WorkQueue;
use std::sync::{Arc, Mutex};
use std::thread;
use std::thread::JoinHandle;
use std::time::Duration;

pub trait PacketSynchronizer: Send {
    fn start(
        &mut self,
        buffer: Arc<Mutex<dyn OrderedBuffer>>,
        work_queue: Arc<WorkQueue>,
        node_id: usize,
        available_channels: &Vec<ChannelID>,
    ) -> ();

    fn packet_event(&self, packet_address: PacketBufferAddress);

    fn stop(&mut self) -> ();
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
    data_version: &DataVersion,
) -> Option<Vec<PacketBufferAddress>> {
    let buffer = ordered_buffer.lock().unwrap();
    let is_data_available: Vec<PacketBufferAddress> = available_channels
        .into_iter()
        .map(|channel_id| {
            if buffer.has_version(&channel_id, data_version) {
                return Some((channel_id.clone(), data_version.clone()));
            }
            None
        })
        .flatten()
        .collect();

    if is_data_available.len() == available_channels.len() {
        return Some(is_data_available);
    }
    None
}

fn get_packets_for_version(
    data_versions: &Vec<PacketBufferAddress>,
    buffer: &mut Arc<Mutex<dyn OrderedBuffer>>,
) -> PacketSet {
    let mut buffer_locked = buffer.lock().unwrap();

    let packet_set = data_versions
        .iter()
        .map(|(channel_id, data_version)| {
            let removed_packet = buffer_locked.consume(&(channel_id.clone(), data_version.clone()));
            if removed_packet.is_err() {
                eprintln!(
                    "Error while reading data {}",
                    removed_packet.as_ref().err().unwrap()
                )
            }
            match removed_packet.unwrap() {
                Some(entry) => (
                    channel_id.clone(),
                    Some(((channel_id.clone(), data_version.clone()), entry)),
                ),
                None => (channel_id.clone(), None),
            }
        })
        .collect();

    PacketSet::new(packet_set)
}

impl PacketSynchronizer for TimestampSynchronizer {
    fn start(
        &mut self,
        buffer: Arc<Mutex<dyn OrderedBuffer>>,
        work_queue: Arc<WorkQueue>,
        node_id: usize,
        available_channels: &Vec<ChannelID>,
    ) -> () {
        let mut buffer_thread = buffer.clone();
        let available_channels = available_channels.clone();

        let receive_thread = self.receive_event.clone();
        let handler = thread::spawn(move || loop {
            let result = receive_thread.recv_timeout(Duration::from_millis(100));
            if let Err(RecvTimeoutError::Timeout) = result {
                continue;
            }

            let data = result.unwrap();
            if data.is_none() {
                return ();
            }

            let data = data.unwrap();

            if let Some(data_versions) =
                synchronize(&mut buffer_thread, &available_channels, &data.1)
            {
                let packet_set = get_packets_for_version(&data_versions, &mut buffer_thread);
                work_queue.push(node_id, packet_set)
            }
        });
        self.thread_handler = Some(handler);
    }

    fn stop(&mut self) -> () {
        self.send_event.send(None).unwrap();
        if self.thread_handler.is_some() {
            self.thread_handler.take().unwrap().join().unwrap();
        }
        ()
    }

    fn packet_event(&self, packet_address: PacketBufferAddress) {
        self.send_event.send(Some(packet_address)).unwrap();
    }
}
