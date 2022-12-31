use super::synchronizers::PacketSynchronizer;

use super::BufferError;
use crate::packet::ChannelID;
use crate::packet::DataVersion;
use crate::packet::{PacketSet, UntypedPacket};
use crossbeam::channel::Receiver;
use crossbeam::channel::RecvTimeoutError;
use crossbeam::channel::{unbounded, Sender};
use crossbeam::deque::Injector;
use futures::channel::oneshot::channel;
use indexmap::IndexMap;
use itertools::Itertools;
use std::sync::Condvar;
use std::sync::{Arc, Mutex};
use std::thread;

use std::collections::HashMap;
use std::thread::JoinHandle;
use std::time::Duration;

use super::PacketBufferAddress;
use super::PacketWithAddress;
use crate::packet::WorkQueue;

#[derive(Default)]
pub struct HashmapBufferedData {
    data: HashMap<PacketBufferAddress, UntypedPacket>,
}

pub trait DataBuffer: Sync + Send {
    fn insert(
        &mut self,
        channel: &ChannelID,
        packet: UntypedPacket,
    ) -> Result<PacketBufferAddress, BufferError>;

    fn consume(&mut self, version: &PacketBufferAddress) -> Option<UntypedPacket>;

    fn get(&mut self, version: &PacketBufferAddress) -> Option<&UntypedPacket>;

    fn available_channels(&self) -> Vec<ChannelID>;
}

pub trait OrderedBuffer: DataBuffer {
    fn has_version(&self, channel: &ChannelID, version: &DataVersion) -> bool;
}

impl DataBuffer for HashmapBufferedData {
    fn insert(
        &mut self,
        channel: &ChannelID,
        packet: UntypedPacket,
    ) -> Result<PacketBufferAddress, BufferError> {
        if self.has_version(&channel, &packet.version) {
            return Err(BufferError::DuplicateDataVersionError((
                channel.clone(),
                packet.version.clone(),
            )));
        }

        let data_version = (channel.clone(), packet.version.clone());
        self.data.insert(data_version.clone(), packet);
        Ok(data_version)
    }

    fn consume(&mut self, version: &PacketBufferAddress) -> Option<UntypedPacket> {
        self.data.remove(&version)
    }

    fn get(&mut self, version: &PacketBufferAddress) -> Option<&UntypedPacket> {
        self.data.get(&version)
    }

    fn available_channels(&self) -> Vec<ChannelID> {
        self.data
            .keys()
            .into_iter()
            .map(|key| key.0.clone())
            .collect_vec()
    }
}

impl OrderedBuffer for HashmapBufferedData {
    fn has_version(&self, channel: &ChannelID, version: &DataVersion) -> bool {
        let data_version = (channel.clone(), version.clone());
        self.data.contains_key(&data_version)
    }
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
) -> Option<DataVersion> {
    let buffer = ordered_buffer.lock().unwrap();
    let is_data_available = available_channels
        .iter()
        .map(|channel_id| buffer.has_version(&channel_id, data_version))
        .all(|has_version| has_version);
    if is_data_available {
        return Some(*data_version);
    }
    None
}

fn get_packets_for_version(
    data_version: &DataVersion,
    buffer: &mut Arc<Mutex<dyn OrderedBuffer>>,
) -> PacketSet {
    let mut buffer_locked = buffer.lock().unwrap();
    let channels: Vec<ChannelID> = buffer_locked.available_channels().clone();

    let packet_set: IndexMap<ChannelID, Option<PacketWithAddress>> = channels
        .iter()
        .map(|channel_id| {
            let removed_packet = buffer_locked.consume(&(channel_id.clone(), data_version.clone()));
            match removed_packet {
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
                println!("ASDASD2");
                continue;
            }

            let data = result.unwrap();
            if data.is_none() {
                return ();
            }

            let data = data.unwrap();

            if let Some(data_version) =
                synchronize(&mut buffer_thread, &available_channels, &data.1)
            {
                let packet_set = get_packets_for_version(&data_version, &mut buffer_thread);
                work_queue.push(node_id, packet_set)
            }
        });
        self.thread_handler = Some(handler);
    }

    fn stop(&mut self) -> () {
        self.send_event.send(None).unwrap();
        if self.thread_handler.is_some() {
            self.thread_handler.take().unwrap().join();
        }
        ()
    }

    fn packet_event(&self, packet_address: PacketBufferAddress) {
        self.send_event.send(Some(packet_address));
    }
}
