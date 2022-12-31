use super::synchronizers::PacketSynchronizer;

use super::BufferError;
use super::DataBuffer;
use super::OrderedBuffer;
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

use std::collections::BTreeMap;

type Buffer = BTreeMap<DataVersion, UntypedPacket>;

#[derive(Default)]
pub struct BtreeBufferedData {
    data: HashMap<ChannelID, Buffer>,
}

impl BtreeBufferedData {
    fn get_channel(&mut self, channel: &ChannelID) -> Result<&mut Buffer, BufferError> {
        Ok(self
            .data
            .get_mut(channel)
            .ok_or(BufferError::InternalError(format!(
                "Cannod find channel {}",
                channel.id
            )))?)
    }

    fn get_or_create_channel(&mut self, channel: &ChannelID) -> &mut Buffer {
        self.data
            .entry(channel.clone())
            .or_insert(Buffer::default())
    }
}

impl DataBuffer for BtreeBufferedData {
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

        let buffer = self.get_or_create_channel(channel);
        let data_version = (channel.clone(), packet.version.clone());
        buffer.insert(packet.version.clone(), packet);
        Ok(data_version)
    }

    fn consume(
        &mut self,
        version: &PacketBufferAddress,
    ) -> Result<Option<UntypedPacket>, BufferError> {
        Ok(self.get_channel(&version.0)?.remove(&version.1))
    }

    fn get(
        &mut self,
        version: &PacketBufferAddress,
    ) -> Result<Option<&UntypedPacket>, BufferError> {
        Ok(self.get_channel(&version.0)?.get(&version.1))
    }

    fn available_channels(&self) -> Vec<ChannelID> {
        self.data
            .keys()
            .into_iter()
            .map(|key| key.clone())
            .collect_vec()
    }
}

impl OrderedBuffer for BtreeBufferedData {
    fn has_version(&self, channel: &ChannelID, version: &DataVersion) -> bool {
        self.data.contains_key(channel) && self.data.get(channel).unwrap().contains_key(version)
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
    _data_version: &DataVersion,
    _buffer: &mut Arc<Mutex<dyn OrderedBuffer>>,
) -> PacketSet {
    PacketSet::default()
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
