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

use itertools::Itertools;

use std::sync::{Arc, Mutex};
use std::thread;

use std::collections::HashMap;
use std::thread::JoinHandle;
use std::time::Duration;

use super::PacketBufferAddress;

use crate::packet::WorkQueue;

use std::collections::BTreeMap;

pub struct FixedSizeBTree {
    data: BTreeMap<DataVersion, UntypedPacket>,
    max_size: usize,
}

impl FixedSizeBTree {
    fn default() -> Self {
        FixedSizeBTree {
            data: Default::default(),
            max_size: 1000,
        }
    }

    fn new(max_size: usize) -> Self {
        FixedSizeBTree {
            data: Default::default(),
            max_size: max_size,
        }
    }

    fn contains_key(&self, version: &DataVersion) -> bool {
        self.data.contains_key(version)
    }

    fn get(&self, version: &DataVersion) -> Option<&UntypedPacket> {
        self.data.get(version)
    }

    fn remove(&mut self, version: &DataVersion) -> Option<UntypedPacket> {
        self.data.remove(version)
    }

    fn insert(&mut self, version: DataVersion, packet: UntypedPacket) {
        self.data.insert(version, packet);
        while self.data.len() > self.max_size {
            let last_entry = self.data.last_entry().unwrap().key().clone();
            self.data.remove(&last_entry);
        }
    }
}

#[derive(Default)]
pub struct BtreeBufferedData {
    data: HashMap<ChannelID, FixedSizeBTree>,
}

impl BtreeBufferedData {
    fn get_channel(&mut self, channel: &ChannelID) -> Result<&mut FixedSizeBTree, BufferError> {
        Ok(self
            .data
            .get_mut(channel)
            .ok_or(BufferError::InternalError(format!(
                "Cannod find channel {}",
                channel.id
            )))?)
    }

    fn get_or_create_channel(&mut self, channel: &ChannelID) -> &mut FixedSizeBTree {
        self.data
            .entry(channel.clone())
            .or_insert(FixedSizeBTree::default())
    }
}

fn cleanup_before(version: &DataVersion, buffer: &mut FixedSizeBTree) {
    buffer.data.split_off(&version);
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
        let data = self.get_channel(&version.0)?.remove(&version.1);
        cleanup_before(&version.1, self.get_channel(&version.0)?);
        Ok(data)
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
