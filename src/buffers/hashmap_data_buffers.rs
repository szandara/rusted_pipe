use super::synchronizers::PacketSynchronizer;
use super::DataBuffer;
use super::OrderedBuffer;

use super::BufferError;
use crate::packet::ChannelID;
use crate::packet::DataVersion;
use crate::packet::{PacketSet, UntypedPacket};
use crossbeam::channel::Receiver;
use crossbeam::channel::RecvTimeoutError;
use crossbeam::channel::{unbounded, Sender};

use indexmap::IndexMap;
use itertools::Itertools;

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

    fn consume(
        &mut self,
        version: &PacketBufferAddress,
    ) -> Result<Option<UntypedPacket>, BufferError> {
        Ok(self.data.remove(&version))
    }

    fn get(
        &mut self,
        version: &PacketBufferAddress,
    ) -> Result<Option<&UntypedPacket>, BufferError> {
        Ok(self.data.get(&version))
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
