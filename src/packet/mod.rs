pub mod typed;
pub mod work_queue;
use std::any::{Any, TypeId};
use std::marker::Copy;

use std::time::{SystemTime, UNIX_EPOCH};

use crossbeam::deque::{Injector, Steal};
use indexmap::IndexMap;
use itertools::Itertools;
use thiserror::Error;

use crate::buffers::PacketWithAddress;

/// Possible inference error
#[derive(Debug, Error, PartialEq, Clone)]
pub enum PacketError {
    #[error("Received data of unexpected type, was expecting {0:?}")]
    UnexpectedDataType(TypeId),
    #[error("Trying to use a channel which does not exist, channel id {0:?}")]
    MissingChannel(ChannelID),
    #[error("Trying to use a channel index which does not exist, channel index {0:?}")]
    MissingChannelIndex(usize),
    #[error("Channel has no data {0:?}")]
    MissingChannelData(usize),
}

#[derive(Debug, Copy, Clone, Hash, Eq, Ord, PartialOrd)]
pub struct DataVersion {
    pub timestamp: u128,
}

impl DataVersion {
    pub fn from_now() -> Self {
        DataVersion {
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis(),
        }
    }
}

impl PartialEq for DataVersion {
    fn eq(&self, other: &Self) -> bool {
        self.timestamp == other.timestamp
    }
}
#[derive(Debug, Copy, Clone)]
pub struct PacketBase<T> {
    pub data: T,
    pub version: DataVersion,
}

pub type Untyped = dyn Any;
pub type Packet<T> = PacketBase<Box<T>>;
pub type PacketView<'a, T> = PacketBase<&'a T>;
pub type UntypedPacket = Packet<Untyped>;

unsafe impl Sync for Packet<dyn Any + 'static> {}
unsafe impl Send for Packet<dyn Any + 'static> {}

pub trait UntypedPacketCast: 'static {
    fn deref_owned<T: 'static>(self) -> Result<Packet<T>, PacketError>;
    fn deref<T: 'static>(&self) -> Result<PacketView<T>, PacketError>;
}

impl<T: 'static> Packet<T> {
    pub fn to_untyped(self) -> UntypedPacket {
        UntypedPacket {
            data: self.data as Box<Untyped>,
            version: self.version,
        }
    }

    pub fn new(data: T, version: DataVersion) -> Self {
        Packet::<T> {
            data: Box::new(data),
            version,
        }
    }

    pub fn view(&self) -> PacketView<T> {
        PacketView::<T> {
            data: self.data.as_ref(),
            version: self.version,
        }
    }
}

impl UntypedPacketCast for UntypedPacket {
    fn deref_owned<T: 'static>(mut self) -> Result<Packet<T>, PacketError> {
        match self.data.downcast::<T>() {
            Ok(casted_type) => Ok(Packet::<T> {
                data: casted_type,
                version: self.version,
            }),
            Err(untyped_box) => {
                self.data = untyped_box;
                Err(PacketError::UnexpectedDataType(TypeId::of::<T>()))
            }
        }
    }

    fn deref<T: 'static>(&self) -> Result<PacketView<T>, PacketError> {
        let data_ref = self
            .data
            .as_ref()
            .downcast_ref::<T>()
            .ok_or(PacketError::UnexpectedDataType(TypeId::of::<T>()))?;
        Ok(PacketView::<T> {
            data: data_ref,
            version: self.version,
        })
    }
}

#[derive(Default, Debug)]
pub struct PacketSet {
    data: IndexMap<ChannelID, Option<PacketWithAddress>>,
}

impl PacketSet {
    pub fn new(data: IndexMap<ChannelID, Option<PacketWithAddress>>) -> Self {
        PacketSet { data }
    }

    pub fn channels(&self) -> usize {
        self.data.len()
    }

    pub fn values(&self) -> Vec<&Option<PacketWithAddress>> {
        self.data.values().collect_vec()
    }

    pub fn has_none(&self) -> bool {
        for v in self.data.values() {
            if v.is_none() {
                return true;
            }
        }
        false
    }

    pub fn get<T: 'static>(&self, channel_number: usize) -> Result<PacketView<T>, PacketError> {
        match self
            .data
            .get_index(channel_number)
            .ok_or(PacketError::MissingChannelIndex(channel_number))?
            .1
        {
            Some(maybe_packet_with_address) => Ok(maybe_packet_with_address.1.deref::<T>()?),
            None => Err(PacketError::MissingChannelData(channel_number)),
        }
    }

    pub fn get_owned<T: 'static>(
        &mut self,
        channel_number: usize,
    ) -> Result<Packet<T>, PacketError> {
        match self
            .data
            .swap_remove_index(channel_number)
            .ok_or(PacketError::MissingChannelIndex(channel_number))?
            .1
        {
            Some(maybe_packet_with_address) => {
                Ok(maybe_packet_with_address.1.deref_owned::<T>()?)
            }
            None => Err(PacketError::MissingChannelData(channel_number)),
        }
    }

    pub fn get_channel<T: 'static>(
        &self,
        channel_id: &ChannelID,
    ) -> Result<PacketView<T>, PacketError> {
        match self
            .data
            .get(channel_id)
            .ok_or(PacketError::MissingChannel(channel_id.clone()))?
        {
            Some(maybe_packet_with_address) => Ok(maybe_packet_with_address.1.deref::<T>()?),
            None => Err(PacketError::MissingChannel(channel_id.clone())),
        }
    }

    pub fn get_channel_owned<T: 'static>(
        &mut self,
        channel_id: &ChannelID,
    ) -> Result<Packet<T>, PacketError> {
        match self
            .data
            .remove(channel_id)
            .ok_or(PacketError::MissingChannel(channel_id.clone()))?
        {
            Some(maybe_packet_with_address) => {
                Ok(maybe_packet_with_address.1.deref_owned::<T>()?)
            }
            None => Err(PacketError::MissingChannel(channel_id.clone())),
        }
    }
}

unsafe impl Send for PacketSet {}

#[derive(Eq, Hash, Debug, Clone)]
pub struct ChannelID {
    pub id: String,
}

impl ChannelID {
    pub fn new(id: String) -> Self {
        ChannelID { id }
    }
}

impl From<&str> for ChannelID {
    fn from(id: &str) -> Self {
        ChannelID { id: id.to_string() }
    }
}

impl From<String> for ChannelID {
    fn from(id: String) -> Self {
        ChannelID { id }
    }
}

impl PartialEq for ChannelID {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}
