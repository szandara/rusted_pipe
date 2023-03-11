use std::any::{Any, TypeId};
use std::marker::Copy;

use std::os::raw::c_void;

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

pub type Packet<T> = PacketBase<Box<T>>;
pub type PacketView<'a, T> = PacketBase<&'a T>;
pub type UntypedPacket = Packet<*const c_void>;

unsafe impl Sync for Packet<dyn Any> {}
unsafe impl Send for Packet<dyn Any> {}

pub trait UntypedPacketCast {
    fn deref_owned<T>(self) -> Result<Packet<T>, PacketError>;
    fn deref<T>(&self) -> Result<PacketView<T>, PacketError>;
}

impl<T: Clone> Packet<T> {
    pub fn to_untyped(self) -> UntypedPacket {
        UntypedPacket {
            data: Box::new(Box::into_raw(self.data) as *const c_void),
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
    fn deref_owned<T>(self) -> Result<Packet<T>, PacketError> {
        Ok(Packet::<T> {
            data: unsafe { Box::from_raw(*self.data as *mut T) as Box<T> },
            version: self.version,
        })

        // match self.data.downcast_ref::<T>() {
        //     Ok(casted_type) => Ok(Packet::<T> {
        //         data: casted_type,
        //         version: self.version,
        //     }),
        //     Err(untyped_box) => {
        //         self.data = untyped_box;
        //         Err(PacketError::UnexpectedDataType(TypeId::of::<T>()))
        //     }
        // }
    }

    fn deref<T>(&self) -> Result<PacketView<T>, PacketError> {
        // let data_ref = self
        //     .data
        //     .as_ref()
        //     .downcast_ref::<T>()
        //     .ok_or(PacketError::UnexpectedDataType(TypeId::of::<T>()))?;
        // Ok(PacketView::<T> {
        //     data: data_ref,
        //     version: self.version,
        // })
        Ok(PacketView::<T> {
            data: unsafe { &*(*self.data as *const T) },
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

pub struct ReadEvent {
    pub packet_data: PacketSet,
}

pub struct WorkQueue {
    queue: Injector<ReadEvent>,
    max_in_queue: usize,
}

impl WorkQueue {
    pub fn default() -> Self {
        WorkQueue {
            queue: Injector::<ReadEvent>::default(),
            max_in_queue: std::usize::MAX,
        }
    }

    pub fn new(max_in_queue: usize) -> Self {
        WorkQueue {
            queue: Injector::<ReadEvent>::default(),
            max_in_queue,
        }
    }

    pub fn push(&self, packet_set: PacketSet) {
        self.queue.push(ReadEvent {
            packet_data: packet_set,
        });
        while self.queue.len() > self.max_in_queue {
            self.queue.steal().is_success();
        }
    }

    pub fn steal(&self) -> Steal<ReadEvent> {
        self.queue.steal()
    }
}

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
