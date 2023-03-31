pub mod typed;
pub mod untyped;
pub mod work_queue;
use std::any::{Any, TypeId};
use std::marker::Copy;

use std::time::{SystemTime, UNIX_EPOCH};

use thiserror::Error;

/// Possible inference error
#[derive(Debug, Error, PartialEq, Clone)]
pub enum PacketError {
    #[error("Received data of unexpected type, was expecting {0:?}")]
    UnexpectedDataType(TypeId),
    #[error("Trying to use a channel which does not exist, channel id {0:?}")]
    MissingChannel(String),
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
pub struct Packet<T> {
    pub data: T,
    pub version: DataVersion,
}

pub type Untyped = dyn Any;
pub type UntypedPacket = Packet<Box<Untyped>>;

pub trait UntypedPacketCast: 'static {
    fn deref_owned<T: 'static>(self) -> Result<Packet<Box<T>>, PacketError>;
}

impl<T: 'static> Packet<T> {
    pub fn to_untyped(self) -> UntypedPacket {
        UntypedPacket {
            data: Box::new(self.data) as Box<Untyped>,
            version: self.version,
        }
    }

    pub fn new(data: T, version: DataVersion) -> Self {
        Packet::<T> { data, version }
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
