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
    MissingChannel(ChannelID),
    #[error("Trying to use a channel index which does not exist, channel index {0:?}")]
    MissingChannelIndex(usize),
    #[error("Channel has no data {0:?}")]
    MissingChannelData(usize),
}

#[derive(Debug, Copy, Clone, Eq, Ord, PartialOrd)]
pub struct DataVersion {
    pub timestamp_ns: u128,
}

impl DataVersion {
    pub fn from_now() -> Self {
        DataVersion {
            timestamp_ns: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("Cannot calculate epoch")
                .as_nanos(),
        }
    }
}

impl PartialEq for DataVersion {
    fn eq(&self, other: &Self) -> bool {
        self.timestamp_ns == other.timestamp_ns
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

#[derive(Eq, Hash, Debug, Clone, PartialEq, PartialOrd, Ord)]
pub struct ChannelID {
    pub id: String,
}

impl std::fmt::Display for ChannelID {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.id)
    }
}

impl From<&&ChannelID> for ChannelID {
    fn from(f: &&Self) -> Self {
        Self { id: f.id.clone() }
    }
}

impl std::cmp::PartialEq<str> for ChannelID {
    fn eq(&self, other: &str) -> bool {
        self.id == other
    }
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
