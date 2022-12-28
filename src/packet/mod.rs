use std::any::{Any, TypeId};
use std::marker::Copy;

use thiserror::Error;

/// Possible inference error
#[derive(Debug, Error, PartialEq, Clone)]
pub enum PacketError {
    #[error("Received data of unexpected type, was expecting {0:?}")]
    UnexpectedDataType(TypeId),
}

#[derive(Debug, Copy, Clone, Hash, Eq)]
pub struct DataVersion {
    pub timestamp: u64,
}

impl PartialEq for DataVersion {
    fn eq(&self, other: &Self) -> bool {
        self.timestamp == other.timestamp
    }
}

#[derive(Debug)]
pub struct PacketBase<T> {
    pub data: T,
    pub version: DataVersion,
}

pub type Packet<T> = PacketBase<Box<T>>;
pub type PacketView<'a, T> = PacketBase<&'a T>;
pub type UntypedPacket = Packet<dyn Any>;

unsafe impl Sync for Packet<dyn Any + 'static> {}
unsafe impl Send for Packet<dyn Any + 'static> {}

pub trait UntypedPacketCast: 'static {
    fn deref_owned<T: 'static>(self) -> Result<Packet<T>, PacketError>;
    fn deref<T: 'static>(&self) -> Result<PacketView<T>, PacketError>;
}

impl<T: 'static> Packet<T> {
    pub fn to_untyped(self) -> UntypedPacket {
        UntypedPacket {
            data: self.data as Box<dyn Any>,
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
