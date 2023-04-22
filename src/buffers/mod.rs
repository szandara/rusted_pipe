//! Module that holds the implementation of a single input buffer for a Node.
//! An input buffer contains an allocated memory that stores data in different ways depending on
//! implementation. Buffers offer two main functionalities:
//! - They are ordered and can be peeked.
//! - They can drop data or block if full depending on configuration.
pub mod single_buffers;
pub mod synchronizers;

use crate::channels::Packet;
use crate::packet::ChannelID;
use crate::packet::DataVersion;
use crate::packet::UntypedPacket;
use thiserror::Error;

pub type PacketBufferAddress = (ChannelID, DataVersion);
pub type PacketWithAddress = (PacketBufferAddress, UntypedPacket);
pub type TypedPacketWithAddress<T> = (PacketBufferAddress, Packet<T>);

#[derive(Debug, Error, PartialEq, Clone)]
pub enum BufferError {
    #[error("Data was received in channel {0:?} with an already existing version.")]
    DuplicateDataVersionError(PacketBufferAddress),
    #[error("Trying to create a channel which already exists {0:?}.")]
    DuplicateChannelError(ChannelID),
    #[error("Problem while processing data: {0:?}.")]
    InternalError(String),
    #[error("Buffer is full")]
    BufferFull,
    #[error(
        "Trying to insert data returned out of order. Min version {0:?}, trying to insert {1:?}"
    )]
    OutOfOrder(u128, u128),
}

/// An iterator over the buffer data.
pub type BufferIterator<'a> = dyn Iterator<Item = &'a DataVersion> + 'a;
