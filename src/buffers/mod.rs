pub mod single_buffers;
pub mod synchronizers;

use std::collections::HashMap;
use std::time::Duration;

use crate::channels::typed_read_channel::OutputDelivery;
use crate::channels::ChannelError;
use crate::channels::Packet;
use crate::packet::typed::ReadChannel1PacketSet;
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
pub type BufferIterator<'a> = dyn Iterator<Item = &'a DataVersion> + 'a;
