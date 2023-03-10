pub mod channel_buffers;
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
}
pub type BufferIterator<'a, T> = dyn Iterator<Item = &'a Packet<T>> + 'a;

pub trait OrderedBuffer {
    fn get(
        &mut self,
        channel: &str,
        version: &DataVersion,
    ) -> Result<Option<UntypedPacket>, BufferError>;

    fn available_channels(&self) -> Vec<&str>;

    fn has_version(&self, channel: &str, version: &DataVersion) -> bool;

    fn peek(&self, channel: &str) -> Option<&DataVersion>;

    fn pop(&mut self, channel: &str) -> Result<Option<UntypedPacket>, BufferError>;

    //fn iterator<'a, T>(&'a self, channel: &ChannelID) -> Option<Box<BufferIterator<T>>>;

    fn are_buffers_empty(&self) -> bool;
}
