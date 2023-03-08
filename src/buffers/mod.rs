pub mod channel_buffers;
pub mod single_buffers;
pub mod synchronizers;


use crate::packet::ChannelID;
use crate::packet::DataVersion;
use crate::packet::UntypedPacket;
use thiserror::Error;

pub type PacketBufferAddress = (ChannelID, DataVersion);
pub type PacketWithAddress = (PacketBufferAddress, UntypedPacket);

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

pub trait DataBuffer {
    fn insert(
        &mut self,
        channel: &ChannelID,
        packet: UntypedPacket,
    ) -> Result<PacketBufferAddress, BufferError>;

    fn get(&mut self, version: &PacketBufferAddress)
        -> Result<Option<&UntypedPacket>, BufferError>;

    fn available_channels(&self) -> Vec<ChannelID>;

    fn create_channel(&mut self, channel: &ChannelID) -> Result<ChannelID, BufferError>;
}

pub type BufferIterator<'a> = dyn Iterator<Item = &'a UntypedPacket> + 'a;

pub trait OrderedBuffer: DataBuffer {
    fn has_version(&self, channel: &ChannelID, version: &DataVersion) -> bool;

    fn peek(&self, channel: &ChannelID) -> Option<&DataVersion>;

    fn pop(&mut self, channel: &ChannelID) -> Result<Option<UntypedPacket>, BufferError>;

    fn iterator<'a>(&'a self, channel: &ChannelID) -> Option<Box<BufferIterator>>;

    fn are_buffers_empty(&self) -> bool;
}
