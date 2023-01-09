pub mod btree_data_buffers;
pub mod circular_data_buffers;
pub mod hashmap_data_buffers;
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
}

pub trait DataBuffer: Sync + Send {
    fn insert(
        &mut self,
        channel: &ChannelID,
        packet: UntypedPacket,
    ) -> Result<PacketBufferAddress, BufferError>;

    fn consume(
        &mut self,
        version: &PacketBufferAddress,
    ) -> Result<Option<UntypedPacket>, BufferError>;

    fn get(&mut self, version: &PacketBufferAddress)
        -> Result<Option<&UntypedPacket>, BufferError>;

    fn available_channels(&self) -> Vec<ChannelID>;

    fn create_channel(&mut self, channel: &ChannelID) -> Result<ChannelID, BufferError>;
}

pub trait OrderedBuffer: DataBuffer {
    fn has_version(&self, channel: &ChannelID, version: &DataVersion) -> bool;
}
