pub mod hashmap_data_buffers;
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
}

pub trait DataBuffer: Sync + Send {
    fn insert(
        &mut self,
        channel: &ChannelID,
        packet: UntypedPacket,
    ) -> Result<PacketBufferAddress, BufferError>;

    fn consume(&mut self, version: &PacketBufferAddress) -> Option<UntypedPacket>;

    fn get(&mut self, version: &PacketBufferAddress) -> Option<&UntypedPacket>;

    fn available_channels(&self) -> Vec<ChannelID>;
}

pub trait OrderedBuffer: DataBuffer {
    fn has_version(&self, channel: &ChannelID, version: &DataVersion) -> bool;
}
