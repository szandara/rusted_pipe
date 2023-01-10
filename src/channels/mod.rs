mod read_channel;
mod write_channel;

use crossbeam::channel::{unbounded, Receiver, Sender, TryRecvError};

use crate::buffers::BufferError;
pub use crate::packet::{
    ChannelID, DataVersion, Packet, PacketError, PacketView, UntypedPacket, UntypedPacketCast,
};

use thiserror::Error;

pub use read_channel::ReadChannel;
pub use write_channel::WriteChannel;

#[derive(Debug, Error, PartialEq, Clone)]
pub enum ChannelError {
    #[error("Trying to use a channel which does not exist, channel id {0:?}")]
    MissingChannel(ChannelID),
    #[error("Trying to use a channel index which does not exist, channel index {0:?}")]
    MissingChannelIndex(usize),
    #[error("Channel has no data {0:?}")]
    MissingChannelData(usize),
    #[error(transparent)]
    ReceiveError(#[from] TryRecvError),
    #[error("Error while sending data {0}")]
    SendError(String),
    #[error(transparent)]
    PacketError(#[from] PacketError),
    #[error("No more data to send. Closing channel.")]
    EndOfStreamError(ChannelID),
    #[error("Error in the buffer operation.")]
    ErrorInBuffer(#[from] BufferError),
    #[error("Channel was not initialized.")]
    NotInitializedError,
}

pub fn untyped_channel() -> (UntypedSenderChannel, UntypedReceiverChannel) {
    let (channel_sender, channel_receiver) = unbounded::<UntypedPacket>();
    return (
        UntypedSenderChannel::new(&channel_sender.clone()),
        UntypedReceiverChannel::new(&channel_receiver.clone()),
    );
}

#[derive(Debug)]
pub struct UntypedReceiverChannel {
    receiver: Receiver<UntypedPacket>,
}

impl UntypedReceiverChannel {
    pub fn new(receiver: &Receiver<UntypedPacket>) -> Self {
        UntypedReceiverChannel {
            receiver: receiver.clone() as Receiver<UntypedPacket>,
        }
    }
    pub fn try_receive(&self) -> Result<UntypedPacket, ChannelError> {
        match self.receiver.try_recv() {
            Ok(packet) => Ok(packet),
            Err(error) => Err(ChannelError::ReceiveError(error)),
        }
    }
}

#[derive(Debug)]
pub struct UntypedSenderChannel {
    sender: Sender<UntypedPacket>,
}

impl UntypedSenderChannel {
    pub fn new(sender: &Sender<UntypedPacket>) -> Self {
        UntypedSenderChannel {
            sender: sender.clone() as Sender<UntypedPacket>,
        }
    }
    pub fn send<T: 'static>(&self, data: Packet<T>) -> Result<(), ChannelError> {
        match self.sender.send(data.to_untyped()) {
            Ok(res) => Ok(res),
            Err(_err) => Err(ChannelError::SendError(
                "Could not send because the channel is disconnected".to_string(),
            )),
        }
    }
}
