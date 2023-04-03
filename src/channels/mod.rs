pub mod read_channel;
pub mod typed_read_channel;
pub mod typed_write_channel;
pub mod untyped_read_channel;
pub mod untyped_write_channel;

use crossbeam::channel::{unbounded, Receiver, RecvError, RecvTimeoutError, Sender, TryRecvError};

pub use crate::packet::{
    ChannelID, DataVersion, Packet, PacketError, UntypedPacket, UntypedPacketCast,
};
use crate::{
    buffers::BufferError,
    packet::{work_queue::WorkQueue, Untyped},
};

use thiserror::Error;

#[derive(Debug, Error, PartialEq, Clone)]
pub enum ChannelError {
    #[error("Trying to use a channel which does not exist, channel id {0:?}")]
    MissingChannel(ChannelID),
    #[error("Trying to use a channel index which does not exist, channel index {0:?}")]
    MissingChannelIndex(usize),
    #[error("Channel has no data {0:?}")]
    MissingChannelData(usize),
    #[error(transparent)]
    TryReceiveError(#[from] TryRecvError),
    #[error(transparent)]
    ReceiveError(#[from] RecvError),
    #[error(transparent)]
    RecvTimeoutError(#[from] RecvTimeoutError),
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
    (
        SenderChannel::new(&channel_sender),
        ReceiverChannel::new(&channel_receiver),
    )
}

pub fn typed_channel<T>() -> (SenderChannel<T>, ReceiverChannel<T>) {
    let (channel_sender, channel_receiver) = unbounded::<Packet<T>>();
    (
        SenderChannel::new(&channel_sender),
        ReceiverChannel::new(&channel_receiver),
    )
}

pub type UntypedReceiverChannel = ReceiverChannel<Box<Untyped>>;
pub type UntypedSenderChannel = SenderChannel<Box<Untyped>>;

#[derive(Debug)]
pub struct ReceiverChannel<T> {
    pub receiver: Receiver<Packet<T>>,
}

impl<T> ReceiverChannel<T> {
    pub fn new(receiver: &Receiver<Packet<T>>) -> Self {
        Self {
            receiver: receiver.clone(),
        }
    }
    pub fn try_receive(&self) -> Result<Packet<T>, ChannelError> {
        match self.receiver.try_recv() {
            Ok(packet) => Ok(packet),
            Err(error) => Err(ChannelError::TryReceiveError(error)),
        }
    }
}

#[derive(Debug)]
pub struct SenderChannel<T> {
    sender: Sender<Packet<T>>,
}

impl<T> SenderChannel<T> {
    pub fn new(sender: &Sender<Packet<T>>) -> Self {
        Self {
            sender: sender.clone(),
        }
    }
    pub fn send(&self, data: Packet<T>) -> Result<(), ChannelError> {
        match self.sender.send(data) {
            Ok(res) => Ok(res),
            Err(_err) => Err(ChannelError::SendError(
                "Could not send because the channel is disconnected".to_string(),
            )),
        }
    }
}

pub trait WriteChannelTrait {
    fn create() -> Self;
}

pub trait ReadChannelTrait {
    type Data;

    fn read(&mut self, channel_id: String, done_notification: Sender<String>) -> bool;

    fn start(&mut self, work_queue: WorkQueue<Self::Data>);

    fn stop(&mut self);
}
