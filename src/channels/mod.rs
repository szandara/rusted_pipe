//! Module for the channels logic. A channel is an input or output of of a node.
//! There are two main types: read channel and write channels.
//!
//! Read channels are attached to nodes that expect incoming data and each ReadChannel
//! can have more than one channel.
//!
//! Write channels instead allow Nodes to push data out into the graph. A WriteChannel can have one
//! or more output channels.
//!
//! Each type has a typed and a non typed version.
//! - Typed versions' data is known at compilation time and will catch graph linking at compile time.
//! - Untyped versions instead have named channels with dynamically typed data. There is an overhead
//! in using this channel due to type casting and are also less secure at compile time.
pub mod read_channel;
pub mod typed_read_channel;
pub mod typed_write_channel;

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

/// Creates an untyped channel set (sender and receiver). An channel
/// is shared between a ReadChannel and a WriteChannel. The channel has an unbounded
/// buffer size that grows indefinitely. It can crash the application if not addressed.
/// These buffers data is generally consumed as fast as possible by the graph.
pub fn untyped_channel() -> (UntypedSenderChannel, UntypedReceiverChannel) {
    let (channel_sender, channel_receiver) = unbounded::<UntypedPacket>();
    (
        SenderChannel::new(&channel_sender),
        ReceiverChannel::new(&channel_receiver),
    )
}

/// Creates an typed channel set (sender and receiver). An channel
/// is shared between a ReadChannel and a WriteChannel. The channel has an unbounded
/// buffer size that grows indefinitely. It can crash the application if not addressed.
/// These buffers data is generally consumed as fast as possible by the graph.
pub fn typed_channel<T>() -> (SenderChannel<T>, ReceiverChannel<T>) {
    let (channel_sender, channel_receiver) = unbounded::<Packet<T>>();
    (
        SenderChannel::new(&channel_sender),
        ReceiverChannel::new(&channel_receiver),
    )
}

pub type UntypedReceiverChannel = ReceiverChannel<Box<Untyped>>;
pub type UntypedSenderChannel = SenderChannel<Box<Untyped>>;

/// A receiver channel data struct.
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

/// A sender channel data struct.
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

/// A generic trait for WriteChannels
pub trait WriteChannelTrait {
    /// Creates a new WriteChannel.
    fn create() -> Self;
}

/// A generic trait for WriteChannels
pub trait ReadChannelTrait {
    type Data;

    /// Read incoming data in one of the channels of the ReadChannel.
    ///
    /// * Arguments
    /// `channel_id` -  The string id of the channel.
    /// `done_notification` - A channel for sending a notification if the buffer has processed
    /// all data.
    ///
    /// * Returns
    /// A ChannelID if something was read, None otherwise.
    fn read(&mut self, channel_id: String, done_notification: Sender<String>) -> Option<ChannelID>;

    /// Starts the channel buffer.
    ///
    /// * Arguments
    /// `work_queue` -  A queue that holds already paired packet sets.
    fn start(&mut self, work_queue: WorkQueue<Self::Data>);

    /// Stops the channel buffer.
    fn stop(&mut self);
}
