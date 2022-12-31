#![feature(get_mut_unchecked)]
#![feature(is_some_and)]

pub mod buffers;
pub mod channels;
pub mod graph;
pub mod packet;

pub use packet::DataVersion;
pub use packet::PacketError;

use channels::ChannelError;
use thiserror::Error;

/// Possible inference error
#[derive(Debug, Error, Clone)]
pub enum RustedPipeError {
    #[error(transparent)]
    PacketError(#[from] PacketError),
    #[error(transparent)]
    ChannelError(#[from] ChannelError),
    #[error("Cannot find node {0:?}, have you added the node to the graph?")]
    MissingNodeError(String),
    #[error("Error while executing processor: {0:?}")]
    ProcessorError(String),
}
