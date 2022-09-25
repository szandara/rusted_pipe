#![feature(get_mut_unchecked)]

pub mod channels;
pub mod packet;
pub mod processors;

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
