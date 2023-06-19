// #![feature(get_mut_unchecked)]

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
    #[error("No more packets to send")]
    EndOfStream(),
}

#[macro_export]
macro_rules! unwrap_or_return {
    ( $e:expr ) => {
        match $e {
            Some(x) => x,
            _ => return None,
        }
    };
}
