pub mod first_sync;
pub mod timestamp;

use crate::buffers::OrderedBuffer;
use crate::buffers::PacketBufferAddress;
use crate::packet::ChannelID;

use crate::packet::WorkQueue;
use std::sync::{Arc, Mutex};

pub trait PacketSynchronizer: Send {
    fn synchronize(
        &self,
        ordered_buffer: &Arc<Mutex<dyn OrderedBuffer>>,
        available_channels: &Vec<ChannelID>,
        work_queue: Arc<WorkQueue>,
    );
}
