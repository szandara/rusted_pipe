use crossbeam::deque::Injector;

use super::{data_buffers::OrderedBuffer, PacketSet};
use super::{ChannelID, DataVersion, PacketBufferAddress, ReadEvent};
use std::sync::{Arc, Mutex};

pub trait PacketSynchronizer: Send {
    fn start(
        &mut self,
        buffer: Arc<Mutex<dyn OrderedBuffer>>,
        work_queue: Arc<Injector<ReadEvent>>,
        node_id: usize,
        available_channels: &Vec<ChannelID>,
    ) -> ();

    fn packet_event(&self, packet_address: PacketBufferAddress);

    fn stop(&mut self) -> ();
}
