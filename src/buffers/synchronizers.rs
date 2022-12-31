

use super::OrderedBuffer;
use super::PacketBufferAddress;
use crate::packet::ChannelID;


use crate::packet::WorkQueue;
use std::sync::{Arc, Mutex};

pub trait PacketSynchronizer: Send {
    fn start(
        &mut self,
        buffer: Arc<Mutex<dyn OrderedBuffer>>,
        work_queue: Arc<WorkQueue>,
        node_id: usize,
        available_channels: &Vec<ChannelID>,
    ) -> ();

    fn packet_event(&self, packet_address: PacketBufferAddress);

    fn stop(&mut self) -> ();
}
