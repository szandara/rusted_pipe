use crate::buffers::OrderedBuffer;

use super::{get_packets_for_version, synchronize, PacketSynchronizer};
use crate::packet::WorkQueue;
use std::sync::{Arc, Mutex};

#[derive(Debug, Default)]
pub struct TimestampSynchronizer {}

impl PacketSynchronizer for TimestampSynchronizer {
    fn synchronize(
        &mut self,
        ordered_buffer: &Arc<Mutex<dyn OrderedBuffer>>,
        work_queue: Arc<WorkQueue>,
    ) {
        loop {
            if let Some(data_versions) = synchronize(&mut ordered_buffer.clone()) {
                if let Some(packet_set) =
                    get_packets_for_version(&data_versions, &mut ordered_buffer.clone())
                {
                    if packet_set.has_none() {
                        break;
                    }
                    work_queue.push(packet_set)
                } else {
                    break;
                }
            } else {
                break;
            }
        }
    }
}
