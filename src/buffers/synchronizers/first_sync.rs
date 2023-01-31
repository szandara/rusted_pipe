use super::get_min_versions;
use super::get_packets_for_version;
use super::synchronize;
use super::OrderedBuffer;
use super::PacketBufferAddress;
use super::PacketSynchronizer;
use crate::packet::ChannelID;
use crate::DataVersion;

use crate::packet::PacketSet;
use crate::packet::WorkQueue;
use std::sync::{Arc, Mutex};

#[derive(Debug, Default)]
pub struct FirstSyncSynchronizer {
    first_sync: Option<DataVersion>,
}

impl PacketSynchronizer for FirstSyncSynchronizer {
    fn synchronize(
        &mut self,
        ordered_buffer: &Arc<Mutex<dyn OrderedBuffer>>,
        work_queue: Arc<WorkQueue>,
    ) {
        let available_channels = ordered_buffer.lock().unwrap().available_channels();
        if self.first_sync.is_none() {
            if let Some(data_versions) = synchronize(&mut ordered_buffer.clone()) {
                self.first_sync = data_versions[&available_channels[0]];
                if let Some(packet_set) =
                    get_packets_for_version(&data_versions, &mut ordered_buffer.clone())
                {
                    work_queue.push(packet_set)
                }
            }
        } else {
            let versions = get_min_versions(&mut ordered_buffer.clone());
        }
    }
}
