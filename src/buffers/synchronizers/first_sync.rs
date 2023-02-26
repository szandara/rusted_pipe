use super::get_min_versions;
use super::get_packets_for_version;
use super::synchronize;
use super::OrderedBuffer;
use super::PacketSynchronizer;
use crate::DataVersion;

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
            if let Some(packet_set) =
                get_packets_for_version(&versions, &mut ordered_buffer.clone())
            {
                work_queue.push(packet_set)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        buffers::synchronizers::tests::{add_data, create_test_buffer},
        channels::ChannelID,
    };

    #[test]
    fn test_first_synch_synchronize_returns_all_data() {
        let buffer = create_test_buffer();
        let safe_buffer: Arc<Mutex<dyn OrderedBuffer>> = Arc::new(Mutex::new(buffer));
        let mut test_synch = FirstSyncSynchronizer::default();

        let channel1 = ChannelID::from("test1");
        let channel2 = ChannelID::from("test2");
        add_data(&safe_buffer, "test1".to_string(), 1);
        add_data(&safe_buffer, "test1".to_string(), 2);
        add_data(&safe_buffer, "test1".to_string(), 3);
        add_data(&safe_buffer, "test1".to_string(), 4);
        add_data(&safe_buffer, "test1".to_string(), 5);

        // No data because the minimum versions do not match
        let work_queue = Arc::new(WorkQueue::default());
        test_synch.synchronize(&safe_buffer, work_queue.clone());
        assert!(work_queue.steal().is_empty());

        add_data(&safe_buffer, "test2".to_string(), 1);

        let work_queue = Arc::new(WorkQueue::default());
        test_synch.synchronize(&safe_buffer, work_queue.clone());
        assert!(work_queue.steal().is_success());

        for i in 2..6 {
            test_synch.synchronize(&safe_buffer, work_queue.clone());
            let packet_data = work_queue.steal().success().unwrap().packet_data;
            let timestamp1 = packet_data
                .get_channel::<String>(&channel1.clone())
                .unwrap()
                .version
                .timestamp;
            assert!(packet_data
                .get_channel::<String>(&channel2.clone())
                .is_err());

            assert_eq!(timestamp1, i);
        }

        assert!(safe_buffer.lock().unwrap().are_buffers_empty());
    }
}
