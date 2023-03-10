use crate::buffers::OrderedBuffer;

use super::{get_packets_for_version, synchronize, PacketSynchronizer};
use crate::packet::WorkQueue;
use std::sync::{Arc, Mutex};

#[derive(Debug, Default)]
pub struct TimestampSynchronizer {}

impl PacketSynchronizer for TimestampSynchronizer {
    fn synchronize(
        &mut self,
        ordered_buffer: Arc<Mutex<dyn OrderedBuffer>>,
        work_queue: Arc<WorkQueue>,
    ) {
        loop {
            if let Some(data_versions) = synchronize(&mut ordered_buffer.clone()) {
                if let Some(packet_set) =
                    get_packets_for_version(&data_versions, &mut ordered_buffer.clone(), true)
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

#[cfg(test)]
mod tests {
    // use super::*;
    // use crate::buffers::synchronizers::tests::{add_data, create_test_buffer};

    // #[test]
    // fn test_timestamp_synchronize_returns_all_data() {
    //     let buffer = create_test_buffer();
    //     let safe_buffer: Arc<Mutex<dyn OrderedBuffer>> = Arc::new(Mutex::new(buffer));
    //     let mut test_synch = TimestampSynchronizer::default();

    //     add_data(&safe_buffer, "test1".to_string(), 2);
    //     add_data(&safe_buffer, "test2".to_string(), 3);
    //     add_data(&safe_buffer, "test1".to_string(), 3);

    //     // No data because the minum versions do not match
    //     let work_queue = Arc::new(WorkQueue::default());
    //     test_synch.synchronize(&safe_buffer, work_queue.clone());
    //     assert!(work_queue.steal().is_empty());

    //     add_data(&safe_buffer, "test2".to_string(), 2);
    //     add_data(&safe_buffer, "test1".to_string(), 4);

    //     let work_queue = Arc::new(WorkQueue::default());
    //     test_synch.synchronize(&safe_buffer, work_queue.clone());
    //     assert!(work_queue.steal().is_success());
    //     assert!(work_queue.steal().is_success());

    //     assert!(!safe_buffer.lock().unwrap().are_buffers_empty());

    //     add_data(&safe_buffer, "test2".to_string(), 4);
    //     test_synch.synchronize(&safe_buffer, work_queue.clone());
    //     assert!(safe_buffer.lock().unwrap().are_buffers_empty());
    // }
}
