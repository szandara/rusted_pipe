use crate::{buffers::OrderedBuffer, DataVersion};

use super::{get_packets_for_version, synchronize, PacketSynchronizer};
use crate::packet::WorkQueue;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

#[derive(Debug, Default)]
pub struct TimestampSynchronizer {}

impl PacketSynchronizer for TimestampSynchronizer {
    fn synchronize(
        &mut self,
        ordered_buffer: Arc<Mutex<dyn OrderedBuffer>>,
    ) -> Option<HashMap<String, Option<DataVersion>>> {
        synchronize(&mut ordered_buffer.clone())
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
