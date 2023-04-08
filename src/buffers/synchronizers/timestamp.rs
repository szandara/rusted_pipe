use crate::{channels::read_channel::ChannelBuffer, DataVersion};

use super::{synchronize, PacketSynchronizer};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

#[derive(Debug, Default)]
pub struct TimestampSynchronizer {}

impl PacketSynchronizer for TimestampSynchronizer {
    fn synchronize(
        &mut self,
        ordered_buffer: Arc<Mutex<dyn ChannelBuffer>>,
    ) -> Option<HashMap<String, Option<DataVersion>>> {
        synchronize(ordered_buffer.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        buffers::synchronizers::tests::{
            add_data, check_packet_set_contains_versions, create_test_buffer,
        },
        channels::read_channel::InputGenerator,
    };

    #[test]
    fn test_timestamp_synchronize_returns_all_data() {
        let buffer = create_test_buffer();
        let safe_buffer = Arc::new(Mutex::new(buffer));
        let mut test_synch = TimestampSynchronizer::default();

        add_data(safe_buffer.clone(), "c1".to_string(), 2);
        add_data(safe_buffer.clone(), "c1".to_string(), 3);
        add_data(safe_buffer.clone(), "c3".to_string(), 2);
        add_data(safe_buffer.clone(), "c3".to_string(), 3);

        // No data because the minum versions do not match
        let synch = test_synch.synchronize(safe_buffer.clone());
        assert!(synch.is_none());

        add_data(safe_buffer.clone(), "c2".to_string(), 2);
        add_data(safe_buffer.clone(), "c2".to_string(), 3);
        add_data(safe_buffer.clone(), "c1".to_string(), 4);
        add_data(safe_buffer.clone(), "c3".to_string(), 4);

        let synch = test_synch.synchronize(safe_buffer.clone());
        check_packet_set_contains_versions(&synch.as_ref().unwrap(), vec![Some(2); 3]);

        safe_buffer
            .lock()
            .unwrap()
            .get_packets_for_version(&synch.unwrap(), true);

        let synch = test_synch.synchronize(safe_buffer.clone());
        check_packet_set_contains_versions(&synch.as_ref().unwrap(), vec![Some(3); 3]);

        safe_buffer
            .lock()
            .unwrap()
            .get_packets_for_version(&synch.unwrap(), true);

        add_data(safe_buffer.clone(), "c2".to_string(), 4);
        let synch = test_synch.synchronize(safe_buffer.clone());
        check_packet_set_contains_versions(&synch.as_ref().unwrap(), vec![Some(4); 3]);

        safe_buffer
            .lock()
            .unwrap()
            .get_packets_for_version(&synch.unwrap(), true);

        assert!(safe_buffer.lock().unwrap().are_buffers_empty());
    }
}
