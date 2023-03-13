use super::PacketSynchronizer;

use crate::buffers::BufferIterator;
use crate::channels::read_channel::ChannelBuffer;
use crate::DataVersion;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

#[derive(Debug, Default)]
pub struct RealTimeSynchronizer {}

fn find_common_min<'a>(mut iterators: Vec<Box<BufferIterator>>) -> Option<DataVersion> {
    let mut min = None;
    let mut matches = 0;
    loop {
        let mut all_end = true;
        for iterator in iterators.iter_mut() {
            if min.is_none() {
                let mut peekable = iterator.peekable();
                let peek_next = peekable.peek();
                if min.is_none() && peek_next.is_some() {
                    min = peekable.next();
                    matches += 1;
                }
                continue;
            }

            while let Some(next) = iterator.next() {
                if next.timestamp > min.unwrap().timestamp {
                    min = Some(next);
                    matches = 1;
                    all_end = false;
                    break;
                } else if next.timestamp == min.unwrap().timestamp {
                    matches += 1;
                    break;
                }
                all_end = false;
            }
        }

        if all_end {
            break;
        }
    }

    if let Some(min) = min {
        if matches == iterators.len() {
            return Some(min.clone());
        }
    }
    None
}

impl PacketSynchronizer for RealTimeSynchronizer {
    fn synchronize(
        &mut self,
        ordered_buffer: Arc<Mutex<dyn ChannelBuffer>>,
    ) -> Option<HashMap<String, Option<DataVersion>>> {
        let mut versions: Option<HashMap<String, Option<DataVersion>>> = None;

        {
            let locked = ordered_buffer.lock().unwrap();
            let mut iters = vec![];
            for channel in locked.available_channels().clone().into_iter() {
                iters.push(locked.iterator(&channel).unwrap());
            }

            if let Some(common_min) = find_common_min(iters) {
                versions = Some(
                    locked
                        .available_channels()
                        .iter()
                        .map(|f| (f.to_string(), Some(common_min.clone())))
                        .collect(),
                );
            }
        }
        versions
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        buffers::synchronizers::tests::{
            add_data, check_packet_set_contains_version, create_test_buffer,
        },
        channels::read_channel::OutputDelivery,
    };

    #[test]
    fn test_first_synch_synchronize_returns_all_data() {
        let buffer = create_test_buffer();
        let safe_buffer = Arc::new(Mutex::new(buffer));
        let mut test_synch = RealTimeSynchronizer::default();

        add_data(safe_buffer.clone(), "c1".to_string(), 1);
        add_data(safe_buffer.clone(), "c1".to_string(), 2);
        add_data(safe_buffer.clone(), "c1".to_string(), 3);
        add_data(safe_buffer.clone(), "c1".to_string(), 4);
        add_data(safe_buffer.clone(), "c1".to_string(), 5);

        // No data because the minimum versions do not match
        let first_sync = test_synch.synchronize(safe_buffer.clone());
        assert!(first_sync.is_none());

        add_data(safe_buffer.clone(), "c2".to_string(), 1);

        let synch = test_synch.synchronize(safe_buffer.clone());
        check_packet_set_contains_version(synch.as_ref().unwrap(), 1);
        safe_buffer
            .lock()
            .unwrap()
            .get_packets_for_version(&synch.unwrap(), false);

        add_data(safe_buffer.clone(), "c2".to_string(), 5);
        let synch = test_synch.synchronize(safe_buffer.clone());
        check_packet_set_contains_version(synch.as_ref().unwrap(), 5);
    }
}
