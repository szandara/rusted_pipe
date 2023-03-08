use super::{get_packets_for_version, PacketSynchronizer};
use crate::DataVersion;

use crate::buffers::{BufferIterator, OrderedBuffer};
use crate::channels::ChannelID;
use crate::packet::WorkQueue;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

#[derive(Debug, Default)]
pub struct FirstSyncSynchronizer {}

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
                if next.version.timestamp > min.unwrap().version.timestamp {
                    min = Some(next);
                    matches = 1;
                    all_end = false;
                    break;
                } else if next.version.timestamp == min.unwrap().version.timestamp {
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
            return Some(min.version.clone());
        }
    }
    None
}

impl PacketSynchronizer for FirstSyncSynchronizer {
    fn synchronize(
        &mut self,
        ordered_buffer: &Arc<Mutex<dyn OrderedBuffer>>,
        work_queue: Arc<WorkQueue>,
    ) {
        let mut versions: Option<HashMap<ChannelID, Option<DataVersion>>> = None;

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
                        .map(|f| (f.clone(), Some(common_min.clone())))
                        .collect(),
                );
            }
        }
        if let Some(versions) = versions {
            if let Some(packet_set) =
                get_packets_for_version(&versions, &mut ordered_buffer.clone(), false)
            {
                work_queue.push(packet_set);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffers::synchronizers::tests::{add_data, create_test_buffer};

    fn test_packet_set_contains_version(work_queue: Arc<WorkQueue>, version: u128) {
        assert!(work_queue
            .steal()
            .success()
            .and_then(|v| {
                let out: Vec<u128> = v
                    .packet_data
                    .values()
                    .iter()
                    .map(|p| p.as_ref().unwrap().1.version.timestamp)
                    .collect();
                return Some(out);
            })
            .unwrap()
            .iter()
            .all(|v| *v == version));
    }

    #[test]
    fn test_first_synch_synchronize_returns_all_data() {
        let buffer = create_test_buffer();
        let safe_buffer: Arc<Mutex<dyn OrderedBuffer>> = Arc::new(Mutex::new(buffer));
        let mut test_synch = FirstSyncSynchronizer::default();
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
        test_packet_set_contains_version(work_queue.clone(), 1);
        assert!(work_queue.steal().is_empty());

        add_data(&safe_buffer, "test2".to_string(), 5);
        test_synch.synchronize(&safe_buffer, work_queue.clone());
        test_packet_set_contains_version(work_queue.clone(), 5);
        assert!(work_queue.steal().is_empty());
    }
}
