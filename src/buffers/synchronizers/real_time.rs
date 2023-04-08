use super::PacketSynchronizer;

use crate::buffers::BufferIterator;
use crate::channels::read_channel::ChannelBuffer;
use crate::DataVersion;

use std::cmp::min;
use std::collections::{BinaryHeap, HashMap, HashSet, VecDeque};
use std::hash::Hash;
use std::iter::Peekable;
use std::sync::{Arc, Mutex};

#[derive(Debug, Default, Clone)]
pub struct RealTimeSynchronizer {
    pub tolerance_ms: u128,
    pub wait_all: bool,
    pub initial_buffering: bool,
    pub has_buffered: bool,
    pub last_returned: Option<u128>,
}

impl RealTimeSynchronizer {
    pub fn new(tolerance_ms: u128, wait_all: bool, initial_buffering: bool) -> Self {
        let has_buffered = !initial_buffering;
        Self {
            tolerance_ms,
            wait_all,
            initial_buffering,
            has_buffered,
            last_returned: None,
        }
    }
}

fn find_common_min<'a>(
    mut iterators: Vec<Box<BufferIterator>>,
    tolerance: u128,
    min_timestamp: u128,
    mut target: u128,
    wait_all: bool,
) -> Option<Vec<Option<DataVersion>>> {
    let iterators_len = iterators.len();

    let mut buffers_tolerance = vec![VecDeque::<u128>::new(); iterators_len];
    let mut matches = vec![None; iterators_len];

    let mut peekers: Vec<Peekable<&mut Box<dyn Iterator<Item = &DataVersion>>>> =
        iterators.iter_mut().map(|i| i.peekable()).collect();
    let mut new_targets = BinaryHeap::default();
    let mut target_set = HashSet::new();
    loop {
        let mut all_tolerance_or_finished = false;

        while !all_tolerance_or_finished {
            let peekers_loop = peekers.iter_mut().enumerate();
            let mut done = 0;
            for (i, peek) in peekers_loop {
                if let Some(peek_next) = peek.peek().cloned() {
                    println!(
                        "Target {:?}, tolerance {}, Next {i}: {}",
                        target, tolerance, peek_next.timestamp
                    );
                    if peek_next.timestamp <= min_timestamp {
                        peek.next();
                        continue;
                    }
                    if target + tolerance < peek_next.timestamp {
                        // Not within tolerance, increment target.
                        if !target_set.contains(&peek_next.timestamp) {
                            println!("New Target {i}: {}", peek_next.timestamp);
                            new_targets.push(peek_next.timestamp);
                            target_set.insert(peek_next.timestamp);
                        }
                        done += 1;
                    } else if target - min(tolerance, target) <= peek_next.timestamp
                        && peek_next.timestamp <= target + tolerance
                    {
                        println!("In tolerance {i}: {}", peek_next.timestamp);
                        buffers_tolerance[i].push_back(peek_next.timestamp);
                        peek.next();
                    } else {
                        peek.next();
                    }
                } else {
                    done += 1;
                }

                if done == iterators_len {
                    all_tolerance_or_finished = true;
                }
            }
        }

        for m in 0..iterators_len {
            matches[m] = None;
        }
        println!("Buffers");
        let buffer_min = buffers_tolerance
            .iter()
            .map(|b| {
                println!("b {:?}", b);
                if b.len() > 0 {
                    return Some(b[0]);
                } else {
                    return None;
                }
            })
            .flatten()
            .min();
        if let Some(min) = buffer_min {
            for (i, tolerance) in buffers_tolerance.iter().enumerate() {
                if tolerance.len() == 0 {
                    continue;
                }
                let target_match = tolerance[0];
                if target_match == min || wait_all {
                    println!("Adding {target_match}");
                    matches[i] = Some(DataVersion {
                        timestamp: target_match,
                    });
                    continue;
                }
                // Check if there are better candidates for a later match.
                let mut min_min = None;
                for (e, tolerance) in buffers_tolerance.iter().enumerate() {
                    if e == i {
                        continue;
                    }
                    if let Some(min_buffer) = tolerance
                        .iter()
                        .map(|f| (*f as i128 - target_match as i128).abs() as u128)
                        .min()
                    {
                        if let Some(u_min_min) = min_min {
                            if u_min_min > min_buffer {
                                min_min = Some(min_buffer);
                            }
                        } else {
                            min_min = Some(min_buffer);
                        }
                    }
                }
                println!("min_min {:?}", min_min);
                if let Some(u_min_min) = min_min {
                    if u_min_min > target_match - min {
                        println!("Adding {target_match}");
                        matches[i] = Some(DataVersion {
                            timestamp: target_match,
                        });
                    }
                }
            }
        }

        if buffer_min.is_none() || wait_all && matches.contains(&None) {
            if let Some(nt) = new_targets.pop() {
                target = nt;
                println!("New target {nt}");
                for b in buffers_tolerance.iter_mut() {
                    while b.len() > 0 && b[0] < nt - tolerance {
                        b.pop_front();
                    }
                }
            } else {
                return None;
            }
            continue;
        }

        println!("Returning {:?}", matches);
        return Some(matches);
    }
}

impl PacketSynchronizer for RealTimeSynchronizer {
    fn synchronize(
        &mut self,
        ordered_buffer: Arc<Mutex<dyn ChannelBuffer>>,
    ) -> Option<HashMap<String, Option<DataVersion>>> {
        let locked = ordered_buffer.lock().unwrap();
        let min_version = locked.min_version();
        if min_version.is_none() {
            println!("No data in buffer");
            return None;
        }
        let mut iters = vec![];

        for channel in locked.available_channels().clone().into_iter() {
            iters.push(locked.iterator(&channel).unwrap());
        }
        let mut wait_all = self.wait_all;
        if !self.has_buffered {
            wait_all = true;
        }

        println!("Wait all {wait_all}");
        if let Some(common_min) = find_common_min(
            iters,
            self.tolerance_ms,
            self.last_returned.unwrap_or(0),
            min_version.unwrap().timestamp,
            wait_all,
        ) {
            self.has_buffered = true;
            let versions_map: HashMap<String, Option<DataVersion>> = locked
                .available_channels()
                .iter()
                .enumerate()
                .map(|(i, c)| (c.to_string(), common_min.get(i).unwrap().clone()))
                .collect();
            self.last_returned = Some(versions_map.values().max().unwrap().unwrap().timestamp);
            return Some(versions_map);
        }

        return None;
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
    fn test_realtime_synchronize_returns_all_data() {
        let buffer = create_test_buffer();
        let safe_buffer = Arc::new(Mutex::new(buffer));
        let mut test_synch = RealTimeSynchronizer::new(0, true, false);

        add_data(safe_buffer.clone(), "c1".to_string(), 1);
        add_data(safe_buffer.clone(), "c1".to_string(), 2);
        add_data(safe_buffer.clone(), "c1".to_string(), 3);
        add_data(safe_buffer.clone(), "c1".to_string(), 4);
        add_data(safe_buffer.clone(), "c1".to_string(), 5);

        // No data because the minimum versions do not match
        let first_sync = test_synch.synchronize(safe_buffer.clone());
        assert!(first_sync.is_none());

        add_data(safe_buffer.clone(), "c2".to_string(), 1);
        add_data(safe_buffer.clone(), "c3".to_string(), 1);

        let synch = test_synch.synchronize(safe_buffer.clone());
        check_packet_set_contains_versions(synch.as_ref().unwrap(), vec![Some(1); 3]);
        safe_buffer
            .lock()
            .unwrap()
            .get_packets_for_version(&synch.unwrap(), false);

        add_data(safe_buffer.clone(), "c2".to_string(), 5);
        add_data(safe_buffer.clone(), "c3".to_string(), 5);
        let synch = test_synch.synchronize(safe_buffer.clone());
        check_packet_set_contains_versions(synch.as_ref().unwrap(), vec![Some(5); 3]);
    }

    #[test]
    fn test_realtime_synchronize_if_with_tolerance() {
        let buffer = create_test_buffer();
        let safe_buffer = Arc::new(Mutex::new(buffer));
        let mut test_synch = RealTimeSynchronizer::new(2, true, false);

        add_data(safe_buffer.clone(), "c1".to_string(), 1);
        add_data(safe_buffer.clone(), "c1".to_string(), 2);
        add_data(safe_buffer.clone(), "c1".to_string(), 3);
        add_data(safe_buffer.clone(), "c1".to_string(), 4);
        add_data(safe_buffer.clone(), "c1".to_string(), 5);

        // No data because the minimum versions do not match
        let first_sync = test_synch.synchronize(safe_buffer.clone());
        assert!(first_sync.is_none());

        add_data(safe_buffer.clone(), "c2".to_string(), 1);
        add_data(safe_buffer.clone(), "c3".to_string(), 2);

        let synch = test_synch.synchronize(safe_buffer.clone());
        check_packet_set_contains_versions(
            synch.as_ref().unwrap(),
            vec![Some(1), Some(1), Some(2)],
        );
        safe_buffer
            .lock()
            .unwrap()
            .get_packets_for_version(&synch.unwrap(), false);

        add_data(safe_buffer.clone(), "c2".to_string(), 4);
        add_data(safe_buffer.clone(), "c3".to_string(), 5);
        let synch = test_synch.synchronize(safe_buffer.clone());
        check_packet_set_contains_versions(
            synch.as_ref().unwrap(),
            vec![Some(3), Some(4), Some(5)],
        );
    }

    #[test]
    fn test_realtime_synchronize_if_not_wait_all() {
        let buffer = create_test_buffer();
        let safe_buffer = Arc::new(Mutex::new(buffer));
        let mut test_synch = RealTimeSynchronizer::new(2, false, false);

        add_data(safe_buffer.clone(), "c1".to_string(), 1);
        add_data(safe_buffer.clone(), "c1".to_string(), 2);
        add_data(safe_buffer.clone(), "c1".to_string(), 3);
        add_data(safe_buffer.clone(), "c1".to_string(), 4);
        add_data(safe_buffer.clone(), "c1".to_string(), 5);

        // No data because the minimum versions do not match
        let first_sync = test_synch.synchronize(safe_buffer.clone());
        check_packet_set_contains_versions(first_sync.as_ref().unwrap(), vec![Some(1), None, None]);

        safe_buffer
            .lock()
            .unwrap()
            .get_packets_for_version(&first_sync.unwrap(), false);

        add_data(safe_buffer.clone(), "c2".to_string(), 1);
        add_data(safe_buffer.clone(), "c3".to_string(), 2);

        let synch = test_synch.synchronize(safe_buffer.clone());
        check_packet_set_contains_versions(synch.as_ref().unwrap(), vec![Some(2), None, Some(2)]);
        safe_buffer
            .lock()
            .unwrap()
            .get_packets_for_version(&synch.unwrap(), false);

        add_data(safe_buffer.clone(), "c2".to_string(), 4);
        add_data(safe_buffer.clone(), "c3".to_string(), 5);
        let synch = test_synch.synchronize(safe_buffer.clone());
        check_packet_set_contains_versions(synch.as_ref().unwrap(), vec![Some(3), None, None]);
        safe_buffer
            .lock()
            .unwrap()
            .get_packets_for_version(&synch.unwrap(), false);

        let synch = test_synch.synchronize(safe_buffer.clone());
        check_packet_set_contains_versions(synch.as_ref().unwrap(), vec![Some(4), Some(4), None]);
        safe_buffer
            .lock()
            .unwrap()
            .get_packets_for_version(&synch.unwrap(), false);

        let synch = test_synch.synchronize(safe_buffer.clone());
        check_packet_set_contains_versions(synch.as_ref().unwrap(), vec![Some(5), None, Some(5)]);
    }

    #[test]
    fn test_realtime_synchronize_if_not_wait_all_with_buffering() {
        let buffer = create_test_buffer();
        let safe_buffer = Arc::new(Mutex::new(buffer));
        let mut test_synch = RealTimeSynchronizer::new(2, false, true);

        add_data(safe_buffer.clone(), "c1".to_string(), 1);
        add_data(safe_buffer.clone(), "c1".to_string(), 2);
        add_data(safe_buffer.clone(), "c1".to_string(), 3);
        add_data(safe_buffer.clone(), "c1".to_string(), 4);
        add_data(safe_buffer.clone(), "c1".to_string(), 5);

        // No data because the minimum versions do not match
        let first_sync = test_synch.synchronize(safe_buffer.clone());
        assert!(first_sync.is_none());

        add_data(safe_buffer.clone(), "c2".to_string(), 1);
        add_data(safe_buffer.clone(), "c3".to_string(), 2);

        let synch = test_synch.synchronize(safe_buffer.clone());
        check_packet_set_contains_versions(
            synch.as_ref().unwrap(),
            vec![Some(1), Some(1), Some(2)],
        );
        safe_buffer
            .lock()
            .unwrap()
            .get_packets_for_version(&synch.unwrap(), false);

        add_data(safe_buffer.clone(), "c2".to_string(), 4);

        let synch = test_synch.synchronize(safe_buffer.clone());
        check_packet_set_contains_versions(synch.as_ref().unwrap(), vec![Some(3), None, None]);
        safe_buffer
            .lock()
            .unwrap()
            .get_packets_for_version(&synch.unwrap(), false);

        add_data(safe_buffer.clone(), "c3".to_string(), 5);

        let synch = test_synch.synchronize(safe_buffer.clone());
        check_packet_set_contains_versions(synch.as_ref().unwrap(), vec![Some(4), Some(4), None]);
        safe_buffer
            .lock()
            .unwrap()
            .get_packets_for_version(&synch.unwrap(), false);

        let synch = test_synch.synchronize(safe_buffer.clone());
        check_packet_set_contains_versions(synch.as_ref().unwrap(), vec![Some(5), None, Some(5)]);
        safe_buffer
            .lock()
            .unwrap()
            .get_packets_for_version(&synch.unwrap(), false);
    }
}
