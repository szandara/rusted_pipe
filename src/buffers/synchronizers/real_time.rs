use super::PacketSynchronizer;

use crate::buffers::BufferIterator;
use crate::channels::read_channel::ChannelBuffer;
use crate::DataVersion;

use std::cmp::{min, Reverse};
use std::collections::{BinaryHeap, HashMap, HashSet, VecDeque};
use std::iter::Peekable;
use std::sync::{Arc, Mutex};
use std::time::Instant;

#[derive(Debug, Default, Clone)]
pub struct RealTimeSynchronizer {
    pub tolerance_ms: u128,
    pub wait_all: bool,
    pub initial_buffering: bool,
    has_buffered: bool,
    all_channels_have_data: bool,
    last_returned: Option<u128>,
}

impl RealTimeSynchronizer {
    pub fn new(tolerance_ms: u128, wait_all: bool, initial_buffering: bool) -> Self {
        let has_buffered = !initial_buffering;
        Self {
            tolerance_ms,
            wait_all,
            initial_buffering,
            has_buffered,
            all_channels_have_data: has_buffered,
            last_returned: None,
        }
    }
}

fn extract_matches(
    buffers: &Vec<VecDeque<u128>>,
    wait_all: bool,
) -> Option<Vec<Option<DataVersion>>> {
    let iterators_len = buffers.len();
    let mut matches = vec![None; iterators_len];
    println!("Buffers");

    let buffer_min = buffers
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

    if buffer_min.is_none() {
        return None;
    }

    if let Some(min) = buffer_min {
        for (i, tolerance) in buffers.iter().enumerate() {
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
            for (e, tolerance) in buffers.iter().enumerate() {
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
    return Some(matches);
}

fn find_common_min<'a>(
    mut iterators: Vec<Box<BufferIterator>>,
    tolerance: u128,
    min_timestamp: u128,
    mut target: u128,
    wait_all: bool,
) -> (Option<Vec<Option<DataVersion>>>, bool) {
    let iterators_len = iterators.len();
    let mut should_rebuffer = false;
    let mut buffers_tolerance = vec![VecDeque::<u128>::new(); iterators_len];

    let mut peekers: Vec<Peekable<&mut Box<dyn Iterator<Item = &DataVersion>>>> =
        iterators.iter_mut().map(|i| i.peekable()).collect();
    let mut new_targets = BinaryHeap::default();
    let mut target_set = HashSet::new();
    let start_duration = Instant::now();
    loop {
        let mut all_tolerance_or_finished = false;

        while !all_tolerance_or_finished {
            let peekers_loop = peekers.iter_mut().enumerate();
            let mut done = 0;

            for (i, peek) in peekers_loop {
                if let Some(peek_next) = peek.peek().cloned() {
                    println!(
                        "Target {:?}, tolerance {}, Next {i}: {}, min_timestamp {}",
                        target, tolerance, peek_next.timestamp, min_timestamp
                    );
                    if peek_next.timestamp <= min_timestamp {
                        println!("Dropping {}, {}", i, peek_next.timestamp);
                        peek.next();
                        should_rebuffer = true;
                        continue;
                    }
                    if target + tolerance < peek_next.timestamp {
                        // Not within tolerance, increment target.
                        if !target_set.contains(&peek_next.timestamp) {
                            println!("New Target {i}: {}", peek_next.timestamp);
                            new_targets.push(Reverse(peek_next.timestamp));
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

        let matches = extract_matches(&buffers_tolerance, wait_all);

        if matches.is_none() || wait_all && matches.as_ref().unwrap().contains(&None) {
            if let Some(nt) = new_targets.pop() {
                target = nt.0;
                println!("New target {target}");
                for b in buffers_tolerance.iter_mut() {
                    while b.len() > 0 && b[0] < target - tolerance {
                        b.pop_front();
                    }
                }
            } else {
                println!("Returning None in {:?}", Instant::now() - start_duration);
                return (None, should_rebuffer);
            }
            continue;
        }

        println!(
            "Returning {:?} in {:?}",
            matches,
            Instant::now() - start_duration
        );
        return (matches, should_rebuffer);
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
        if !self.has_buffered && !self.all_channels_have_data {
            wait_all = true;
        }

        println!("Wait all {wait_all}");
        let (packets, should_rebuffer) = find_common_min(
            iters,
            self.tolerance_ms,
            self.last_returned.unwrap_or(0),
            min_version.unwrap().timestamp,
            wait_all,
        );

        if let Some(common_min) = packets {
            if self.initial_buffering && !self.wait_all {
                println!(
                    "Buffer status has_buffered {}, have_data {}, should_rebuffer {}",
                    self.has_buffered, self.all_channels_have_data, should_rebuffer
                );
                // initial buffering and wait all does not make much sense.
                if !self.has_buffered && self.all_channels_have_data && !should_rebuffer {
                    // If we are buffering and all channels had data from a previous sync
                    // and we do not need to drop packets anymore, we can consider
                    // buffering done.
                    self.has_buffered = true;
                } else if !self.has_buffered && !self.all_channels_have_data {
                    // if we are buffering and this is true, it means we have
                    // enough data on all channels. Re-run the sync without waiting for all data
                    // to continue the normal computation.
                    self.all_channels_have_data = true;
                    return None;
                } else if self.has_buffered && should_rebuffer {
                    // During our last sync we have detected that buffering is necessary.
                    // Dispatch the last data and force rebuffering on the next iteration.
                    println!("Rebuffering!");
                    self.has_buffered = false;
                    self.all_channels_have_data = false;
                }
            }

            let versions_map: HashMap<String, Option<DataVersion>> = locked
                .available_channels()
                .iter()
                .enumerate()
                .map(|(i, c)| (c.to_string(), common_min.get(i).unwrap().clone()))
                .collect();
            println!("Packet {:?}", versions_map);
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
        let buffer_sycn = test_synch.synchronize(safe_buffer.clone());
        assert!(buffer_sycn.is_none());

        let synch = test_synch.synchronize(safe_buffer.clone());
        check_packet_set_contains_versions(synch.as_ref().unwrap(), vec![Some(1), Some(1), None]);
        safe_buffer
            .lock()
            .unwrap()
            .get_packets_for_version(&synch.unwrap(), false);

        let synch = test_synch.synchronize(safe_buffer.clone());
        check_packet_set_contains_versions(synch.as_ref().unwrap(), vec![Some(2), None, Some(2)]);
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
