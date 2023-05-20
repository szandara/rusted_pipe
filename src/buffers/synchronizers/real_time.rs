use super::PacketSynchronizer;

use crate::buffers::BufferIterator;
use crate::channels::read_channel::ChannelBuffer;
use crate::channels::ChannelID;
use crate::DataVersion;
use crate::unwrap_or_return;
use std::cmp::{min, Reverse};
use std::collections::{BinaryHeap, HashMap, HashSet, VecDeque};
use std::iter::Peekable;
use std::sync::{Arc, RwLock, PoisonError};

/// A synchronizer mostly used for run time computations. It has several configuration
/// parameters that allow the handling of different use cases when dealing with
/// eterogeneous consumer latency.
///
/// Syncrhonization is done within timestamp windows of a user given tolerance.
/// This module tries to match the minimum (candidate) next available timestamp in all the buffers
/// if all other channels have data within the tolerance, a tuple is created and returned.
/// If not, the synchronizer drops the current candidate to move to the next possible timestamp
/// as candidate, and so on until a possible tuple is found. If no solution exists it will return
/// None and keep the data in the buffers.
///
/// If one of the data in the buffers is older than the latest shipped minimum, the module
/// treats this as out of sync, which means that one channel is lagging behind. If configured
/// the module can occasionally start buffering interally to re-sync the buffers.
#[derive(Debug, Default, Clone)]
pub struct RealTimeSynchronizer {
    pub tolerance_ns: u128,
    pub wait_all: bool,
    pub rebuffer: bool,
    has_buffered: bool,
    all_channels_have_data: bool,
    last_returned: Option<u128>,
}

impl RealTimeSynchronizer {
    /// Creates a new instance.
    ///
    /// # Arguments
    ///
    /// * `tolerance_ns` - A tolerance in nanoseconds for matching tuples. This value
    /// makes sure that any returned tuple is within
    /// (min_timestamp - tolerance_ns) < min_timestamp < (min_timestamp + tolerance_ns)
    ///
    /// * `wait_all` - If true, synchronize only returns of all channels in the ReadChannel have data.
    ///
    /// * `buffering` - Force the ReadChannel to buffer data if the channels Are out of sync. This can happen
    /// if the ReadChannel input comes from producers with different latencies. Buffering happens any time
    /// that an out of sync is detected. An out of sync happens when one of the channel in the ReadChannel
    /// has to drop data because too old.
    pub fn new(tolerance_ns: u128, wait_all: bool, buffering: bool) -> Self {
        let has_buffered = !buffering;
        Self {
            tolerance_ns,
            wait_all,
            rebuffer: buffering,
            has_buffered,
            all_channels_have_data: has_buffered,
            last_returned: None,
        }
    }
}

/// Get a list of lists of timestamp entries and tries to find a synch solution.
fn extract_matches(
    buffers: &Vec<VecDeque<u128>>,
    wait_all: bool,
) -> Option<Vec<Option<DataVersion>>> {
    let iterators_len = buffers.len();
    let mut matches = vec![None; iterators_len];

    let buffer_min = buffers
        .iter()
        .filter_map(|b| {
            if !b.is_empty() {
                Some(b[0])
            } else {
                None
            }
        })
        .min();

    buffer_min?;

    if let Some(min) = buffer_min {
        for (i, tolerance) in buffers.iter().enumerate() {
            if tolerance.is_empty() {
                continue;
            }
            let target_match = tolerance[0];
            if target_match == min || wait_all {
                matches[i] = Some(DataVersion {
                    timestamp_ns: target_match,
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
                    .map(|f| (*f as i128 - target_match as i128).unsigned_abs())
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

            if let Some(u_min_min) = min_min {
                if u_min_min > target_match - min {
                    matches[i] = Some(DataVersion {
                        timestamp_ns: target_match,
                    });
                }
            }
        }
    }
    Some(matches)
}

/// Core method of the syncrhonizer that tries to find a matching solution
/// or returns None otherwise.
/// It starts from a chosen target that is usually the minumum timestamp among all channels.
///
/// * Arguments
///
/// * `iterators` - An iterator over all channels.
/// * `tolerance` - tolerance in nanoseconds.
/// * `min_timestamp` - Minimum allowed timestamp. Any packet found below this timestamp
/// would trigger a buffering state and will be dropped.
/// * `target` - Initial timestamp candidate, typically the minimum over all channels.
/// * `wait_all` - Only returns if all channels have data.
///
/// * Returns
///
/// A tuple containing the result of the matching if it exists and a boolean telling if an out of sync was detected.
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
    loop {
        let mut all_tolerance_or_finished = false;

        while !all_tolerance_or_finished {
            let peekers_loop = peekers.iter_mut().enumerate();
            let mut done = 0;

            for (i, peek) in peekers_loop {
                if let Some(peek_next) = peek.peek().cloned() {
                    if peek_next.timestamp_ns <= min_timestamp {
                        peek.next();
                        should_rebuffer = true;
                        continue;
                    }
                    if target + tolerance < peek_next.timestamp_ns {
                        // Not within tolerance, increment target.
                        if !target_set.contains(&peek_next.timestamp_ns) {
                            new_targets.push(Reverse(peek_next.timestamp_ns));
                            target_set.insert(peek_next.timestamp_ns);
                        }
                        done += 1;
                    } else if target - min(tolerance, target) <= peek_next.timestamp_ns
                        && peek_next.timestamp_ns <= target + tolerance
                    {
                        buffers_tolerance[i].push_back(peek_next.timestamp_ns);
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

        // Safe unwrap
        if matches.is_none() || wait_all && matches.as_ref().unwrap().contains(&None) {
            if let Some(nt) = new_targets.pop() {
                target = nt.0;
                for b in buffers_tolerance.iter_mut() {
                    while !b.is_empty() && b[0] < target - tolerance {
                        b.pop_front();
                    }
                }
            } else {
                return (None, should_rebuffer);
            }
            continue;
        }
        return (matches, should_rebuffer);
    }
}

impl PacketSynchronizer for RealTimeSynchronizer {
    fn synchronize(
        &mut self,
        ordered_buffer: Arc<RwLock<dyn ChannelBuffer>>,
    ) -> Option<HashMap<ChannelID, Option<DataVersion>>> {
        let locked = ordered_buffer.read().unwrap_or_else(PoisonError::into_inner);
        let min_version = locked.min_version()?;

        let mut iters = vec![];

        for channel in locked.available_channels().clone().into_iter() {
            let iterator = if let Some(iterator) = locked.iterator(channel) {iterator} else {
                eprintln!("Cannot synchronize because {channel} iterator is not available");
                return None;
            };
            iters.push(iterator);
        }
        let mut wait_all = self.wait_all;
        if !self.has_buffered && !self.all_channels_have_data {
            wait_all = true;
        }

        let (packets, should_rebuffer) = find_common_min(
            iters,
            self.tolerance_ns,
            self.last_returned.unwrap_or(0),
            min_version.timestamp_ns,
            wait_all,
        );

        if let Some(common_min) = packets {
            if self.rebuffer && !self.wait_all {
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

            let versions_map: Option<HashMap<ChannelID, Option<DataVersion>>> = locked
                .available_channels()
                .iter()
                .enumerate()
                .map(|(i, c)| Some((ChannelID::from(c), *common_min.get(i)?)))
                .collect();

            let versions_map = unwrap_or_return!(versions_map);

            let max_v = if let Some(val) = versions_map.values().max() { val } else {
                eprintln!("Cannot find max value in synchronization. Returning None");
                return None;
            };

            let max_v = if let Some(val) = max_v { val } else {
                eprintln!("Cannot find max value in synchronization. Returning None");
                return None;
            };
            self.last_returned = Some(max_v.timestamp_ns);
            return Some(versions_map);
        }

        None
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
        let safe_buffer = Arc::new(RwLock::new(buffer));
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
            .write()
            .unwrap()
            .get_packets_for_version(&synch.unwrap(), false);

        add_data(safe_buffer.clone(), "c2".to_string(), 5);
        add_data(safe_buffer.clone(), "c3".to_string(), 5);
        let synch = test_synch.synchronize(safe_buffer);
        check_packet_set_contains_versions(synch.as_ref().unwrap(), vec![Some(5); 3]);
    }

    #[test]
    fn test_realtime_synchronize_if_with_tolerance() {
        let buffer = create_test_buffer();
        let safe_buffer = Arc::new(RwLock::new(buffer));
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
            .write()
            .unwrap()
            .get_packets_for_version(&synch.unwrap(), false);

        add_data(safe_buffer.clone(), "c2".to_string(), 4);
        add_data(safe_buffer.clone(), "c3".to_string(), 5);
        let synch = test_synch.synchronize(safe_buffer);
        check_packet_set_contains_versions(
            synch.as_ref().unwrap(),
            vec![Some(3), Some(4), Some(5)],
        );
    }

    #[test]
    fn test_realtime_synchronize_if_not_wait_all() {
        let buffer = create_test_buffer();
        let safe_buffer = Arc::new(RwLock::new(buffer));
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
            .write()
            .unwrap()
            .get_packets_for_version(&first_sync.unwrap(), false);

        add_data(safe_buffer.clone(), "c2".to_string(), 1);
        add_data(safe_buffer.clone(), "c3".to_string(), 2);

        let synch = test_synch.synchronize(safe_buffer.clone());
        check_packet_set_contains_versions(synch.as_ref().unwrap(), vec![Some(2), None, Some(2)]);
        safe_buffer
            .write()
            .unwrap()
            .get_packets_for_version(&synch.unwrap(), false);

        add_data(safe_buffer.clone(), "c2".to_string(), 4);
        add_data(safe_buffer.clone(), "c3".to_string(), 5);
        let synch = test_synch.synchronize(safe_buffer.clone());
        check_packet_set_contains_versions(synch.as_ref().unwrap(), vec![Some(3), None, None]);
        safe_buffer
            .write()
            .unwrap()
            .get_packets_for_version(&synch.unwrap(), false);

        let synch = test_synch.synchronize(safe_buffer.clone());
        check_packet_set_contains_versions(synch.as_ref().unwrap(), vec![Some(4), Some(4), None]);
        safe_buffer
            .write()
            .unwrap()
            .get_packets_for_version(&synch.unwrap(), false);

        let synch = test_synch.synchronize(safe_buffer);
        check_packet_set_contains_versions(synch.as_ref().unwrap(), vec![Some(5), None, Some(5)]);
    }

    #[test]
    fn test_realtime_synchronize_if_not_wait_all_with_buffering() {
        let buffer = create_test_buffer();
        let safe_buffer = Arc::new(RwLock::new(buffer));
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
            .write()
            .unwrap()
            .get_packets_for_version(&synch.unwrap(), false);

        let synch = test_synch.synchronize(safe_buffer.clone());
        check_packet_set_contains_versions(synch.as_ref().unwrap(), vec![Some(2), None, Some(2)]);
        safe_buffer
            .write()
            .unwrap()
            .get_packets_for_version(&synch.unwrap(), false);

        add_data(safe_buffer.clone(), "c2".to_string(), 4);

        let synch = test_synch.synchronize(safe_buffer.clone());
        check_packet_set_contains_versions(synch.as_ref().unwrap(), vec![Some(3), None, None]);
        safe_buffer
            .write()
            .unwrap()
            .get_packets_for_version(&synch.unwrap(), false);

        add_data(safe_buffer.clone(), "c3".to_string(), 5);

        let synch = test_synch.synchronize(safe_buffer.clone());
        check_packet_set_contains_versions(synch.as_ref().unwrap(), vec![Some(4), Some(4), None]);
        safe_buffer
            .write()
            .unwrap()
            .get_packets_for_version(&synch.unwrap(), false);

        let synch = test_synch.synchronize(safe_buffer.clone());
        check_packet_set_contains_versions(synch.as_ref().unwrap(), vec![Some(5), None, Some(5)]);
        safe_buffer
            .write()
            .unwrap()
            .get_packets_for_version(&synch.unwrap(), false);
    }
}
