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

#[derive(Default, Debug, Clone)]
struct BufferingSynch {
    // Is buffering active
    pub do_buffering: bool,
    // Are we waiting for buffering?
    pub _needs_buffering: bool,
    // Next target using buffering timing estimation
    pub next_target: Option<DataVersion>,
    // Estimated buffering time
    pub _buffering_time: Option<u128>
}

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
    buffering: BufferingSynch,
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
        assert!(buffering == false, "Buffering is not supported yet");
        let do_buffering = buffering;
        let buffering_sync = BufferingSynch {do_buffering, _needs_buffering: !do_buffering, next_target: None, _buffering_time: None};
        Self {
            tolerance_ns,
            wait_all,
            buffering: buffering_sync,
            last_returned: None,
        }
    }
}

/// Get a list of lists of timestamp entries and tries to find a synch solution.
/// 
/// * Arguments
/// `buffers` - A vector of ordered data each containing all data within the tolerance
/// given by the user of a single channel in reverse order. (latest is first entry)
/// `wait_all` - If true it returns Some() only if all channels have a match.
/// 
/// Returns
/// An optional (if there is a matching tuple) vector of data versions containing the matched
/// data. If `wait_all` is false, any channel can have a None entry.
fn extract_matches(
    buffers: &Vec<VecDeque<u128>>,
    wait_all: bool
) -> Option<Vec<Option<DataVersion>>> {
    let data: Vec<Option<DataVersion>> = buffers
        .iter()
        .map(|b| {
            if !b.is_empty() {
                Some(DataVersion {
                    timestamp_ns: b[0],
                })
            } else {
                None
            }
        })
        .collect();
    if wait_all && data.contains(&None) {
        return None;
    }
    return Some(data);
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
fn find_common_max(
    mut iterators: Vec<Box<BufferIterator>>,
    tolerance: u128,
    min_timestamp: u128,
    mut target: u128,
    wait_all: bool,
) -> Option<Vec<Option<DataVersion>>> {
    let iterators_len = iterators.len();
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
            let tolerance_target = if tolerance > target { 0 } else { target - tolerance };
            for (i, peek) in peekers_loop {
                if let Some(peek_next) = peek.peek().cloned() {
                    // If there is a buffer that is lagging behind we continue
                    // the computation but make sure to rebuffer.
                    // By lagging behind it's intended that a channel has data that is behind
                    // the timestamp that was recently shipped.
                    if peek_next.timestamp_ns <= min_timestamp {
                        peek.next();
                        continue;
                    }
                    // If the value is within tolerance we just add it to our candidates.
                    if tolerance_target > 0 && tolerance_target > peek_next.timestamp_ns {
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
        // Decide if we want to try a new target. The previous target did not produce any match.
        if matches.is_none() || wait_all && matches.as_ref().unwrap().contains(&None) {
            if let Some(nt) = new_targets.pop() {
                
                target = nt.0;
                for b in buffers_tolerance.iter_mut() {
                    while !b.is_empty() && b[0] > target + tolerance {
                        b.pop_front();
                    }
                }
            } else {
                return None;
            }
            continue;
        }
        return matches;
    }
}

impl PacketSynchronizer for RealTimeSynchronizer {
    fn synchronize(
        &mut self,
        ordered_buffer: Arc<RwLock<dyn ChannelBuffer>>,
    ) -> Option<HashMap<ChannelID, Option<DataVersion>>> {
        let locked = ordered_buffer.read().unwrap_or_else(PoisonError::into_inner);
        let target = if let Some(t) = self.buffering.next_target.as_ref() {t} else {locked.max_version()?};
        let mut iters = vec![];

        for channel in locked.available_channels().clone().into_iter() {
            let iterator = if let Some(iterator) = locked.iterator(channel) {iterator} else {
                eprintln!("Cannot synchronize because {channel} iterator is not available");
                return None;
            };
            iters.push(iterator);
        }
        let mut wait_all = self.wait_all;
        if self.buffering.do_buffering {
            wait_all = true;
        }

        let packets = find_common_max(
            iters,
            self.tolerance_ns,
            self.last_returned.unwrap_or(0),
            target.timestamp_ns,
            wait_all,
        );

        if let Some(common_min) = packets {
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
            vec![Some(3), Some(1), Some(2)],
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
            vec![Some(5), Some(4), Some(5)],
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
        check_packet_set_contains_versions(first_sync.as_ref().unwrap(), vec![Some(5), None, None]);

        safe_buffer
            .write()
            .unwrap()
            .get_packets_for_version(&first_sync.unwrap(), false);

        add_data(safe_buffer.clone(), "c2".to_string(), 1);
        add_data(safe_buffer.clone(), "c3".to_string(), 2);

        let synch = test_synch.synchronize(safe_buffer.clone());
        assert!(synch.is_none());

        add_data(safe_buffer.clone(), "c1".to_string(), 6);
        add_data(safe_buffer.clone(), "c2".to_string(), 10);
        add_data(safe_buffer.clone(), "c3".to_string(), 11);

        let synch = test_synch.synchronize(safe_buffer.clone());
        check_packet_set_contains_versions(synch.as_ref().unwrap(), vec![None, Some(10), Some(11)]);
        safe_buffer
            .write()
            .unwrap()
            .get_packets_for_version(&synch.unwrap(), false);

        add_data(safe_buffer.clone(), "c2".to_string(), 11);
        add_data(safe_buffer.clone(), "c3".to_string(),12);
        let synch = test_synch.synchronize(safe_buffer.clone());
        check_packet_set_contains_versions(synch.as_ref().unwrap(), vec![None, None, Some(12)]);
        safe_buffer
            .write()
            .unwrap()
            .get_packets_for_version(&synch.unwrap(), false);

        add_data(safe_buffer.clone(), "c2".to_string(), 13);
        add_data(safe_buffer.clone(), "c2".to_string(), 14);
        add_data(safe_buffer.clone(), "c2".to_string(), 15);
        add_data(safe_buffer.clone(), "c3".to_string(),14);
        add_data(safe_buffer.clone(), "c3".to_string(),15);

        let synch = test_synch.synchronize(safe_buffer.clone());
        check_packet_set_contains_versions(synch.as_ref().unwrap(), vec![None, Some(15), Some(15)]);
        safe_buffer
            .write()
            .unwrap()
            .get_packets_for_version(&synch.unwrap(), false);

        let synch = test_synch.synchronize(safe_buffer);
        assert!(synch.is_none());
    }

    #[test]
    #[should_panic]
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
        check_packet_set_contains_versions(synch.as_ref().unwrap(), vec![Some(1), Some(2), None]);
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
