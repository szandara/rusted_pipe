//! Module that deals with data sychornization for a node. Non source nodes have a read channel that
//! listens to incoming data. Read Channels have a set of input channels that vary depending on the node.
//! The amount of input channels is reflected in the Processor 'handle' method.
//! Synchronizers are modules that decide when to create a packet set to feed to the handle method. A packet set
//! contains a tuple of data mapped by their channel number. Depending on their configuration
//! synchronizers can generate packet set with empty data but the processor must be ready to handle the lack of data.
//! It's up to the user to create a pipeline with the right synchorization.

pub mod real_time;
pub mod timestamp;

use crate::channels::read_channel::ChannelBuffer;
use crate::channels::ChannelID;
use crate::DataVersion;

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// Trait that defines how a synchronizer must behave.
pub trait PacketSynchronizer: Send {
    /// Accepts a reference to a Channel Buffer that has
    /// access to the ReadChannel buffer and returns an optional set of
    /// packets for each channel.
    ///
    /// If no synchronization is possible it returns None.
    /// If a synchronization is possible a HashMap with one entry per channel
    /// and an optional data entry for that channel is returned.
    fn synchronize(
        &mut self,
        ordered_buffer: Arc<RwLock<dyn ChannelBuffer>>,
    ) -> Option<HashMap<ChannelID, Option<DataVersion>>>;
}

/// Synchronize a read channel if the minimum entry has an exact match in each channel.
fn exact_synchronize(
    ordered_buffer: Arc<RwLock<dyn ChannelBuffer>>,
) -> Option<HashMap<ChannelID, Option<DataVersion>>> {
    let min_version = get_min_versions(ordered_buffer);

    let version = min_version.values().next()?;
    if min_version.values().all(|v| v.is_some()) && min_version.values().all(|v| v == version) {
        return Some(min_version);
    }
    None
}

/// Gets the minimum version of each buffer in the channel.
fn get_min_versions<'a>(
    buffer: Arc<RwLock<dyn ChannelBuffer + 'a>>,
) -> HashMap<ChannelID, Option<DataVersion>> {
    let mut out_map = HashMap::<ChannelID, Option<DataVersion>>::default();
    let buffer = if let Ok(data) = buffer.read() {data} else {return out_map;};

    for channel in buffer.available_channels().iter() {
        out_map.insert(ChannelID::from(channel), buffer.peek(channel).cloned());
    }
    out_map
}

#[cfg(test)]
pub mod tests {
    use std::{
        collections::HashMap,
        sync::{Arc, RwLock},
    };

    use itertools::Itertools;

    use crate::{
        buffers::{
            single_buffers::{FixedSizeBuffer, RtRingBuffer},
            synchronizers::exact_synchronize,
        },
        channels::{typed_read_channel::ReadChannel3, ChannelID, Packet},
        DataVersion, graph::metrics::BufferMonitor,
    };

    pub fn create_test_buffer() -> ReadChannel3<String, String, String> {
        ReadChannel3::create(
            RtRingBuffer::<String>::new(100, false, BufferMonitor::default()),
            RtRingBuffer::<String>::new(100, false, BufferMonitor::default()),
            RtRingBuffer::<String>::new(100, false, BufferMonitor::default()),
        )
    }

    pub fn check_packet_set_contains_versions(
        versions: &HashMap<ChannelID, Option<DataVersion>>,
        expected_versions: Vec<Option<u128>>,
    ) {
        let keys = versions.keys().sorted();
        let timestamps = keys
            .map(|v| {
                if let Some(version) = versions.get(v).unwrap() {
                    return Some(version.timestamp_ns);
                }
                None
            })
            .collect::<Vec<Option<u128>>>();
        assert!(
            timestamps == expected_versions,
            "returned {:?}, expected {:?}",
            timestamps,
            expected_versions
        );
    }

    pub fn add_data(
        buffer: Arc<RwLock<ReadChannel3<String, String, String>>>,
        channel_id: String,
        version_timestamp: u128,
    ) {
        let packet = Packet::<String> {
            data: "data".to_string(),
            version: DataVersion {
                timestamp_ns: version_timestamp,
            },
        };
        if channel_id == "c1" {
            buffer
                .write()
                .unwrap()
                .c1()
                .buffer
                .insert(packet)
                .unwrap();
        } else if channel_id == "c2" {
            buffer
                .write()
                .unwrap()
                .c2()
                .buffer
                .insert(packet)
                .unwrap();
        } else if channel_id == "c3" {
            buffer
                .write()
                .unwrap()
                .c3()
                .buffer
                .insert(packet)
                .unwrap();
        }
    }

    #[test]
    fn test_timestamp_synchronize_is_none_if_no_data_on_channel() {
        let buffer = create_test_buffer();

        let safe_buffer = Arc::new(RwLock::new(buffer));

        add_data(safe_buffer.clone(), "c1".to_string(), 2);
        add_data(safe_buffer.clone(), "c1".to_string(), 3);

        let packet_set = exact_synchronize(safe_buffer);
        assert!(packet_set.is_none());
    }
}
