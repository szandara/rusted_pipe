pub mod real_time;
pub mod timestamp;

use crate::channels::read_channel::ChannelBuffer;
use crate::DataVersion;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

pub trait PacketSynchronizer: Send {
    fn synchronize(
        &mut self,
        ordered_buffer: Arc<Mutex<dyn ChannelBuffer>>,
    ) -> Option<HashMap<String, Option<DataVersion>>>;
}

fn synchronize(
    ordered_buffer: Arc<Mutex<dyn ChannelBuffer>>,
) -> Option<HashMap<String, Option<DataVersion>>> {
    let min_version = get_min_versions(ordered_buffer);

    let version = min_version.values().next().unwrap();
    if min_version.values().all(|v| v.is_some()) && min_version.values().all(|v| v == version) {
        return Some(min_version);
    }
    None
}

fn get_min_versions(buffer: Arc<Mutex<dyn ChannelBuffer>>) -> HashMap<String, Option<DataVersion>> {
    let buffer = buffer.lock().unwrap();
    let mut out_map = HashMap::<String, Option<DataVersion>>::default();

    for channel in buffer.available_channels().iter() {
        out_map.insert(channel.to_string(), buffer.peek(channel).cloned());
    }
    out_map
}

#[cfg(test)]
pub mod tests {
    use std::{
        collections::HashMap,
        sync::{Arc, Mutex},
    };

    use crate::{
        buffers::{
            single_buffers::{FixedSizeBuffer, RtRingBuffer},
            synchronizers::synchronize,
        },
        channels::{typed_read_channel::ReadChannel2, Packet},
        DataVersion,
    };

    pub fn create_test_buffer() -> ReadChannel2<String, String> {
        ReadChannel2::create(
            RtRingBuffer::<String>::new(100, false),
            RtRingBuffer::<String>::new(100, false),
        )
    }

    pub fn check_packet_set_contains_version(
        versions: &HashMap<String, Option<DataVersion>>,
        version: u128,
    ) {
        assert!(versions
            .values()
            .all(|v| v.is_some() && v.unwrap().timestamp == version));
    }

    pub fn add_data(
        buffer: Arc<Mutex<ReadChannel2<String, String>>>,
        channel_id: String,
        version_timestamp: u128,
    ) {
        let packet = Packet::<String> {
            data: Box::new("data".to_string()),
            version: DataVersion {
                timestamp: version_timestamp,
            },
        };
        if channel_id == "c1" {
            buffer
                .lock()
                .unwrap()
                .c1()
                .buffer
                .insert(packet.clone())
                .unwrap();
        } else if channel_id == "c2" {
            buffer
                .lock()
                .unwrap()
                .c2()
                .buffer
                .insert(packet.clone())
                .unwrap();
        }
    }

    #[test]
    fn test_timestamp_synchronize_is_none_if_no_data_on_channel() {
        let buffer = create_test_buffer();

        let safe_buffer = Arc::new(Mutex::new(buffer));

        add_data(safe_buffer.clone(), "c1".to_string(), 2);
        add_data(safe_buffer.clone(), "c1".to_string(), 3);

        let packet_set = synchronize(safe_buffer.clone());
        assert!(packet_set.is_none());
    }
}
