pub mod first_sync;
pub mod timestamp;

use crate::buffers::OrderedBuffer;
use crate::DataVersion;

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};

pub trait PacketSynchronizer: Send {
    fn synchronize(
        &mut self,
        ordered_buffer: Arc<Mutex<dyn OrderedBuffer>>,
    ) -> Option<HashMap<String, Option<DataVersion>>>;
}

fn synchronize(
    ordered_buffer: Arc<Mutex<dyn OrderedBuffer>>,
) -> Option<HashMap<String, Option<DataVersion>>> {
    let min_version = get_min_versions(ordered_buffer);
    let version = min_version.values().next().unwrap();
    if min_version.values().all(|v| v.is_some()) && min_version.values().all(|v| v == version) {
        return Some(min_version);
    }
    None
}

fn get_min_versions(buffer: Arc<Mutex<dyn OrderedBuffer>>) -> HashMap<String, Option<DataVersion>> {
    let buffer = buffer.lock().unwrap();
    let mut out_map = HashMap::<String, Option<DataVersion>>::default();

    for channel in buffer.available_channels().iter() {
        out_map.insert(channel.to_string(), buffer.peek(channel).cloned());
    }
    out_map
}

#[cfg(test)]
pub mod tests {
    use std::sync::{Arc, Mutex};

    use crate::{
        buffers::{
            single_buffers::{FixedSizeBuffer, RtRingBuffer},
            synchronizers::synchronize,
            OrderedBuffer,
        },
        channels::{read_channel::ReadChannel2, Packet},
        DataVersion,
    };

    pub fn create_test_buffer() -> ReadChannel2<String, String> {
        ReadChannel2::create(
            RtRingBuffer::<String>::new(100, false),
            RtRingBuffer::<String>::new(100, false),
        )
    }

    pub fn add_data(
        buffer: &mut ReadChannel2<String, String>,
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
            buffer.c1().buffer.insert(packet.clone()).unwrap();
        } else if channel_id == "c2" {
            buffer.c2().buffer.insert(packet.clone()).unwrap();
        }
    }

    #[test]
    fn test_timestamp_synchronize_is_none_if_no_data_on_channel() {
        let mut buffer = create_test_buffer();

        add_data(&mut buffer, "test1".to_string(), 2);
        add_data(&mut buffer, "test1".to_string(), 3);

        let safe_buffer: Arc<Mutex<dyn OrderedBuffer>> = Arc::new(Mutex::new(buffer));

        let packet_set = synchronize(safe_buffer.clone());
        assert!(packet_set.is_none());
    }
}
