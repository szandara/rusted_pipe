use super::single_buffers::FixedSizeBuffer;
use super::BufferError;
use super::DataBuffer;
use super::OrderedBuffer;
use crate::buffers::single_buffers::RingBuffer;
use crate::packet::ChannelID;
use crate::packet::DataVersion;
use crate::packet::UntypedPacket;

use super::PacketBufferAddress;
use std::collections::HashMap;

pub struct CircularBufferedData {
    data: HashMap<ChannelID, RingBuffer>,
    max_size: usize,
    remove_before_consumed: bool,
}

fn create_buffer(max_size: usize) -> RingBuffer {
    RingBuffer::new(max_size)
}

impl CircularBufferedData {
    pub fn new(max_size: usize, remove_before_consumed: bool) -> Self {
        CircularBufferedData {
            data: Default::default(),
            max_size,
            remove_before_consumed,
        }
    }

    fn get_channel(&mut self, channel: &ChannelID) -> Result<&mut RingBuffer, BufferError> {
        Ok(self
            .data
            .get_mut(channel)
            .ok_or(BufferError::InternalError(format!(
                "Cannod find channel {}",
                channel.id
            )))?)
    }

    fn get_or_create_channel(&mut self, channel: &ChannelID) -> &mut RingBuffer {
        self.data
            .entry(channel.clone())
            .or_insert(create_buffer(self.max_size))
    }
}

impl DataBuffer for CircularBufferedData {
    fn insert(
        &mut self,
        channel: &ChannelID,
        packet: UntypedPacket,
    ) -> Result<PacketBufferAddress, BufferError> {
        if self.has_version(&channel, &packet.version) {
            return Err(BufferError::DuplicateDataVersionError((
                channel.clone(),
                packet.version.clone(),
            )));
        }

        let buffer = self.get_channel(channel)?;
        let data_version = (channel.clone(), packet.version.clone());
        buffer.insert(packet.version, packet);
        Ok(data_version)
    }

    fn consume(
        &mut self,
        version: &PacketBufferAddress,
    ) -> Result<Option<UntypedPacket>, BufferError> {
        if self.remove_before_consumed {
            self.get_channel(&version.0)?.cleanup_before(&version.1);
        }
        let data = self.get_channel(&version.0)?.remove(&version.1);

        Ok(data)
    }

    fn get(
        &mut self,
        version: &PacketBufferAddress,
    ) -> Result<Option<&UntypedPacket>, BufferError> {
        Ok(self.get_channel(&version.0)?.get(&version.1))
    }

    fn available_channels(&self) -> Vec<ChannelID> {
        self.data.keys().cloned().collect()
    }

    fn create_channel(&mut self, channel: &ChannelID) -> Result<ChannelID, BufferError> {
        if self.data.contains_key(channel) {
            return Err(BufferError::DuplicateChannelError(channel.clone()));
        }
        self.get_or_create_channel(channel);
        Ok(channel.clone())
    }
}

impl OrderedBuffer for CircularBufferedData {
    fn has_version(&self, channel: &ChannelID, version: &DataVersion) -> bool {
        self.data.contains_key(channel) && self.data.get(channel).unwrap().contains_key(version)
    }
}

// #[cfg(test)]
// mod circular_buffer_tests {
//     use super::*;
//     use crate::channels::Packet;
//     use crate::packet::UntypedPacketCast;
//     use std::cmp;

//     #[test]
//     fn test_buffer_inserts_and_drops_data_if_past_capacity() {
//         let max_size = 20;
//         let mut buffer = RingBuffer::new(max_size);
//         for i in 0..(max_size + 10) as u64 {
//             let version = DataVersion { timestamp: i };
//             let packet = Packet::<String>::new("test".to_string(), version.clone());
//             buffer.insert(version, packet.to_untyped());
//             assert_eq!(
//                 buffer.len(),
//                 cmp::min(max_size, usize::try_from(i + 1).unwrap())
//             );
//         }
//     }

//     #[test]
//     fn test_buffer_contains_key_returns_expected() {
//         let mut buffer = RingBuffer::new(2);
//         for i in 0..3 {
//             let version = DataVersion { timestamp: i };
//             let packet = Packet::<String>::new("test".to_string(), version.clone());
//             buffer.insert(version, packet.to_untyped());
//             assert!(buffer.contains_key(&DataVersion { timestamp: i }));
//         }
//         assert!(!buffer.contains_key(&DataVersion { timestamp: 0 }));
//     }

//     #[test]
//     fn test_buffer_get_returns_expected_data() {
//         let mut buffer = RingBuffer::new(2);
//         for i in 0..3 {
//             let version = DataVersion { timestamp: i };
//             let packet = Packet::<String>::new(format!("test {}", i).to_string(), version.clone());
//             buffer.insert(version, packet.to_untyped());
//             let untyped_data = buffer.get(&DataVersion { timestamp: i }).unwrap();
//             let data = untyped_data.deref::<String>().unwrap();
//             assert_eq!(*data.data, format!("test {}", i).to_string());
//         }
//     }

//     #[test]
//     fn test_buffer_get_consumes_data_and_removes_from_buffer() {
//         let mut buffer = RingBuffer::new(2);
//         for i in 0..3 {
//             let version = DataVersion { timestamp: i };
//             let packet = Packet::<String>::new(format!("test {}", i).to_string(), version.clone());
//             buffer.insert(version, packet.to_untyped());
//             let untyped_data = buffer.remove(&DataVersion { timestamp: i }).unwrap();
//             let data = untyped_data.deref::<String>().unwrap();
//             assert_eq!(*data.data, format!("test {}", i).to_string());
//             assert!(!buffer.contains_key(&version));
//         }
//         assert_eq!(buffer.len(), 0);
//     }
// }

#[cfg(test)]
mod circular_buffer_data_tests {
    use super::*;
    use crate::channels::Packet;
    use crate::packet::UntypedPacketCast;
    use rand::seq::SliceRandom;

    #[test]
    fn test_buffer_errors_if_inserts_on_missing_channel() {
        let max_size = 20;
        let mut buffer = CircularBufferedData::new(max_size, false);

        let channel_0 = ChannelID {
            id: "ch0".to_string(),
        };
        let channel_1 = ChannelID {
            id: "ch1".to_string(),
        };
        buffer.create_channel(&channel_1).unwrap();
        let version = DataVersion { timestamp: 1 };

        let packet = Packet::<String>::new("test".to_string(), version.clone());
        assert!(buffer.insert(&channel_0, packet.to_untyped()).is_err())
    }

    #[test]
    fn test_buffer_throws_if_same_channel_created() {
        let max_size = 20;
        let mut buffer = CircularBufferedData::new(max_size, false);

        let channel_0 = ChannelID {
            id: "ch0".to_string(),
        };
        assert!(buffer.create_channel(&channel_0).is_ok());
        assert!(buffer.create_channel(&channel_0).is_err());
    }

    #[test]
    fn test_buffer_inserts_returns_data_and_gets_retained() {
        let max_size = 20;
        let mut buffer = CircularBufferedData::new(max_size, false);

        let channel_0 = ChannelID {
            id: "ch0".to_string(),
        };
        let channel_1 = ChannelID {
            id: "ch1".to_string(),
        };
        buffer.create_channel(&channel_0).unwrap();
        buffer.create_channel(&channel_1).unwrap();
        let version = DataVersion { timestamp: 1 };

        let packet = Packet::<String>::new("test".to_string(), version.clone());
        buffer.insert(&channel_0, packet.to_untyped()).unwrap();
        for _i in 0..2 {
            let untyped = buffer
                .get(&(channel_0.clone(), version.clone()))
                .unwrap()
                .unwrap();
            let data = untyped.deref::<String>().unwrap();
            assert_eq!(*data.data, "test");
        }

        assert!(buffer
            .get(&(channel_1.clone(), version.clone()))
            .unwrap()
            .is_none())
    }

    #[test]
    fn test_buffer_insert_random_order_then_removes_old_data_once_consumed() {
        let max_size = 100;
        let mut buffer = CircularBufferedData::new(max_size, true);

        let channel_0 = ChannelID {
            id: "ch0".to_string(),
        };
        let channel_1 = ChannelID {
            id: "ch1".to_string(),
        };
        buffer.create_channel(&channel_0).unwrap();
        buffer.create_channel(&channel_1).unwrap();

        let vals: Vec<u64> = (0..100).collect();

        for i in vals {
            let version = DataVersion { timestamp: i };
            let packet = Packet::<String>::new("test_0".to_string(), version.clone());
            buffer.insert(&channel_0, packet.to_untyped()).unwrap();
            let packet = Packet::<String>::new("test_1".to_string(), version.clone());
            buffer.insert(&channel_1, packet.to_untyped()).unwrap();
        }
        let version = DataVersion { timestamp: 10 };
        let address = (channel_0.clone(), version.clone());
        let _ = buffer.consume(&address).unwrap().unwrap();

        for old_version in 0..9 {
            let version = DataVersion {
                timestamp: old_version,
            };
            let address = (channel_0.clone(), version.clone());
            assert!(
                buffer.get(&address).unwrap().is_none(),
                "Found {}",
                old_version
            )
        }
    }
}
