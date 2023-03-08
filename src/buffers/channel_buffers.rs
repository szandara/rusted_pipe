use super::BufferError;
use super::BufferIterator;
use super::DataBuffer;
use super::OrderedBuffer;
use crate::buffers::single_buffers::FixedSizeBuffer;
use crate::packet::ChannelID;
use crate::packet::DataVersion;
use crate::packet::UntypedPacket;

use super::PacketBufferAddress;
use itertools::Itertools;
use std::collections::HashMap;

pub struct BoundedBufferedData<T: FixedSizeBuffer> {
    data: HashMap<ChannelID, T>,
    max_size: usize,
    block_full: bool,
}

impl<T: FixedSizeBuffer> BoundedBufferedData<T> {
    pub fn new(max_size: usize, block_full: bool) -> Self {
        BoundedBufferedData {
            data: HashMap::<ChannelID, T>::default(),
            max_size,
            block_full,
        }
    }

    fn get_channel(&mut self, channel: &ChannelID) -> Result<&mut T, BufferError> {
        Ok(self
            .data
            .get_mut(channel)
            .ok_or(BufferError::InternalError(format!(
                "Cannod find channel {}",
                channel.id
            )))?)
    }

    fn get_or_create_channel(&mut self, channel: &ChannelID) -> &mut T {
        self.data
            .entry(channel.clone())
            .or_insert(T::new(self.max_size, self.block_full))
    }
}

impl<T: FixedSizeBuffer> DataBuffer for BoundedBufferedData<T> {
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

        log::debug!(
            "Adding version {:?} to channle {:?}, size {:?}",
            packet.version,
            channel,
            buffer.len()
        );

        let data_version = (channel.clone(), packet.version.clone());
        buffer.insert(packet.version.clone(), packet)?;
        Ok(data_version)
    }

    fn get(
        &mut self,
        version: &PacketBufferAddress,
    ) -> Result<Option<&UntypedPacket>, BufferError> {
        Ok(self.get_channel(&version.0)?.get(&version.1))
    }

    fn available_channels(&self) -> Vec<ChannelID> {
        self.data
            .keys()
            .into_iter()
            .map(|key| key.clone())
            .collect_vec()
    }

    fn create_channel(&mut self, channel: &ChannelID) -> Result<ChannelID, BufferError> {
        if self.data.contains_key(channel) {
            return Err(BufferError::DuplicateChannelError(channel.clone()));
        }
        self.get_or_create_channel(channel);
        Ok(channel.clone())
    }
}

impl<T: FixedSizeBuffer> OrderedBuffer for BoundedBufferedData<T> {
    fn has_version(&self, channel: &ChannelID, version: &DataVersion) -> bool {
        match self.data.get(channel) {
            Some(channel) => channel.contains_key(version),
            _ => false,
        }
    }

    fn peek(&self, channel: &ChannelID) -> Option<&DataVersion> {
        match self.data.get(channel) {
            Some(channel) => channel.peek(),
            _ => None,
        }
    }

    fn pop(&mut self, channel_id: &ChannelID) -> Result<Option<UntypedPacket>, BufferError> {
        let channel = (self.get_channel(channel_id))?;
        return Ok(channel.pop());
    }

    fn are_buffers_empty(&self) -> bool {
        for channel in self.data.iter() {
            if channel.1.len() > 0 {
                return false;
            }
        }
        return true;
    }

    fn iterator(&self, channel: &ChannelID) -> Option<Box<BufferIterator>> {
        match self.data.get(channel) {
            Some(channel) => Some(channel.iter()),
            _ => None,
        }
    }
}

#[cfg(test)]
mod btree_buffer_tests {
    use super::*;
    use crate::buffers::single_buffers::{FixedSizeBTree, RtRingBuffer};
    use crate::channels::Packet;
    use crate::packet::UntypedPacketCast;

    macro_rules! param_test {
        ($($type:ident)*) => {
        $(
            paste::item! {
                #[test]
                fn [< test_buffer_errors_if_inserts_on_missing_channel _ $type >] () {
                    test_buffer_errors_if_inserts_on_missing_channel::<$type>();
                }

                #[test]
                fn [< test_buffer_throws_if_same_channel_created _ $type >] () {
                    test_buffer_throws_if_same_channel_created::<$type>();
                }

                #[test]
                fn [< test_buffer_inserts_returns_data_and_gets_retained _ $type >] () {
                    test_buffer_inserts_returns_data_and_gets_retained::<$type>();
                }
            }
        )*
        }
    }

    fn test_buffer_errors_if_inserts_on_missing_channel<T: FixedSizeBuffer>() {
        let max_size = 20;
        let mut buffer = BoundedBufferedData::<T>::new(max_size, false);

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

    fn test_buffer_throws_if_same_channel_created<T: FixedSizeBuffer>() {
        let max_size = 20;
        let mut buffer = BoundedBufferedData::<T>::new(max_size, false);

        let channel_0 = ChannelID {
            id: "ch0".to_string(),
        };
        assert!(buffer.create_channel(&channel_0).is_ok());
        assert!(buffer.create_channel(&channel_0).is_err());
    }

    fn test_buffer_inserts_returns_data_and_gets_retained<T: FixedSizeBuffer>() {
        let max_size = 20;
        let mut buffer = BoundedBufferedData::<T>::new(max_size, false);

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

    param_test!(FixedSizeBTree);
    param_test!(RtRingBuffer);
}
