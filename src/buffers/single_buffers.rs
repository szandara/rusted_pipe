use crate::channels::UntypedPacket;
use crate::DataVersion;
use ringbuffer::{AllocRingBuffer, RingBuffer, RingBufferExt, RingBufferRead, RingBufferWrite};

type _RingBuffer = AllocRingBuffer<UntypedPacket>;

pub trait FixedSizeBuffer {
    fn new(max_size: usize, block_full: bool) -> Self;

    fn contains_key(&self, version: &DataVersion) -> bool;

    fn get(&self, version: &DataVersion) -> Option<&UntypedPacket>;

    fn insert(&mut self, version: DataVersion, packet: UntypedPacket) -> Result<(), BufferError>;

    fn len(&self) -> usize;

    fn peek(&self) -> Option<&DataVersion>;

    fn pop(&mut self) -> Option<UntypedPacket>;
}

pub struct RtRingBuffer {
    buffer: _RingBuffer,
    block_full: bool,
}

impl RtRingBuffer {
    pub fn find_version(&self, version: &DataVersion) -> Option<&UntypedPacket> {
        self.buffer.iter().find(|packet| packet.version == *version)
    }
}

impl FixedSizeBuffer for RtRingBuffer {
    fn new(mut max_size: usize, block_full: bool) -> Self {
        if !max_size.is_power_of_two() {
            max_size = (2 as usize).pow(max_size.ilog2() / (2 as usize).ilog2() + 1);
        }
        return RtRingBuffer {
            buffer: _RingBuffer::with_capacity(max_size),
            block_full,
        };
    }

    fn contains_key(&self, version: &DataVersion) -> bool {
        self.find_version(version).is_some()
    }

    fn get(&self, version: &DataVersion) -> Option<&UntypedPacket> {
        self.find_version(version)
    }

    fn insert(&mut self, _version: DataVersion, packet: UntypedPacket) -> Result<(), BufferError> {
        if self.block_full && self.buffer.is_full() {
            return Err(BufferError::BufferFull);
        }
        self.buffer.push(packet);
        Ok(())
    }

    fn len(&self) -> usize {
        self.buffer.len()
    }

    fn peek(&self) -> Option<&DataVersion> {
        if let Some(peek) = self.buffer.peek() {
            return Some(&peek.version);
        }
        None
    }

    fn pop(&mut self) -> Option<UntypedPacket> {
        self.buffer.dequeue()
    }
}

use std::collections::BTreeMap;

use super::BufferError;

pub struct FixedSizeBTree {
    data: BTreeMap<DataVersion, UntypedPacket>,
    max_size: usize,
    block_full: bool,
}

impl FixedSizeBTree {
    pub fn default() -> Self {
        FixedSizeBTree {
            data: Default::default(),
            max_size: 1000,
            block_full: false,
        }
    }
}

impl FixedSizeBuffer for FixedSizeBTree {
    fn new(max_size: usize, block_full: bool) -> Self {
        FixedSizeBTree {
            data: Default::default(),
            max_size,
            block_full,
        }
    }
    fn contains_key(&self, version: &DataVersion) -> bool {
        self.data.contains_key(version)
    }

    fn get(&self, version: &DataVersion) -> Option<&UntypedPacket> {
        self.data.get(version)
    }

    fn insert(&mut self, version: DataVersion, packet: UntypedPacket) -> Result<(), BufferError> {
        while self.data.len() >= self.max_size {
            if self.block_full {
                return Err(BufferError::BufferFull);
            }
            self.data.pop_first();
        }
        self.data.insert(version, packet);
        Ok(())
    }

    fn len(&self) -> usize {
        return self.data.len();
    }

    fn peek(&self) -> Option<&DataVersion> {
        match self.data.first_key_value() {
            Some(data) => Some(data.0),
            None => None,
        }
    }

    fn pop(&mut self) -> Option<UntypedPacket> {
        if let Some(value) = self.data.pop_first() {
            return Some(value.1);
        }
        None
    }
}

#[cfg(test)]
mod fixed_size_buffer_tests {
    use super::*;
    use crate::channels::Packet;
    use crate::packet::UntypedPacketCast;

    macro_rules! param_test {
        ($($type:ident)*) => {
        $(
            paste::item! {
                #[test]
                fn [< test_buffer_inserts_and_drops_data_if_past_capacity _ $type >] () {
                    test_buffer_inserts_and_drops_data_if_past_capacity::<$type>();
                }
                #[test]
                fn [< test_buffer_contains_key_returns_expected _ $type >] () {
                    test_buffer_contains_key_returns_expected::<$type>();
                }
                #[test]
                fn [< test_buffer_get_returns_expected_data _ $type >] () {
                    test_buffer_get_returns_expected_data::<$type>();
                }
                #[test]
                fn [< test_buffer_insert_returns_errr_if_full_and_block _ $type >] () {
                    test_buffer_insert_returns_errr_if_full_and_block::<$type>();
                }
            }
        )*
        }
    }

    fn test_buffer_inserts_and_drops_data_if_past_capacity<T: FixedSizeBuffer>() {
        let max_size = 32;
        let mut buffer = T::new(max_size, false);
        for i in 0..(max_size + 10) as u128 {
            let version = DataVersion { timestamp: i };
            let packet = Packet::<String>::new("test".to_string(), version.clone());
            buffer.insert(version, packet.to_untyped()).unwrap();
            println!("{}", i);
            if i >= max_size as u128 {
                assert_eq!(buffer.peek().unwrap().timestamp, (i - max_size as u128) + 1);
            } else {
                assert_eq!(buffer.peek().unwrap().timestamp, 0);
            }
        }
    }

    fn test_buffer_contains_key_returns_expected<T: FixedSizeBuffer>() {
        let mut buffer = T::new(2, false);
        for i in 0..3 {
            let version = DataVersion { timestamp: i };
            let packet = Packet::<String>::new("test".to_string(), version.clone());
            buffer.insert(version, packet.to_untyped()).unwrap();
            assert!(buffer.contains_key(&DataVersion { timestamp: i }));
        }
        assert!(!buffer.contains_key(&DataVersion { timestamp: 0 }));
    }

    fn test_buffer_get_returns_expected_data<T: FixedSizeBuffer>() {
        let mut buffer = T::new(2, false);
        for i in 0..3 {
            let version = DataVersion { timestamp: i };
            let packet = Packet::<String>::new(format!("test {}", i).to_string(), version.clone());
            buffer.insert(version, packet.to_untyped()).unwrap();
            let untyped_data = buffer.get(&DataVersion { timestamp: i }).unwrap();
            let data = untyped_data.deref::<String>().unwrap();
            assert_eq!(*data.data, format!("test {}", i).to_string());
        }
    }

    fn test_buffer_insert_returns_errr_if_full_and_block<T: FixedSizeBuffer>() {
        let mut buffer = T::new(2, true);
        for i in 0..3 {
            let version = DataVersion { timestamp: i };
            let packet = Packet::<String>::new(format!("test {}", i).to_string(), version.clone());
            if i == 2 {
                assert_eq!(
                    buffer.insert(version, packet.to_untyped()).unwrap_err(),
                    BufferError::BufferFull
                );
            } else {
                buffer.insert(version, packet.to_untyped()).unwrap();
            }
        }
    }

    param_test!(FixedSizeBTree);
    param_test!(RtRingBuffer);
}
