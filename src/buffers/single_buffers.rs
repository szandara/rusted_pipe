use crate::{channels::Packet, DataVersion};
use ringbuffer::{AllocRingBuffer, RingBuffer, RingBufferExt, RingBufferRead, RingBufferWrite};

type _RingBuffer<T> = AllocRingBuffer<Packet<T>>;

pub trait FixedSizeBuffer {
    type Data: Clone;

    fn contains_key(&self, version: &DataVersion) -> bool;

    fn get(&self, version: &DataVersion) -> Option<&Packet<Self::Data>>;

    fn insert(&mut self, packet: Packet<Self::Data>) -> Result<(), BufferError>;

    fn len(&self) -> usize;

    fn peek(&self) -> Option<&DataVersion>;

    fn iter(&self) -> Box<BufferIterator<Self::Data>>;

    fn pop(&mut self) -> Option<Packet<Self::Data>>;
}

#[derive(Default)]
pub struct RtRingBuffer<T> {
    buffer: _RingBuffer<T>,
    block_full: bool,
}

impl<T> RtRingBuffer<T> {
    pub fn find_version(&self, version: &DataVersion) -> Option<&Packet<T>> {
        self.buffer.iter().find(|packet| packet.version == *version)
    }

    pub fn new(mut max_size: usize, block_full: bool) -> Self {
        if !max_size.is_power_of_two() {
            max_size = (2 as usize).pow(max_size.ilog2() / (2 as usize).ilog2() + 1);
        }
        return RtRingBuffer {
            buffer: _RingBuffer::with_capacity(max_size),
            block_full,
        };
    }
}

impl<T: Clone> FixedSizeBuffer for RtRingBuffer<T> {
    type Data = T;

    fn contains_key(&self, version: &DataVersion) -> bool {
        self.find_version(version).is_some()
    }

    fn get(&self, version: &DataVersion) -> Option<&Packet<T>> {
        self.find_version(version)
    }

    fn insert(&mut self, packet: Packet<T>) -> Result<(), BufferError> {
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

    fn pop(&mut self) -> Option<Packet<T>> {
        self.buffer.dequeue()
    }

    fn iter(&self) -> Box<BufferIterator<T>> {
        Box::new(self.buffer.iter()) as Box<BufferIterator<T>>
    }
}

use std::collections::BTreeMap;

use super::{BufferError, BufferIterator};

pub struct FixedSizeBTree<T> {
    data: BTreeMap<DataVersion, Packet<T>>,
    max_size: usize,
    block_full: bool,
}

impl<T> FixedSizeBTree<T> {
    pub fn default() -> Self {
        FixedSizeBTree {
            data: Default::default(),
            max_size: 1000,
            block_full: false,
        }
    }

    pub fn new(max_size: usize, block_full: bool) -> Self {
        FixedSizeBTree {
            data: Default::default(),
            max_size,
            block_full,
        }
    }
}

impl<T: Clone> FixedSizeBuffer for FixedSizeBTree<T> {
    type Data = T;

    fn contains_key(&self, version: &DataVersion) -> bool {
        self.data.contains_key(version)
    }

    fn get(&self, version: &DataVersion) -> Option<&Packet<T>> {
        self.data.get(version)
    }

    fn insert(&mut self, packet: Packet<T>) -> Result<(), BufferError> {
        while self.data.len() >= self.max_size {
            if self.block_full {
                return Err(BufferError::BufferFull);
            }
            self.data.pop_first();
        }
        self.data.insert(packet.version.clone(), packet);
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

    fn pop(&mut self) -> Option<Packet<T>> {
        if let Some(value) = self.data.pop_first() {
            return Some(value.1);
        }
        None
    }

    fn iter(&self) -> Box<BufferIterator<T>> {
        Box::new(self.data.values().clone()) as Box<BufferIterator<T>>
    }
}

#[cfg(test)]
mod fixed_size_buffer_tests {
    use super::*;
    use crate::channels::Packet;

    macro_rules! param_test {
        ($($type:ident)*) => {
        $(
            paste::item! {
                #[test]
                fn [< test_buffer_inserts_and_drops_data_if_past_capacity _ $type >] () {
                    let mut buffer = $type::new(32, false);
                    test_buffer_inserts_and_drops_data_if_past_capacity::<$type<String>>(buffer);
                }
                #[test]
                fn [< test_buffer_contains_key_returns_expected _ $type >] () {
                    let mut buffer = $type::new(2, false);
                    test_buffer_contains_key_returns_expected::<$type<String>>(buffer);
                }
                #[test]
                fn [< test_buffer_get_returns_expected_data _ $type >] () {
                    let mut buffer = $type::new(2, false);
                    test_buffer_get_returns_expected_data::<$type<String>>(buffer);
                }
                #[test]
                fn [< test_buffer_insert_returns_errr_if_full_and_block _ $type >] () {
                    let mut buffer = $type::new(2, true);
                    test_buffer_insert_returns_errr_if_full_and_block::<$type<String>>(buffer);
                }
            }
        )*
        }
    }

    fn test_buffer_inserts_and_drops_data_if_past_capacity<T: FixedSizeBuffer<Data = String>>(
        mut buffer: T,
    ) {
        let max_size = 32;
        for i in 0..(max_size + 10) as u128 {
            let version = DataVersion { timestamp: i };
            let packet = Packet::<String>::new("test".to_string(), version.clone());
            buffer.insert(packet).unwrap();
            if i >= max_size as u128 {
                assert_eq!(buffer.peek().unwrap().timestamp, (i - max_size as u128) + 1);
            } else {
                assert_eq!(buffer.peek().unwrap().timestamp, 0);
            }
        }
    }

    fn test_buffer_contains_key_returns_expected<T: FixedSizeBuffer<Data = String>>(mut buffer: T) {
        for i in 0..3 {
            let version = DataVersion { timestamp: i };
            let packet = Packet::<String>::new("test".to_string(), version.clone());
            buffer.insert(packet).unwrap();
            assert!(buffer.contains_key(&DataVersion { timestamp: i }));
        }
        assert!(!buffer.contains_key(&DataVersion { timestamp: 0 }));
    }

    fn test_buffer_get_returns_expected_data<T: FixedSizeBuffer<Data = String>>(mut buffer: T) {
        for i in 0..3 {
            let version = DataVersion { timestamp: i };
            let packet = Packet::<String>::new(format!("test {}", i).to_string(), version.clone());
            buffer.insert(packet).unwrap();
            let data = buffer.get(&DataVersion { timestamp: i }).unwrap();
            assert_eq!(*data.data, format!("test {}", i).to_string());
        }
    }

    fn test_buffer_insert_returns_errr_if_full_and_block<T: FixedSizeBuffer<Data = String>>(
        mut buffer: T,
    ) {
        for i in 0..3 {
            let version = DataVersion { timestamp: i };
            let packet = Packet::<String>::new(format!("test {}", i).to_string(), version.clone());
            if i == 2 {
                assert_eq!(buffer.insert(packet).unwrap_err(), BufferError::BufferFull);
            } else {
                buffer.insert(packet).unwrap();
            }
        }
    }

    param_test!(FixedSizeBTree);
    param_test!(RtRingBuffer);
}
