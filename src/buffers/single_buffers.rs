use crate::{channels::Packet, DataVersion, graph::metrics::BufferMonitor};
use ringbuffer::{AllocRingBuffer, RingBuffer, RingBufferExt, RingBufferRead, RingBufferWrite};

use std::{collections::BTreeMap};
use super::{BufferError, BufferIterator};


type _RingBuffer<T> = AllocRingBuffer<Packet<T>>;

pub trait LenTrait {
    /// Current length of the buffer.
    fn len(&self) -> usize;
}

/// Trait describing an input buffer which composes one of the channels of
/// a ReadChannel.
pub trait FixedSizeBuffer: LenTrait {
    /// The data type handled.
    type Data;

    /// True if it contains a query data version.
    ///
    /// * Arguments
    ///
    /// `version` - Version to match.
    ///
    /// * Returns
    /// True if the data is in the buffer.
    fn contains_key(&self, version: &DataVersion) -> bool;
    /// Get a reference to a data packet by version.
    ///
    /// * Arguments
    ///
    /// `version` - Version to find.
    ///
    /// * Returns
    /// Some packet data if found or None.
    fn get(&self, version: &DataVersion) -> Option<&Packet<Self::Data>>;
    /// Insert a packet into the buffer. The assumption is that
    /// no data with a version older than the latest entry is inserted.
    /// This is how it can guarantee that the buffer is ordered.
    ///
    /// * Arguments
    ///
    /// `packet` - Data enclosed in a packet.
    ///
    /// * Returns
    /// Ok if the data could be inserted or error in opposite case.
    fn insert(&mut self, packet: Packet<Self::Data>) -> Result<(), BufferError>;
    /// Peek the head of the buffer, oldest entry in the buffer.
    fn peek(&self) -> Option<&DataVersion>;
    /// Gets an iterator to the data.
    fn iter(&self) -> Box<BufferIterator>;
    /// Removes the head of the buffer, oldest entry in the buffer.
    fn pop(&mut self) -> Option<Packet<Self::Data>>;
    /// Checks if a timestamp would violate the data ordering.
    /// * Arguments
    ///
    /// `timestamp_ns` - Timestamp to test.
    ///
    /// * Returns
    /// Ok if the data could be inserted or error in the opposite case.
    fn check_order(&self, timestamp_ns: u128) -> Result<(), BufferError> {
        if let Some(p) = self.peek() {
            if timestamp_ns <= p.timestamp_ns {
                return Err(BufferError::OutOfOrder(timestamp_ns, p.timestamp_ns));
            }
        }
        Ok(())
    }
}

/// An implementation of 'FixedSizeBuffer' using a ring buffer.
#[derive(Default)]
pub struct RtRingBuffer<T> {
    buffer: _RingBuffer<T>,
    block_full: bool,
    monitor: BufferMonitor
}

impl<T> RtRingBuffer<T> {
    /// Creates a new instance.
    ///
    /// * Arguments
    ///
    /// `max_size` -  The max allowed size in the buffer.
    /// `block_full` -  Block if full, it would return an error when inserting, if false,
    /// it will drop oldest data.
    /// `monitor` - True if the buffer should collect metrics.
    pub fn new(mut max_size: usize, block_full: bool, monitor: BufferMonitor) -> Self {
        if !max_size.is_power_of_two() {
            max_size = 2_usize.pow(max_size.ilog2() / 2_usize.ilog2() + 1);
        }
        RtRingBuffer {
            buffer: _RingBuffer::with_capacity(max_size),
            block_full,
            monitor
        }
    }

    pub fn find_version(&self, version: &DataVersion) -> Option<&Packet<T>> {
        self.buffer.iter().find(|packet| packet.version == *version)
    }
}

impl<T> LenTrait for RtRingBuffer<T> {
    fn len(&self) -> usize {
        self.buffer.len()
    }
}

impl<T> FixedSizeBuffer for RtRingBuffer<T> {
    type Data = T;

    fn contains_key(&self, version: &DataVersion) -> bool {
        self.find_version(version).is_some()
    }

    fn get(&self, version: &DataVersion) -> Option<&Packet<T>> {
        self.find_version(version)
    }

    fn insert(&mut self, packet: Packet<T>) -> Result<(), BufferError> {
        self.check_order(packet.version.timestamp_ns)?;
        if self.block_full && self.buffer.is_full() {
            return Err(BufferError::BufferFull);
        }
        self.monitor.inc();
        self.buffer.push(packet);
        Ok(())
    }

    fn peek(&self) -> Option<&DataVersion> {
        if let Some(peek) = self.buffer.peek() {
            return Some(&peek.version);
        }
        None
    }

    fn pop(&mut self) -> Option<Packet<T>> {
        let packet = self.buffer.dequeue();
        if packet.is_some() {
            self.monitor.dec();
        }
        packet
    }

    fn iter(&self) -> Box<BufferIterator> {
        Box::new(self.buffer.iter().map(|p| &p.version)) as Box<BufferIterator>
    }
}


/// An implementation of 'FixedSizeBuffer' using a BTree. The buffer
/// is indexed by data version and it's ordered.
pub struct FixedSizeBTree<T> {
    data: BTreeMap<DataVersion, Packet<T>>,
    max_size: usize,
    block_full: bool,
    monitor: BufferMonitor
}

impl<T> FixedSizeBTree<T> {
    /// Creates a new instance with a 1000 size and
    /// will drop if passed.
    pub fn default() -> Self {
        FixedSizeBTree {
            data: Default::default(),
            max_size: 1000,
            block_full: false,
            monitor: BufferMonitor::default()
        }
    }
    /// Creates a new instance.
    ///
    /// * Arguments
    ///
    /// `max_size` -  The max allowed size in the buffer.
    /// `block_full` -  Block if full, it would return an error when inserting, if false,
    /// it will drop oldest data.
    pub fn new(max_size: usize, block_full: bool, monitor: BufferMonitor) -> Self {
        FixedSizeBTree {
            data: Default::default(),
            max_size,
            block_full,
            monitor
        }
    }
}


impl<T> LenTrait for FixedSizeBTree<T> {

    fn len(&self) -> usize {
        self.data.len()
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
        self.check_order(packet.version.timestamp_ns)?;
        while self.data.len() >= self.max_size {
            if self.block_full {
                return Err(BufferError::BufferFull);
            }
            self.data.pop_first();
        }
        self.data.insert(packet.version, packet);
        self.monitor.inc();
        Ok(())
    }

    fn peek(&self) -> Option<&DataVersion> {
        match self.data.first_key_value() {
            Some(data) => Some(data.0),
            None => None,
        }
    }

    fn pop(&mut self) -> Option<Packet<T>> {
        if let Some(value) = self.data.pop_first() {
            self.monitor.dec();
            return Some(value.1);
        }
        None
    }

    fn iter(&self) -> Box<BufferIterator> {
        Box::new(self.data.values().map(|p| &p.version)) as Box<BufferIterator>
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
                #[allow(non_snake_case)]
                fn [< test_buffer_inserts_and_drops_data_if_past_capacity _ $type >] () {
                    let buffer = $type::new(32, false, BufferMonitor::default());
                    test_buffer_inserts_and_drops_data_if_past_capacity::<$type<String>>(buffer);
                }
                #[test]
                #[allow(non_snake_case)]
                fn [< test_buffer_contains_key_returns_expected _ $type >] () {
                    let buffer = $type::new(2, false, BufferMonitor::default());
                    test_buffer_contains_key_returns_expected::<$type<String>>(buffer);
                }
                #[test]
                #[allow(non_snake_case)]
                fn [< test_buffer_get_returns_expected_data _ $type >] () {
                    let buffer = $type::new(2, false, BufferMonitor::default());
                    test_buffer_get_returns_expected_data::<$type<String>>(buffer);
                }
                #[test]
                #[allow(non_snake_case)]
                fn [< test_buffer_insert_returns_errr_if_full_and_block _ $type >] () {
                    let buffer = $type::new(2, true, BufferMonitor::default());
                    test_buffer_insert_returns_errr_if_full_and_block::<$type<String>>(buffer);
                }
                #[test]
                #[allow(non_snake_case)]
                fn [< test_buffer_returns_error_if_data_out_of_order _ $type >] () {
                    let buffer = $type::new(3, true, BufferMonitor::default());
                    test_buffer_returns_error_if_data_out_of_order::<$type<String>>(buffer);
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
            let version = DataVersion { timestamp_ns: i };
            let packet = Packet::<String>::new("test".to_string(), version);
            buffer.insert(packet).unwrap();
            if i >= max_size as u128 {
                assert_eq!(
                    buffer.peek().unwrap().timestamp_ns,
                    (i - max_size as u128) + 1
                );
            } else {
                assert_eq!(buffer.peek().unwrap().timestamp_ns, 0);
            }
        }
    }

    fn test_buffer_contains_key_returns_expected<T: FixedSizeBuffer<Data = String>>(mut buffer: T) {
        for i in 0..3 {
            let version = DataVersion { timestamp_ns: i };
            let packet = Packet::<String>::new("test".to_string(), version);
            buffer.insert(packet).unwrap();
            assert!(buffer.contains_key(&DataVersion { timestamp_ns: i }));
        }
        assert!(!buffer.contains_key(&DataVersion { timestamp_ns: 0 }));
    }

    fn test_buffer_returns_error_if_data_out_of_order<T: FixedSizeBuffer<Data = String>>(
        mut buffer: T,
    ) {
        let version = DataVersion { timestamp_ns: 1 };
        let packet = Packet::<String>::new("test".to_string(), version);
        buffer.insert(packet).unwrap();
        assert!(buffer.contains_key(&DataVersion { timestamp_ns: 1 }));

        let version = DataVersion { timestamp_ns: 0 };
        let packet = Packet::<String>::new("test".to_string(), version);
        assert!(buffer.insert(packet).is_err());
    }

    fn test_buffer_get_returns_expected_data<T: FixedSizeBuffer<Data = String>>(mut buffer: T) {
        for i in 0..3 {
            let version = DataVersion { timestamp_ns: i };
            let packet = Packet::<String>::new(format!("test {}", i).to_string(), version);
            buffer.insert(packet).unwrap();
            let data = buffer.get(&DataVersion { timestamp_ns: i }).unwrap();
            assert_eq!(*data.data, format!("test {}", i).to_string());
        }
    }

    fn test_buffer_insert_returns_errr_if_full_and_block<T: FixedSizeBuffer<Data = String>>(
        mut buffer: T,
    ) {
        for i in 0..3 {
            let version = DataVersion { timestamp_ns: i };
            let packet = Packet::<String>::new(format!("test {}", i).to_string(), version);
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
