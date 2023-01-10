use crate::channels::UntypedPacket;
use crate::DataVersion;
use ringbuffer::{AllocRingBuffer, RingBuffer, RingBufferExt, RingBufferWrite};

type _RingBuffer = AllocRingBuffer<Option<UntypedPacket>>;

pub trait FixedSizeBuffer {
    fn new(max_size: usize) -> Self;

    fn contains_key(&self, version: &DataVersion) -> bool;

    fn get(&self, version: &DataVersion) -> Option<&UntypedPacket>;

    fn remove(&mut self, version: &DataVersion) -> Option<UntypedPacket>;

    fn insert(&mut self, version: DataVersion, packet: UntypedPacket);

    fn len(&self) -> usize;

    fn peek(&self) -> Option<&DataVersion>;
}

pub struct RtRingBuffer {
    buffer: _RingBuffer,
}

impl RtRingBuffer {
    pub fn find_version(&self, version: &DataVersion) -> Option<&UntypedPacket> {
        if let Some(maybe_found) = self.buffer.iter().find(|packet| {
            packet
                .as_ref()
                .is_some_and(|packet| packet.version == *version)
        }) {
            return maybe_found.as_ref();
        }
        None
    }

    // pub fn take_version(&mut self, version: &DataVersion) -> Option<UntypedPacket> {
    //     if let Some(maybe_found) = self.consumer.iter_mut().find(|packet| {
    //         packet
    //             .as_ref()
    //             .is_some_and(|packet| packet.version == *version)
    //     }) {
    //         return maybe_found.take();
    //     }
    //     None
    // }
}

impl FixedSizeBuffer for RtRingBuffer {
    fn new(mut max_size: usize) -> Self {
        if !max_size.is_power_of_two() {
            println!("Pre {}", max_size);
            max_size = (2 as usize).pow(max_size.ilog2() / (2 as usize).ilog2());
        }
        return RtRingBuffer {
            buffer: _RingBuffer::with_capacity(max_size),
        };
    }

    fn contains_key(&self, version: &DataVersion) -> bool {
        self.find_version(version).is_some()
    }

    fn get(&self, version: &DataVersion) -> Option<&UntypedPacket> {
        self.find_version(version)
    }

    fn remove(&mut self, version: &DataVersion) -> Option<UntypedPacket> {
        //self.take_version(version)
        None
    }

    fn insert(&mut self, _version: DataVersion, packet: UntypedPacket) {
        self.buffer.push(Some(packet));
    }

    fn len(&self) -> usize {
        0
    }

    fn peek(&self) -> Option<&DataVersion> {
        if let Some(peek) = self.buffer.peek() {
            if let Some(data) = peek.as_ref() {
                return Some(&data.version);
            }
        }
        None
    }
}

use std::collections::BTreeMap;

pub struct FixedSizeBTree {
    data: BTreeMap<DataVersion, UntypedPacket>,
    max_size: usize,
}

impl FixedSizeBTree {
    pub fn default() -> Self {
        FixedSizeBTree {
            data: Default::default(),
            max_size: 1000,
        }
    }
}

impl FixedSizeBuffer for FixedSizeBTree {
    fn new(max_size: usize) -> Self {
        FixedSizeBTree {
            data: Default::default(),
            max_size: max_size,
        }
    }
    fn contains_key(&self, version: &DataVersion) -> bool {
        self.data.contains_key(version)
    }

    fn get(&self, version: &DataVersion) -> Option<&UntypedPacket> {
        self.data.get(version)
    }

    fn remove(&mut self, version: &DataVersion) -> Option<UntypedPacket> {
        println!("Removing {:?}", version);
        self.data.remove(version)
    }

    fn insert(&mut self, version: DataVersion, packet: UntypedPacket) {
        println!("Inserting {:?}", version);
        self.data.insert(version, packet);
        while self.data.len() > self.max_size {
            let last_entry = self.data.first_entry().unwrap().key().clone();
            self.data.remove(&last_entry);
            println!("BTree Dropped");
        }
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
}

#[cfg(test)]
mod fixed_size_buffer_tests {
    use super::*;
    use crate::channels::Packet;
    use crate::packet::UntypedPacketCast;
    use std::cmp;

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
                fn [< test_buffer_get_consumes_data_and_removes_from_buffer _ $type >] () {
                    test_buffer_get_consumes_data_and_removes_from_buffer::<$type>();
                }
            }
        )*
        }
    }

    fn test_buffer_inserts_and_drops_data_if_past_capacity<T: FixedSizeBuffer>() {
        let mut buffer = T::new(20);
        let max_size = 20;
        for i in 0..(max_size + 10) as u64 {
            let version = DataVersion { timestamp: i };
            let packet = Packet::<String>::new("test".to_string(), version.clone());
            buffer.insert(version, packet.to_untyped());
            assert_eq!(
                buffer.len(),
                cmp::min(max_size, usize::try_from(i + 1).unwrap())
            );
        }
    }

    fn test_buffer_contains_key_returns_expected<T: FixedSizeBuffer>() {
        let mut buffer = T::new(2);
        for i in 0..3 {
            let version = DataVersion { timestamp: i };
            let packet = Packet::<String>::new("test".to_string(), version.clone());
            buffer.insert(version, packet.to_untyped());
            assert!(buffer.contains_key(&DataVersion { timestamp: i }));
        }
        assert!(!buffer.contains_key(&DataVersion { timestamp: 0 }));
    }

    fn test_buffer_get_returns_expected_data<T: FixedSizeBuffer>() {
        let mut buffer = T::new(2);
        for i in 0..3 {
            let version = DataVersion { timestamp: i };
            let packet = Packet::<String>::new(format!("test {}", i).to_string(), version.clone());
            buffer.insert(version, packet.to_untyped());
            let untyped_data = buffer.get(&DataVersion { timestamp: i }).unwrap();
            let data = untyped_data.deref::<String>().unwrap();
            assert_eq!(*data.data, format!("test {}", i).to_string());
        }
    }

    fn test_buffer_get_consumes_data_and_removes_from_buffer<T: FixedSizeBuffer>() {
        let mut buffer = T::new(2);
        for i in 0..3 {
            let version = DataVersion { timestamp: i };
            let packet = Packet::<String>::new(format!("test {}", i).to_string(), version.clone());
            buffer.insert(version, packet.to_untyped());
            let untyped_data = buffer.remove(&DataVersion { timestamp: i }).unwrap();
            let data = untyped_data.deref::<String>().unwrap();
            assert_eq!(*data.data, format!("test {}", i).to_string());
            assert!(!buffer.contains_key(&version));
        }
    }
    param_test!(FixedSizeBTree);
    //param_test!(RtRingBuffer);
}
