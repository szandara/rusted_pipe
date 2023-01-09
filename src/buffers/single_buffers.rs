use crate::channels::UntypedPacket;
use crate::DataVersion;

use super::DataBuffer;

use ringbuf::consumer::Consumer;
use ringbuf::producer::Producer;
use std::sync::Arc;

use ringbuf::HeapRb;

type _RingBuffer = HeapRb<Option<UntypedPacket>>;

pub trait FixedSizeBuffer {
    fn contains_key(&self, version: &DataVersion) -> bool;

    fn get(&self, version: &DataVersion) -> Option<&UntypedPacket>;

    fn remove(&mut self, version: &DataVersion) -> Option<UntypedPacket>;

    fn insert(&mut self, version: DataVersion, packet: UntypedPacket);

    fn len(&self) -> usize;

    fn cleanup_before(&mut self, version: &DataVersion);
}

pub struct RingBuffer {
    consumer: Consumer<Option<UntypedPacket>, Arc<_RingBuffer>>,
    producer: Producer<Option<UntypedPacket>, Arc<_RingBuffer>>,
}

impl RingBuffer {
    pub fn new(max_size: usize) -> Self {
        let buffer = _RingBuffer::new(max_size);
        let (producer, consumer) = buffer.split();
        let partial = RingBuffer { producer, consumer };
        return partial;
    }

    pub fn find_index(&self, version: &DataVersion) -> Option<usize> {
        for (i, packet) in self.consumer.iter().enumerate() {
            if packet
                .as_ref()
                .is_some_and(|packet| packet.version == *version)
            {
                return Some(i);
            }
        }
        None
    }

    pub fn find_version(&self, version: &DataVersion) -> Option<&UntypedPacket> {
        if let Some(maybe_found) = self.consumer.iter().find(|packet| {
            packet
                .as_ref()
                .is_some_and(|packet| packet.version == *version)
        }) {
            return maybe_found.as_ref();
        }
        None
    }

    pub fn take_version(&mut self, version: &DataVersion) -> Option<UntypedPacket> {
        println!("Search for {}", version.timestamp);
        if let Some(maybe_found) = self.consumer.iter_mut().find(|packet| {
            packet
                .as_ref()
                .is_some_and(|packet| packet.version == *version)
        }) {
            return maybe_found.take();
        }
        None
    }
}

impl FixedSizeBuffer for RingBuffer {
    fn contains_key(&self, version: &DataVersion) -> bool {
        self.find_version(version).is_some()
    }

    fn get(&self, version: &DataVersion) -> Option<&UntypedPacket> {
        self.find_version(version)
    }

    fn remove(&mut self, version: &DataVersion) -> Option<UntypedPacket> {
        self.take_version(version)
    }

    fn insert(&mut self, _version: DataVersion, packet: UntypedPacket) {
        if self.producer.is_full() {
            self.consumer.pop();
        }
        self.producer.push(Some(packet)).unwrap();
    }

    fn len(&self) -> usize {
        self.consumer.len()
    }

    fn cleanup_before(&mut self, version: &DataVersion) {
        if let Some(index) = self.find_index(version) {
            self.consumer.skip(index);
        }
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

    pub fn new(max_size: usize) -> Self {
        FixedSizeBTree {
            data: Default::default(),
            max_size: max_size,
        }
    }
}

impl FixedSizeBuffer for FixedSizeBTree {
    fn contains_key(&self, version: &DataVersion) -> bool {
        self.data.contains_key(version)
    }

    fn get(&self, version: &DataVersion) -> Option<&UntypedPacket> {
        self.data.get(version)
    }

    fn remove(&mut self, version: &DataVersion) -> Option<UntypedPacket> {
        self.data.remove(version)
    }

    fn insert(&mut self, version: DataVersion, packet: UntypedPacket) {
        self.data.insert(version, packet);
        while self.data.len() > self.max_size {
            let last_entry = self.data.first_entry().unwrap().key().clone();
            self.data.remove(&last_entry);
        }
    }

    fn len(&self) -> usize {
        return self.data.len();
    }

    fn cleanup_before(&mut self, version: &DataVersion) {
        self.data = self.data.split_off(&version);
    }
}

#[cfg(test)]
mod fixed_size_buffer_tests {
    use super::*;
    use crate::channels::Packet;
    use crate::packet::UntypedPacketCast;
    use std::cmp;

    macro_rules! param_test {
        ($($name:ident: ($type:ident, $value:expr))*) => {
        $(
            paste::item! {
                #[test]
                fn [< $name _ $type >] () {
                    $name(Box::new($value));
                }
            }
        )*
        }
    }

    fn test_buffer_inserts_and_drops_data_if_past_capacity(mut buffer: Box<dyn FixedSizeBuffer>) {
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

    fn test_buffer_contains_key_returns_expected(mut buffer: Box<dyn FixedSizeBuffer>) {
        for i in 0..3 {
            let version = DataVersion { timestamp: i };
            let packet = Packet::<String>::new("test".to_string(), version.clone());
            buffer.insert(version, packet.to_untyped());
            assert!(buffer.contains_key(&DataVersion { timestamp: i }));
        }
        assert!(!buffer.contains_key(&DataVersion { timestamp: 0 }));
    }

    fn test_buffer_get_returns_expected_data(mut buffer: Box<dyn FixedSizeBuffer>) {
        for i in 0..3 {
            let version = DataVersion { timestamp: i };
            let packet = Packet::<String>::new(format!("test {}", i).to_string(), version.clone());
            buffer.insert(version, packet.to_untyped());
            let untyped_data = buffer.get(&DataVersion { timestamp: i }).unwrap();
            let data = untyped_data.deref::<String>().unwrap();
            assert_eq!(*data.data, format!("test {}", i).to_string());
        }
    }

    fn test_buffer_get_consumes_data_and_removes_from_buffer(mut buffer: Box<dyn FixedSizeBuffer>) {
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

    param_test!(test_buffer_get_consumes_data_and_removes_from_buffer: (RingBuffer, RingBuffer::new(2)));
    param_test!(test_buffer_get_returns_expected_data: (RingBuffer, RingBuffer::new(2)));
    param_test!(test_buffer_contains_key_returns_expected: (RingBuffer, RingBuffer::new(2)));
    param_test!(test_buffer_inserts_and_drops_data_if_past_capacity:  (RingBuffer, RingBuffer::new(20)));

    param_test!(test_buffer_get_consumes_data_and_removes_from_buffer: (FixedSizeBTree, FixedSizeBTree::new(2)));
    param_test!(test_buffer_get_returns_expected_data: (FixedSizeBTree, FixedSizeBTree::new(2)));
    param_test!(test_buffer_contains_key_returns_expected: (FixedSizeBTree, FixedSizeBTree::new(2)));
    param_test!(test_buffer_inserts_and_drops_data_if_past_capacity:  (FixedSizeBTree, FixedSizeBTree::new(20)));
}
