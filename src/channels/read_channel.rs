use super::typed_read_channel::BufferReceiver;
use super::typed_read_channel::ChannelBuffer;
use super::typed_read_channel::OutputDelivery;
use super::ChannelError;
use super::ChannelID;
use super::Packet;

use crate::buffers::single_buffers::FixedSizeBuffer;
use crate::buffers::single_buffers::RtRingBuffer;

use crate::packet::Untyped;
use crate::packet::UntypedPacket;

use crate::buffers::synchronizers::timestamp::TimestampSynchronizer;
use crate::buffers::synchronizers::PacketSynchronizer;

use crossbeam::channel::Select;
use itertools::Itertools;
use ringbuffer::RingBuffer;

use crate::packet::WorkQueue;
use indexmap::IndexMap;
use std::borrow::BorrowMut;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

unsafe impl Send for UntypedBufferReceiver {}

pub struct UntypedBufferReceiver {
    buffered_data: IndexMap<String, BufferReceiver<Untyped, RtRingBuffer<Untyped>>>,
    synch_strategy: Box<dyn PacketSynchronizer>,
    work_queue: Option<Arc<WorkQueue<UntypedPacket>>>,
    connected_channels: Vec<String>,
}

const MAX_BUFFER_PER_CHANNEL: usize = 2000;

impl UntypedBufferReceiver {
    pub fn default() -> Self {
        UntypedBufferReceiver {
            buffered_data: Default::default(),
            synch_strategy: Box::new(TimestampSynchronizer::default()),
            work_queue: None,
            connected_channels: vec![],
        }
    }

    pub fn new(
        buffered_data: IndexMap<String, BufferReceiver<Untyped, RtRingBuffer<Untyped>>>,
        synch_strategy: Box<dyn PacketSynchronizer>,
    ) -> Self {
        UntypedBufferReceiver {
            buffered_data,
            synch_strategy,
            work_queue: None,
            connected_channels: vec![],
        }
    }

    pub fn add_channel(
        &mut self,
        channel: String,
        receiver: BufferReceiver<Untyped, RtRingBuffer<Untyped>>,
    ) -> Result<String, ChannelError> {
        self.buffered_data.insert(channel.clone(), receiver);
        Ok(channel)
    }

    pub fn get_channel(
        &self,
        channel: &str,
    ) -> Option<&BufferReceiver<Untyped, RtRingBuffer<Untyped>>> {
        self.buffered_data.get(channel)
    }

    pub fn get_channel_mut(
        &mut self,
        channel: &str,
    ) -> Option<&mut BufferReceiver<Untyped, RtRingBuffer<Untyped>>> {
        self.buffered_data.get_mut(channel)
    }

    pub fn available_channels(&self) -> Vec<&String> {
        self.buffered_data.keys().collect_vec()
    }

    pub fn start(&mut self, work_queue: Arc<WorkQueue<UntypedPacket>>) {
        self.work_queue = Some(work_queue);
    }

    pub fn stop(&mut self) {}
}

impl<'a> ChannelBuffer for UntypedBufferReceiver {
    fn available_channels(&self) -> Vec<&str> {
        self.buffered_data
            .keys()
            .into_iter()
            .map(|key| key.as_str())
            .collect_vec()
    }

    fn has_version(&self, channel: &str, version: &crate::DataVersion) -> bool {
        if let Some(buffer) = self.get_channel(channel) {
            return buffer.buffer.find_version(version).is_some();
        }
        false
    }

    fn peek(&self, channel: &str) -> Option<&crate::DataVersion> {
        if let Some(buffer) = self.get_channel(channel) {
            return buffer.buffer.peek();
        }
        None
    }

    fn iterator(&self, channel: &str) -> Option<Box<crate::buffers::BufferIterator>> {
        if let Some(buffer) = self.get_channel(channel) {
            return Some(buffer.buffer.iter());
        }
        None
    }

    fn are_buffers_empty(&self) -> bool {
        self.buffered_data.values().all(|b| b.buffer.len() == 0)
    }

    fn try_receive(&mut self, timeout: std::time::Duration) -> Result<bool, ChannelError> {
        let mut select = Select::new();
        for (id, rec) in &self.buffered_data {
            if let Some(channel) = rec.channel.as_ref() {
                self.connected_channels.push(id.clone());
                select.recv(&channel.receiver);
            }
        }

        if let Ok(channel) = select.ready_timeout(timeout) {
            if let Some(ch) = self.connected_channels.get(channel) {
                let mut buffer = self.buffered_data.get_mut(&*ch);
                let msg = buffer
                    .as_ref()
                    .unwrap()
                    .channel
                    .as_ref()
                    .unwrap()
                    .receiver
                    .recv()?;

                buffer.as_mut().unwrap().buffer.insert(msg)?;
                return Ok(true);
            }
        }
        Ok(false)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use crate::buffers::single_buffers::RtRingBuffer;
    use crate::buffers::BufferError;
    use crate::channels::read_channel::UntypedBufferReceiver;
    use crate::channels::typed_read_channel::BufferReceiver;
    use crate::channels::typed_read_channel::ChannelBuffer;
    use crate::channels::untyped_channel;
    use crate::channels::ReceiverChannel;
    use crate::channels::UntypedPacket;
    use crate::channels::UntypedSenderChannel;
    use crate::packet::Packet;
    use crate::packet::Untyped;
    use crate::packet::WorkQueue;
    use crate::ChannelError;
    use crate::DataVersion;
    use crossbeam::channel::TryRecvError;

    fn create_buffer(
        channel: ReceiverChannel<Untyped>,
    ) -> BufferReceiver<Untyped, RtRingBuffer<Untyped>> {
        let buffer = RtRingBuffer::<Untyped>::new(2, true);
        BufferReceiver {
            buffer: Box::new(buffer),
            channel: Some(channel),
        }
    }

    fn test_read_channel() -> (UntypedBufferReceiver, UntypedSenderChannel) {
        let mut read_channel = UntypedBufferReceiver::default();
        assert_eq!(read_channel.available_channels().len(), 0);
        let crossbeam_channels = untyped_channel();
        let buffered_data = create_buffer(crossbeam_channels.1);

        read_channel
            .add_channel("test_channel_1".to_string(), buffered_data)
            .unwrap();

        (read_channel, crossbeam_channels.0)
    }

    #[test]
    fn test_read_channel_add_channel_maintains_order_in_keys() {
        let mut read_channel = test_read_channel().0;
        assert_eq!(read_channel.available_channels().len(), 1);
        assert_eq!(read_channel.available_channels(), vec!["test_channel_1"]);

        let crossbeam_channels = untyped_channel();

        let buffered_data = create_buffer(crossbeam_channels.1);
        read_channel
            .add_channel("test3".to_string(), buffered_data)
            .unwrap();
        assert_eq!(read_channel.available_channels().len(), 2);
        assert_eq!(
            read_channel.available_channels(),
            vec!["test_channel_1", "test3"]
        );
    }

    #[test]
    fn test_read_channel_try_read_returns_error_if_no_data() {
        let (read_channel, _) = test_read_channel();
        assert_eq!(
            read_channel
                .get_channel("test_channel_1")
                .as_ref()
                .unwrap()
                .channel
                .as_ref()
                .unwrap()
                .try_receive()
                .err()
                .unwrap(),
            ChannelError::TryReceiveError(TryRecvError::Disconnected)
        );
    }
    #[test]
    fn test_read_channel_try_read_returns_ok_if_data() {
        let (mut read_channel, crossbeam_channels) = test_read_channel();
        let queue = Arc::new(WorkQueue::default());
        read_channel.start(queue.clone());

        crossbeam_channels
            .send(Packet::new("my_data".to_string(), DataVersion { timestamp: 1 }).to_untyped())
            .unwrap();

        assert_eq!(
            read_channel
                .try_receive(Duration::from_millis(100))
                .unwrap(),
            true
        );
    }

    #[test]
    fn test_read_channel_try_read_returns_error_when_push_if_not_initialized() {
        let (read_channel, _) = test_read_channel();
        assert!(read_channel
            .get_channel("test_channel_1")
            .as_ref()
            .unwrap()
            .channel
            .as_ref()
            .unwrap()
            .try_receive()
            .is_err());
    }

    #[test]
    fn test_read_channel_try_read_returns_error_when_buffer_is_full() {
        let mut read_channel = UntypedBufferReceiver::default();
        let mut senders = vec![];
        for i in 0..2 {
            let crossbeam_channels = untyped_channel();
            let buffered_data = create_buffer(crossbeam_channels.1);
            let channel = format!("test{}", i);
            read_channel.add_channel(channel, buffered_data).unwrap();
            senders.push(crossbeam_channels.0);
        }

        let mut packet = Packet::new("my_data".to_string(), DataVersion { timestamp: 1 });

        read_channel.start(Arc::new(WorkQueue::<UntypedPacket>::default()));
        senders
            .get(0)
            .unwrap()
            .send(packet.clone().to_untyped())
            .unwrap();
        assert_eq!(
            read_channel.try_receive(Duration::from_millis(50)).unwrap(),
            true
        );
        packet.version.timestamp = 2;
        senders
            .get(0)
            .unwrap()
            .send(packet.clone().to_untyped())
            .unwrap();
        assert_eq!(
            read_channel.try_receive(Duration::from_millis(50)).unwrap(),
            true
        );

        packet.version.timestamp = 3;
        senders.get(0).unwrap().send(packet.to_untyped()).unwrap();
        assert_eq!(
            read_channel
                .try_receive(Duration::from_millis(50))
                .err()
                .unwrap(),
            ChannelError::ErrorInBuffer(BufferError::BufferFull)
        );
    }

    #[test]
    fn test_read_channel_fails_if_channel_not_added() {
        let (read_channel, _) = test_read_channel();
        assert!(read_channel.get_channel("test3").is_none());
    }
}
