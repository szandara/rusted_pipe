//! A typed-erased WriteChannel for a set of possible data outputs.
//! This channel works with UntypedPackets that move around Boxed Any data
//! which is then converted back into the expected format by the user.
//!
//! There is an additional cost in boxing the data and unboxing it plus
//! casting it to the correct type. Furthermore there is no type safety
//! at compile type for the creation of your graph.
//!
//! On the other hand, these channels can grow their inputs dynamically.
//! It's possible to add a new channel at any time before the graph is created and
//! started.
use std::collections::HashMap;
use std::time::Duration;

use super::read_channel::get_data;
use super::read_channel::BufferReceiver;
use super::read_channel::ChannelBuffer;
use super::read_channel::InputGenerator;
use crate::buffers::single_buffers::LenTrait;
use crate::graph::metrics::BufferMonitorBuilder;
use super::ChannelError;
use super::ChannelID;
use super::UntypedPacket;

use crate::buffers::single_buffers::FixedSizeBuffer;
use crate::buffers::single_buffers::RtRingBuffer;
use crate::DataVersion;

use crate::packet::untyped::UntypedPacketSet;
use crate::packet::Untyped;

use crossbeam::channel::Select;

use indexmap::IndexMap;
use itertools::Itertools;

unsafe impl Send for UntypedReadChannel {}

/// Untyped ReadChannel structure. The channels are stored as a
/// map of buffers where each buffer handles data from an incoming channel.
pub struct UntypedReadChannel {
    // Map of channels. Channels are stored as string keys.
    buffered_data: IndexMap<ChannelID, BufferReceiver<RtRingBuffer<Box<Untyped>>>>,
}

impl Default for UntypedReadChannel {
    /// A default UntypedReadChannel with no channels.
    fn default() -> Self {
        UntypedReadChannel {
            buffered_data: Default::default(),
        }
    }
}

impl UntypedReadChannel {
    
    /// A pre-initialized UntypedReadChannel.
    pub fn new(
        buffered_data: IndexMap<ChannelID, BufferReceiver<RtRingBuffer<Box<Untyped>>>>,
    ) -> Self {
        UntypedReadChannel {
            buffered_data,
        }
    }

    /// Add a named channel and its corresponding buffer. The buffer
    /// will collect all incoming data and serve it to the synchronizer.
    pub fn add_channel(
        &mut self,
        channel: ChannelID,
        receiver: BufferReceiver<RtRingBuffer<Box<Untyped>>>,
    ) -> Result<ChannelID, ChannelError> {
        self.buffered_data.insert(channel.clone(), receiver);
        Ok(channel)
    }

    /// Get a reference to a channel if it exists or None.
    pub fn get_channel(
        &self,
        channel: &ChannelID,
    ) -> Option<&BufferReceiver<RtRingBuffer<Box<Untyped>>>> {
        self.buffered_data.get(channel)
    }

    /// Get a mutable reference to a channel if it exists or None.
    pub fn get_channel_mut(
        &mut self,
        channel: &ChannelID,
    ) -> Option<&mut BufferReceiver<RtRingBuffer<Box<Untyped>>>> {
        self.buffered_data.get_mut(channel)
    }
    
    fn _wait_for_data(&self, timeout: Duration) -> Option<&ChannelID> {
        let mut select = Select::new();
        let mut connected_channels = vec![]; 
        for (id, rec) in &self.buffered_data {
            if let Some(channel) = rec.channel.as_ref() {
                connected_channels.push(id);
                select.recv(&channel.receiver);
            }
        }

        if let Ok(channel) = select.ready_timeout(timeout) {
            return connected_channels.get(channel).copied()
        }
        None
    }
}

impl ChannelBuffer for UntypedReadChannel {
    fn available_channels(&self) -> Vec<&ChannelID> {
        self.buffered_data.keys().collect_vec()
    }

    fn has_version(&self, channel: &ChannelID, version: &crate::DataVersion) -> bool {
        if let Some(buffer) = self.get_channel(channel) {
            return buffer.buffer.find_version(version).is_some();
        }
        false
    }

    fn peek(&self, channel: &ChannelID) -> Option<&crate::DataVersion> {
        if let Some(buffer) = self.get_channel(channel) {
            return buffer.buffer.peek();
        }
        None
    }

    fn iterator(&self, channel: &ChannelID) -> Option<Box<crate::buffers::BufferIterator>> {
        if let Some(buffer) = self.get_channel(channel) {
            return Some(buffer.buffer.iter());
        }
        None
    }

    fn are_buffers_empty(&self) -> bool {
        self.buffered_data.values().all(|b| b.buffer.len() == 0)
    }

    fn try_receive(&mut self, timeout: std::time::Duration) -> Result<Option<&ChannelID>, ChannelError> {
        if let Some(ch) = self._wait_for_data(timeout).cloned() {
            if let Some((_, channel, buffer)) = self.buffered_data.get_full_mut(&ch) {
                let msg = buffer.channel.as_ref()
                .expect("Buffer has not receiver channel. This is a bug")
                .receiver
                .recv()?;

                buffer.buffer.insert(msg)?;
                return Ok(Some(channel));
            }
        }
        Ok(None)
    }

    fn max_version(&self) -> Option<&DataVersion> {
        self.buffered_data.values().filter_map(|b|  b.buffer.back()).max()
    }

    fn wait_for_data(&self, _timeout: std::time::Duration) -> Result<bool, ChannelError> {
        todo!()
    }
}

impl InputGenerator for UntypedReadChannel {
    type INPUT = UntypedPacketSet;

    fn get_packets_for_version(
        &mut self,
        data_versions: &HashMap<ChannelID, Option<DataVersion>>,
        exact_match: bool,
    ) -> Option<Self::INPUT> {
        let mut packet_set = IndexMap::<ChannelID, Option<UntypedPacket>>::default();

        data_versions.iter().for_each(|(channel_id, data_version)| {
            match self.get_channel_mut(channel_id) {
                Some(channel) => {
                    let data = get_data(&mut channel.buffer, data_version, exact_match);
                    if let Some(data) = data {
                        packet_set.insert(channel_id.clone(), Some(data));
                        return;
                    }
                }
                None => {
                    eprintln!("Cannot find channel {channel_id}")
                }
            }
            packet_set.insert(channel_id.clone(), None);
        });

        if packet_set.values().any(|v| v.is_none()) {
            eprintln!(
                "Not all data could be extracted from the channel. Skipping this packet with {:?}",
                data_versions
            );
            return None;
        }
        Some(UntypedPacketSet::new(packet_set))
    }

    fn create_channels(_buffer_size: usize, _block_on_full: bool, _monitor: BufferMonitorBuilder) -> Self {
        Self::default()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::buffers::single_buffers::RtRingBuffer;
    use crate::buffers::BufferError;
    use crate::channels::read_channel::BufferReceiver;
    use crate::channels::read_channel::ChannelBuffer;
    use crate::channels::ChannelID;

    use crate::channels::untyped_channel;
    use crate::channels::untyped_read_channel::UntypedReadChannel;
    use crate::channels::ReceiverChannel;
    use crate::channels::UntypedSenderChannel;
    use crate::graph::metrics::BufferMonitor;
    use crate::packet::Packet;
    use crate::packet::Untyped;
    use crate::ChannelError;
    use crate::DataVersion;
    use crossbeam::channel::TryRecvError;

    fn create_buffer(
        channel: ReceiverChannel<Box<Untyped>>,
    ) -> BufferReceiver<RtRingBuffer<Box<Untyped>>> {
        let buffer = RtRingBuffer::<Box<Untyped>>::new(2, true, BufferMonitor::default());
        BufferReceiver {
            buffer: Box::new(buffer),
            channel: Some(channel),
        }
    }

    fn create_read_channel() -> (UntypedReadChannel, UntypedSenderChannel) {
        let mut read_channel = UntypedReadChannel::default();
        assert_eq!(read_channel.available_channels().len(), 0);
        let crossbeam_channels = untyped_channel();
        let buffered_data = create_buffer(crossbeam_channels.1);

        read_channel
            .add_channel(ChannelID::from("test_channel_1"), buffered_data)
            .unwrap();

        (read_channel, crossbeam_channels.0)
    }

    #[test]
    fn test_read_channel_add_channel_maintains_order_in_keys() {
        let mut read_channel = create_read_channel().0;
        assert_eq!(read_channel.available_channels().len(), 1);
        assert_eq!(read_channel.available_channels(), vec!["test_channel_1"]);

        let crossbeam_channels = untyped_channel();

        let buffered_data = create_buffer(crossbeam_channels.1);
        read_channel
            .add_channel(ChannelID::from("test3"), buffered_data)
            .unwrap();
        assert_eq!(read_channel.available_channels().len(), 2);
        assert_eq!(
            read_channel.available_channels(),
            vec!["test_channel_1", "test3"]
        );
    }

    #[test]
    fn test_read_channel_try_read_returns_error_if_no_data() {
        let (read_channel, _) = create_read_channel();
        assert_eq!(
            read_channel
                .get_channel(&ChannelID::from("test_channel_1"))
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
        let (mut read_channel, crossbeam_channels) = create_read_channel();

        crossbeam_channels
            .send(Packet::new("my_data".to_string(), DataVersion { timestamp_ns: 1 }).to_untyped())
            .unwrap();

        assert_eq!(
            read_channel
                .try_receive(Duration::from_millis(100))
                .unwrap(),
            Some(&ChannelID::from("test_channel_1"))
        );
    }

    #[test]
    fn test_read_channel_try_read_returns_error_when_push_if_not_initialized() {
        let (read_channel, _) = create_read_channel();
        assert!(read_channel
            .get_channel(&ChannelID::from("test_channel_1"))
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
        let mut read_channel = UntypedReadChannel::default();
        let mut senders = vec![];
        for i in 0..2 {
            let crossbeam_channels = untyped_channel();
            let buffered_data = create_buffer(crossbeam_channels.1);
            let channel = ChannelID::from(format!("test{}", i));
            read_channel.add_channel(channel, buffered_data).unwrap();
            senders.push(crossbeam_channels.0);
        }

        let mut packet = Packet::new("my_data".to_string(), DataVersion { timestamp_ns: 1 });

        senders
            .get(0)
            .unwrap()
            .send(packet.clone().to_untyped())
            .unwrap();
        assert_eq!(
            read_channel.try_receive(Duration::from_millis(50)).unwrap(),
            Some(&ChannelID::from("test0"))
        );
        packet.version.timestamp_ns = 2;
        senders
            .get(0)
            .unwrap()
            .send(packet.clone().to_untyped())
            .unwrap();
        assert_eq!(
            read_channel.try_receive(Duration::from_millis(50)).unwrap(),
            Some(&ChannelID::from("test0"))
        );

        packet.version.timestamp_ns = 3;
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
        let (read_channel, _) = create_read_channel();
        assert!(read_channel
            .get_channel(&ChannelID::from("test3"))
            .is_none());
    }
}
