use super::ChannelError;
use super::UntypedReceiverChannel;

use crate::buffers::single_buffers::RtRingBuffer;
use crate::packet::ChannelID;

use crate::packet::UntypedPacket;

use crate::buffers::channel_buffers::BoundedBufferedData;
use crate::buffers::synchronizers::timestamp::TimestampSynchronizer;
use crate::buffers::synchronizers::PacketSynchronizer;

use crate::buffers::{OrderedBuffer, PacketBufferAddress};
use crossbeam::channel::Receiver;

use itertools::Itertools;

use crate::packet::ReadEvent;

use crate::packet::WorkQueue;
use indexmap::IndexMap;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

unsafe impl Send for ReadChannel {}

pub struct ReadChannel {
    buffered_data: Arc<Mutex<dyn OrderedBuffer>>,
    synch_strategy: Box<dyn PacketSynchronizer>,
    channels: IndexMap<ChannelID, UntypedReceiverChannel>, // Keep the channel order
    channel_index: HashMap<ChannelID, usize>,
    work_queue: Option<Arc<WorkQueue>>,
}

unsafe impl Send for ReadEvent {}
const MAX_BUFFER_PER_CHANNEL: usize = 2000;

impl ReadChannel {
    pub fn default() -> Self {
        ReadChannel {
            buffered_data: Arc::new(Mutex::new(BoundedBufferedData::<RtRingBuffer>::new(
                MAX_BUFFER_PER_CHANNEL,
                false,
            ))),
            synch_strategy: Box::new(TimestampSynchronizer::default()),
            channels: Default::default(),
            channel_index: Default::default(),
            work_queue: None,
        }
    }

    pub fn new(
        buffered_data: Arc<Mutex<dyn OrderedBuffer>>,
        synch_strategy: Box<dyn PacketSynchronizer>,
    ) -> Self {
        ReadChannel {
            buffered_data,
            synch_strategy,
            channels: Default::default(),
            channel_index: Default::default(),
            work_queue: None,
        }
    }

    pub fn add_channel(
        &mut self,
        channel_id: &ChannelID,
        receiver: UntypedReceiverChannel,
    ) -> Result<ChannelID, ChannelError> {
        self.channels.insert(channel_id.clone(), receiver);
        self.channel_index
            .insert(channel_id.clone(), self.channels.len() - 1);
        self.buffered_data
            .lock()
            .unwrap()
            .create_channel(channel_id)?;
        Ok(channel_id.clone())
    }

    pub fn selector<'b, 'a>(&'a self) -> Vec<Receiver<UntypedPacket>> {
        self.channels
            .values()
            .into_iter()
            .map(|rec| rec.receiver.clone())
            .collect()
    }
    fn insert_packet(
        &mut self,
        packet: UntypedPacket,
        channel: ChannelID,
    ) -> Result<PacketBufferAddress, ChannelError> {
        let packet_address = self
            .buffered_data
            .lock()
            .unwrap()
            .insert(&channel, packet)?;

        if let Some(maybe_work_queue) = &self.work_queue {
            self.synch_strategy
                .synchronize(&mut self.buffered_data, maybe_work_queue.clone());
            return Ok(packet_address);
        }
        return Err(ChannelError::NotInitializedError);
    }

    pub fn try_read_index(
        &mut self,
        channel_index: usize,
    ) -> Result<PacketBufferAddress, ChannelError> {
        let (channel_id, read_channel) = self
            .channels
            .get_index(channel_index)
            .ok_or(ChannelError::MissingChannelIndex(channel_index))?;

        let packet = read_channel.try_receive()?;
        let channel_id = channel_id.clone();

        self.insert_packet(packet, channel_id)
    }

    pub fn try_read(&mut self, channel: &ChannelID) -> Result<PacketBufferAddress, ChannelError> {
        let channel = channel.clone();

        let packet = self
            .channels
            .get(&channel)
            .ok_or(ChannelError::MissingChannel(channel.clone()))?
            .try_receive()?;

        self.insert_packet(packet, channel)
    }

    pub fn available_channels(&self) -> Vec<ChannelID> {
        self.channels
            .keys()
            .into_iter()
            .map(|key| key.clone())
            .collect_vec()
    }

    pub fn start(&mut self, _: usize, work_queue: Arc<WorkQueue>) -> () {
        self.work_queue = Some(work_queue);
    }

    pub fn stop(&mut self) -> () {}

    pub fn are_buffers_empty(&self) -> bool {
        self.buffered_data.lock().unwrap().are_buffers_empty()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::Mutex;

    use crate::buffers::channel_buffers::BoundedBufferedData;
    use crate::buffers::single_buffers::FixedSizeBTree;
    use crate::buffers::synchronizers::timestamp::TimestampSynchronizer;
    use crate::buffers::OrderedBuffer;
    use crate::channels::untyped_channel;
    use crate::channels::ChannelID;
    use crate::channels::ReadChannel;
    use crate::channels::UntypedSenderChannel;
    use crate::packet::Packet;
    use crate::packet::WorkQueue;
    use crate::ChannelError;
    use crate::DataVersion;
    use crossbeam::channel::TryRecvError;

    fn test_read_channel() -> (ReadChannel, UntypedSenderChannel) {
        let mut read_channel = ReadChannel::default();
        assert_eq!(read_channel.available_channels().len(), 0);
        let crossbeam_channels = untyped_channel();
        read_channel
            .add_channel(&ChannelID::from("test_channel_1"), crossbeam_channels.1)
            .unwrap();

        (read_channel, crossbeam_channels.0)
    }

    #[test]
    fn test_read_channel_add_channel_maintains_order_in_keys() {
        let mut read_channel = test_read_channel().0;
        assert_eq!(read_channel.available_channels().len(), 1);
        assert_eq!(
            read_channel.available_channels(),
            vec![ChannelID::from("test_channel_1")]
        );

        let crossbeam_channels = untyped_channel();
        read_channel
            .add_channel(&ChannelID::from("test3"), crossbeam_channels.1)
            .unwrap();
        assert_eq!(read_channel.available_channels().len(), 2);
        assert_eq!(
            read_channel.available_channels(),
            vec![ChannelID::from("test_channel_1"), ChannelID::from("test3")]
        );
    }

    #[test]
    fn test_read_channel_try_read_returns_error_if_no_data() {
        let (mut read_channel, _) = test_read_channel();
        assert_eq!(
            read_channel
                .try_read(&ChannelID::from("test_channel_1"))
                .err()
                .unwrap(),
            ChannelError::ReceiveError(TryRecvError::Disconnected)
        );
    }
    #[test]
    fn test_read_channel_try_read_returns_ok_if_data() {
        let (mut read_channel, crossbeam_channels) = test_read_channel();
        crossbeam_channels
            .send(Packet::new(
                "my_data".to_string(),
                DataVersion { timestamp: 1 },
            ))
            .unwrap();
        read_channel.start(0, Arc::new(WorkQueue::default()));
        assert_eq!(
            read_channel
                .try_read(&ChannelID::from("test_channel_1"))
                .ok()
                .unwrap(),
            (
                ChannelID::from("test_channel_1"),
                DataVersion { timestamp: 1 }
            )
        );
    }
    #[test]
    fn test_read_channel_try_read_returns_error_if_no_channel() {
        let mut read_channel = test_read_channel().0;
        assert_eq!(
            read_channel
                .try_read(&ChannelID::from("test_fake"))
                .err()
                .unwrap(),
            ChannelError::MissingChannel(ChannelID::from("test_fake"))
        );
    }

    #[test]
    fn test_read_channel_try_read_returns_error_when_push_if_not_initialized() {
        let (mut read_channel, _) = test_read_channel();
        assert!(read_channel
            .try_read(&ChannelID::from("test_channel_1"))
            .is_err());
    }

    #[test]
    fn test_read_channel_try_read_returns_error_when_buffer_is_full() {
        let buffered_data = Arc::new(Mutex::new(BoundedBufferedData::<FixedSizeBTree>::new(
            2, true,
        )));
        let synch_strategy = Box::new(TimestampSynchronizer::default());
        let mut read_channel = ReadChannel::new(buffered_data, synch_strategy);

        for i in 0..2 {
            let crossbeam_channels = untyped_channel();
            let channel = &ChannelID::from(format!("test{}", i).as_str());
            read_channel
                .add_channel(channel, crossbeam_channels.1)
                .unwrap();
        }

        let channel = &ChannelID::from("test0");
        let mut packet = Packet::new("my_data".to_string(), DataVersion { timestamp: 1 });

        read_channel.start(100, Arc::new(WorkQueue::default()));
        read_channel
            .insert_packet(packet.clone().to_untyped(), channel.clone())
            .unwrap();
        packet.version.timestamp = 2;
        read_channel
            .insert_packet(packet.clone().to_untyped(), channel.clone())
            .unwrap();
        packet.version.timestamp = 3;
        assert!(read_channel
            .insert_packet(packet.clone().to_untyped(), channel.clone())
            .is_err());
    }

    #[test]
    fn test_read_channel_fails_if_channel_not_added() {
        let buffer = BoundedBufferedData::<FixedSizeBTree>::new(100, false);
        let safe_buffer: Arc<Mutex<dyn OrderedBuffer>> = Arc::new(Mutex::new(buffer));

        let packet = Packet::<String> {
            data: Box::new("data".to_string()),
            version: DataVersion { timestamp: 0 },
        };

        safe_buffer
            .lock()
            .unwrap()
            .create_channel(&ChannelID::new("test1".to_string()))
            .unwrap();

        assert!(safe_buffer
            .lock()
            .unwrap()
            .insert(
                &ChannelID::new("test1".to_string()),
                packet.clone().to_untyped(),
            )
            .is_ok());

        assert!(safe_buffer
            .lock()
            .unwrap()
            .insert(
                &ChannelID::new("test3".to_string()),
                packet.clone().to_untyped(),
            )
            .is_err());
    }
}
