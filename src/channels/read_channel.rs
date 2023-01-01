use super::ChannelError;
use super::UntypedReceiverChannel;

use crate::buffers::BufferError;
use crate::packet::ChannelID;

use crate::packet::UntypedPacket;

use crate::buffers::btree_data_buffers::BtreeBufferedData;
use crate::buffers::hashmap_data_buffers::HashmapBufferedData;
use crate::buffers::synchronizers::PacketSynchronizer;
use crate::buffers::synchronizers::TimestampSynchronizer;

use crate::buffers::{DataBuffer, OrderedBuffer, PacketBufferAddress};
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
    initialized: bool,
}

unsafe impl Send for ReadEvent {}
const MAX_BUFFER_PER_CHANNEL: usize = 1000;

impl ReadChannel {
    pub fn default() -> Self {
        ReadChannel {
            buffered_data: Arc::new(Mutex::new(BtreeBufferedData::new(MAX_BUFFER_PER_CHANNEL))),
            synch_strategy: Box::new(TimestampSynchronizer::default()),
            channels: Default::default(),
            channel_index: Default::default(),
            initialized: false,
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
            initialized: false,
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
        self.synch_strategy.packet_event(packet_address.clone());
        Ok(packet_address)
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

    pub fn start(&mut self, node_id: usize, work_queue: Arc<WorkQueue>) -> () {
        self.synch_strategy.start(
            self.buffered_data.clone(),
            work_queue,
            node_id,
            &self.available_channels(),
        )
    }

    pub fn stop(&mut self) -> () {
        self.synch_strategy.stop()
    }
}

#[cfg(test)]
mod tests {
    use crate::channels::untyped_channel;
    use crate::channels::ChannelID;
    use crate::channels::ReadChannel;
    use crate::channels::UntypedSenderChannel;
    use crate::packet::Packet;
    use crate::ChannelError;
    use crate::DataVersion;
    use crossbeam::channel::TryRecvError;

    fn test_read_channel() -> (ReadChannel, UntypedSenderChannel) {
        let mut read_channel = ReadChannel::default();
        assert_eq!(read_channel.available_channels().len(), 0);
        let crossbeam_channels = untyped_channel();
        read_channel.add_channel(&ChannelID::from("test_channel_1"), crossbeam_channels.1);

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
        read_channel.add_channel(&ChannelID::from("test3"), crossbeam_channels.1);
        assert_eq!(read_channel.available_channels().len(), 2);
        assert_eq!(
            read_channel.available_channels(),
            vec![ChannelID::from("test_channel_1"), ChannelID::from("test3")]
        );
    }

    // #[test]
    // fn test_read_channel_get_packets_packetset_has_all_channels_if_no_version() {
    //     let mut read_channel = test_read_channel().0;
    //     let packetset = read_channel.get_packets_for_version(&DataVersion { timestamp: 1 });

    //     assert_eq!(packetset.channels(), 1);
    //     assert!(packetset
    //         .get_channel::<String>(&ChannelID::from("test_channel_1"))
    //         .ok()
    //         .is_none());
    // }

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
}
