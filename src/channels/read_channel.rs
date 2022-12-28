use super::data_buffers::{DataBuffer, HashmapBufferedData};
use super::ChannelError;
use super::{ChannelID, DataVersion};
use super::{Packet, PacketView, UntypedPacket};
use super::{PacketBufferAddress, PacketWithAddress, UntypedReceiverChannel};
use crossbeam::channel::Receiver;

use crate::packet::UntypedPacketCast;
use indexmap::{map::Keys, IndexMap};
use std::collections::HashMap;

#[derive(Default)]
pub struct PacketSet {
    ordered_channel_data: Vec<Option<PacketWithAddress>>,
    channel_index_mapping: HashMap<ChannelID, usize>,
}

impl PacketSet {
    fn new(
        ordered_channel_data: Vec<Option<PacketWithAddress>>,
        channel_index_mapping: HashMap<ChannelID, usize>,
    ) -> Self {
        PacketSet {
            ordered_channel_data,
            channel_index_mapping,
        }
    }

    pub fn channels(&self) -> usize {
        self.channel_index_mapping.len()
    }

    pub fn get_owned<T: 'static>(
        &mut self,
        channel_number: usize,
    ) -> Result<Packet<T>, ChannelError> {
        match self
            .ordered_channel_data
            .get_mut(channel_number)
            .ok_or(ChannelError::MissingChannelIndex(channel_number))?
            .take()
        {
            Some(maybe_packet_with_address) => {
                Ok(maybe_packet_with_address.1.deref_owned::<T>()?)
            }
            None => Err(ChannelError::MissingChannelData(channel_number)),
        }
    }

    pub fn get<T: 'static>(&self, channel_number: usize) -> Result<PacketView<T>, ChannelError> {
        match self
            .ordered_channel_data
            .get(channel_number)
            .ok_or(ChannelError::MissingChannelIndex(channel_number))?
        {
            Some(maybe_packet_with_address) => Ok(maybe_packet_with_address.1.deref::<T>()?),
            None => Err(ChannelError::MissingChannelData(channel_number)),
        }
    }

    pub fn get_channel_index(&self, channel_id: &ChannelID) -> Result<usize, ChannelError> {
        Ok(*self
            .channel_index_mapping
            .get(&channel_id)
            .ok_or(ChannelError::MissingChannel(channel_id.clone()))?)
    }

    pub fn get_channel<T: 'static>(
        &self,
        channel_id: &ChannelID,
    ) -> Result<PacketView<T>, ChannelError> {
        self.get(self.get_channel_index(channel_id)?)
    }

    pub fn get_channel_owned<T: 'static>(
        &mut self,
        channel_id: &ChannelID,
    ) -> Result<Packet<T>, ChannelError> {
        self.get_owned(self.get_channel_index(channel_id)?)
    }
}

unsafe impl Send for PacketSet {}

pub struct ReadChannel {
    channels: IndexMap<ChannelID, UntypedReceiverChannel>, // Keep the channel order
    buffered_data: Box<dyn DataBuffer>,
    channel_index: HashMap<ChannelID, usize>,
}

unsafe impl Send for ReadChannel {}

impl ReadChannel {
    pub fn default() -> Self {
        Self {
            channels: Default::default(),
            buffered_data: Box::new(HashmapBufferedData::default()),
            channel_index: Default::default(),
        }
    }

    // pub fn synchronize(&self, data_version: &DataVersion) -> bool {
    //     self.buffered_data.synchronize(data_version)
    // }

    pub fn add_channel(&mut self, channel_id: &ChannelID, receiver: UntypedReceiverChannel) {
        self.channels.insert(channel_id.clone(), receiver);
        self.channel_index
            .insert(channel_id.clone(), self.channels.len() - 1);
    }

    pub fn selector<'b, 'a>(&'a self) -> Vec<Receiver<UntypedPacket>> {
        self.channels
            .values()
            .into_iter()
            .map(|rec| rec.receiver.clone())
            .collect()
    }

    fn try_read_result(
        &mut self,
        packet: UntypedPacket,
        channel: ChannelID,
    ) -> Result<PacketBufferAddress, ChannelError> {
        let packet_address = self.buffered_data.insert(&channel, packet)?;
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

        self.try_read_result(packet, channel_id)
    }

    pub fn try_read(&mut self, channel: &ChannelID) -> Result<PacketBufferAddress, ChannelError> {
        let channel = channel.clone();

        let packet = self
            .channels
            .get(&channel)
            .ok_or(ChannelError::MissingChannel(channel.clone()))?
            .try_receive()?;

        self.try_read_result(packet, channel)
    }

    pub fn available_channels(&self) -> Keys<ChannelID, UntypedReceiverChannel> {
        self.channels.keys()
    }

    // pub fn get_packets_for_version(&mut self, data_version: &DataVersion) -> PacketSet {
    //     let channels: Vec<ChannelID> = self.available_channels().cloned().collect();

    //     let packet_set = channels
    //         .iter()
    //         .map(|channel_id| {
    //             let removed_packet = self
    //                 .buffered_data
    //                 .consume(&(channel_id.clone(), data_version.clone()));
    //             match removed_packet {
    //                 Some(entry) => Some(PacketWithAddress(
    //                     (channel_id.clone(), data_version.clone()),
    //                     entry,
    //                 )),
    //                 None => None,
    //             }
    //         })
    //         .collect();

    //     PacketSet::new(packet_set, self.channel_index.clone())
    // }
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
            read_channel
                .available_channels()
                .collect::<Vec<&ChannelID>>(),
            vec![&ChannelID::from("test_channel_1")]
        );

        let crossbeam_channels = untyped_channel();
        read_channel.add_channel(&ChannelID::from("test3"), crossbeam_channels.1);
        assert_eq!(read_channel.available_channels().len(), 2);
        assert_eq!(
            read_channel
                .available_channels()
                .collect::<Vec<&ChannelID>>(),
            vec![
                &ChannelID::from("test_channel_1"),
                &ChannelID::from("test3")
            ]
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
