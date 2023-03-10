use std::sync::Arc;
use std::sync::Mutex;

use super::ChannelError;
use super::SenderChannel;
use super::UntypedSenderChannel;
use crate::packet::ChannelID;
use crate::packet::DataVersion;
use crate::packet::Packet;

use indexmap::{map::Keys, IndexMap};

#[derive(Debug, Default)]
pub struct TypedWriteChannel<U, T: PacketSender<U>> {
    pub channels: Arc<Mutex<T>>,
}

pub trait PacketSender<T> {
    fn write(
        &self,
        channel_id: &ChannelID,
        data: T,
        version: &DataVersion,
    ) -> Result<(), ChannelError>;

    //pub fn available_channels(&self) -> Keys<ChannelID, Vec<UntypedSenderChannel>>;

    //pub fn add_channel(&mut self, channel: &ChannelID, data_queue: UntypedSenderChannel);
}

//unsafe impl Send for WriteChannel {}

macro_rules! write_channels {
    ($struct_name:ident, $($T:ident),+) => {
       struct $struct_name<$($T: Clone),+> {
            $(
                $T: SenderChannel<T>,
            )+
        }

        impl<$($T: Clone),+> PacketSender for $struct_name<$($T),+> {
            fn write(
                &self,
                channel_id: &ChannelID,
                data: T,
                version: &DataVersion,
            ) -> Result<(), ChannelError> {
                $(
                    if channel == stringify!($T) {
                        return Ok(Some(self.$T.buffer.get(version).unwrap().clone().to_untyped()));
                    }
                )+

                Ok(None)
            }

        }

    };
}

write_channels!(WriteChannel1, c1);
write_channels!(WriteChannel2, c1, c2);
write_channels!(WriteChannel3, c1, c2, c3);
write_channels!(WriteChannel4, c1, c2, c3, c4);
write_channels!(WriteChannel5, c1, c2, c3, c4, c5);
write_channels!(WriteChannel6, c1, c2, c3, c4, c5, c6);
write_channels!(WriteChannel7, c1, c2, c3, c4, c5, c6, c7);
write_channels!(WriteChannel8, c1, c2, c3, c4, c5, c6, c7, c8);

impl TypedWriteChannel {
    pub fn write<T: 'static + Clone>(
        &self,
        channel_id: &ChannelID,
        data: T,
        version: &DataVersion,
    ) -> Result<(), ChannelError> {
        let data_queues = self
            .channels
            .get(channel_id)
            .ok_or(ChannelError::MissingChannel(channel_id.clone()))?;
        data_queues
            .iter()
            .map(|sender| Ok(sender.send(Packet::<T>::new(data.clone(), version.clone()))?))
            .collect::<Result<(), ChannelError>>()?;
        Ok(())
    }

    pub fn available_channels(&self) -> Keys<ChannelID, Vec<UntypedSenderChannel>> {
        self.channels.keys()
    }

    pub fn add_channel(&mut self, channel: &ChannelID, data_queue: UntypedSenderChannel) {
        self.channels
            .entry(channel.clone())
            .or_insert(vec![])
            .push(data_queue);
    }
}

#[cfg(test)]
mod tests {
    use crate::channels::untyped_channel;
    use crate::channels::ChannelID;
    use crate::channels::UntypedReceiverChannel;
    use crate::channels::WriteChannel;
    use crate::ChannelError;
    use crate::DataVersion;

    fn create_write_channel() -> (WriteChannel, UntypedReceiverChannel) {
        let mut write_channel = WriteChannel::default();
        assert_eq!(write_channel.available_channels().len(), 0);
        let crossbeam_channels = untyped_channel();
        write_channel.add_channel(&ChannelID::from("test_channel_1"), crossbeam_channels.0);

        (write_channel, crossbeam_channels.1)
    }

    #[test]
    fn test_add_channel_maintains_order_in_keys() {
        let mut write_channel = create_write_channel().0;
        assert_eq!(write_channel.available_channels().len(), 1);
        assert_eq!(
            write_channel
                .available_channels()
                .collect::<Vec<&ChannelID>>(),
            vec![&ChannelID::from("test_channel_1")]
        );

        let crossbeam_channels = untyped_channel();
        write_channel.add_channel(&ChannelID::from("a_test3"), crossbeam_channels.0);
        assert_eq!(write_channel.available_channels().len(), 2);
        assert_eq!(
            write_channel
                .available_channels()
                .collect::<Vec<&ChannelID>>(),
            vec![
                &ChannelID::from("test_channel_1"),
                &ChannelID::from("a_test3")
            ]
        );
    }

    #[test]
    fn test_send_on_missing_channel_returns_error() {
        let write_channel = create_write_channel().0;
        assert_eq!(
            write_channel
                .write(
                    &ChannelID::from("test_channel_fake"),
                    "TestData".to_string(),
                    &DataVersion { timestamp: 1 }
                )
                .err()
                .unwrap(),
            ChannelError::MissingChannel(ChannelID::from("test_channel_fake"))
        );
    }

    #[test]
    fn test_send_on_existing_channel_fans_out_to_all_receivers() {
        let (mut write_channel, existing_read_channel) = create_write_channel();

        let mut read_channels = vec![];

        for _n in 0..2 {
            let channel = untyped_channel();
            write_channel.add_channel(&ChannelID::from("test_channel_2"), channel.0);
            read_channels.push(channel.1)
        }

        write_channel
            .write(
                &ChannelID::from("test_channel_2"),
                "TestData".to_string(),
                &DataVersion { timestamp: 1 },
            )
            .unwrap();

        assert!(existing_read_channel.try_receive().is_err());

        // for channel in read_channels {
        //     assert_eq!(
        //         channel
        //             .try_receive()
        //             .unwrap()
        //             .data
        //             .downcast_ref::<String>()
        //             .unwrap(),
        //         &"TestData".to_string()
        //     );
        // }
    }
}
