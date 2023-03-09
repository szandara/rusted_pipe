use std::sync::Arc;
use std::sync::Mutex;

use super::ChannelError;
use super::Packet;
use super::UntypedReceiverChannel;

use crate::buffers::single_buffers::FixedSizeBuffer;
use crate::buffers::single_buffers::RtRingBuffer;
use crate::packet::ChannelID;

use crate::buffers::synchronizers::PacketSynchronizer;
use crate::packet::UntypedPacket;

use crate::buffers::{OrderedBuffer, PacketBufferAddress};
use crossbeam::channel::Receiver;

use crate::packet::ReadEvent;
use crate::packet::WorkQueue;
use ::core::mem::MaybeUninit;

struct BufferReceiver<U, T: FixedSizeBuffer<Data = U>> {
    pub buffer: T,
    pub channel: MaybeUninit<Receiver<Packet<U>>>,
}

impl<U, T: FixedSizeBuffer<Data = U>> BufferReceiver<U, T> {
    fn link(&mut self, receiver: Receiver<Packet<U>>) {
        self.channel.write(receiver);
    }
}

pub struct ReadChannel<T: Reader> {
    synch_strategy: Box<dyn PacketSynchronizer>,
    work_queue: Option<Arc<WorkQueue>>,
    channels: T,
}

pub trait Reader {}

macro_rules! read_channels {
    ($struct_name:ident, $($T:ident),+) => {
        paste::item! {
            enum [<$struct_name Channels>] {
                $(
                    $T,
                )+
            }
        }

        struct $struct_name<$($T: Clone),+> {
            $(
                $T: BufferReceiver<$T, RtRingBuffer<$T>>,
            )+
        }

        impl<$($T: Clone),+> Reader for $struct_name<$($T),+> {
        }

        impl<$($T: Clone),+> $struct_name<$($T),+> {
            fn new($($T: RtRingBuffer<$T>),+) -> Self {
                Self {
                    $(
                        $T: BufferReceiver {buffer: $T, channel: MaybeUninit::<Receiver<Packet<$T>>>::uninit()},
                    )+
                }
            }

            $(
                fn $T(&mut self) -> &mut BufferReceiver<$T, RtRingBuffer<$T>> {
                    &mut self.$T
                }
            )+

            // $(
            //     fn paste::item! { [<mut _ $T(&self)>] } -> &BufferReceiver<$T, RtRingBuffer<$T>> {
            //         &self.$T
            //     }
            // )+
            // fn select(&self, channel: paste::item!{[<$struct_name Channels>]} ) -> &BufferReceiver<$T, RtRingBuffer<$T>> {
            //     match channel {
            //         $(
            //             paste::item!{[<$struct_name Channels :: $T>]} => &self.$T,
            //         )+
            //     }
            // }
        }
    };
}

read_channels!(ReadChannel1, c1);
read_channels!(ReadChannel2, c1, c2);
read_channels!(ReadChannel3, c1, c2, c3);
read_channels!(ReadChannel4, c1, c2, c3, c4);
read_channels!(ReadChannel5, c1, c2, c3, c4, c5);
read_channels!(ReadChannel6, c1, c2, c3, c4, c5, c6);
read_channels!(ReadChannel7, c1, c2, c3, c4, c5, c6, c7);
read_channels!(ReadChannel8, c1, c2, c3, c4, c5, c6, c7, c8);

unsafe impl Send for ReadEvent {}
const MAX_BUFFER_PER_CHANNEL: usize = 2000;

impl<T: Reader> ReadChannel<T> {
    // pub fn default() -> Self {
    //     ReadChannel {
    //         buffered_data: Arc::new(Mutex::new(BoundedBufferedData::<RtRingBuffer>::new(
    //             MAX_BUFFER_PER_CHANNEL,
    //             false,
    //         ))),
    //         synch_strategy: Box::new(TimestampSynchronizer::default()),
    //         channels: Default::default(),
    //         channel_index: Default::default(),
    //         work_queue: None,
    //     }
    // }

    pub fn new(
        synch_strategy: Box<dyn PacketSynchronizer>,
        work_queue: Option<Arc<WorkQueue>>,
        channels: T,
    ) -> Self {
        ReadChannel {
            synch_strategy,
            work_queue: work_queue,
            channels: channels,
        }
    }

    // pub fn add_channel(
    //     &mut self,
    //     channel_id: &ChannelID,
    //     receiver: UntypedReceiverChannel,
    // ) -> Result<ChannelID, ChannelError> {
    //     self.channels.insert(channel_id.clone(), receiver);

    //     self.buffered_data
    //         .lock()
    //         .unwrap()
    //         .create_channel(channel_id)?;
    //     Ok(channel_id.clone())
    // }

    // pub fn selector<'b, 'a>(&'a self) -> Vec<Receiver<UntypedPacket>> {
    //     self.channels
    //         .values()
    //         .into_iter()
    //         .map(|rec| rec.receiver.clone())
    //         .collect()
    // }

    // fn insert_packet(
    //     &mut self,
    //     packet: UntypedPacket,
    //     channel: ChannelID,
    // ) -> Result<PacketBufferAddress, ChannelError> {
    //     let packet_address = self
    //         .buffered_data
    //         .lock()
    //         .unwrap()
    //         .insert(&channel, packet)?;

    //     if let Some(maybe_work_queue) = &self.work_queue {
    //         self.synch_strategy
    //             .synchronize(&mut self.buffered_data, maybe_work_queue.clone());
    //         return Ok(packet_address);
    //     }
    //     return Err(ChannelError::NotInitializedError);
    // }

    // pub fn try_read_index(
    //     &mut self,
    //     channel_index: usize,
    // ) -> Result<PacketBufferAddress, ChannelError> {
    //     let (channel_id, read_channel) = self
    //         .channels
    //         .get_index(channel_index)
    //         .ok_or(ChannelError::MissingChannelIndex(channel_index))?;

    //     let packet = read_channel.try_receive()?;
    //     let channel_id = channel_id.clone();

    //     self.insert_packet(packet, channel_id)
    // }

    // pub fn try_read(&mut self, channel: &ChannelID) -> Result<PacketBufferAddress, ChannelError> {
    //     let channel = channel.clone();

    //     let packet = self
    //         .channels
    //         .get(&channel)
    //         .ok_or(ChannelError::MissingChannel(channel.clone()))?
    //         .try_receive()?;

    //     self.insert_packet(packet, channel)
    // }

    // pub fn available_channels(&self) -> Vec<ChannelID> {
    //     self.channels
    //         .keys()
    //         .into_iter()
    //         .map(|key| key.clone())
    //         .collect_vec()
    // }

    pub fn start(&mut self, _: usize, work_queue: Arc<WorkQueue>) -> () {
        self.work_queue = Some(work_queue);
    }

    pub fn stop(&mut self) -> () {}
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::Mutex;

    //use crate::buffers::channel_buffers::BoundedBufferedData;
    use crate::buffers::single_buffers::FixedSizeBTree;
    use crate::buffers::single_buffers::FixedSizeBuffer;
    use crate::buffers::single_buffers::RtRingBuffer;
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
    use crossbeam::channel::unbounded;
    use crossbeam::channel::TryRecvError;

    use super::ReadChannel2;

    // fn test_read_channel() -> (
    //     ReadChannel<ReadChannel2<String, String>>,
    //     UntypedSenderChannel,
    // ) {
    //     let mut read_channel = ReadChannel::default();
    //     assert_eq!(read_channel.available_channels().len(), 0);
    //     let crossbeam_channels = untyped_channel();
    //     read_channel
    //         .add_channel(&ChannelID::from("test_channel_1"), crossbeam_channels.1)
    //         .unwrap();

    //     (read_channel, crossbeam_channels.0)
    // }

    // #[test]
    // fn test_read_channel_add_channel_maintains_order_in_keys() {
    //     let mut read_channel = test_read_channel().0;
    //     assert_eq!(read_channel.available_channels().len(), 1);
    //     assert_eq!(
    //         read_channel.available_channels(),
    //         vec![ChannelID::from("test_channel_1")]
    //     );

    //     let crossbeam_channels = untyped_channel();
    //     read_channel
    //         .add_channel(&ChannelID::from("test3"), crossbeam_channels.1)
    //         .unwrap();
    //     assert_eq!(read_channel.available_channels().len(), 2);
    //     assert_eq!(
    //         read_channel.available_channels(),
    //         vec![ChannelID::from("test_channel_1"), ChannelID::from("test3")]
    //     );
    // }

    // #[test]
    // fn test_read_channel_try_read_returns_error_if_no_data() {
    //     let (mut read_channel, _) = test_read_channel();
    //     assert_eq!(
    //         read_channel
    //             .try_read(&ChannelID::from("test_channel_1"))
    //             .err()
    //             .unwrap(),
    //         ChannelError::ReceiveError(TryRecvError::Disconnected)
    //     );
    // }
    // #[test]
    // fn test_read_channel_try_read_returns_ok_if_data() {
    //     let (mut read_channel, crossbeam_channels) = test_read_channel();
    //     crossbeam_channels
    //         .send(Packet::new(
    //             "my_data".to_string(),
    //             DataVersion { timestamp: 1 },
    //         ))
    //         .unwrap();
    //     read_channel.start(0, Arc::new(WorkQueue::default()));
    //     assert_eq!(
    //         read_channel
    //             .try_read(&ChannelID::from("test_channel_1"))
    //             .ok()
    //             .unwrap(),
    //         (
    //             ChannelID::from("test_channel_1"),
    //             DataVersion { timestamp: 1 }
    //         )
    //     );
    // }
    // #[test]
    // fn test_read_channel_try_read_returns_error_if_no_channel() {
    //     let mut read_channel = test_read_channel().0;
    //     assert_eq!(
    //         read_channel
    //             .try_read(&ChannelID::from("test_fake"))
    //             .err()
    //             .unwrap(),
    //         ChannelError::MissingChannel(ChannelID::from("test_fake"))
    //     );
    // }

    // #[test]
    // fn test_read_channel_try_read_returns_error_when_push_if_not_initialized() {
    //     let (mut read_channel, _) = test_read_channel();
    //     assert!(read_channel
    //         .try_read(&ChannelID::from("test_channel_1"))
    //         .is_err());
    // }

    #[test]
    fn test_read_channel_try_read_returns_error_when_buffer_is_full() {
        // let buffered_data = Arc::new(Mutex::new(BoundedBufferedData::<FixedSizeBTree>::new(
        //     2, true,
        // )));
        let synch_strategy = Box::new(TimestampSynchronizer::default());
        let mut read_channel2 = ReadChannel2::new(
            RtRingBuffer::<String>::new(2, true),
            RtRingBuffer::<String>::new(2, true),
        );
        let mut read_channel = ReadChannel::new(
            synch_strategy,
            Some(Arc::new(WorkQueue::default())),
            read_channel2,
        );

        let (channel_sender, channel_receiver) = unbounded::<Packet<String>>();
        read_channel.channels.c1().link(channel_receiver);

        let (channel_sender, channel_receiver) = unbounded::<Packet<String>>();
        read_channel.channels.c2().link(channel_receiver);

        let mut packet = Packet::new("my_data".to_string(), DataVersion { timestamp: 1 });

        read_channel.start(100, Arc::new(WorkQueue::default()));
        read_channel
            .channels
            .c1()
            .buffer
            .insert(packet.clone())
            .unwrap();
        packet.version.timestamp = 2;
        read_channel
            .channels
            .c1()
            .buffer
            .insert(packet.clone())
            .unwrap();
        packet.version.timestamp = 3;
        assert!(read_channel
            .channels
            .c1()
            .buffer
            .insert(packet.clone())
            .is_err());
    }

    // #[test]
    // fn test_read_channel_fails_if_channel_not_added() {
    //     let buffer = BoundedBufferedData::<FixedSizeBTree>::new(100, false);
    //     let safe_buffer: Arc<Mutex<dyn OrderedBuffer>> = Arc::new(Mutex::new(buffer));

    //     let packet = Packet::<String> {
    //         data: Box::new("data".to_string()),
    //         version: DataVersion { timestamp: 0 },
    //     };

    //     safe_buffer
    //         .lock()
    //         .unwrap()
    //         .create_channel(&ChannelID::new("test1".to_string()))
    //         .unwrap();

    //     assert!(safe_buffer
    //         .lock()
    //         .unwrap()
    //         .insert(
    //             &ChannelID::new("test1".to_string()),
    //             packet.clone().to_untyped(),
    //         )
    //         .is_ok());

    //     assert!(safe_buffer
    //         .lock()
    //         .unwrap()
    //         .insert(
    //             &ChannelID::new("test3".to_string()),
    //             packet.clone().to_untyped(),
    //         )
    //         .is_err());
    // }
}
