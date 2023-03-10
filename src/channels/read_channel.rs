use std::sync::Arc;

use super::ChannelError;
use super::ReceiverChannel;
use crate::buffers::single_buffers::FixedSizeBuffer;
use crate::buffers::single_buffers::RtRingBuffer;
use crate::buffers::synchronizers::PacketSynchronizer;
use crate::buffers::BufferError;
use crate::buffers::OrderedBuffer;
use crate::packet::ReadEvent;
use crate::packet::UntypedPacket;
use crate::packet::WorkQueue;
use crate::DataVersion;
use crossbeam::channel::{select, unbounded};
use itertools::all;
use std::time::Duration;

struct BufferReceiver<U, T: FixedSizeBuffer<Data = U>> {
    pub buffer: T,
    pub channel: Option<ReceiverChannel<U>>,
}

impl<U, T: FixedSizeBuffer<Data = U>> BufferReceiver<U, T> {
    pub fn link(&mut self, receiver: ReceiverChannel<U>) {
        self.channel = Some(receiver);
    }

    pub fn try_read(&mut self) -> Result<DataVersion, ChannelError> {
        if let Some(channel) = self.channel.as_ref() {
            let packet = channel.try_receive()?;
            let version = packet.version.clone();
            self.buffer.insert(packet)?;
            return Ok(version);
        }
        Err(ChannelError::NotInitializedError)
    }
}

pub struct ReadChannel<T: Reader> {
    pub synch_strategy: Box<dyn PacketSynchronizer>,
    pub work_queue: Option<Arc<WorkQueue>>,
    pub channels: T,
}

pub trait Reader {
    fn try_receive(&mut self, timeout: Duration) -> Result<(), ChannelError>;
}

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

        impl<$($T: Clone),+> OrderedBuffer for $struct_name<$($T),+> {
            fn get(&mut self, channel: &str, version: &DataVersion) -> Result<Option<UntypedPacket>, BufferError> {
                $(
                    if channel == stringify!($T) {
                        return Ok(Some(self.$T.buffer.get(version).unwrap().clone().to_untyped()));
                    }
                )+

                Ok(None)
            }

            fn available_channels(&self) -> Vec<&str> {
                vec![$(stringify!($T),)+]
            }

            fn has_version(&self, channel: &str, version: &DataVersion) -> bool {
                $(
                    if channel == stringify!($T) {
                        return self.$T.buffer.get(version).is_some();
                    }
                )+
                false
            }

            fn peek(&self, channel: &str) -> Option<&DataVersion> {
                $(
                    if channel == stringify!($T) {
                        return self.$T.buffer.peek();
                    }
                )+
                None
            }

            fn pop(&mut self, channel: &str) -> Result<Option<UntypedPacket>, BufferError> {
                $(
                    if channel == stringify!($T) {
                        return Ok(Some(self.$T.buffer.pop().unwrap().clone().to_untyped()));
                    }
                )+
                Ok(None)
            }

            fn are_buffers_empty(&self) -> bool {
                [$(
                    self.$T.buffer.len() == 0,
                )+].iter().all(|b| *b)
            }
        }

        impl<$($T: Clone),+> Reader for $struct_name<$($T),+> {
            fn try_receive(&mut self, timeout: Duration) -> Result<(), ChannelError>{
                select! {
                    $(
                        recv(self.$T.channel.as_ref().unwrap().receiver) -> msg => Some(self.$T.buffer.insert(msg?)),
                    )+
                    default(timeout) => None,
                };
                Ok(())
            }
        }

        impl<$($T: Clone),+> $struct_name<$($T),+> {
            fn new($($T: RtRingBuffer<$T>),+) -> Self {
                Self {
                    $(
                        $T: BufferReceiver {buffer: $T, channel: None},
                    )+
                }
            }

            $(
                fn $T(&mut self) -> &mut BufferReceiver<$T, RtRingBuffer<$T>> {
                    &mut self.$T
                }
            )+
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

impl<T: Reader> ReadChannel<T> {
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

    pub fn start(&mut self, _: usize, work_queue: Arc<WorkQueue>) -> () {
        self.work_queue = Some(work_queue);
    }

    pub fn stop(&mut self) -> () {}
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::buffers::single_buffers::FixedSizeBuffer;
    use crate::buffers::single_buffers::RtRingBuffer;
    use crate::buffers::synchronizers::timestamp::TimestampSynchronizer;

    use crate::channels::typed_channel;

    use crate::channels::ReadChannel;
    use crate::channels::SenderChannel;

    use crate::packet::Packet;
    use crate::packet::WorkQueue;
    use crate::DataVersion;

    use super::ReadChannel2;

    fn test_read_channel() -> (
        ReadChannel<ReadChannel2<String, String>>,
        SenderChannel<String>,
    ) {
        let synch_strategy = Box::new(TimestampSynchronizer::default());
        let read_channel2 = ReadChannel2::new(
            RtRingBuffer::<String>::new(2, true),
            RtRingBuffer::<String>::new(2, true),
        );
        let mut read_channel = ReadChannel::new(
            synch_strategy,
            Some(Arc::new(WorkQueue::default())),
            read_channel2,
        );

        let (channel_sender, channel_receiver) = typed_channel::<String>();
        read_channel.channels.c1().link(channel_receiver);

        (read_channel, channel_sender)
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
            read_channel.channels.c1().try_read().ok().unwrap(),
            DataVersion { timestamp: 1 }
        );
    }

    #[test]
    fn test_read_channel_try_read_returns_error_when_push_if_not_initialized() {
        let (mut read_channel, _) = test_read_channel();
        assert!(read_channel.channels.c1().try_read().is_err());
        assert!(read_channel.channels.c2().try_read().is_err());
    }

    #[test]
    fn test_read_channel_try_read_returns_error_when_buffer_is_full() {
        // let buffered_data = Arc::new(Mutex::new(BoundedBufferedData::<FixedSizeBTree>::new(
        //     2, true,
        // )));
        let synch_strategy = Box::new(TimestampSynchronizer::default());
        let read_channel2 = ReadChannel2::new(
            RtRingBuffer::<String>::new(2, true),
            RtRingBuffer::<String>::new(2, true),
        );
        let mut read_channel = ReadChannel::new(
            synch_strategy,
            Some(Arc::new(WorkQueue::default())),
            read_channel2,
        );

        let (_, channel_receiver) = typed_channel::<String>();
        read_channel.channels.c1().link(channel_receiver);

        let (_, channel_receiver) = typed_channel::<String>();
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
}
