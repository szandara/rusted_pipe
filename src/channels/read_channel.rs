use super::ChannelError;
use super::Packet;

use super::ReceiverChannel;
use crate::buffers::single_buffers::FixedSizeBuffer;
use crate::buffers::single_buffers::RtRingBuffer;
use crate::buffers::synchronizers::PacketSynchronizer;
use crate::buffers::BufferError;
use crate::buffers::BufferIterator;
use crate::packet::typed::PacketSetTrait;
use crate::packet::ReadEvent;
use crate::packet::UntypedPacket;
use crate::packet::WorkQueue;
use crate::DataVersion;

use crossbeam::channel::select;
use crossbeam::channel::Sender;
use paste::item;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

pub struct BufferReceiver<U, T: FixedSizeBuffer<Data = U> + ?Sized> {
    pub buffer: Box<T>,
    pub channel: Option<ReceiverChannel<U>>,
}

pub trait ChannelBuffer {
    fn available_channels(&self) -> Vec<&str>;

    fn has_version(&self, channel: &str, version: &DataVersion) -> bool;

    fn peek(&self, channel: &str) -> Option<&DataVersion>;

    fn pop(&mut self, channel: &str) -> Result<Option<UntypedPacket>, BufferError>;

    fn iterator(&self, channel: &str) -> Option<Box<BufferIterator>>;

    fn are_buffers_empty(&self) -> bool;

    fn try_receive(&mut self, timeout: Duration) -> Result<bool, ChannelError>;
}

pub struct NoBuffer {}

impl OutputDelivery for NoBuffer {
    type OUTPUT = ReadChannel1PacketSet<String>;

    fn get_packets_for_version(
        &mut self,
        _data_versions: &HashMap<String, Option<DataVersion>>,
        _exact_match: bool,
    ) -> Option<Self::OUTPUT> {
        todo!()
    }
}

impl ChannelBuffer for NoBuffer {
    fn available_channels(&self) -> Vec<&str> {
        todo!()
    }

    fn has_version(&self, _: &str, _: &DataVersion) -> bool {
        todo!()
    }

    fn peek(&self, _: &str) -> Option<&DataVersion> {
        todo!()
    }

    fn pop(&mut self, _: &str) -> Result<Option<UntypedPacket>, BufferError> {
        todo!()
    }

    fn are_buffers_empty(&self) -> bool {
        todo!()
    }

    fn try_receive(&mut self, _: Duration) -> Result<bool, ChannelError> {
        todo!()
    }

    fn iterator(&self, _: &str) -> Option<Box<BufferIterator>> {
        todo!()
    }
}

impl<U, T: FixedSizeBuffer<Data = U> + ?Sized> BufferReceiver<U, T> {
    pub fn link(&mut self, receiver: ReceiverChannel<U>) {
        self.channel = Some(receiver);
    }

    pub fn try_read(&mut self) -> Result<DataVersion, ChannelError> {
        if let Some(channel) = self.channel.as_ref() {
            let packet = channel.try_receive()?;
            let version = packet.version;
            self.buffer.insert(packet)?;
            return Ok(version);
        }
        Err(ChannelError::NotInitializedError)
    }
}

pub struct ReadChannel<T: OutputDelivery> {
    pub synch_strategy: Box<dyn PacketSynchronizer>,
    pub work_queue: Option<Arc<WorkQueue<T::OUTPUT>>>,
    pub channels: Arc<Mutex<T>>,
}

unsafe impl<T: OutputDelivery + ChannelBuffer + Send> Sync for ReadChannel<T> {}
unsafe impl<T: OutputDelivery + ChannelBuffer + Send> Send for ReadChannel<T> {}

fn get_data<T: Clone>(
    buffer: &mut RtRingBuffer<T>,
    data_version: &Option<DataVersion>,
    exact_match: bool,
) -> Option<Packet<T>> {
    loop {
        let removed_packet = buffer.pop();

        if let Some(entry) = removed_packet {
            if let Some(data_version) = data_version {
                if entry.version == *data_version {
                    return Some(entry);
                } else if exact_match {
                    break;
                }
            }
            if exact_match {
                break;
            }
        } else {
            break;
        }
    }
    return None;
}

pub trait Reader {}

macro_rules! read_channels {
    ($struct_name:ident, $($T:ident),+) => {
        item!{
            use crate::packet::typed::[<$struct_name PacketSet>];
            #[allow(non_camel_case_types)]
            pub struct $struct_name<$($T: Clone),+> {
                $(
                    $T: BufferReceiver<$T, RtRingBuffer<$T>>,
                )+
            }
        }

        #[allow(non_camel_case_types)]
        impl<$($T: Clone),+> Reader for $struct_name<$($T),+> {}

        #[allow(non_camel_case_types)]
        impl<$($T: Clone),+> ChannelBuffer for $struct_name<$($T),+> {
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
                        if let Some(data) = self.$T.buffer.pop() {
                            return Ok(Some(data.to_untyped()));
                        }

                        return Ok(None);
                    }
                )+
                Ok(None)
            }

            fn are_buffers_empty(&self) -> bool {
                [$(
                    self.$T.buffer.len() == 0,
                )+].iter().all(|b| *b)
            }

            fn try_receive(&mut self, timeout: Duration) -> Result<bool, ChannelError>{
                let has_data = select! {
                    $(
                        recv(self.$T.channel
                            .as_ref()
                            .expect(&format!("Channel not connected {} {}",
                                stringify!($struct_name), stringify!($T))).receiver) -> msg =>
                                    {Some(self.$T.buffer.insert(msg?))},
                    )+
                    default(timeout) => None,
                };
                Ok(has_data.is_some())
            }

            fn iterator(&self, channel: &str) -> Option<Box<BufferIterator>> {
                $(
                    if channel == stringify!($T) {
                        return Some(self.$T.buffer.iter());
                    }
                )+
                None
            }
        }

        #[allow(non_camel_case_types, dead_code)]
        impl<$($T: Clone),+> $struct_name<$($T),+> {
            pub fn create($($T: RtRingBuffer<$T>),+) -> Self {
                Self {
                    $(
                        $T: BufferReceiver {buffer: Box::new($T), channel: None},
                    )+
                }
            }

            $(
                pub fn $T(&mut self) -> &mut BufferReceiver<$T, RtRingBuffer<$T>> {
                    &mut self.$T
                }
            )+


        }

        item! {
            #[allow(non_camel_case_types)]
            impl<$($T: Clone),+> OutputDelivery for $struct_name<$($T),+> {
                type OUTPUT = [<$struct_name PacketSet>]<$($T),+>;

                fn get_packets_for_version(
                    &mut self,
                    data_versions: &HashMap<String, Option<DataVersion>>,
                    exact_match: bool,
                ) -> Option<Self::OUTPUT> {
                    let mut result = [<$struct_name PacketSet>]::<$($T),+>::create();

                    $(
                        let channel = stringify!($T);
                        let version = data_versions.get(channel).expect("Cannot find channel {channel}");
                        let data = get_data(&mut self.$T.buffer, version, exact_match);
                        result.[<set_ $T>](data);
                    )+


                    Some(result)
                }
            }
        }
    };
}

pub trait OutputDelivery {
    type OUTPUT: PacketSetTrait + Send;
    fn get_packets_for_version(
        &mut self,
        data_versions: &HashMap<String, Option<DataVersion>>,
        exact_match: bool,
    ) -> Option<Self::OUTPUT>;
}

read_channels!(ReadChannel1, c1);
read_channels!(ReadChannel2, c1, c2);
read_channels!(ReadChannel3, c1, c2, c3);
read_channels!(ReadChannel4, c1, c2, c3, c4);
read_channels!(ReadChannel5, c1, c2, c3, c4, c5);
read_channels!(ReadChannel6, c1, c2, c3, c4, c5, c6);
read_channels!(ReadChannel7, c1, c2, c3, c4, c5, c6, c7);
read_channels!(ReadChannel8, c1, c2, c3, c4, c5, c6, c7, c8);

unsafe impl<T: Send> Send for ReadEvent<T> {}

impl<T: OutputDelivery + ChannelBuffer + 'static> ReadChannel<T> {
    pub fn new(
        synch_strategy: Box<dyn PacketSynchronizer>,
        work_queue: Option<Arc<WorkQueue<T::OUTPUT>>>,
        channels: T,
    ) -> Self {
        ReadChannel {
            synch_strategy,
            work_queue,
            channels: Arc::new(Mutex::new(channels)),
        }
    }

    pub fn synchronize(&mut self) {
        if let Some(queue) = self.work_queue.as_ref() {
            let synch = self.synch_strategy.synchronize(self.channels.clone());
            if let Some(sync) = synch {
                println!("{:?}", sync);
                let value = self
                    .channels
                    .lock()
                    .unwrap()
                    .get_packets_for_version(&sync, false);
                if let Some(value) = value {
                    queue.push(value);
                }
            }
        }
    }

    pub fn read(&mut self, channel_id: String, done_notification: Sender<String>) -> bool {
        let data = self
            .channels
            .lock()
            .unwrap()
            .try_receive(Duration::from_millis(10));
        match data {
            Ok(has_data) => {
                if has_data {
                    self.synchronize()
                }
            }
            Err(err) => {
                eprintln!("Exception while reading {err:?}");
                match err {
                    crate::channels::ChannelError::ReceiveError(_) => {
                        if self.channels.lock().unwrap().are_buffers_empty() {
                            done_notification.send(channel_id).unwrap();
                        }
                        eprintln!("Channel is disonnected, closing");
                        return false;
                    }
                    _ => {
                        eprintln!("Sending done {channel_id}");
                        if self.channels.lock().unwrap().are_buffers_empty() {
                            done_notification.send(channel_id).unwrap();
                        }
                    }
                }
            }
        }
        true
    }

    pub fn start(&mut self, work_queue: Arc<WorkQueue<T::OUTPUT>>) {
        self.work_queue = Some(work_queue);
    }

    pub fn stop(&mut self) {}
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

    use crate::packet::typed::ReadChannel2PacketSet;
    use crate::packet::Packet;
    use crate::packet::WorkQueue;
    use crate::DataVersion;

    use super::ReadChannel2;

    fn test_read_channel() -> (
        ReadChannel<ReadChannel2<String, String>>,
        SenderChannel<String>,
    ) {
        let synch_strategy = Box::new(TimestampSynchronizer::default());
        let read_channel2 = ReadChannel2::create(
            RtRingBuffer::<String>::new(2, true),
            RtRingBuffer::<String>::new(2, true),
        );
        let read_channel = ReadChannel::new(
            synch_strategy,
            Some(Arc::new(WorkQueue::default())),
            read_channel2,
        );

        let (channel_sender, channel_receiver) = typed_channel::<String>();
        read_channel
            .channels
            .lock()
            .unwrap()
            .c1()
            .link(channel_receiver);

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
        read_channel.start(Arc::new(WorkQueue::default()));
        assert_eq!(
            read_channel
                .channels
                .lock()
                .unwrap()
                .c1()
                .try_read()
                .ok()
                .unwrap(),
            DataVersion { timestamp: 1 }
        );
    }

    #[test]
    fn test_read_channel_try_read_returns_error_when_push_if_not_initialized() {
        let (read_channel, _) = test_read_channel();
        assert!(read_channel
            .channels
            .lock()
            .unwrap()
            .c1()
            .try_read()
            .is_err());
        assert!(read_channel
            .channels
            .lock()
            .unwrap()
            .c2()
            .try_read()
            .is_err());
    }

    #[test]
    fn test_read_channel_try_read_returns_error_when_buffer_is_full() {
        let synch_strategy = Box::new(TimestampSynchronizer::default());
        let read_channel2 = ReadChannel2::create(
            RtRingBuffer::<String>::new(2, true),
            RtRingBuffer::<String>::new(2, true),
        );
        let mut read_channel = ReadChannel::new(
            synch_strategy,
            Some(Arc::new(
                WorkQueue::<ReadChannel2PacketSet<String, String>>::default(),
            )),
            read_channel2,
        );

        let (_, channel_receiver) = typed_channel::<String>();
        read_channel
            .channels
            .lock()
            .unwrap()
            .c1()
            .link(channel_receiver);

        let (_, channel_receiver) = typed_channel::<String>();
        read_channel
            .channels
            .lock()
            .unwrap()
            .c2()
            .link(channel_receiver);

        let mut packet = Packet::new("my_data".to_string(), DataVersion { timestamp: 1 });

        read_channel.start(Arc::new(WorkQueue::default()));
        read_channel
            .channels
            .lock()
            .unwrap()
            .c1()
            .buffer
            .insert(packet.clone())
            .unwrap();
        packet.version.timestamp = 2;
        read_channel
            .channels
            .lock()
            .unwrap()
            .c1()
            .buffer
            .insert(packet.clone())
            .unwrap();
        packet.version.timestamp = 3;
        assert!(read_channel
            .channels
            .lock()
            .unwrap()
            .c1()
            .buffer
            .insert(packet.clone())
            .is_err());
    }
}
