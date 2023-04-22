//! Module for the generic ReadChannel. A ReadChannel is used by processing nodes
//! to receive input data while waiting idle. Reach channels' main logic is to
//! allocate space for the incoming data and synchronize that data using the
//! user configured syncrhonizer.
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use crossbeam::channel::Sender;

use crate::{
    buffers::{single_buffers::RtRingBuffer, synchronizers::PacketSynchronizer},
    packet::work_queue::WorkQueue,
};

use std::collections::HashMap;

use crate::{
    buffers::{single_buffers::FixedSizeBuffer, BufferIterator},
    packet::typed::PacketSetTrait,
    DataVersion,
};

use super::{ChannelError, ChannelID, Packet, ReadChannelTrait, ReceiverChannel};

/// A struct that holds a single FixedSizeBuffer and
/// an optional ReceiverChannel that maps its data into that buffer.
pub struct BufferReceiver<T: FixedSizeBuffer + ?Sized> {
    /// The fixed buffer implementation.
    pub buffer: Box<T>,
    /// An optional ReceiverChannel with the data type.
    /// It can be None if the channel is not yet connected.
    pub channel: Option<ReceiverChannel<T::Data>>,
}

impl<T: FixedSizeBuffer + ?Sized> BufferReceiver<T> {
    /// Link a receiver channel to a data transport. From now on
    /// the channel can start reading data.
    pub fn link(&mut self, receiver: ReceiverChannel<T::Data>) {
        if self.channel.is_some() {
            panic!("Channel is already linked!");
        }
        self.channel = Some(receiver);
    }

    /// Tries to read data from the data transport channel or an error
    /// it the channel has no connection yet.
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

/// A trait for buffer data manipulation. Mostly used by the synchronizer
/// to find matching tuples.
pub trait ChannelBuffer {
    /// A list of named channels.
    fn available_channels(&self) -> Vec<&ChannelID>;
    /// True if a channel has the given data version.
    ///
    /// * Arguments
    /// `channel` - The name of the channel to inquire.
    /// `version` - The data version to find.
    fn has_version(&self, channel: &ChannelID, version: &DataVersion) -> bool;
    /// Gets the minimum version over all channels.
    fn min_version(&self) -> Option<&DataVersion>;
    /// Returns a reference of the head of the buffer in `channel`.
    ///
    /// * Arguments
    /// `channel` - The name of the channel to inquire.
    fn peek(&self, channel: &ChannelID) -> Option<&DataVersion>;
    /// Returns an iterator in `channel`.
    ///
    /// * Arguments
    /// `channel` - The name of the channel to inquire.
    fn iterator(&self, channel: &ChannelID) -> Option<Box<BufferIterator>>;
    /// Returns true if there is no data in any buffer.
    fn are_buffers_empty(&self) -> bool;
    /// Tries to read data for up to 'timeout' duration.
    ///
    /// * Arguments
    /// `timeout` - How long to wait for the data.
    fn try_receive(&mut self, timeout: Duration) -> Result<bool, ChannelError>;
}

/// A trait for generating packet set from an existing ReadChannel.
pub trait InputGenerator {
    type INPUT: PacketSetTrait + Send;
    /// Pulls the data specified in data_versions out from the buffers.
    /// For each channel it drops the data before the chosen version.
    ///
    /// * Arguments
    /// `data_versions` - A map containing a channel name an an optional data version.
    /// `exact_match` - Only returns data if an exact match exists.
    ///
    /// Returns None if the data cannot be fetched.
    fn get_packets_for_version(
        &mut self,
        data_versions: &HashMap<ChannelID, Option<DataVersion>>,
        exact_match: bool,
    ) -> Option<Self::INPUT>;

    fn create_channels(buffer_size: usize, block_on_full: bool) -> Self;
}

/// A generic ReadChannel that holds a reference to a struct that has
/// a set of trait for managing the internal channels.
pub struct ReadChannel<T: InputGenerator + ChannelBuffer + Send> {
    /// What synch strategy to use when trying to synchronize the buffers.
    pub synch_strategy: Box<dyn PacketSynchronizer>,
    /// A work queue that holds the already matched tuples.
    pub work_queue: Option<WorkQueue<T::INPUT>>,
    /// A reference to the channels of the ReadChannel.
    pub channels: Arc<Mutex<T>>,
}

unsafe impl<T: InputGenerator + ChannelBuffer + Send> Sync for ReadChannel<T> {}
unsafe impl<T: InputGenerator + ChannelBuffer + Send> Send for ReadChannel<T> {}

impl<T: InputGenerator + ChannelBuffer + Send + 'static> ReadChannelTrait for ReadChannel<T> {
    type Data = T::INPUT;

    fn read(&mut self, channel_id: String, done_notification: Sender<String>) -> bool {
        let data = self
            .channels
            .lock()
            .unwrap()
            .try_receive(Duration::from_millis(100));
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

    fn start(&mut self, work_queue: WorkQueue<Self::Data>) {
        self.work_queue = Some(work_queue);
    }

    fn stop(&mut self) {}
}

impl<T: InputGenerator + ChannelBuffer + Send + 'static> ReadChannel<T> {
    pub fn new(
        synch_strategy: Box<dyn PacketSynchronizer>,
        work_queue: Option<WorkQueue<T::INPUT>>,
        channels: T,
    ) -> Self {
        ReadChannel {
            synch_strategy,
            work_queue,
            channels: Arc::new(Mutex::new(channels)),
        }
    }

    pub fn create(
        block_channel_full: bool,
        channel_buffer_size: usize,
        process_buffer_size: usize,
        synch_strategy: Box<dyn PacketSynchronizer>,
    ) -> Self {
        let channels = T::create_channels(channel_buffer_size, block_channel_full);
        Self {
            synch_strategy,
            work_queue: Some(WorkQueue::<T::INPUT>::new(process_buffer_size)),
            channels: Arc::new(Mutex::new(channels)),
        }
    }

    pub fn synchronize(&mut self) {
        if let Some(queue) = self.work_queue.as_ref() {
            let synch = self.synch_strategy.synchronize(self.channels.clone());
            if let Some(sync) = synch {
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
}

pub fn get_data<T>(
    buffer: &mut RtRingBuffer<T>,
    data_version: &Option<DataVersion>,
    exact_match: bool,
) -> Option<Packet<T>> {
    if data_version.is_none() {
        return None;
    }
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

#[cfg(test)]
mod tests {
    use crate::buffers::single_buffers::RtRingBuffer;
    use crate::buffers::synchronizers::timestamp::TimestampSynchronizer;

    use crate::channels::read_channel::ReadChannel;
    use crate::channels::read_channel::ReadChannelTrait;
    use crate::channels::typed_channel;
    use crate::channels::ChannelID;
    use crate::channels::ReceiverChannel;
    use crate::channels::SenderChannel;

    use crate::channels::typed_read_channel::ReadChannel2;
    use crate::channels::untyped_channel;
    use crate::channels::untyped_read_channel::UntypedReadChannel;
    use crate::packet::typed::ReadChannel2PacketSet;
    use crate::packet::work_queue::WorkQueue;
    use crate::packet::Packet;
    use crate::packet::Untyped;
    use crate::DataVersion;

    use super::BufferReceiver;

    fn create_typed_read_channel() -> (
        ReadChannel<ReadChannel2<String, String>>,
        SenderChannel<String>,
    ) {
        let synch_strategy = Box::new(TimestampSynchronizer::default());
        let read_channel2 = ReadChannel2::create(
            RtRingBuffer::<String>::new(2, true),
            RtRingBuffer::<String>::new(2, true),
        );
        let read_channel =
            ReadChannel::new(synch_strategy, Some(WorkQueue::default()), read_channel2);

        let (channel_sender, channel_receiver) = typed_channel::<String>();
        read_channel
            .channels
            .lock()
            .unwrap()
            .c1()
            .link(channel_receiver);

        (read_channel, channel_sender)
    }

    fn create_untyped_buffer(
        channel: ReceiverChannel<Box<Untyped>>,
    ) -> BufferReceiver<RtRingBuffer<Box<Untyped>>> {
        let buffer = RtRingBuffer::<Box<Untyped>>::new(2, true);
        BufferReceiver {
            buffer: Box::new(buffer),
            channel: Some(channel),
        }
    }

    fn create_untyped_read_channel(
    ) -> (ReadChannel<UntypedReadChannel>, SenderChannel<Box<Untyped>>) {
        let synch_strategy = Box::new(TimestampSynchronizer::default());
        let read_channel2 = UntypedReadChannel::default();
        let read_channel =
            ReadChannel::new(synch_strategy, Some(WorkQueue::default()), read_channel2);

        let (channel_sender, channel_receiver) = untyped_channel();
        read_channel
            .channels
            .lock()
            .unwrap()
            .add_channel(
                ChannelID::from("test0"),
                create_untyped_buffer(channel_receiver),
            )
            .unwrap();

        (read_channel, channel_sender)
    }

    #[test]
    fn test_read_channel_try_read_returns_ok_if_data() {
        let (mut read_channel, crossbeam_channels) = create_typed_read_channel();
        crossbeam_channels
            .send(Packet::new(
                "my_data".to_string(),
                DataVersion { timestamp_ns: 1 },
            ))
            .unwrap();
        read_channel.start(WorkQueue::default());
        assert_eq!(
            read_channel
                .channels
                .lock()
                .unwrap()
                .c1()
                .try_read()
                .ok()
                .unwrap(),
            DataVersion { timestamp_ns: 1 }
        );
    }

    #[test]
    fn test_untyped_read_channel_try_read_returns_ok_if_data() {
        let (mut read_channel, crossbeam_channels) = create_untyped_read_channel();
        crossbeam_channels
            .send(Packet::new("my_data".to_string(), DataVersion { timestamp_ns: 1 }).to_untyped())
            .unwrap();
        read_channel.start(WorkQueue::default());
        assert_eq!(
            read_channel
                .channels
                .lock()
                .unwrap()
                .get_channel_mut(&ChannelID::from("test0"))
                .unwrap()
                .try_read()
                .unwrap(),
            DataVersion { timestamp_ns: 1 }
        );

        assert!(read_channel
            .channels
            .lock()
            .unwrap()
            .get_channel_mut(&ChannelID::from("test0"))
            .unwrap()
            .try_read()
            .is_err());
    }

    #[test]
    fn test_read_channel_try_read_returns_error_when_push_if_not_initialized() {
        let (read_channel, _) = create_typed_read_channel();
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
            Some(WorkQueue::<ReadChannel2PacketSet<String, String>>::default()),
            read_channel2,
        );

        let (s1, channel_receiver) = typed_channel::<String>();
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

        let mut packet = Packet::new("my_data".to_string(), DataVersion { timestamp_ns: 1 });

        read_channel.start(WorkQueue::default());
        s1.send(packet.clone()).unwrap();

        packet.version.timestamp_ns = 2;
        s1.send(packet.clone()).unwrap();

        packet.version.timestamp_ns = 3;
        s1.send(packet.clone()).unwrap();
        //assert!(read_channel.read("c1".to_string(), done.0));
        assert!(read_channel
            .channels
            .lock()
            .unwrap()
            .c1()
            .try_read()
            .is_ok());
        assert!(read_channel
            .channels
            .lock()
            .unwrap()
            .c1()
            .try_read()
            .is_ok());
        assert!(read_channel
            .channels
            .lock()
            .unwrap()
            .c1()
            .try_read()
            .is_err());
    }

    #[test]
    #[should_panic]
    fn test_read_channel_panics_if_already_linked() {
        let (_, channel_receiver) = typed_channel::<String>();
        let (read_channel, _) = create_typed_read_channel();
        read_channel
            .channels
            .lock()
            .unwrap()
            .c1()
            .link(channel_receiver);
    }
}
