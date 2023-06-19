//! A typed ReadChannel for a set of possible data inputs.
//! There are currently only a maximum of 8 typed entry channels.
use super::read_channel::get_data;
use super::read_channel::BufferReceiver;
use super::read_channel::ChannelBuffer;
use super::read_channel::InputGenerator;
use super::ChannelID;
use crate::{buffers::single_buffers::RtRingBuffer, graph::metrics::BufferMonitorBuilder};
use crossbeam::channel::Select;

use super::ChannelError;
use crate::buffers::single_buffers::FixedSizeBuffer;
use crate::buffers::single_buffers::LenTrait;
use crate::buffers::BufferIterator;
use crate::packet::work_queue::ReadEvent;
use crate::DataVersion;

use crossbeam::channel::select;
use paste::item;
use std::collections::HashMap;
use std::time::Duration;

struct NamedBufferReceiver<T: FixedSizeBuffer> {
    receiver: BufferReceiver<T>,
    id: ChannelID,
}

macro_rules! read_channels {
    ($struct_name:ident, $($T:ident),+) => {
        item!{
            use crate::packet::typed::[<$struct_name PacketSet>];
            #[allow(non_camel_case_types)]
            pub struct $struct_name<$($T: Clone),+> {
                $(
                    $T: NamedBufferReceiver<RtRingBuffer<$T>>,
                )+
                channels: Vec<ChannelID>,
            }
        }

        #[allow(non_camel_case_types)]
        impl<$($T: Clone + Send),+> ChannelBuffer for $struct_name<$($T),+> {
            fn available_channels(&self) -> Vec<&ChannelID> {
                self.channels.iter().collect()
            }

            fn has_version(&self, channel: &ChannelID, version: &DataVersion) -> bool {
                $(
                    if channel == &self.$T.id {
                        return self.$T.receiver.buffer.get(version).is_some();
                    }
                )+
                false
            }

            fn peek(&self, channel: &ChannelID) -> Option<&DataVersion> {
                $(
                    if channel == &self.$T.id {
                        return self.$T.receiver.buffer.peek();
                    }
                )+
                None
            }

            fn are_buffers_empty(&self) -> bool {
                [$(
                    self.$T.receiver.buffer.len() == 0,
                )+].iter().all(|b| *b)
            }

            fn wait_for_data(&self, timeout: Duration) -> Result<bool, ChannelError>{
                let mut select = Select::new();
                $(select.recv(&self.$T.receiver.channel.as_ref().expect(&format!("Node {} has no reader channel {}",
                    stringify!($struct_name), self.$T.id)).receiver);)+

                match select.ready_timeout(timeout) {
                    Err(_) => Ok(false),
                    Ok(_) => Ok(true),
                }
            }

            fn try_receive(&mut self, timeout: Duration) -> Result<Option<&ChannelID>, ChannelError>{
                let has_data = select! {
                    $(
                        recv(self.$T.receiver.channel
                            .as_ref()
                            .expect(&format!("Node {} has no reader channel {}",
                                stringify!($struct_name), self.$T.id)).receiver) -> msg =>
                                    {
                                        if self.$T.receiver.buffer.insert(msg?).is_ok() {
                                            Some(&self.$T.id)
                                        } else {
                                            None
                                        }
                                    },
                    )+
                    default(timeout) => None,
                };
                Ok(has_data)
            }

            fn iterator(&self, channel: &ChannelID) -> Option<Box<BufferIterator>> {
                $(
                    if channel == &self.$T.id {
                        return Some(self.$T.receiver.buffer.iter());
                    }
                )+
                None
            }

            fn min_version(&self) -> Option<&DataVersion> {
                let vals = [$(
                    self.$T.receiver.buffer.peek(),
                )+];
                let mut min = None;
                for val in vals {
                    if min.is_none() && val.is_some() {
                        min = val;
                        continue;
                    }
                    if let Some(val) = val {
                        // Safe to unwrap here
                        if val.timestamp_ns < min.unwrap_or(val).timestamp_ns  {
                            min = Some(val);
                        }
                    }
                }
                return min;
            }
        }

        #[allow(non_camel_case_types, dead_code)]
        impl<$($T: Clone + Send),+> $struct_name<$($T),+> {
            pub fn create($($T: RtRingBuffer<$T>),+) -> Self {
                Self {
                    $(
                        $T: NamedBufferReceiver {
                            receiver: BufferReceiver {buffer: Box::new($T), channel: None},
                            id: ChannelID::from(stringify!($T))
                        },
                    )+
                    channels: vec![$(ChannelID::from(stringify!($T)),)+],
                }
            }

            $(
                pub fn $T(&mut self) -> &mut BufferReceiver<RtRingBuffer<$T>> {
                    &mut self.$T.receiver
                }
            )+
        }

        item! {
            #[allow(non_camel_case_types)]
            impl<$($T: Clone + Send),+> InputGenerator for $struct_name<$($T),+> {
                type INPUT = [<$struct_name PacketSet>]<$($T),+>;

                fn create_channels(
                    buffer_size: usize,
                    block_on_full: bool,
                    monitor: BufferMonitorBuilder
                ) -> $struct_name<$($T),+> {
                    $struct_name::create(
                        $(RtRingBuffer::<$T>::new(buffer_size, block_on_full, monitor.make_channel(stringify!($T)))),+
                    )
                }

                fn get_packets_for_version(
                    &mut self,
                    data_versions: &HashMap<ChannelID, Option<DataVersion>>,
                    exact_match: bool,
                ) -> Option<Self::INPUT> {
                    let mut result = [<$struct_name PacketSet>]::<$($T),+>::create();

                    $(
                        let version = data_versions.get(&self.$T.id).expect(&format!("Cannot find channel {}", self.$T.id));
                        let data = get_data(&mut self.$T.receiver.buffer, version, exact_match);
                        result.[<set_ $T>](data);
                    )+


                    Some(result)
                }
            }
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

// Use this trait when no input channel is expected.
pub struct NoBuffer {}

impl InputGenerator for NoBuffer {
    type INPUT = ReadChannel1PacketSet<String>;

    fn get_packets_for_version(
        &mut self,
        _data_versions: &HashMap<ChannelID, Option<DataVersion>>,
        _exact_match: bool,
    ) -> Option<Self::INPUT> {
        todo!()
    }

    fn create_channels(
        _buffer_size: usize,
        _block_on_full: bool,
        _monitor: BufferMonitorBuilder,
    ) -> Self {
        todo!()
    }
}

impl ChannelBuffer for NoBuffer {
    fn available_channels(&self) -> Vec<&ChannelID> {
        todo!()
    }

    fn min_version(&self) -> Option<&DataVersion> {
        todo!();
    }

    fn has_version(&self, _: &ChannelID, _: &DataVersion) -> bool {
        todo!()
    }

    fn peek(&self, _: &ChannelID) -> Option<&DataVersion> {
        todo!()
    }

    fn are_buffers_empty(&self) -> bool {
        todo!()
    }

    fn try_receive(&mut self, _: Duration) -> Result<Option<&ChannelID>, ChannelError> {
        todo!()
    }

    fn iterator(&self, _: &ChannelID) -> Option<Box<BufferIterator>> {
        todo!()
    }

    fn wait_for_data(&self, _: Duration) -> Result<bool, ChannelError> {
        todo!()
    }
}

unsafe impl<T: Send> Send for ReadEvent<T> {}
