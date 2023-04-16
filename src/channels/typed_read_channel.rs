use super::read_channel::get_data;
use super::read_channel::BufferReceiver;
use super::read_channel::ChannelBuffer;
use super::read_channel::InputGenerator;
use super::ChannelError;
use crate::buffers::single_buffers::FixedSizeBuffer;
use crate::buffers::single_buffers::RtRingBuffer;
use crate::buffers::BufferIterator;
use crate::packet::work_queue::ReadEvent;
use crate::DataVersion;

use crossbeam::channel::select;
use paste::item;
use std::collections::HashMap;
use std::time::Duration;

macro_rules! read_channels {
    ($struct_name:ident, $($T:ident),+) => {
        item!{
            use crate::packet::typed::[<$struct_name PacketSet>];
            #[allow(non_camel_case_types)]
            pub struct $struct_name<$($T: Clone),+> {
                $(
                    $T: BufferReceiver<RtRingBuffer<$T>>,
                )+
            }
        }

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
                                    {println!("On buffer {:?}, got data on channel {:?}", stringify!($struct_name), stringify!($T));Some(self.$T.buffer.insert(msg?))},
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

            fn min_version(&self) -> Option<&DataVersion> {
                let vals = [$(
                    self.$T.buffer.peek(),
                )+];
                let mut min = None;
                for val in vals {
                    if min.is_none() && val.is_some() {
                        min = val;
                        continue;
                    }
                    if let Some(val) = val {
                        if val.timestamp_ns < min.unwrap().timestamp_ns  {
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
                        $T: BufferReceiver {buffer: Box::new($T), channel: None},
                    )+
                }
            }

            $(
                pub fn $T(&mut self) -> &mut BufferReceiver<RtRingBuffer<$T>> {
                    &mut self.$T
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
                ) -> $struct_name<$($T),+> {
                    $struct_name::create(
                        $(RtRingBuffer::<$T>::new(buffer_size, block_on_full)),+
                    )
                }

                fn get_packets_for_version(
                    &mut self,
                    data_versions: &HashMap<String, Option<DataVersion>>,
                    exact_match: bool,
                ) -> Option<Self::INPUT> {
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
        _data_versions: &HashMap<String, Option<DataVersion>>,
        _exact_match: bool,
    ) -> Option<Self::INPUT> {
        todo!()
    }

    fn create_channels(_buffer_size: usize, _block_on_full: bool) -> Self {
        todo!()
    }
}

impl ChannelBuffer for NoBuffer {
    fn available_channels(&self) -> Vec<&str> {
        todo!()
    }

    fn min_version(&self) -> Option<&DataVersion> {
        todo!();
    }

    fn has_version(&self, _: &str, _: &DataVersion) -> bool {
        todo!()
    }

    fn peek(&self, _: &str) -> Option<&DataVersion> {
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

unsafe impl<T: Send> Send for ReadEvent<T> {}
