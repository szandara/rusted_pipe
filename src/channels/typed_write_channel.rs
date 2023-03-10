use crate::DataVersion;

use super::{ChannelError, Packet, SenderChannel};

//unsafe impl Send for WriteChannel {}

pub struct BufferWriter<U> {
    pub channels: Vec<SenderChannel<U>>,
}

impl<U: Clone> BufferWriter<U> {
    pub fn default() -> Self {
        Self { channels: vec![] }
    }
    pub fn link(&mut self, sender: SenderChannel<U>) {
        self.channels.push(sender);
    }
    pub fn write(&self, data: U, version: &DataVersion) -> Result<(), ChannelError> {
        self.channels
            .iter()
            .map(|sender| Ok(sender.send(Packet::<U>::new(data.clone(), version.clone()))?))
            .collect::<Result<(), ChannelError>>()?;
        Ok(())
    }
}

macro_rules! write_channels {
    ($struct_name:ident, $($T:ident),+) => {
       struct $struct_name<$($T: Clone),+> {
            $(
                $T: BufferWriter<$T>,
            )+
        }

        impl<$($T: Clone),+> $struct_name<$($T),+> {
            pub fn create() -> Self {
                Self {
                    $(
                        $T: BufferWriter::<$T>::default(),
                    )+
                }
            }

            $(
                pub fn $T(&mut self) -> &mut BufferWriter<$T> {
                    &mut self.$T
                }
            )+
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

#[cfg(test)]
mod tests {
    use crate::channels::typed_channel;
    use crate::channels::untyped_channel;
    use crate::channels::ChannelID;
    use crate::channels::ReceiverChannel;
    use crate::channels::UntypedReceiverChannel;
    use crate::channels::WriteChannel;
    use crate::ChannelError;
    use crate::DataVersion;

    use super::WriteChannel3;

    fn create_write_channel() -> (
        WriteChannel3<String, String, String>,
        ReceiverChannel<String>,
    ) {
        let mut write_channel = WriteChannel3::<String, String, String>::create();
        let crossbeam_channels = typed_channel::<String>();
        write_channel.c1.link(crossbeam_channels.0);
        (write_channel, crossbeam_channels.1)
    }

    #[test]
    fn test_send_on_existing_channel_fans_out_to_all_receivers() {
        let (mut write_channel, existing_read_channel) = create_write_channel();

        let mut read_channels = vec![];

        for _n in 0..2 {
            let channel = typed_channel::<String>();
            write_channel.c1.link(channel.0);
            read_channels.push(channel.1)
        }

        write_channel
            .c1
            .write("TestData".to_string(), &DataVersion { timestamp: 1 })
            .unwrap();

        for channel in read_channels {
            assert_eq!(*channel.try_receive().unwrap().data, "TestData".to_string());
        }
    }
}
