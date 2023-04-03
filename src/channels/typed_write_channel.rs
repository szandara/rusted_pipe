use super::{ChannelError, Packet, SenderChannel};
use crate::channels::WriteChannelTrait;
use crate::DataVersion;

pub struct TypedWriteChannel<OUTPUT: WriteChannelTrait + ?Sized> {
    pub writer: Box<OUTPUT>,
}

pub struct BufferWriter<U> {
    pub channels: Vec<SenderChannel<U>>,
}

impl<U: Clone + 'static> BufferWriter<U> {
    pub fn default() -> Self {
        Self { channels: vec![] }
    }
    pub fn link(&mut self, sender: SenderChannel<U>) {
        self.channels.push(sender);
    }
    pub fn write(&self, data: U, version: &DataVersion) -> Result<(), ChannelError> {
        self.channels
            .iter()
            .try_for_each(|sender| sender.send(Packet::<U>::new(data.clone(), version.clone())))?;
        Ok(())
    }
}

macro_rules! write_channels {
    ($struct_name:ident, $($T:ident),+) => {
        #[allow(non_camel_case_types)]
       pub struct $struct_name<$($T: Clone + 'static),+> {
            $(
                $T: BufferWriter<$T>,
            )+
        }

        #[allow(non_camel_case_types)]
        impl<$($T: Clone),+> WriteChannelTrait for $struct_name<$($T),+> {
            fn create() -> Self {
                Self {
                    $(
                        $T: BufferWriter::<$T>::default(),
                    )+
                }
            }
        }

        #[allow(non_camel_case_types, dead_code)]
        impl<$($T: Clone),+> $struct_name<$($T),+> {

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
    use crate::channels::WriteChannelTrait;

    use crate::channels::ReceiverChannel;

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
        let (mut write_channel, _existing_read_channel) = create_write_channel();

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
