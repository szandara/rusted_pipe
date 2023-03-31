use crate::packet::Packet;
use paste::item;

pub trait PacketSetTrait {}

macro_rules! typed_packet {
    ($struct_name:ident, $($T:ident),+) => {
        #[allow(non_camel_case_types)]
        pub struct $struct_name<$($T: Clone),+> {
            $(
                $T : Option<Packet<$T>>,
            )+
        }

        #[allow(non_camel_case_types)]
        impl<$($T: Clone),+> PacketSetTrait for $struct_name<$($T),+>  {}

        #[allow(non_camel_case_types)]
        unsafe impl<$($T: Clone),+> Send for $struct_name<$($T),+>  {}

        item! {
            #[allow(non_camel_case_types)]
            pub enum [<$struct_name Channels>] {
                $(
                    $T,
                )+
            }
        }


        #[allow(non_camel_case_types)]
        impl<$($T: Clone),+> $struct_name<$($T),+> {
            pub fn new($($T: Option<Packet<$T>>,)+) -> Self {
                Self {
                    $(
                        $T,
                    )+
                }
            }

            pub fn create() -> Self {
                Self {
                    $(
                        $T: Option::<Packet<$T>>::None,
                    )+
                }
            }

            $(
                pub fn $T(&self) -> Option<&Packet<$T>> {
                    if let Some(data) = self.$T.as_ref() {
                        return Some(&data);
                    }
                    None
                }
            )+

            item! {
                $(
                    pub fn [<$T _ owned>](&mut self) -> Option<Packet<$T>> {
                        if let Some(_) = self.$T {
                            return self.$T.take();
                        }
                        None
                    }
                )+
            }

            item! {
                $(
                    pub fn [<set _ $T>](&mut self, data: Option<Packet<$T>>) {
                        self.$T = data;
                    }
                )+
            }

            pub fn values(&self) -> ($(Option<&Packet<$T>>,)+) {
               (
                    $(
                        self.$T.as_ref(),
                    )+
               )
            }

            pub fn has_none(&self) -> bool {
                let values = vec![ $(
                    self.$T.is_some(),
                )+ ];
                values.iter().all(|v| *v)
            }
        }
    };
}

typed_packet!(ReadChannel1PacketSet, c1);
typed_packet!(ReadChannel2PacketSet, c1, c2);
typed_packet!(ReadChannel3PacketSet, c1, c2, c3);
typed_packet!(ReadChannel4PacketSet, c1, c2, c3, c4);
typed_packet!(ReadChannel5PacketSet, c1, c2, c3, c4, c5);
typed_packet!(ReadChannel6PacketSet, c1, c2, c3, c4, c5, c6);
typed_packet!(ReadChannel7PacketSet, c1, c2, c3, c4, c5, c6, c7);
typed_packet!(ReadChannel8PacketSet, c1, c2, c3, c4, c5, c6, c7, c8);

// impl TypedPacketSet {
//     pub fn new(data: IndexMap<ChannelID, Option<PacketWithAddress>>) -> Self {
//         PacketSet { data }
//     }

//     pub fn channels(&self) -> usize {
//         self.data.len()
//     }

//     pub fn values(&self) -> Vec<&Option<PacketWithAddress>> {
//         self.data.values().collect_vec()
//     }

//     pub fn has_none(&self) -> bool {
//         for v in self.data.values() {
//             if v.is_none() {
//                 return true;
//             }
//         }
//         false
//     }

//     pub fn get<T: 'static>(&self, channel_number: usize) -> Result<PacketView<T>, PacketError> {
//         match self
//             .data
//             .get_index(channel_number)
//             .ok_or(PacketError::MissingChannelIndex(channel_number))?
//             .1
//         {
//             Some(maybe_packet_with_address) => Ok(maybe_packet_with_address.1.deref::<T>()?),
//             None => Err(PacketError::MissingChannelData(channel_number)),
//         }
//     }

//     pub fn get_owned<T: 'static>(
//         &mut self,
//         channel_number: usize,
//     ) -> Result<Packet<T>, PacketError> {
//         match self
//             .data
//             .swap_remove_index(channel_number)
//             .ok_or(PacketError::MissingChannelIndex(channel_number))?
//             .1
//         {
//             Some(maybe_packet_with_address) => {
//                 Ok(maybe_packet_with_address.1.deref_owned::<T>()?)
//             }
//             None => Err(PacketError::MissingChannelData(channel_number)),
//         }
//     }

//     pub fn get_channel<T: 'static>(
//         &self,
//         channel_id: &ChannelID,
//     ) -> Result<PacketView<T>, PacketError> {
//         match self
//             .data
//             .get(channel_id)
//             .ok_or(PacketError::MissingChannel(channel_id.clone()))?
//         {
//             Some(maybe_packet_with_address) => Ok(maybe_packet_with_address.1.deref::<T>()?),
//             None => Err(PacketError::MissingChannel(channel_id.clone())),
//         }
//     }

//     pub fn get_channel_owned<T: 'static>(
//         &mut self,
//         channel_id: &ChannelID,
//     ) -> Result<Packet<T>, PacketError> {
//         match self
//             .data
//             .remove(channel_id)
//             .ok_or(PacketError::MissingChannel(channel_id.clone()))?
//         {
//             Some(maybe_packet_with_address) => {
//                 Ok(maybe_packet_with_address.1.deref_owned::<T>()?)
//             }
//             None => Err(PacketError::MissingChannel(channel_id.clone())),
//         }
//     }
// }
