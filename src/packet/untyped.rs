use std::any::TypeId;

use indexmap::IndexMap;
use itertools::Itertools;

use crate::PacketError;

use super::{typed::PacketSetTrait, Packet, UntypedPacket, UntypedPacketCast};

impl UntypedPacketCast for UntypedPacket {
    fn deref_owned<T: 'static>(mut self) -> Result<Packet<Box<T>>, PacketError> {
        match self.data.downcast::<T>() {
            Ok(casted_type) => Ok(Packet::<Box<T>> {
                data: casted_type,
                version: self.version,
            }),
            Err(untyped_box) => {
                self.data = untyped_box;
                Err(PacketError::UnexpectedDataType(TypeId::of::<T>()))
            }
        }
    }
}

#[derive(Default, Debug)]
pub struct UntypedPacketSet {
    data: IndexMap<String, Option<UntypedPacket>>,
}

impl UntypedPacketSet {
    pub fn new(data: IndexMap<String, Option<UntypedPacket>>) -> Self {
        UntypedPacketSet { data }
    }

    pub fn channels(&self) -> usize {
        self.data.len()
    }

    pub fn values(&self) -> Vec<&Option<UntypedPacket>> {
        self.data.values().collect_vec()
    }

    pub fn has_none(&self) -> bool {
        for v in self.data.values() {
            if v.is_none() {
                return true;
            }
        }
        false
    }

    pub fn get_owned<T: 'static>(
        &mut self,
        channel_number: usize,
    ) -> Result<Packet<Box<T>>, PacketError> {
        match self
            .data
            .swap_remove_index(channel_number)
            .ok_or(PacketError::MissingChannelIndex(channel_number))?
            .1
        {
            Some(maybe_packet_with_address) => Ok(maybe_packet_with_address.deref_owned::<T>()?),
            None => Err(PacketError::MissingChannelData(channel_number)),
        }
    }

    pub fn get_channel_owned<T: 'static>(
        &mut self,
        channel_id: &String,
    ) -> Result<Packet<Box<T>>, PacketError> {
        match self
            .data
            .remove(channel_id)
            .ok_or(PacketError::MissingChannel(channel_id.clone()))?
        {
            Some(maybe_packet_with_address) => Ok(maybe_packet_with_address.deref_owned::<T>()?),
            None => Err(PacketError::MissingChannel(channel_id.clone())),
        }
    }
}

impl PacketSetTrait for UntypedPacketSet {}
unsafe impl Send for UntypedPacketSet {}
