use crate::channels::WriteChannel;
use crate::packet::PacketSet;
use crate::RustedPipeError;

use super::Graph;
use super::Processor;
use graphviz_rust::dot_structures;
use graphviz_rust::parse;
// use opencv::core::Rect;
// use opencv::core::Scalar;
// use opencv::core::ToInputOutputArray;
// use opencv::imgproc::{rectangle, LineTypes};
// use opencv::prelude::Mat;
use std::fs;
use std::sync::Arc;
use std::sync::Mutex;
use thiserror::Error;

struct BoundinBoxRender {
    id: String,
}
impl BoundinBoxRender {
    fn default() -> Self {
        Self {
            id: "test".to_string(),
        }
    }
}

impl Processor for BoundinBoxRender {
    fn handle(
        &mut self,
        mut _input: PacketSet,
        output_channel: Arc<Mutex<WriteChannel>>,
    ) -> Result<(), RustedPipeError> {
        // let bbox = _input.get_owned::<Rect>(0).unwrap();
        // let mut image = _input.get_owned::<Mat>(1).unwrap();
        // let color = Scalar::from((255.0, 0.0, 0.0));
        // let thikness_px = 2;

        // let mut im_array = image.data.input_output_array().unwrap();
        // rectangle(
        //     &mut im_array,
        //     *bbox.data,
        //     color,
        //     thikness_px,
        //     LineTypes::LINE_4 as i32,
        //     0,
        // );
        Ok(())
    }

    fn id(&self) -> &String {
        return &self.id;
    }
}
