extern crate rusted_pipe;

use std::{
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};

use rusted_pipe::{
    graph::{
        test_nodes::{BoundinBoxRender, CarDetector, ImageReader, OcrReader},
        Graph, Node,
    },
    packet::{ChannelID, WorkQueue},
};

fn setup_test() -> Graph {
    let processor = ImageReader::default();
    let image_input = Node::default(Arc::new(Mutex::new(processor)), WorkQueue::default(), true);

    let processor = CarDetector::default();
    let detector = Node::default(Arc::new(Mutex::new(processor)), WorkQueue::default(), false);

    let processor = BoundinBoxRender::default();
    let boundingbox = Node::default(Arc::new(Mutex::new(processor)), WorkQueue::default(), false);

    let processor = OcrReader::default();
    let ocr = Node::default(Arc::new(Mutex::new(processor)), WorkQueue::default(), false);

    let mut graph = Graph::new();

    graph.add_node(image_input).unwrap();
    graph.add_node(detector).unwrap();
    graph.add_node(boundingbox).unwrap();
    graph.add_node(ocr).unwrap();

    graph
        .link(
            &"ImageReader".to_string(),
            &ChannelID::from("image"),
            &"CarDetector".to_string(),
            &ChannelID::from("image"),
        )
        .unwrap();
    graph
        .link(
            &"CarDetector".to_string(),
            &ChannelID::from("cars"),
            &"BoundinBoxRender".to_string(),
            &ChannelID::from("cars"),
        )
        .unwrap();

    graph
        .link(
            &"ImageReader".to_string(),
            &ChannelID::from("image"),
            &"BoundinBoxRender".to_string(),
            &ChannelID::from("image"),
        )
        .unwrap();

    // graph
    //     .link(
    //         &"ImageReader".to_string(),
    //         &ChannelID::from("image"),
    //         &"OcrReader".to_string(),
    //         &ChannelID::from("image"),
    //     )
    //     .unwrap();

    return graph;
}

fn main() {
    let mut graph = setup_test();

    graph.start();
    thread::sleep(Duration::from_millis(2600));
    graph.stop();
}
