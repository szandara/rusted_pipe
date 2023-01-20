extern crate rusted_pipe;

use std::{
    sync::{Arc, Mutex},
    thread,
    time::{Duration, Instant},
};

use crossbeam::channel::{unbounded, Receiver, Sender};
use rusted_pipe::{
    buffers::{
        channel_buffers::BoundedBufferedData,
        single_buffers::{FixedSizeBTree, RtRingBuffer},
        synchronizers::TimestampSynchronizer,
    },
    channels::{ReadChannel, WriteChannel},
    graph::{Graph, Node, Processor},
    packet::{ChannelID, PacketSet, WorkQueue},
    DataVersion, RustedPipeError,
};

fn new_node(processor: impl Processor + 'static, work_queue: WorkQueue, is_source: bool) -> Node {
    let write_channel = WriteChannel::default();
    let read_channel = ReadChannel::new(
        Arc::new(Mutex::new(BoundedBufferedData::<RtRingBuffer>::new(
            200000, true,
        ))),
        Box::new(TimestampSynchronizer::default()),
    );
    Node::new(
        Arc::new(Mutex::new(processor)),
        work_queue,
        is_source,
        read_channel,
        write_channel,
    )
}

struct TestNodeProducer {
    id: String,
    produce_time_ms: u64,
    max_packets: usize,
    counter: usize,
}

impl TestNodeProducer {
    fn new(id: String, produce_time_ms: u64, max_packets: usize) -> Self {
        TestNodeProducer {
            id,
            produce_time_ms,
            max_packets,
            counter: 0,
        }
    }
}

impl Processor for TestNodeProducer {
    fn handle(
        &mut self,
        mut _input: PacketSet,
        output_channel: Arc<Mutex<WriteChannel>>,
    ) -> Result<(), RustedPipeError> {
        //let now = Instant::now();
        spin_sleep::sleep(Duration::from_millis(self.produce_time_ms));
        if self.counter >= self.max_packets {
            return Err(RustedPipeError::EndOfStream());
        }
        output_channel
            .lock()
            .unwrap()
            .write::<String>(
                &ChannelID::from("output_channel0"),
                "Test".to_string(),
                &DataVersion {
                    timestamp: self.counter as u128,
                },
            )
            .unwrap();
        self.counter += 1;
        //println!("Passed {}", now.elapsed().as_nanos());
        Ok(())
    }

    fn id(&self) -> &String {
        return &self.id;
    }
}

struct TestNodeConsumer {
    id: String,
    output: Sender<PacketSet>,
    consume_time_ms: u64,
}
impl TestNodeConsumer {
    fn new(output: Sender<PacketSet>, consume_time_ms: u64) -> Self {
        TestNodeConsumer {
            id: "consumer".to_string(),
            output,
            consume_time_ms,
        }
    }
}

impl Processor for TestNodeConsumer {
    fn handle(
        &mut self,
        mut _input: PacketSet,
        _output_channel: Arc<Mutex<WriteChannel>>,
    ) -> Result<(), RustedPipeError> {
        self.output.send(_input).unwrap();
        thread::sleep(Duration::from_millis(self.consume_time_ms));
        Ok(())
    }

    fn id(&self) -> &String {
        return &self.id;
    }
}

fn setup_default_test(
    node0: TestNodeProducer,
    node1: TestNodeProducer,
    consume_time_ms: u64,
    consumer_queue_strategy: WorkQueue,
) -> (Graph, Receiver<PacketSet>) {
    let node0 = new_node(node0, WorkQueue::default(), true);
    let node1 = new_node(node1, WorkQueue::default(), true);

    let (output, output_check) = unbounded();
    let process_terminal = TestNodeConsumer::new(output.clone(), consume_time_ms);
    let process_terminal = new_node(process_terminal, consumer_queue_strategy, false);

    let mut graph = Graph::new();

    graph.add_node(node0).unwrap();
    graph.add_node(node1).unwrap();
    graph.add_node(process_terminal).unwrap();

    graph
        .link(
            &"producer1".to_string(),
            &ChannelID::from("output_channel0"),
            &"consumer".to_string(),
            &ChannelID::from("in_channel0"),
        )
        .unwrap();
    graph
        .link(
            &"producer2".to_string(),
            &ChannelID::from("output_channel0"),
            &"consumer".to_string(),
            &ChannelID::from("in_channel1"),
        )
        .unwrap();

    return (graph, output_check);
}

fn main() {
    let max_packets = 100000;
    let mock_processing_time_ms = 0;

    let node0 = TestNodeProducer::new(
        "producer1".to_string(),
        mock_processing_time_ms,
        max_packets,
    );
    let node1 = TestNodeProducer::new(
        "producer2".to_string(),
        mock_processing_time_ms,
        max_packets,
    );

    let (mut graph, receiver) = setup_default_test(node0, node1, 0, WorkQueue::default());

    graph.start();
    let mut packets = 0;

    let start = Instant::now();
    loop {
        let data = receiver.recv_timeout(Duration::from_millis(500));
        if let Err(_) = data {
            eprintln!("Passed deadline");
            break;
        } else {
            //println!("Read {:?}", data.unwrap().get::<String>(0).unwrap().version)
        }
        packets += 1;
        if packets >= max_packets {
            break;
        }
    }
    println!(
        "Received {} packets in {}",
        packets,
        start.elapsed().as_millis()
    );
    graph.stop();
}
