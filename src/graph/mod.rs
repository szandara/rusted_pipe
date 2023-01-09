mod formatter;
mod test_nodes;

use std::fmt;

use std::sync::Arc;
use std::sync::Mutex;

use std::sync::atomic::AtomicBool;
use std::thread;
use std::thread::JoinHandle;

use crate::packet::WorkQueue;
use crossbeam::channel::Select;

use std::sync::atomic::{AtomicUsize, Ordering};

use crate::packet::PacketSet;

use super::channels::{untyped_channel, ReadChannel, WriteChannel};
use crate::packet::ChannelID;

use super::RustedPipeError;
use indexmap::IndexMap;

type ProcessorSafe = Arc<Mutex<dyn Processor>>;

fn new_node(processor: impl Processor + 'static, work_queue: WorkQueue) -> Node {
    Node::new(Arc::new(Mutex::new(processor)), work_queue)
}

pub struct Graph {
    nodes: IndexMap<String, Node>,
    running: Arc<AtomicBool>,
    node_threads: Vec<JoinHandle<()>>,
    read_threads: Vec<JoinHandle<()>>,
}

impl Graph {
    pub fn new() -> Self {
        Graph {
            nodes: IndexMap::<String, Node>::default(),
            running: Arc::new(AtomicBool::new(false)),
            node_threads: Vec::<JoinHandle<()>>::default(),
            read_threads: Vec::<JoinHandle<()>>::default(),
        }
    }

    pub fn add_node(&mut self, node: Node) -> Result<(), RustedPipeError> {
        let node_id = node.id.clone();
        self.nodes.entry(node_id).or_insert(node);
        Ok(())
    }

    pub fn link(
        &mut self,
        from_node_id: &String,
        from_port: &ChannelID,
        to_node_id: &String,
        to_port: &ChannelID,
    ) -> Result<(), RustedPipeError> {
        let (channel_sender, channel_receiver) = untyped_channel();

        self.nodes
            .get_mut(from_node_id)
            .ok_or(RustedPipeError::MissingNodeError(from_node_id.clone()))?
            .write_channel
            .add_channel(from_port, channel_sender);

        let receiver_node = self
            .nodes
            .get_mut(to_node_id)
            .ok_or(RustedPipeError::MissingNodeError(to_node_id.clone()))?;

        receiver_node.is_source = false;

        receiver_node
            .read_channel
            .add_channel(to_port, channel_receiver)
            .unwrap();

        Ok(())
    }

    pub fn start(&mut self) {
        let mut node_id = 0;
        let mut workers = Vec::default();
        self.running.swap(true, Ordering::Relaxed);

        while !self.nodes.is_empty() {
            let processor = self.nodes.pop().unwrap();

            let (_id, is_source, write_channel, mut read_channel, handler, work_queue) =
                processor.1.start();

            let assigned_node_id = node_id;
            let work_queue = Arc::new(work_queue);
            let arc_write_channel = Arc::new(Mutex::new(write_channel));
            let reading_running_thread = self.running.clone();

            read_channel.start(assigned_node_id, work_queue.clone());

            if !is_source {
                self.read_threads.push(thread::spawn(move || {
                    read_channel_data(reading_running_thread, read_channel)
                }));
            }

            let work_queue_processor = work_queue.clone();
            workers.push(ProcessorWorker {
                work_queue: work_queue_processor,
                processor: handler.clone(),
                write_channel: arc_write_channel.clone(),
                status: Arc::new(AtomicUsize::new(0)),
                is_source,
            });

            node_id += 1;
        }

        let consume_running_thread = self.running.clone();
        self.node_threads.push(thread::spawn(move || {
            consume(consume_running_thread, workers)
        }));
    }

    fn stop(&mut self) {
        self.running.swap(false, Ordering::Relaxed);
        for n in 0..self.node_threads.len() {
            self.node_threads.remove(n).join().unwrap();
        }
        for n in 0..self.read_threads.len() {
            self.read_threads.remove(n).join().unwrap();
        }
    }
}

struct ProcessorWorker {
    work_queue: Arc<WorkQueue>,
    processor: ProcessorSafe,
    write_channel: Arc<Mutex<WriteChannel>>,
    status: Arc<AtomicUsize>,
    is_source: bool,
}

fn consume(running: Arc<AtomicBool>, workers: Vec<ProcessorWorker>) {
    let thread_pool = futures::executor::ThreadPool::new().expect("Failed to build pool");

    while running.load(Ordering::Relaxed) {
        for worker in workers.iter() {
            if worker.status.load(Ordering::SeqCst) == 0 {
                let lock_status = worker.status.clone();
                let arc_write_channel = worker.write_channel.clone();
                let worker_clone = worker.processor.clone();
                if worker.is_source {
                    worker.status.store(1, Ordering::SeqCst);
                    let future = async move {
                        worker_clone
                            .lock()
                            .unwrap()
                            .handle(PacketSet::default(), arc_write_channel)
                            .unwrap();
                        lock_status.store(0, Ordering::SeqCst);
                    };

                    thread_pool.spawn_ok(future);
                } else {
                    let task = worker.work_queue.steal();

                    if let Some(read_event) = task.success() {
                        worker.status.store(1, Ordering::SeqCst);
                        let future = async move {
                            worker_clone
                                .lock()
                                .unwrap()
                                .handle(read_event.packet_data, arc_write_channel)
                                .unwrap();

                            lock_status.store(0, Ordering::SeqCst);
                        };
                        thread_pool.spawn_ok(future);
                    }
                }
            }
        }
    }
}

fn read_channel_data(running: Arc<AtomicBool>, mut read_channel: ReadChannel) {
    let max_range = read_channel.available_channels().len();

    if max_range == 0 {
        return;
    }

    let channels = read_channel.selector();
    let mut selector = Select::new();
    for channel in &channels {
        selector.recv(channel);
    }

    while running.load(Ordering::Relaxed) {
        let channel_index = selector.ready();
        let _read_version = read_channel.try_read_index(channel_index);
    }
    read_channel.stop();
}

/// PROCESSORS

pub struct Node {
    id: String,
    is_source: bool,
    write_channel: WriteChannel,
    read_channel: ReadChannel,
    handler: ProcessorSafe,
    work_queue: WorkQueue,
}

impl fmt::Debug for Node {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.id)
    }
}

impl Node {
    fn new(handler: ProcessorSafe, work_queue: WorkQueue) -> Self {
        Node {
            id: handler.lock().unwrap().id().clone(),
            is_source: true,
            write_channel: WriteChannel::default(),
            read_channel: ReadChannel::default(),
            handler: handler.clone(),
            work_queue,
        }
    }

    fn start(
        self,
    ) -> (
        String,
        bool,
        WriteChannel,
        ReadChannel,
        ProcessorSafe,
        WorkQueue,
    ) {
        (
            self.id,
            self.is_source,
            self.write_channel,
            self.read_channel,
            self.handler,
            self.work_queue,
        )
    }
}

pub trait Processor: Sync + Send {
    fn handle(
        &mut self,
        input: PacketSet,
        output: Arc<Mutex<WriteChannel>>,
    ) -> Result<(), RustedPipeError>;

    fn id(&self) -> &String;
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::DataVersion;
    use crossbeam::channel::unbounded;
    use crossbeam::channel::Receiver;
    use crossbeam::channel::Sender;
    use std::time::Duration;
    use std::time::Instant;
    use std::time::{SystemTime, UNIX_EPOCH};

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
            thread::sleep(Duration::from_millis(self.produce_time_ms));
            let s = SystemTime::now();
            output_channel
                .lock()
                .unwrap()
                .write::<String>(
                    &ChannelID::from("output_channel0"),
                    "Test".to_string(),
                    &DataVersion {
                        timestamp: self.counter as u64,
                    },
                )
                .unwrap();
            let e = SystemTime::now().duration_since(s).unwrap();
            println!(
                "P {}, Sending {} at {} in {}",
                self.id,
                self.counter,
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .expect("Time went backwards")
                    .as_nanos(),
                e.as_nanos()
            );
            self.counter += 1;
            Ok(())
        }

        fn id(&self) -> &String {
            return &self.id;
        }
    }

    struct TestNodeConsumer {
        id: String,
        output: Sender<PacketSet>,
        counter: u32,
        consume_time_ms: u64,
    }
    impl TestNodeConsumer {
        fn new(output: Sender<PacketSet>, consume_time_ms: u64) -> Self {
            TestNodeConsumer {
                id: "consumer".to_string(),
                output,
                consume_time_ms,
                counter: 0,
            }
        }
    }

    impl Processor for TestNodeConsumer {
        fn handle(
            &mut self,
            mut _input: PacketSet,
            _output_channel: Arc<Mutex<WriteChannel>>,
        ) -> Result<(), RustedPipeError> {
            println!(
                "Receved {} at {}",
                self.counter,
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .expect("Time went backwards")
                    .as_millis()
            );
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
        let node0 = new_node(node0, WorkQueue::default());
        let node1 = new_node(node1, WorkQueue::default());

        let (output, output_check) = unbounded();
        let process_terminal = TestNodeConsumer::new(output.clone(), consume_time_ms);
        let process_terminal = new_node(process_terminal, consumer_queue_strategy);

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

    fn check_results(results: &Vec<PacketSet>, max_packets: usize) {
        assert_eq!(results.len(), max_packets);
        for result in results {
            assert_eq!(result.channels(), 2);
            assert_eq!(*result.get::<String>(0).unwrap().data, "Test".to_string());
            assert_eq!(*result.get::<String>(1).unwrap().data, "Test".to_string());
        }
    }

    #[test]
    fn test_linked_nodes_can_send_and_receive_data() {
        let max_packets = 100;
        let mock_processing_time_ms = 3;

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

        let (mut graph, output_check) = setup_default_test(node0, node1, 0, WorkQueue::default());

        let mut results = Vec::with_capacity(max_packets);
        let deadline = Instant::now() + Duration::from_millis(400);
        graph.start();

        for i in 0..max_packets {
            let data = output_check.recv_deadline(deadline);
            if data.is_err() {
                break;
            }
            results.push(data.unwrap());
        }

        check_results(&results, max_packets);

        graph.stop();
    }

    #[test]
    fn test_linked_nodes_can_send_and_receive_data_different_produce_speed() {
        let max_packets = 10;
        let mock_processing_time_ms = 3;
        let collection_time_ms: u64 = 400;

        let node0 = TestNodeProducer::new(
            "producer1".to_string(),
            mock_processing_time_ms * 10,
            max_packets,
        );
        let node1 = TestNodeProducer::new(
            "producer2".to_string(),
            mock_processing_time_ms,
            max_packets,
        );

        let (mut graph, output_check) = setup_default_test(node0, node1, 0, WorkQueue::default());

        let mut results = Vec::with_capacity(max_packets);
        let deadline = Instant::now() + Duration::from_millis(collection_time_ms);

        graph.start();

        for i in 0..max_packets {
            let data = output_check.recv_deadline(deadline);
            if data.is_err() {
                break;
            }
            results.push(data.unwrap());
        }

        check_results(&results, max_packets);
    }

    #[test]
    fn test_slow_consumers_data_is_dropped_real_time_queue() {
        let max_packets = 3;
        let mock_processing_time_ms = 4;
        let collection_time_ms: u64 = 600;

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

        let (mut graph, output_check) = setup_default_test(node0, node1, 100, WorkQueue::new(1));

        let mut results = Vec::with_capacity(max_packets);
        let deadline = Instant::now() + Duration::from_millis(collection_time_ms);

        graph.start();

        for i in 0..max_packets {
            let data = output_check.recv_deadline(deadline);
            if data.is_err() {
                break;
            }
            results.push(data.unwrap());
        }

        check_results(&results, max_packets);

        let expected_versions: Vec<u64> = vec![5, 22, 42];
        for (i, expected_version) in expected_versions.into_iter().enumerate() {
            let v1 = results[i].get::<String>(0).unwrap().version.timestamp;
            let v2 = results[i].get::<String>(1).unwrap().version.timestamp;
            let range = ((expected_version - 5)..(expected_version + 5)).collect::<Vec<u64>>();
            assert!(
                range.contains(&v1),
                "V1 {} not in expected_version {}",
                v1,
                expected_version
            );
            assert!(
                range.contains(&v2),
                "V2 {} not in expected_version {}",
                v2,
                expected_version
            );
        }
    }
}
