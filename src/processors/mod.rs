use std::fmt;

use std::sync::Arc;
use std::sync::Mutex;

use std::sync::atomic::AtomicBool;
use std::thread;
use std::thread::JoinHandle;

use crossbeam::channel::Select;
use crossbeam::deque::Injector;

use std::sync::atomic::{AtomicUsize, Ordering};

use super::channels::{untyped_channel, ChannelID, PacketSet, ReadChannel, WriteChannel};
use super::RustedPipeError;
use indexmap::IndexMap;

type ProcessorSafe = Arc<dyn Processor>;

struct ReadEvent {
    processor_index: usize,
    packet_data: PacketSet,
}

unsafe impl Send for ReadEvent {}

pub struct Graph {
    nodes: IndexMap<String, Node>,
    running: Arc<AtomicBool>,
    node_threads: Vec<JoinHandle<()>>,
}

impl Graph {
    pub fn new() -> Self {
        Graph {
            nodes: IndexMap::<String, Node>::default(),
            running: Arc::new(AtomicBool::new(false)),
            node_threads: Vec::<JoinHandle<()>>::default(),
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
            .add_channel(to_port, channel_receiver);

        Ok(())
    }

    pub fn start(&mut self) -> Vec<thread::JoinHandle<()>> {
        let mut node_id = 0;
        let mut handles = Vec::new();
        let mut workers = Vec::default();
        self.running.swap(true, Ordering::Relaxed);

        while !self.nodes.is_empty() {
            let processor = self.nodes.pop().unwrap();

            let (_id, is_source, write_channel, read_channel, handler) = processor.1.start();

            let assigned_node_id = node_id;
            let work_queue = Arc::new(Injector::<ReadEvent>::default());
            let work_queue_reader = work_queue.clone();
            let arc_write_channel = Arc::new(Mutex::new(write_channel));
            let reading_running_thread = self.running.clone();

            if !is_source {
                handles.push(thread::spawn(move || {
                    read_channel_data(
                        reading_running_thread,
                        assigned_node_id,
                        read_channel,
                        work_queue_reader,
                    )
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
        return handles;
    }

    fn stop(&mut self) {
        self.running.swap(false, Ordering::Relaxed);
        for n in 0..self.node_threads.len() {
            self.node_threads.remove(n).join().unwrap();
        }
    }
}

struct ProcessorWorker {
    work_queue: Arc<Injector<ReadEvent>>,
    processor: Arc<dyn Processor>,
    write_channel: Arc<Mutex<WriteChannel>>,
    status: Arc<AtomicUsize>,
    is_source: bool,
}

fn consume(running: Arc<AtomicBool>, mut workers: Vec<ProcessorWorker>) {
    let thread_pool = futures::executor::ThreadPool::new().expect("Failed to build pool");

    while running.load(Ordering::Relaxed) == true {
        for worker in workers.iter_mut() {
            if worker.status.load(Ordering::SeqCst) == 0 {
                let lock_status = worker.status.clone();
                let mutex_processor = Mutex::new(worker.processor.clone());
                let arc_write_channel = worker.write_channel.clone();

                if worker.is_source {
                    worker.status.store(1, Ordering::SeqCst);
                    let future = async move {
                        mutex_processor
                            .lock()
                            .unwrap()
                            .handle(PacketSet::default(), arc_write_channel);
                        lock_status.store(0, Ordering::SeqCst);
                    };

                    thread_pool.spawn_ok(future);
                } else {
                    let task = worker.work_queue.steal();

                    if let Some(read_event) = task.success() {
                        worker.status.store(1, Ordering::SeqCst);
                        let future = async move {
                            mutex_processor
                                .lock()
                                .unwrap()
                                .handle(read_event.packet_data, arc_write_channel);

                            lock_status.store(0, Ordering::SeqCst);
                        };
                        thread_pool.spawn_ok(future);
                    }
                }
            }
        }
    }
}

fn read_channel_data(
    running: Arc<AtomicBool>,
    _assigned_id: usize,
    mut read_channel: ReadChannel,
    _work_queue: Arc<Injector<ReadEvent>>,
) {
    let max_range = read_channel.available_channels().len();

    if max_range == 0 {
        return;
    }

    let channels = read_channel.selector();
    let mut selector = Select::new();
    for channel in &channels {
        selector.recv(channel);
    }

    while running.load(Ordering::Relaxed) == true {
        let channel_index = selector.ready();
        let _read_version = read_channel.try_read_index(channel_index);
        // if let Ok(version) = read_version {
        //     if read_channel.synchronize(&version.1) {
        //         let packet_set = read_channel.get_packets_for_version(&version.1);
        //         work_queue.push(ReadEvent {
        //             processor_index: assigned_id,
        //             packet_data: packet_set,
        //         })
        //     }
        // }
    }
}

/// PROCESSORS

pub struct Node {
    id: String,
    is_source: bool,
    write_channel: WriteChannel,
    read_channel: ReadChannel,
    handler: ProcessorSafe,
}

impl fmt::Debug for Node {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.id)
    }
}

impl Node {
    fn default_channels(handler: ProcessorSafe) -> Self {
        Node {
            id: handler.id().clone(),
            is_source: true,
            write_channel: WriteChannel::default(),
            read_channel: ReadChannel::default(),
            handler,
        }
    }

    fn new(handler: ProcessorSafe, write_channel: WriteChannel, read_channel: ReadChannel) -> Self {
        Node {
            id: handler.id().clone(),
            is_source: true,
            write_channel: write_channel,
            read_channel: read_channel,
            handler,
        }
    }

    fn start(self) -> (String, bool, WriteChannel, ReadChannel, ProcessorSafe) {
        (
            self.id,
            self.is_source,
            self.write_channel,
            self.read_channel,
            self.handler,
        )
    }
}

pub trait Processor: Sync + Send {
    fn handle(
        &self,
        input: PacketSet,
        output: Arc<Mutex<WriteChannel>>,
    ) -> Result<(), RustedPipeError>;
    fn id(&self) -> &String;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ChannelError;
    use crate::DataVersion;
    use mockall::predicate::*;
    use mockall::*;
    use std::time::Duration;

    #[automock]
    trait MockConsumer {
        fn handle(
            &self,
            _input: &PacketSet,
            _output_channel: Arc<Mutex<WriteChannel>>,
        ) -> Result<(), RustedPipeError>;
    }

    // Simple Testing Processors
    struct TestNodeProducer {
        id: String,
    }
    struct TestNodeConsumer {
        id: String,
        mock_object: MockMockConsumer,
    }

    impl Processor for TestNodeProducer {
        fn handle(
            &self,
            _input: PacketSet,
            output_channel: Arc<Mutex<WriteChannel>>,
        ) -> Result<(), RustedPipeError> {
            output_channel
                .lock()
                .unwrap()
                .write::<String>(
                    &ChannelID::from("output_channel0"),
                    "Test".to_string(),
                    &DataVersion { timestamp: 1 },
                )
                .unwrap();
            Err(super::RustedPipeError::ChannelError(
                ChannelError::EndOfStreamError(ChannelID::from("output_channel0")),
            ))
        }

        fn id(&self) -> &String {
            return &self.id;
        }
    }

    impl TestNodeConsumer {
        fn new() -> Self {
            let mut mock_object = MockMockConsumer::new();
            let mock_result: Result<(), RustedPipeError> = Ok(());
            mock_object
                .expect_handle()
                .times(1)
                .withf(
                    |_input: &PacketSet, _output_channel: &Arc<Mutex<WriteChannel>>| {
                        _input.channels() == 2
                            && *_input.get::<String>(0).unwrap().data == "Test".to_string()
                            && *_input.get::<String>(1).unwrap().data == "Test".to_string()
                            && _input.get::<String>(1).unwrap().version
                                == DataVersion { timestamp: 1 }
                            && _input.get::<String>(0).unwrap().version
                                == DataVersion { timestamp: 1 }
                    },
                )
                .return_const(mock_result.clone());

            TestNodeConsumer {
                id: "consumer".to_string(),
                mock_object,
            }
        }
    }

    impl Processor for TestNodeConsumer {
        fn handle(
            &self,
            _input: PacketSet,
            _output_channel: Arc<Mutex<WriteChannel>>,
        ) -> Result<(), RustedPipeError> {
            self.mock_object
                .handle(&_input, _output_channel.clone())
                .ok();
            Err(super::RustedPipeError::ChannelError(
                ChannelError::EndOfStreamError(ChannelID::from("output_channel0")),
            ))
        }

        fn id(&self) -> &String {
            return &self.id;
        }
    }

    #[test]
    fn test_linked_nodes_can_send_and_receive_data() {
        let node0 = TestNodeProducer {
            id: "producer1".to_string(),
        };
        let node0 = Arc::new(node0);
        let node0 = Node::default_channels(node0);

        let node1 = TestNodeProducer {
            id: "producer2".to_string(),
        };
        let node1 = Arc::new(node1);
        let node1 = Node::default_channels(node1);

        let process_terminal = TestNodeConsumer::new();
        let mut process_terminal_handler = Arc::new(process_terminal);
        let process_terminal = Node::default_channels(process_terminal_handler.clone());

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

        let processors = graph.start();
        thread::sleep(Duration::from_millis(400));
        graph.stop();
        for processor in processors {
            let join_result = processor.join();
            assert!(join_result.is_ok(), "{:?}", join_result.err().unwrap());
        }
        unsafe {
            Arc::get_mut_unchecked(&mut process_terminal_handler)
                .mock_object
                .checkpoint();
        }
    }
}
