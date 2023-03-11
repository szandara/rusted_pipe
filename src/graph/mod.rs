pub mod formatter;

use std::collections::HashSet;
use std::fmt;

use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;

use std::sync::MutexGuard;
use std::thread;
use std::thread::JoinHandle;
use std::time::Duration;

use crate::buffers::single_buffers::FixedSizeBuffer;
use crate::buffers::OrderedBuffer;
use crate::channels::read_channel::BufferReceiver;
use crate::channels::typed_channel;
use crate::channels::typed_write_channel::BufferWriter;
use crate::channels::typed_write_channel::TypedWriteChannel;
use crate::channels::typed_write_channel::Writer;
use crate::packet::WorkQueue;
use atomic::Atomic;
use crossbeam::channel::unbounded;
use crossbeam::channel::Receiver;

use crossbeam::channel::Sender;
use futures::io::Write;

use crate::packet::PacketSet;
use downcast_rs::{impl_downcast, Downcast};
use std::sync::atomic::Ordering;

use super::channels::ReadChannel;

use super::RustedPipeError;

type ProcessorSafe<T> = Arc<Mutex<dyn Processor<WRITE = T>>>;

pub enum Nodes<READ: OrderedBuffer, WRITE: Writer + 'static> {
    TerminalHandler(Box<TerminalNode<READ>>),
    NodeHandler(Box<Node<READ, WRITE>>),
    SourceHandler(Box<SourceNode<WRITE>>),
}

enum Processors<WRITE: Writer + 'static> {
    Processor(Box<dyn Processor<WRITE = WRITE>>),
    TerminalProcessor(Box<dyn TerminalProcessor>),
}

/// PROCESSORS
pub struct Node<READ: OrderedBuffer, WRITE: Writer + 'static> {
    pub id: String,
    pub read_channel: ReadChannel<READ>,
    pub handler: Box<dyn Processor<WRITE = WRITE>>,
    pub work_queue: WorkQueue,
    pub write_channel: TypedWriteChannel<WRITE>,
}

pub struct SourceNode<WRITE: Writer + 'static> {
    pub id: String,
    pub write_channel: TypedWriteChannel<WRITE>,
    pub handler: Box<dyn Processor<WRITE = WRITE>>,
    pub work_queue: WorkQueue,
}

pub struct TerminalNode<READ: OrderedBuffer> {
    pub id: String,
    pub read_channel: ReadChannel<READ>,
    pub handler: Box<dyn TerminalProcessor>,
    pub work_queue: WorkQueue,
}

pub trait NodeTrait<READ: OrderedBuffer, WRITE: Writer> {
    fn start(
        self,
    ) -> (
        String,
        Option<TypedWriteChannel<WRITE>>,
        Option<ReadChannel<READ>>,
        Option<ProcessorSafe<WRITE>>,
        WorkQueue,
    );
}

impl<READ: OrderedBuffer, WRITE: Writer> fmt::Debug for Node<READ, WRITE> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.id)
    }
}

pub trait Processor: Sync + Send {
    type WRITE: Writer;
    fn handle(
        &mut self,
        input: PacketSet,
        output: MutexGuard<TypedWriteChannel<Self::WRITE>>,
    ) -> Result<(), RustedPipeError>;

    fn id(&self) -> &String;
}

pub trait TerminalProcessor: Sync + Send {
    fn handle(&mut self, input: PacketSet) -> Result<(), RustedPipeError>;

    fn id(&self) -> &String;
}

pub struct Graph {
    //nodes: IndexMap<String, Node>,
    running_nodes: HashSet<String>,
    running: Arc<Atomic<GraphStatus>>,
    thread_control: Vec<Wait>,
    pool: futures::executor::ThreadPool,
    node_threads: Vec<JoinHandle<()>>,
    read_threads: Vec<JoinHandle<()>>,
    worker_done: (Sender<usize>, Receiver<usize>),
    reader_empty: (Sender<String>, Receiver<String>),
}

pub fn link<U: Clone>(
    read: &mut BufferReceiver<U, impl FixedSizeBuffer<Data = U>>,
    write: &mut BufferWriter<U>,
) -> Result<(), RustedPipeError> {
    let (channel_sender, channel_receiver) = typed_channel::<U>();
    read.link(channel_receiver);
    write.link(channel_sender);

    Ok(())
}

impl Graph {
    pub fn new() -> Self {
        Graph {
            //nodes: IndexMap::<String, Node>::default(),
            running: Arc::new(Atomic::<GraphStatus>::new(GraphStatus::Running)),
            running_nodes: Default::default(),
            thread_control: vec![],
            pool: futures::executor::ThreadPool::new().expect("Failed to build pool"),
            node_threads: Vec::<JoinHandle<()>>::default(),
            read_threads: Vec::<JoinHandle<()>>::default(),
            worker_done: unbounded::<usize>(),
            reader_empty: unbounded::<String>(),
        }
    }

    // pub fn start(&self) {
    //     let thread_pool = futures::executor::ThreadPool::new().expect("Failed to build pool");

    //     while self.running.load(Ordering::Relaxed) != GraphStatus::Terminating {
    //         for worker in self.thread_control {}
    //     }
    // }

    pub fn start_node<READ: OrderedBuffer + 'static, WRITE: Writer + Send + 'static>(
        &mut self,
        id: usize,
        processor: Nodes<READ, WRITE>,
    ) {
        println!("Starting Node {id}");
        self.running.swap(GraphStatus::Running, Ordering::Relaxed);

        let reading_running_thread = self.running.clone();

        let workers = match processor {
            Nodes::NodeHandler(node) => {
                let (id, work_queue, mut read_channel, handler, write_channel) = (
                    node.id,
                    node.work_queue,
                    node.read_channel,
                    node.handler,
                    node.write_channel,
                );
                let work_queue = Arc::new(work_queue);
                read_channel.start(work_queue.clone());
                let done_channel = self.reader_empty.0.clone();
                self.read_threads.push(thread::spawn(move || {
                    read_channel_data(id, reading_running_thread, read_channel, done_channel)
                }));

                let work_queue_processor = work_queue;
                ProcessorWorker::<WRITE> {
                    work_queue: work_queue_processor,
                    processor: Arc::new(Mutex::new(Processors::Processor(handler))),
                    write_channel: Some(Arc::new(Mutex::new(write_channel))),
                    status: Arc::new(Atomic::new(WorkerStatus::Idle)),
                    is_source: false,
                }
            }
            Nodes::SourceHandler(node) => {
                let work_queue = Arc::new(node.work_queue);
                let work_queue_processor = work_queue;
                ProcessorWorker {
                    work_queue: work_queue_processor,
                    processor: Arc::new(Mutex::new(Processors::Processor(node.handler))),
                    write_channel: Some(Arc::new(Mutex::new(node.write_channel))),
                    status: Arc::new(Atomic::new(WorkerStatus::Idle)),
                    is_source: true,
                }
            }
            Nodes::TerminalHandler(node) => {
                let (id, work_queue, mut read_channel, handler) =
                    (node.id, node.work_queue, node.read_channel, node.handler);
                let work_queue = Arc::new(work_queue);
                read_channel.start(work_queue.clone());
                let done_channel = self.reader_empty.0.clone();
                self.read_threads.push(thread::spawn(move || {
                    read_channel_data(id, reading_running_thread, read_channel, done_channel)
                }));

                let work_queue_processor = work_queue;
                ProcessorWorker {
                    work_queue: work_queue_processor,
                    processor: Arc::new(Mutex::new(Processors::TerminalProcessor(handler))),
                    write_channel: None,
                    status: Arc::new(Atomic::new(WorkerStatus::Idle)),
                    is_source: false,
                }
            }
        };

        let consume_running_thread = self.running.clone();
        let done_channel = self.worker_done.0.clone();

        let wait = Arc::new((Mutex::new(WorkerStatus::Idle), Condvar::new()));
        let wait_clone = wait.clone();
        let thread_clone = self.pool.clone();
        self.node_threads.push(thread::spawn(move || {
            consume(
                id,
                consume_running_thread,
                wait_clone,
                workers,
                done_channel,
                thread_clone,
            )
        }));
        self.thread_control.push(wait.clone());
        println!("Done Starting Node {id}");
    }

    pub fn stop(&mut self, wait_for_data: bool) {
        let mut empty_set = HashSet::new();
        let mut empty_receiver_set = HashSet::new();

        if wait_for_data {
            // Wait for all buffers to be empty
            self.running
                .swap(GraphStatus::WaitingForDataToTerminate, Ordering::Relaxed);

            while empty_set.len() != self.running_nodes.len() {
                let done = self.worker_done.1.recv().unwrap();
                empty_set.insert(done);
            }
            while empty_receiver_set.len() != self.read_threads.len() {
                //self.read_threads.len() {
                let done = self
                    .reader_empty
                    .1
                    .recv_timeout(Duration::from_millis(2000))
                    .unwrap();
                empty_receiver_set.insert(done);
            }
        }
        self.running
            .swap(GraphStatus::Terminating, Ordering::Relaxed);

        while self.node_threads.len() > 0 {
            self.node_threads.pop().unwrap().join().unwrap();
        }

        while self.read_threads.len() > 0 {
            self.read_threads.pop().unwrap().join().unwrap();
        }
    }
}

struct ProcessorWorker<WRITE: Writer + Send + 'static> {
    work_queue: Arc<WorkQueue>,
    processor: Arc<Mutex<Processors<WRITE>>>,
    write_channel: Option<Arc<Mutex<TypedWriteChannel<WRITE>>>>,
    status: Arc<Atomic<WorkerStatus>>,
    is_source: bool,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum GraphStatus {
    Running = 0,
    Terminating = 1,
    WaitingForDataToTerminate = 2,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum WorkerStatus {
    Idle = 0,
    Running = 1,
    Terminating = 2,
}

type Wait = Arc<(Mutex<WorkerStatus>, Condvar)>;

fn consume<WRITE>(
    id: usize,
    running: Arc<Atomic<GraphStatus>>,
    _free: Wait,
    worker: ProcessorWorker<WRITE>,
    done_notification: Sender<usize>,
    thread_pool: futures::executor::ThreadPool,
) where
    WRITE: Writer + 'static + Send,
{
    while running.load(Ordering::Relaxed) != GraphStatus::Terminating {
        if worker.status.load(Ordering::Relaxed) == WorkerStatus::Idle {
            let lock_status = worker.status.clone();

            let mut packet = PacketSet::default();
            if !worker.is_source {
                let task = worker.work_queue.steal();
                if let Some(read_event) = task.success() {
                    println!("Task");
                    packet = read_event.packet_data;
                } else {
                    // TODO: make work_queue timeout
                    println!("Sending DONE {id}");
                    if running.load(Ordering::Relaxed) == GraphStatus::WaitingForDataToTerminate {
                        done_notification.send(id).unwrap();
                    }
                    thread::sleep(Duration::from_millis(5));
                    continue;
                }
            }

            worker
                .status
                .store(WorkerStatus::Running, Ordering::Relaxed);

            let processor_clone = worker.processor.clone();
            let arc_write_channel = worker.write_channel.clone();
            let future = async move {
                let result = match &mut *processor_clone.lock().unwrap() {
                    Processors::Processor(proc) => {
                        proc.handle(packet, arc_write_channel.unwrap().lock().unwrap())
                    }
                    Processors::TerminalProcessor(proc) => proc.handle(packet),
                };
                match result {
                    Ok(_) => lock_status.store(WorkerStatus::Idle, Ordering::Relaxed),
                    Err(RustedPipeError::EndOfStream()) => {
                        println!("Terminating worker {id:?}");
                        lock_status.store(WorkerStatus::Idle, Ordering::Relaxed)
                    }
                    Err(err) => {
                        println!("Error in worker {id:?}: {err:?}");
                        lock_status.store(WorkerStatus::Idle, Ordering::Relaxed)
                    }
                }
            };

            thread_pool.spawn_ok(future);
        }
        thread::sleep(Duration::from_millis(5));
        //println!("ASSD");
        // if terminated == workers.len() {
        //     println!("All workers are terminated");
        //     break;
        // }
    }
    println!("{:?} - {:?}", worker.status, running);
}

fn read_channel_data<T>(
    id: String,
    running: Arc<Atomic<GraphStatus>>,
    mut read_channel: ReadChannel<T>,
    done_notification: Sender<String>,
) where
    T: OrderedBuffer + 'static,
{
    println!("Reading read {}", id);
    let id = id.to_string();
    while running.load(Ordering::Relaxed) != GraphStatus::Terminating {
        read_channel.read(id.clone(), done_notification.clone());
    }
    println!("Terminating read {}", id);
    read_channel.stop();
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::buffers::single_buffers::RtRingBuffer;
    use crate::buffers::synchronizers::timestamp::TimestampSynchronizer;
    use crate::buffers::NoBuffer;
    use crate::channels::read_channel::ReadChannel2;
    use crate::channels::typed_write_channel::WriteChannel1;

    use crate::DataVersion;
    use crossbeam::channel::unbounded;
    use crossbeam::channel::Receiver;
    use crossbeam::channel::RecvTimeoutError;
    use crossbeam::channel::Sender;
    use std::time::Duration;
    use std::time::Instant;
    use std::time::{SystemTime, UNIX_EPOCH};

    struct TestNodeProducer {
        id: String,
        produce_time_ms: u64,
        counter: usize,
        max_packets: usize,
    }

    impl TestNodeProducer {
        fn new(id: String, produce_time_ms: u64, max_packets: usize) -> Self {
            TestNodeProducer {
                id,
                produce_time_ms,
                counter: 0,
                max_packets,
            }
        }
    }

    impl Processor for TestNodeProducer {
        type WRITE = WriteChannel1<String>;
        fn handle(
            &mut self,
            mut _input: PacketSet,
            mut output_channel: MutexGuard<TypedWriteChannel<Self::WRITE>>,
        ) -> Result<(), RustedPipeError> {
            thread::sleep(Duration::from_millis(self.produce_time_ms));
            if self.counter == self.max_packets {
                return Err(RustedPipeError::EndOfStream());
            }

            let s = SystemTime::now();
            output_channel
                .writer
                .c1()
                .write(
                    "Test".to_string(),
                    &DataVersion {
                        timestamp: self.counter as u128,
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

    impl TerminalProcessor for TestNodeConsumer {
        fn handle(&mut self, mut _input: PacketSet) -> Result<(), RustedPipeError> {
            println!(
                "Recevied {} at {}",
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

    fn setup_test() -> Graph {
        Graph::new()
    }

    fn create_source_node(producer: TestNodeProducer) -> SourceNode<WriteChannel1<String>> {
        let write_channel1 = WriteChannel1::<String>::create();
        let write_channel = TypedWriteChannel {
            writer: Box::new(write_channel1),
        };
        let id = producer.id.clone();
        SourceNode {
            handler: Box::new(producer),
            work_queue: WorkQueue::default(),
            write_channel,
            id,
        }
    }

    fn create_consumer_node(
        consumer: TestNodeConsumer,
        consumer_queue_strategy: WorkQueue,
        buffer_size: usize,
        block_full: bool,
    ) -> TerminalNode<ReadChannel2<String, String>> {
        let synch_strategy = Box::new(TimestampSynchronizer::default());
        let read_channel2 = ReadChannel2::create(
            RtRingBuffer::<String>::new(buffer_size, block_full),
            RtRingBuffer::<String>::new(buffer_size, block_full),
        );
        let read_channel = ReadChannel::new(
            synch_strategy,
            Some(Arc::new(WorkQueue::default())),
            read_channel2,
        );

        let id = consumer.id.clone();
        TerminalNode {
            handler: Box::new(consumer),
            read_channel,
            work_queue: consumer_queue_strategy,
            id,
        }
    }

    fn setup_default_test(
        node0: TestNodeProducer,
        node1: TestNodeProducer,
        consume_time_ms: u64,
        consumer_queue_strategy: WorkQueue,
    ) -> (Graph, Receiver<PacketSet>) {
        let mut node0 = create_source_node(node0);
        let mut node1 = create_source_node(node1);

        let (output, output_check) = unbounded();
        let process_terminal = TestNodeConsumer::new(output.clone(), consume_time_ms);
        let process_terminal =
            create_consumer_node(process_terminal, consumer_queue_strategy, 2000, false);

        link(
            process_terminal.read_channel.channels.lock().unwrap().c1(),
            node0.write_channel.writer.c1(),
        )
        .expect("Cannot link channels");

        link(
            process_terminal.read_channel.channels.lock().unwrap().c2(),
            node1.write_channel.writer.c1(),
        )
        .expect("Cannot link channels");

        let mut graph = setup_test();
        graph.start_node::<NoBuffer, WriteChannel1<String>>(
            0,
            Nodes::SourceHandler(Box::new(node0)),
        );
        graph.start_node::<NoBuffer, WriteChannel1<String>>(
            1,
            Nodes::SourceHandler(Box::new(node1)),
        );
        graph.start_node::<ReadChannel2<String, String>, WriteChannel1<String>>(
            2,
            Nodes::TerminalHandler(Box::new(process_terminal)),
        );
        (graph, output_check)
    }

    fn check_results(results: &Vec<PacketSet>, max_packets: usize) {
        assert_eq!(results.len(), max_packets);
        for (i, result) in results.iter().enumerate() {
            assert_eq!(result.channels(), 2);
            let data_0 = result.get::<String>(0);
            let data_1 = result.get::<String>(1);
            assert!(data_0.is_ok(), "At packet {}", i);
            assert!(data_1.is_ok(), "At packet {}", i);
            assert_eq!(*data_0.unwrap().data, "Test".to_string(), "At packet {}", i);
            assert_eq!(*data_1.unwrap().data, "Test".to_string(), "At packet {}", i);
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
        let deadline = Instant::now() + Duration::from_millis(700);

        for _ in 0..max_packets {
            let data = output_check.recv_deadline(deadline);
            if data.is_err() {
                break;
            }
            results.push(data.unwrap());
        }

        check_results(&results, max_packets);

        graph.stop(false);
    }

    #[test]
    fn test_graph_waits_for_data_if_stop_flag() {
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
        println!("ASD");
        let (mut graph, output_check) = setup_default_test(node0, node1, 10, WorkQueue::default());

        let mut results = Vec::with_capacity(max_packets);
        let deadline = Instant::now() + Duration::from_millis(500);

        thread::sleep(Duration::from_millis(100));
        println!("ASD");
        graph.stop(true);

        for _ in 0..max_packets {
            let data = output_check.recv_deadline(deadline);
            if data.is_err() {
                break;
            }
            results.push(data.unwrap());
        }
        check_results(&results, max_packets);
    }

    #[test]
    fn test_linked_nodes_can_send_and_receive_at_different_produce_speed() {
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

        for _i in 0..max_packets {
            let data = output_check.recv_deadline(deadline);
            if data.is_err() {
                break;
            }
            results.push(data.unwrap());
        }

        check_results(&results, max_packets);
        graph.stop(false);
    }

    #[test]
    fn test_slow_consumers_data_is_dropped_real_time_queue() {
        let max_packets = 100;
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

        let expected_versions: Vec<u128> = vec![8, 20, 30];
        for _i in 0..expected_versions.len() {
            let data = output_check.recv_deadline(deadline);
            if data.is_err() {
                println!("Error receiving {:?}", data.err().unwrap());
                break;
            }
            results.push(data.unwrap());
        }

        check_results(&results, expected_versions.len());

        for (i, expected_version) in expected_versions.into_iter().enumerate() {
            let v1 = results[i].get::<String>(0).unwrap().version.timestamp;
            let v2 = results[i].get::<String>(1).unwrap().version.timestamp;
            let range = ((expected_version - 8)..(expected_version + 8)).collect::<Vec<u128>>();
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
        graph.stop(false);
    }

    fn test_slow_consumers_blocks_if_configured(block_full: bool) {
        let max_packets = 10;
        let collection_time_ms: u64 = 50;

        // Very slow producer
        let node0 = TestNodeProducer::new("producer1".to_string(), 60, max_packets);
        let node1 = TestNodeProducer::new("producer2".to_string(), 5, max_packets);

        let mut node0 = create_source_node(node0);
        let mut node1 = create_source_node(node1);

        let (output, output_check) = unbounded();
        let process_terminal = TestNodeConsumer::new(output.clone(), 0);

        let process_terminal =
            create_consumer_node(process_terminal, WorkQueue::default(), 2, block_full);
        let mut graph = setup_test();

        let deadline = Instant::now() + Duration::from_millis(collection_time_ms);

        link(
            process_terminal.read_channel.channels.lock().unwrap().c1(),
            node0.write_channel.writer.c1(),
        )
        .expect("Cannot link channels");

        link(
            process_terminal.read_channel.channels.lock().unwrap().c2(),
            node1.write_channel.writer.c1(),
        )
        .expect("Cannot link channels");

        graph.start_node::<NoBuffer, WriteChannel1<String>>(
            0,
            Nodes::SourceHandler(Box::new(node0)),
        );
        graph.start_node::<NoBuffer, WriteChannel1<String>>(
            1,
            Nodes::SourceHandler(Box::new(node1)),
        );
        graph.start_node::<ReadChannel2<String, String>, WriteChannel1<String>>(
            2,
            Nodes::TerminalHandler(Box::new(process_terminal)),
        );

        assert_eq!(
            output_check.recv_deadline(deadline).err().unwrap(),
            RecvTimeoutError::Timeout
        );

        // If the queue does not block, the oldest message (timestamp 0) is overridden in the buffer
        // so when the slow producer sends its message it cannot be synced with the fast producer.
        let deadline = Instant::now() + Duration::from_millis(collection_time_ms);
        if block_full {
            assert!(output_check.recv_deadline(deadline).is_ok());
        } else {
            assert_eq!(
                output_check.recv_deadline(deadline).err().unwrap(),
                RecvTimeoutError::Timeout
            );
        }
    }

    macro_rules! param_test {
        ($name:ident, ($($block:ident),+)) => {
            $(
                paste::item! {
                    #[test]
                    fn [< $name _ $block >] () {
                        $name($block);
                    }
                }
            )+
        }
    }

    param_test!(test_slow_consumers_blocks_if_configured, (true, false));
}
