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
use crate::channels::read_channel::OutputDelivery;
use crate::channels::typed_channel;
use crate::channels::typed_write_channel::BufferWriter;
use crate::channels::typed_write_channel::TypedWriteChannel;
use crate::channels::typed_write_channel::Writer;
use crate::packet::WorkQueue;
use atomic::Atomic;
use crossbeam::channel::unbounded;
use crossbeam::channel::Receiver;

use crossbeam::channel::Sender;

use crate::packet::PacketSet;
use std::sync::atomic::Ordering;

use super::channels::ReadChannel;

use super::RustedPipeError;

pub enum Nodes<INPUT: OutputDelivery + OrderedBuffer, OUTPUT: Writer + 'static> {
    TerminalHandler(Box<TerminalNode<INPUT>>),
    NodeHandler(Box<Node<INPUT, OUTPUT>>),
    SourceHandler(Box<SourceNode<OUTPUT>>),
}

enum Processors<INPUT: OutputDelivery + OrderedBuffer, OUTPUT: Writer + 'static> {
    SourceProcessor(Box<dyn SourceProcessor<WRITE = OUTPUT>>),
    Processor(Box<dyn Processor<INPUT, WRITE = OUTPUT>>),
    TerminalProcessor(Box<dyn TerminalProcessor<INPUT>>),
}

/// PROCESSORS
pub struct Node<INPUT: OutputDelivery + OrderedBuffer, OUTPUT: Writer + 'static> {
    pub id: String,
    pub read_channel: ReadChannel<INPUT>,
    pub handler: Box<dyn Processor<INPUT, WRITE = OUTPUT>>,
    pub work_queue: WorkQueue<INPUT::OUTPUT>,
    pub write_channel: TypedWriteChannel<OUTPUT>,
}

pub struct SourceNode<WRITE: Writer + 'static> {
    pub id: String,
    pub write_channel: TypedWriteChannel<WRITE>,
    pub handler: Box<dyn SourceProcessor<WRITE = WRITE>>,
}

pub struct TerminalNode<INPUT: OutputDelivery + OrderedBuffer> {
    pub id: String,
    pub read_channel: ReadChannel<INPUT>,
    pub handler: Box<dyn TerminalProcessor<INPUT>>,
    pub work_queue: WorkQueue<INPUT::OUTPUT>,
}

impl<INPUT: OutputDelivery + OrderedBuffer, OUTPUT: Writer> fmt::Debug for Node<INPUT, OUTPUT> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.id)
    }
}

pub trait SourceProcessor: Sync + Send {
    type WRITE: Writer;
    fn handle(
        &mut self,
        output: MutexGuard<TypedWriteChannel<Self::WRITE>>,
    ) -> Result<(), RustedPipeError>;

    fn id(&self) -> &String;
}

pub trait Processor<INPUT: OutputDelivery>: Sync + Send {
    type WRITE: Writer;
    fn handle(
        &mut self,
        input: INPUT::OUTPUT,
        output: MutexGuard<TypedWriteChannel<Self::WRITE>>,
    ) -> Result<(), RustedPipeError>;

    fn id(&self) -> &String;
}

pub trait TerminalProcessor<INPUT: OutputDelivery>: Sync + Send {
    fn handle(&mut self, input: INPUT::OUTPUT) -> Result<(), RustedPipeError>;

    fn id(&self) -> &String;
}

pub struct Graph {
    //nodes: IndexMap<String, Node>,
    running: Arc<Atomic<GraphStatus>>,
    thread_control: Vec<Wait>,
    pool: futures::executor::ThreadPool,
    node_threads: Vec<(String, JoinHandle<()>)>,
    read_threads: Vec<(String, JoinHandle<()>)>,
    worker_done: (Sender<String>, Receiver<String>),
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
            running: Arc::new(Atomic::<GraphStatus>::new(GraphStatus::Running)),
            thread_control: vec![],
            pool: futures::executor::ThreadPool::new().expect("Failed to build pool"),
            node_threads: Vec::<(String, JoinHandle<()>)>::default(),
            read_threads: Vec::<(String, JoinHandle<()>)>::default(),
            worker_done: unbounded::<String>(),
            reader_empty: unbounded::<String>(),
        }
    }

    // pub fn start(&self) {
    //     let thread_pool = futures::executor::ThreadPool::new().expect("Failed to build pool");

    //     while self.running.load(Ordering::Relaxed) != GraphStatus::Terminating {
    //         for worker in self.thread_control {}
    //     }
    // }

    fn get_worker<
        INPUT: Send + OutputDelivery + OrderedBuffer + 'static,
        OUTPUT: Writer + Send + 'static,
    >(
        &mut self,
        node: Nodes<INPUT, OUTPUT>,
    ) -> (String, ProcessorWorker<INPUT, OUTPUT>) {
        let reading_running_thread = self.running.clone();
        match node {
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
                let id_clone = id.clone();

                println!("ADDING READER");
                self.read_threads.push((
                    id.clone(),
                    thread::spawn(move || {
                        read_channel_data(
                            id_clone,
                            reading_running_thread,
                            read_channel,
                            done_channel,
                        )
                    }),
                ));

                let work_queue_processor = work_queue;
                (
                    id.clone(),
                    ProcessorWorker::<INPUT, OUTPUT> {
                        work_queue: Some(work_queue_processor),
                        processor: Arc::new(Mutex::new(Processors::Processor(handler))),
                        write_channel: Some(Arc::new(Mutex::new(write_channel))),
                        status: Arc::new(Atomic::new(WorkerStatus::Idle)),
                        is_source: false,
                    },
                )
            }
            Nodes::SourceHandler(node) => (
                node.id.clone(),
                ProcessorWorker {
                    work_queue: None,
                    processor: Arc::new(Mutex::new(Processors::SourceProcessor(node.handler))),
                    write_channel: Some(Arc::new(Mutex::new(node.write_channel))),
                    status: Arc::new(Atomic::new(WorkerStatus::Idle)),
                    is_source: true,
                },
            ),
            Nodes::TerminalHandler(node) => {
                let (id, work_queue, mut read_channel, handler) =
                    (node.id, node.work_queue, node.read_channel, node.handler);
                let work_queue = Arc::new(work_queue);
                read_channel.start(work_queue.clone());
                let done_channel = self.reader_empty.0.clone();
                let id_clone = id.clone();
                println!("ADDING READING THREAD {}", self.read_threads.len());
                self.read_threads.push((
                    id.clone(),
                    thread::spawn(move || {
                        read_channel_data(id, reading_running_thread, read_channel, done_channel)
                    }),
                ));
                println!("ADDING READING THREAD {}", self.read_threads.len());

                let work_queue_processor = work_queue;
                (
                    id_clone,
                    ProcessorWorker {
                        work_queue: Some(work_queue_processor),
                        processor: Arc::new(Mutex::new(Processors::TerminalProcessor(handler))),
                        write_channel: None,
                        status: Arc::new(Atomic::new(WorkerStatus::Idle)),
                        is_source: false,
                    },
                )
            }
        }
    }

    pub fn start_node<
        INPUT: Send + OutputDelivery + OrderedBuffer + 'static,
        OUTPUT: Writer + Send + 'static,
    >(
        &mut self,
        processor: Nodes<INPUT, OUTPUT>,
    ) {
        self.running.swap(GraphStatus::Running, Ordering::Relaxed);

        let consume_running_thread = self.running.clone();

        let (node_id, worker) = self.get_worker(processor);

        let done_channel = self.worker_done.0.clone();

        let wait = Arc::new((Mutex::new(WorkerStatus::Idle), Condvar::new()));
        let wait_clone = wait.clone();
        let thread_clone = self.pool.clone();
        let id_move = node_id.clone();
        self.node_threads.push((
            node_id.clone(),
            thread::spawn(move || {
                consume(
                    id_move,
                    consume_running_thread,
                    wait_clone,
                    worker,
                    done_channel,
                    thread_clone,
                )
            }),
        ));
        self.thread_control.push(wait.clone());
        println!("Done Starting Node {node_id}");
    }

    pub fn stop(&mut self, wait_for_data: bool, timeout: Option<Duration>) {
        let mut empty_set = HashSet::new();
        let mut empty_receiver_set = HashSet::new();

        if wait_for_data {
            // Wait for all buffers to be empty
            self.running
                .swap(GraphStatus::WaitingForDataToTerminate, Ordering::Relaxed);

            while !self.node_threads.iter().all(|n| empty_set.contains(&n.0)) {
                println!(
                    "Waiting node threads received done from {} out of {}",
                    empty_set.len(),
                    self.node_threads.len()
                );
                if let Some(duration) = timeout {
                    let done = self.worker_done.1.recv_timeout(duration).unwrap();
                    empty_set.insert(done);
                } else {
                    let done = self.worker_done.1.recv().unwrap();
                    empty_set.insert(done);
                }
            }
            while !self.read_threads.iter().all(|n| empty_set.contains(&n.0)) {
                println!(
                    "Waiting reader threads received done from {} out of {}",
                    empty_receiver_set.len(),
                    self.read_threads.len()
                );
                if let Some(duration) = timeout {
                    let done = self.reader_empty.1.recv_timeout(duration).unwrap();
                    empty_receiver_set.insert(done);
                } else {
                    let done = self.reader_empty.1.recv().unwrap();
                    empty_receiver_set.insert(done);
                }
            }
        }
        self.running
            .swap(GraphStatus::Terminating, Ordering::Relaxed);

        while self.node_threads.len() > 0 {
            self.node_threads.pop().unwrap().1.join().unwrap();
        }

        while self.read_threads.len() > 0 {
            self.read_threads.pop().unwrap().1.join().unwrap();
        }
    }
}

struct ProcessorWorker<INPUT: OutputDelivery + OrderedBuffer, OUTPUT: Writer + Send + 'static> {
    work_queue: Option<Arc<WorkQueue<INPUT::OUTPUT>>>,
    processor: Arc<Mutex<Processors<INPUT, OUTPUT>>>,
    write_channel: Option<Arc<Mutex<TypedWriteChannel<OUTPUT>>>>,
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

fn consume<INPUT: OutputDelivery + OrderedBuffer + Send + 'static, OUTPUT>(
    id: String,
    running: Arc<Atomic<GraphStatus>>,
    _free: Wait,
    worker: ProcessorWorker<INPUT, OUTPUT>,
    done_notification: Sender<String>,
    thread_pool: futures::executor::ThreadPool,
) where
    OUTPUT: Writer + 'static + Send,
{
    while running.load(Ordering::Relaxed) != GraphStatus::Terminating {
        if worker.status.load(Ordering::Relaxed) == WorkerStatus::Idle {
            let lock_status = worker.status.clone();

            let mut packet = None;
            if let Some(work_queue) = worker.work_queue.as_ref() {
                let task = work_queue.steal();
                if let Some(read_event) = task.success() {
                    println!("Task");
                    packet = Some(read_event.packet_data);
                } else {
                    // TODO: make work_queue timeout
                    println!("Sending DONE {}", id.clone());
                    if running.load(Ordering::Relaxed) == GraphStatus::WaitingForDataToTerminate {
                        done_notification.send(id.clone()).unwrap();
                    }
                    thread::sleep(Duration::from_millis(5));
                    continue;
                }
            }

            worker
                .status
                .store(WorkerStatus::Running, Ordering::Relaxed);

            let processor_clone = worker.processor.clone();
            let id_thread = id.clone();
            let arc_write_channel = worker.write_channel.clone();
            let done_clone = done_notification.clone();
            let future = async move {
                let result = match &mut *processor_clone.lock().unwrap() {
                    Processors::Processor(proc) => {
                        proc.handle(packet.unwrap(), arc_write_channel.unwrap().lock().unwrap())
                    }
                    Processors::TerminalProcessor(proc) => proc.handle(packet.unwrap()),
                    Processors::SourceProcessor(proc) => {
                        proc.handle(arc_write_channel.unwrap().lock().unwrap())
                    }
                };
                match result {
                    Ok(_) => lock_status.store(WorkerStatus::Idle, Ordering::Relaxed),
                    Err(RustedPipeError::EndOfStream()) => {
                        println!("Terminating worker {id_thread:?}");
                        lock_status.store(WorkerStatus::Terminating, Ordering::Relaxed);
                        done_clone.send(id_thread.clone()).unwrap();
                    }
                    Err(err) => {
                        println!("Error in worker {id_thread:?}: {err:?}");
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

fn read_channel_data<T: OutputDelivery>(
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

    use crate::packet::typed::ReadChannel2PacketSet;
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

    impl SourceProcessor for TestNodeProducer {
        type WRITE = WriteChannel1<String>;
        fn handle(
            &mut self,
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
        output: Sender<ReadChannel2PacketSet<String, String>>,
        counter: u32,
        consume_time_ms: u64,
    }
    impl TestNodeConsumer {
        fn new(
            output: Sender<ReadChannel2PacketSet<String, String>>,
            consume_time_ms: u64,
        ) -> Self {
            TestNodeConsumer {
                id: "consumer".to_string(),
                output,
                consume_time_ms,
                counter: 0,
            }
        }
    }

    impl TerminalProcessor<ReadChannel2<String, String>> for TestNodeConsumer {
        fn handle(
            &mut self,
            input: ReadChannel2PacketSet<String, String>,
        ) -> Result<(), RustedPipeError> {
            println!(
                "Recevied {} at {}",
                self.counter,
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .expect("Time went backwards")
                    .as_millis()
            );
            self.output.send(input).unwrap();
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
            write_channel,
            id,
        }
    }

    fn create_consumer_node(
        consumer: TestNodeConsumer,
        consumer_queue_strategy: WorkQueue<ReadChannel2PacketSet<String, String>>,
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
            Some(Arc::new(
                WorkQueue::<ReadChannel2PacketSet<String, String>>::default(),
            )),
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
        consumer_queue_strategy: WorkQueue<ReadChannel2PacketSet<String, String>>,
    ) -> (Graph, Receiver<ReadChannel2PacketSet<String, String>>) {
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
        graph.start_node::<NoBuffer, WriteChannel1<String>>(Nodes::SourceHandler(Box::new(node0)));
        graph.start_node::<NoBuffer, WriteChannel1<String>>(Nodes::SourceHandler(Box::new(node1)));
        graph.start_node::<ReadChannel2<String, String>, WriteChannel1<String>>(
            Nodes::TerminalHandler(Box::new(process_terminal)),
        );
        (graph, output_check)
    }

    fn check_results(results: &Vec<ReadChannel2PacketSet<String, String>>, max_packets: usize) {
        assert_eq!(results.len(), max_packets);
        for (i, result) in results.iter().enumerate() {
            let data_0 = result.c1();
            let data_1 = result.c2();
            assert!(data_0.is_some(), "At packet {}", i);
            assert!(data_1.is_some(), "At packet {}", i);
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

        graph.stop(false, None);
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

        let (mut graph, output_check) = setup_default_test(node0, node1, 10, WorkQueue::default());

        let mut results = Vec::with_capacity(max_packets);
        let deadline = Instant::now() + Duration::from_millis(500);

        thread::sleep(Duration::from_millis(100));

        graph.stop(true, Some(Duration::from_secs(1)));

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
        graph.stop(false, None);
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
            let v1 = results[i].c1().unwrap().version.timestamp;
            let v2 = results[i].c2().unwrap().version.timestamp;
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
        graph.stop(false, None);
    }

    fn test_slow_consumers_blocks_if_configured(block_full: bool) {
        let max_packets = 10;
        let collection_time_ms: u64 = 50;

        // Very slow producer
        let node0 = TestNodeProducer::new("producer1".to_string(), 100, max_packets);
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

        graph.start_node::<NoBuffer, WriteChannel1<String>>(Nodes::SourceHandler(Box::new(node0)));
        graph.start_node::<NoBuffer, WriteChannel1<String>>(Nodes::SourceHandler(Box::new(node1)));
        graph.start_node::<ReadChannel2<String, String>, WriteChannel1<String>>(
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
