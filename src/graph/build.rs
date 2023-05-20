use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Condvar, Mutex},
    thread::{self, JoinHandle},
    time::Duration,
};

use crate::channels::WriteChannelTrait;
use crate::channels::{typed_read_channel::NoBuffer, typed_write_channel::WriteChannel1};
use crate::{
    buffers::single_buffers::FixedSizeBuffer,
    channels::{
        read_channel::{BufferReceiver, ChannelBuffer, InputGenerator},
        typed_channel,
        typed_write_channel::{BufferWriter, TypedWriteChannel},
    },
    graph::{
        processor::Processors,
        runtime::{read_channel_data, ConsumerThread},
    },
    RustedPipeError,
};
use crate::{channels::ReadChannelTrait, graph::metrics::ProfilerTag};
use atomic::{Atomic, Ordering};
use crossbeam::channel::{unbounded, Receiver, Sender};
use itertools::Itertools;
use log::debug;
use rusty_pool::ThreadPool;

use super::{
    metrics::Metrics,
    processor::{Node, Nodes, SourceNode, TerminalNode},
    runtime::Wait,
};
use crate::packet::work_queue::WorkQueue;

pub struct Graph {
    running: Arc<Atomic<GraphStatus>>,
    thread_control: Vec<Wait>,
    pool: ThreadPool,
    node_threads: HashMap<String, JoinHandle<()>>,
    read_threads: HashMap<String, JoinHandle<()>>,
    worker_done: (Sender<String>, Receiver<String>),
    reader_empty: (Sender<String>, Receiver<String>),
    metrics: Metrics,
}

pub fn link<U: Clone + 'static>(
    write: &mut BufferWriter<U>,
    read: &mut BufferReceiver<impl FixedSizeBuffer<Data = U>>,
) -> Result<(), RustedPipeError> {
    let (channel_sender, channel_receiver) = typed_channel::<U>();
    read.link(channel_receiver);
    write.link(channel_sender);

    Ok(())
}

impl Graph {
    pub fn new(metrics_backend: Metrics) -> Self {
        Graph {
            running: Arc::new(Atomic::<GraphStatus>::new(GraphStatus::Running)),
            thread_control: vec![],
            pool: ThreadPool::default(),
            node_threads: Default::default(),
            read_threads: Default::default(),
            worker_done: unbounded::<String>(),
            reader_empty: unbounded::<String>(),
            metrics: metrics_backend,
        }
    }

    fn track_node_thread(&mut self, id: String, handle: JoinHandle<()>) {
        if self.read_threads.insert(id.clone(), handle).is_some() {
            panic!("Node {id} already exists");
        }
    }

    fn get_worker<
        INPUT: Send + InputGenerator + ChannelBuffer + 'static,
        OUTPUT: WriteChannelTrait + Send + 'static,
    >(
        &mut self,
        node: Nodes<INPUT, OUTPUT>,
    ) -> (String, ProcessorWorker<INPUT, OUTPUT>) {
        let reading_running_thread = self.running.clone();
        match node {
            Nodes::Node(node) => {
                let (id, work_queue, mut read_channel, handler, write_channel) = (
                    node.id,
                    node.work_queue,
                    node.read_channel,
                    node.handler,
                    node.write_channel,
                );
                read_channel.start(work_queue.clone());
                let done_channel = self.reader_empty.0.clone();
                let id_clone = id.clone();

                self.track_node_thread(
                    id.clone(),
                    thread::spawn(move || {
                        read_channel_data(
                            id_clone,
                            reading_running_thread,
                            read_channel,
                            done_channel,
                            
                        )
                    }),
                );

                let work_queue_processor = work_queue;
                (
                    id,
                    ProcessorWorker::<INPUT, OUTPUT> {
                        work_queue: Some(work_queue_processor),
                        processor: Processors::Processor(handler),
                        write_channel: Some(write_channel),
                    },
                )
            }
            Nodes::SourceNode(node) => (
                node.id.clone(),
                ProcessorWorker {
                    work_queue: None,
                    processor: Processors::SourceProcessor(node.handler),
                    write_channel: Some(node.write_channel),
                },
            ),
            Nodes::TerminalNode(node) => {
                let (id, work_queue, mut read_channel, handler) =
                    (node.id, node.work_queue, node.read_channel, node.handler);
                read_channel.start(work_queue.clone());
                let done_channel = self.reader_empty.0.clone();
                let id_clone = id.clone();

                self.track_node_thread(
                    id.clone(),
                    thread::spawn(move || {
                        read_channel_data(id, reading_running_thread, read_channel, done_channel)
                    }),
                );

                let work_queue_processor = work_queue;
                (
                    id_clone,
                    ProcessorWorker {
                        work_queue: Some(work_queue_processor),
                        processor: Processors::TerminalProcessor(handler),
                        write_channel: None,
                    },
                )
            }
        }
    }

    pub fn start_source_node<OUTPUT: WriteChannelTrait + Send + 'static>(
        &mut self,
        node: SourceNode<OUTPUT>,
    ) {
        self._start_node::<NoBuffer, OUTPUT>(Nodes::SourceNode(Box::new(node)));
    }

    pub fn start_node<
        INPUT: Send + InputGenerator + ChannelBuffer + 'static,
        OUTPUT: WriteChannelTrait + Send + 'static,
    >(
        &mut self,
        node: Node<INPUT, OUTPUT>,
    ) {
        self._start_node::<INPUT, OUTPUT>(Nodes::Node(Box::new(node)));
    }

    pub fn start_terminal_node<INPUT: Send + InputGenerator + ChannelBuffer + 'static>(
        &mut self,
        node: TerminalNode<INPUT>,
    ) {
        self._start_node::<INPUT, WriteChannel1<String>>(Nodes::TerminalNode(Box::new(node)));
    }

    fn _start_node<
        INPUT: Send + InputGenerator + ChannelBuffer + 'static,
        OUTPUT: WriteChannelTrait + Send + 'static,
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

        let profiler: Option<_> = self.metrics.profiler().as_ref().map(|profiler| profiler.profiler.tag_wrapper());

        if self
            .node_threads
            .insert(
                node_id.clone(),
                thread::spawn(move || {
                    let profiler_tag = match profiler {
                        Some(taggers) => ProfilerTag::from_tuple(taggers),
                        None => ProfilerTag::no_profiler(),
                    };

                    let mut consumer = ConsumerThread::new(
                        id_move,
                        consume_running_thread,
                        wait_clone,
                        worker,
                        done_channel,
                        thread_clone,
                        profiler_tag,
                    );
                    consumer.consume();
                }),
            )
            .is_some()
        {
            panic!("Node {node_id} already started!");
        }

        self.thread_control.push(wait);
        println!("Done Starting Node {node_id}");
    }

    pub fn stop(mut self, wait_for_data: bool, timeout: Option<Duration>) {
        let mut empty_set = HashSet::new();
        let mut empty_receiver_set = HashSet::new();

        if wait_for_data {
            // Wait for all buffers to be empty
            self.running
                .swap(GraphStatus::WaitingForDataToTerminate, Ordering::Relaxed);
            println!("Waiting for data to be consumed");
            while !self
                .node_threads
                .iter()
                .all(|n| empty_set.contains(n.0.as_str()))
            {
                debug!(
                    "Waiting node threads received done from {} out of {}",
                    empty_set.len(),
                    self.node_threads.len()
                );
                if let Some(duration) = timeout {
                    let done = self.worker_done.1.recv_timeout(duration).expect("Waiting for consumer nodes: Did not receive all done messages on time");
                    empty_set.insert(done);
                } else {
                    let done = self.worker_done.1.recv().expect("Waiting for consumer nodes: Synchronization channels have been closed. Terminating graph without waiting.");
                    empty_set.insert(done);
                }
            }
            while !self
                .read_threads
                .iter()
                .all(|n| empty_set.contains(n.0.as_str()))
            {
                debug!(
                    "Waiting reader threads received done from {} out of {}",
                    empty_receiver_set.len(),
                    self.read_threads.len()
                );
                if let Some(duration) = timeout {
                    let done = self.reader_empty.1.recv_timeout(duration).expect("Waiting for reader nodes: Did not receive all done messages on time");
                    empty_receiver_set.insert(done);
                } else {
                    let done = self.reader_empty.1.recv().expect("Waiting for reader nodes: Synchronization channels have been closed. Terminating graph without waiting.");
                    empty_receiver_set.insert(done);
                }
            }
        }
        self.running
            .swap(GraphStatus::Terminating, Ordering::Relaxed);

        
        let keys = self.node_threads.keys().cloned().collect_vec();
        for id in keys {
            println!("Waiting for node {id} to stop");
            self.node_threads.remove(&id).expect("Thread ID not found").join().unwrap_or_else(|_| panic!("Cannot join thread {id}"));
        }

        let keys = self.read_threads.keys().cloned().collect_vec();
        for id in keys {
            println!("Waiting for reader {id} to stop");
            self.read_threads.remove(&id).expect("Thread ID not found").join().unwrap_or_else(|_| panic!("Cannot join thread {id}"));
        }
        println!("Waiting for metrics to stop");
        self.metrics.stop();
    }
}

pub(super) struct ProcessorWorker<
    INPUT: InputGenerator + ChannelBuffer,
    OUTPUT: WriteChannelTrait + Send + 'static,
> {
    pub work_queue: Option<WorkQueue<INPUT::INPUT>>,
    pub processor: Processors<INPUT, OUTPUT>,
    pub write_channel: Option<TypedWriteChannel<OUTPUT>>,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum GraphStatus {
    Running = 0,
    Terminating = 1,
    WaitingForDataToTerminate = 2,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum WorkerStatus {
    Idle = 0,
    Running = 1,
    Terminating = 2,
}
