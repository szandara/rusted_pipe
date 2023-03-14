use std::{
    collections::HashSet,
    sync::{Arc, Condvar, Mutex},
    thread::{self, JoinHandle},
    time::Duration,
};

use atomic::{Atomic, Ordering};
use crossbeam::channel::{unbounded, Receiver, Sender};

use crate::{
    buffers::single_buffers::FixedSizeBuffer,
    channels::{
        typed_channel,
        typed_read_channel::{BufferReceiver, ChannelBuffer, OutputDelivery},
        typed_write_channel::{BufferWriter, TypedWriteChannel, Writer},
    },
    graph::{
        processor::Processors,
        runtime::{consume, read_channel_data},
    },
    packet::WorkQueue,
    RustedPipeError,
};

use super::{processor::Nodes, runtime::Wait};

pub struct Graph {
    running: Arc<Atomic<GraphStatus>>,
    thread_control: Vec<Wait>,
    pool: futures::executor::ThreadPool,
    node_threads: Vec<(String, JoinHandle<()>)>,
    read_threads: Vec<(String, JoinHandle<()>)>,
    worker_done: (Sender<String>, Receiver<String>),
    reader_empty: (Sender<String>, Receiver<String>),
}

pub fn link<U: Clone + 'static>(
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

    fn get_worker<
        INPUT: Send + OutputDelivery + ChannelBuffer + 'static,
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
                    },
                )
            }
        }
    }

    pub fn start_node<
        INPUT: Send + OutputDelivery + ChannelBuffer + 'static,
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

pub(super) struct ProcessorWorker<
    INPUT: OutputDelivery + ChannelBuffer,
    OUTPUT: Writer + Send + 'static,
> {
    pub work_queue: Option<Arc<WorkQueue<INPUT::OUTPUT>>>,
    pub processor: Arc<Mutex<Processors<INPUT, OUTPUT>>>,
    pub write_channel: Option<Arc<Mutex<TypedWriteChannel<OUTPUT>>>>,
    pub status: Arc<Atomic<WorkerStatus>>,
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
