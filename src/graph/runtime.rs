use super::{
    build::{ProcessorWorker, WorkerStatus},
    metrics::{ProfilerTag},
    processor::{Processors},
};
use crate::{channels::{read_channel::ReadChannel, typed_write_channel::TypedWriteChannel}, packet::work_queue::WorkQueue};
use crate::channels::ReadChannelTrait;
use crate::channels::WriteChannelTrait;
use crate::graph::build::GraphStatus;
use crate::{
    channels::read_channel::{ChannelBuffer, InputGenerator},
    RustedPipeError,
};
use atomic::{Atomic, Ordering};
use crossbeam::channel::Sender;
use lazy_static::lazy_static;
use log::{debug, warn};
use prometheus::{histogram_opts, register_histogram_vec};
use prometheus::{Histogram, HistogramVec};
use rusty_pool::ThreadPool;
use std::{
    sync::{Arc, Condvar, Mutex, PoisonError},
    thread,
    time::Duration
};

lazy_static! {
    static ref METRICS_TIMER: HistogramVec = register_histogram_vec!(
        histogram_opts!(
            "processing_time",
            format!("Timing for a single processor run."),
        ),
        &["node_id"]
    )
    .expect("Cannot create processing_time metrics");
}

pub(super) fn read_channel_data<T: InputGenerator + ChannelBuffer + Send>(
    id: String,
    running: Arc<Atomic<GraphStatus>>,
    mut read_channel: ReadChannel<T>,
    done_notification: Sender<String>,
) where
    T: ChannelBuffer + 'static,
{
    let id = id;
    while running.load(Ordering::Relaxed) != GraphStatus::Terminating {
        read_channel.read(id.clone(), done_notification.clone());
    }
    read_channel.stop();
}

pub(super) type Wait = Arc<(Mutex<WorkerStatus>, Condvar)>;

pub(super) struct ConsumerThread<INPUT, OUTPUT>
where
    INPUT: InputGenerator + ChannelBuffer + Send + 'static,
    OUTPUT: WriteChannelTrait + 'static + Send,
{
    id: String,
    running: Arc<Atomic<GraphStatus>>,
    _free: Wait,
    done_notification: Sender<String>,
    thread_pool: ThreadPool,
    metrics_timer: Histogram,
    profiler: Arc<ProfilerTag>,
    shared_writer: Option<Arc<Mutex<TypedWriteChannel<OUTPUT>>>>,
    shared_processor: Arc<Mutex<Processors<INPUT, OUTPUT>>>,
    status: Arc<Atomic<WorkerStatus>>,
    work_queue: Option<WorkQueue<INPUT::INPUT>>
}

impl<INPUT, OUTPUT> ConsumerThread<INPUT, OUTPUT>
where
    INPUT: InputGenerator + ChannelBuffer + Send + 'static,
    OUTPUT: WriteChannelTrait + 'static + Send,
{
    pub(super) fn new(
        id: String,
        running: Arc<Atomic<GraphStatus>>,
        free: Wait,
        worker: ProcessorWorker<INPUT, OUTPUT>,
        done_notification: Sender<String>,
        thread_pool: ThreadPool,
        profiler: ProfilerTag,
    ) -> Self {
        let metrics_timer = METRICS_TIMER.with_label_values(&[&id]);
        
        let mut shared_writer = None;
        if let Some(channel) = worker.write_channel {
            shared_writer = Some(Arc::new(Mutex::new(channel)));
        }
       
        let shared_processor = Arc::new(Mutex::new(worker.processor));
        let status = Arc::new(Atomic::new(WorkerStatus::Idle));
        let work_queue = worker.work_queue;
        Self {
            id,
            running,
            _free: free,
            done_notification,
            thread_pool,
            metrics_timer,
            profiler: Arc::new(profiler),
            shared_writer,
            shared_processor,
            status,
            work_queue
        }
    }

    pub(super) fn consume(&mut self) {
        while self.running.load(Ordering::Relaxed) != GraphStatus::Terminating {
            if self.status.load(Ordering::Relaxed) == WorkerStatus::Idle {
                let lock_status = self.status.clone();

                let mut packet = None;
                if let Some(work_queue) = self.work_queue.as_mut() {
                    let task = work_queue.get(Some(Duration::from_millis(100)));
                    if let Ok(read_event) = task {
                        packet = Some(read_event.packet_data);
                    } else {
                        if self.running.load(Ordering::Relaxed)
                            == GraphStatus::WaitingForDataToTerminate
                        {
                            debug!("Sending done {}", self.id);
                            let _ = self.done_notification.send(self.id.clone());
                        }

                        continue;
                    }
                }
                self.status.store(WorkerStatus::Running, Ordering::Relaxed);

                let processor_clone = self.shared_processor.clone();
                let profiler_clone = self.profiler.clone();
                let id_thread = self.id.clone();
                let arc_write_channel = self.shared_writer.clone();
                let done_clone = self.done_notification.clone();
                let metrics_clone = self.metrics_timer.clone();

                let future = move || {
                    profiler_clone.add("consumer".to_string(), id_thread.clone());
                    let timer = metrics_clone.start_timer();
                    let result = match &mut *processor_clone.lock().unwrap_or_else(PoisonError::into_inner) {
                        Processors::Processor(proc) => {
                            if let Some(packet) = packet {
                                let write_channel = arc_write_channel.expect(&format!("Consumer thread for node {} was created without write channel", id_thread));
                                let write_channel = write_channel.lock().unwrap_or_else(PoisonError::into_inner);
                                
                                proc.handle(packet, write_channel)
                            } else {
                                warn!("Packet is None, not processing");
                                return;
                            }

                        }
                        Processors::TerminalProcessor(proc) => {
                            if let Some(packet) = packet {
                                proc.handle(packet)
                            } else {
                                warn!("Packet is None, not processing");
                                return;
                            }
                        },
                        Processors::SourceProcessor(proc) => {
                            let write_channel = arc_write_channel.expect(&format!("Consumer thread for node {} was created without write channel", id_thread));
                            let write_channel = write_channel.lock().unwrap_or_else(PoisonError::into_inner);
                            
                            proc.handle(write_channel)
                        }
                    };
                    
                    profiler_clone
                        .remove("consumer".to_string(), id_thread.clone());
                    timer.observe_duration();
                    match result {
                        Ok(_) => lock_status.store(WorkerStatus::Idle, Ordering::Relaxed),
                        Err(RustedPipeError::EndOfStream()) => {
                            eprintln!("Terminating worker {id_thread:?}");
                            lock_status.store(WorkerStatus::Terminating, Ordering::Relaxed);
                            let _ = done_clone.send(id_thread.clone());
                        }
                        Err(err) => {
                            eprintln!("Error in worker {id_thread:?}: {err:?}");
                            lock_status.store(WorkerStatus::Terminating, Ordering::Relaxed);
                        }
                    };
                    
                };

                let handle = self.thread_pool.evaluate(future);
                if handle.try_await_complete().is_err() {
                    eprintln!("Thread panicked in worker {:?}", self.id.clone());
                    self.status
                        .store(WorkerStatus::Idle, Ordering::Relaxed);
                }
            } else {
                thread::sleep(Duration::from_millis(100));
                if self.running.load(Ordering::Relaxed) == GraphStatus::WaitingForDataToTerminate {
                    debug!("Sending done {}", self.id);
                   let _ = self.done_notification.send(self.id.clone());
                }
            }
            
        }
        println!("Worker {} exited", self.id);
    }
}
