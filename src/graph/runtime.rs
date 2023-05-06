use super::{
    graph::{ProcessorWorker, WorkerStatus},
    metrics::ProfilerTag,
    processor::Processors,
};
use crate::channels::read_channel::ReadChannel;
use crate::channels::ReadChannelTrait;
use crate::channels::WriteChannelTrait;
use crate::graph::graph::GraphStatus;
use crate::{
    channels::read_channel::{ChannelBuffer, InputGenerator},
    RustedPipeError,
};
use atomic::{Atomic, Ordering};
use crossbeam::channel::Sender;
use log::debug;
use prometheus::register_histogram;
use prometheus::Histogram;
use std::{
    sync::{Arc, Condvar, Mutex},
    thread,
    time::Duration,
};

pub(super) fn read_channel_data<T: InputGenerator + ChannelBuffer + Send>(
    id: String,
    running: Arc<Atomic<GraphStatus>>,
    mut read_channel: ReadChannel<T>,
    done_notification: Sender<String>,
) where
    T: ChannelBuffer + 'static,
{
    let id = id.to_string();
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
    worker: ProcessorWorker<INPUT, OUTPUT>,
    done_notification: Sender<String>,
    thread_pool: futures::executor::ThreadPool,
    metrics_timer: Histogram,
    profiler: Arc<ProfilerTag>,
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
        thread_pool: futures::executor::ThreadPool,
        profiler: ProfilerTag,
    ) -> Self {
        let metrics_timer = register_histogram!(id.clone(), format!("Timing for node: {}", &id))
            .expect(&format!("Cannot create timer {}", id.clone()));

        Self {
            id,
            running,
            _free: free,
            worker,
            done_notification,
            thread_pool,
            metrics_timer,
            profiler: Arc::new(profiler),
        }
    }

    pub(super) fn consume(&self) {
        while self.running.load(Ordering::Relaxed) != GraphStatus::Terminating {
            if self.worker.status.load(Ordering::Relaxed) == WorkerStatus::Idle {
                let lock_status = self.worker.status.clone();

                let mut packet = None;
                if let Some(work_queue) = self.worker.work_queue.as_ref() {
                    let task = work_queue.get(Some(Duration::from_millis(100)));
                    if let Ok(read_event) = task {
                        packet = Some(read_event.packet_data);
                    } else {
                        if self.running.load(Ordering::Relaxed)
                            == GraphStatus::WaitingForDataToTerminate
                        {
                            debug!("Sending done {}", self.id);
                            self.done_notification.send(self.id.clone()).unwrap();
                        }

                        continue;
                    }
                }

                self.worker
                    .status
                    .store(WorkerStatus::Running, Ordering::Relaxed);

                let processor_clone = self.worker.processor.clone();
                let id_thread = self.id.clone();
                let arc_write_channel = self.worker.write_channel.clone();
                let done_clone = self.done_notification.clone();
                let metrics_clone = self.metrics_timer.clone();
                let profiler_clone = self.profiler.clone();
                let future = async move {
                    profiler_clone.add("consumer".to_string(), id_thread.clone());

                    let timer = metrics_clone.start_timer();
                    let result = match &mut *processor_clone.lock().unwrap() {
                        Processors::Processor(proc) => {
                            proc.handle(packet.unwrap(), arc_write_channel.unwrap().lock().unwrap())
                        }
                        Processors::TerminalProcessor(proc) => proc.handle(packet.unwrap()),
                        Processors::SourceProcessor(proc) => {
                            proc.handle(arc_write_channel.unwrap().lock().unwrap())
                        }
                    };
                    profiler_clone.remove("consumer".to_string(), id_thread.clone());
                    timer.observe_duration();
                    match result {
                        Ok(_) => lock_status.store(WorkerStatus::Idle, Ordering::Relaxed),
                        Err(RustedPipeError::EndOfStream()) => {
                            eprintln!("Terminating worker {id_thread:?}");
                            lock_status.store(WorkerStatus::Terminating, Ordering::Relaxed);
                            done_clone.send(id_thread.clone()).unwrap();
                        }
                        Err(err) => {
                            eprintln!("Error in worker {id_thread:?}: {err:?}");
                            lock_status.store(WorkerStatus::Idle, Ordering::Relaxed)
                        }
                    }
                };

                self.thread_pool.spawn_ok(future);
            }
            // Looping until the application is not shut down.
            thread::sleep(Duration::from_millis(5));
        }
    }
}
