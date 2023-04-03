use std::{
    sync::{Arc, Condvar, Mutex},
    thread,
    time::Duration,
};

use crate::channels::read_channel::ReadChannel;
use crate::channels::ReadChannelTrait;
use crate::channels::WriteChannelTrait;
use crate::{
    channels::read_channel::{ChannelBuffer, InputGenerator},
    RustedPipeError,
};
use atomic::{Atomic, Ordering};
use crossbeam::channel::Sender;

use super::{
    graph::{ProcessorWorker, WorkerStatus},
    processor::Processors,
};
use crate::graph::graph::GraphStatus;

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

pub(super) fn consume<INPUT, OUTPUT>(
    id: String,
    running: Arc<Atomic<GraphStatus>>,
    _free: Wait,
    worker: ProcessorWorker<INPUT, OUTPUT>,
    done_notification: Sender<String>,
    thread_pool: futures::executor::ThreadPool,
) where
    INPUT: InputGenerator + ChannelBuffer + Send + 'static,
    OUTPUT: WriteChannelTrait + 'static + Send,
{
    while running.load(Ordering::Relaxed) != GraphStatus::Terminating {
        if worker.status.load(Ordering::Relaxed) == WorkerStatus::Idle {
            let lock_status = worker.status.clone();

            let mut packet = None;
            if let Some(work_queue) = worker.work_queue.as_ref() {
                let task = work_queue.get(Some(Duration::from_millis(100)));
                if let Ok(read_event) = task {
                    packet = Some(read_event.packet_data);
                } else {
                    if running.load(Ordering::Relaxed) == GraphStatus::WaitingForDataToTerminate {
                        println!("Sending done {id}");
                        done_notification.send(id.clone()).unwrap();
                    }

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

            thread_pool.spawn_ok(future);
        }
        // Looping until the application is not shut down.
        thread::sleep(Duration::from_millis(5));
    }
}
