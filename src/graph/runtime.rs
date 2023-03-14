use std::{
    sync::{Arc, Condvar, Mutex},
    thread,
    time::Duration,
};

use atomic::{Atomic, Ordering};
use crossbeam::channel::Sender;

use crate::{
    channels::{
        typed_read_channel::{ChannelBuffer, OutputDelivery},
        typed_write_channel::Writer,
        ReadChannel,
    },
    RustedPipeError,
};

use super::{
    graph::{ProcessorWorker, WorkerStatus},
    processor::Processors,
};
use crate::graph::graph::GraphStatus;

pub(super) fn read_channel_data<T: OutputDelivery>(
    id: String,
    running: Arc<Atomic<GraphStatus>>,
    mut read_channel: ReadChannel<T>,
    done_notification: Sender<String>,
) where
    T: ChannelBuffer + 'static,
{
    println!("Reading read {}", id);
    let id = id.to_string();
    while running.load(Ordering::Relaxed) != GraphStatus::Terminating {
        read_channel.read(id.clone(), done_notification.clone());
    }
    println!("Terminating read {}", id);
    read_channel.stop();
}

pub(super) type Wait = Arc<(Mutex<WorkerStatus>, Condvar)>;

pub(super) fn consume<INPUT: OutputDelivery + ChannelBuffer + Send + 'static, OUTPUT>(
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
                    packet = Some(read_event.packet_data);
                } else {
                    // TODO: make work_queue timeout
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
    }
}
