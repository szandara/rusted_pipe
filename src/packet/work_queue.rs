use std::time::Duration;

use crossbeam::channel::{bounded, unbounded, Receiver, Sender};

use crate::{channels::ChannelError, buffers::single_buffers::LenTrait, graph::metrics::BufferMonitor};

pub struct ReadEvent<T> {
    pub packet_data: T,
}

pub struct WorkQueue<T> {
    notifier: Sender<ReadEvent<T>>,
    queue: Receiver<ReadEvent<T>>,
    max_in_queue: usize,
    /// A monitor for upcoming work.    
    monitor: BufferMonitor
}

impl<T> Clone for WorkQueue<T> {
    fn clone(&self) -> Self {
        Self {
            notifier: self.notifier.clone(),
            queue: self.queue.clone(),
            max_in_queue: self.max_in_queue,
            monitor: self.monitor.clone()
        }
    }
}

impl<T> LenTrait for WorkQueue<T> {
    fn len(&self) -> usize {
        self.queue.len()
    }
}

impl<T> Default for WorkQueue<T> {
    fn default() -> Self {
        let (notifier, queue) = unbounded::<ReadEvent<T>>();
        WorkQueue {
            queue,
            notifier,
            max_in_queue: std::usize::MAX,
            monitor: BufferMonitor::default()
        }
    }
}

impl<T> WorkQueue<T> {
    
    pub fn new(max_in_queue: usize, monitor: BufferMonitor) -> Self {
        let (notifier, queue) = bounded::<ReadEvent<T>>(max_in_queue);
        WorkQueue {
            queue,
            notifier,
            max_in_queue,
            monitor
        }
    }

    pub fn push(&mut self, packet_set: T) {
        while self.queue.len() >= self.max_in_queue {
            self.queue
                .recv()
                .expect("Something is wrong, the work queue is closed.");
            self.monitor.dec();
        }
        self.notifier
            .send(ReadEvent {
                packet_data: packet_set,
            })
            .expect("Something is wrong, the work queue is closed.");
        self.monitor.inc();
    }

    pub fn get(&mut self, timeout: Option<Duration>) -> Result<ReadEvent<T>, ChannelError> {
        let event = match timeout {
            Some(timeout) => Ok(self.queue.recv_timeout(timeout)?),
            None => Ok(self.queue.recv()?),
        };
        self.monitor.dec();
        event
    }
}
