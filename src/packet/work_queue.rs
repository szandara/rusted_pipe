use std::time::Duration;

use crossbeam::channel::{bounded, unbounded, Receiver, Sender};

use crate::channels::ChannelError;

pub struct ReadEvent<T> {
    pub packet_data: T,
}

pub struct WorkQueue<T> {
    notifier: Sender<ReadEvent<T>>,
    queue: Receiver<ReadEvent<T>>,
    max_in_queue: usize,
}

impl<T> WorkQueue<T> {
    pub fn default() -> Self {
        let (notifier, queue) = unbounded::<ReadEvent<T>>();
        WorkQueue {
            queue,
            notifier,
            max_in_queue: std::usize::MAX,
        }
    }

    pub fn new(max_in_queue: usize) -> Self {
        let (notifier, queue) = bounded::<ReadEvent<T>>(max_in_queue);
        WorkQueue {
            queue,
            notifier,
            max_in_queue,
        }
    }

    pub fn push(&self, packet_set: T) {
        while self.queue.len() >= self.max_in_queue {
            self.queue
                .recv()
                .expect("Something is wrong, the work queue is closed.");
        }
        self.notifier
            .send(ReadEvent {
                packet_data: packet_set,
            })
            .expect("Something is wrong, the work queue is closed.");
    }

    pub fn get(&self, timeout: Option<Duration>) -> Result<ReadEvent<T>, ChannelError> {
        match timeout {
            Some(timeout) => Ok(self.queue.recv_timeout(timeout)?),
            None => Ok(self.queue.recv()?),
        }
    }
}
