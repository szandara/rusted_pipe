use std::{fmt, sync::MutexGuard};

use crate::buffers::synchronizers::PacketSynchronizer;
use crate::channels::WriteChannelTrait;
use crate::packet::work_queue::WorkQueue;
use crate::{
    channels::{
        read_channel::ReadChannel,
        read_channel::{ChannelBuffer, InputGenerator},
        typed_write_channel::TypedWriteChannel,
    },
    RustedPipeError,
};

pub enum Nodes<INPUT: InputGenerator + ChannelBuffer + Send, OUTPUT: WriteChannelTrait + 'static> {
    TerminalHandler(Box<TerminalNode<INPUT>>),
    NodeHandler(Box<Node<INPUT, OUTPUT>>),
    SourceHandler(Box<SourceNode<OUTPUT>>),
}

pub enum Processors<INPUT: InputGenerator + ChannelBuffer, OUTPUT: WriteChannelTrait + 'static> {
    SourceProcessor(Box<dyn SourceProcessor<OUTPUT = OUTPUT>>),
    Processor(Box<dyn Processor<INPUT = INPUT, OUTPUT = OUTPUT>>),
    TerminalProcessor(Box<dyn TerminalProcessor<INPUT = INPUT>>),
}

/// PROCESSORS
pub struct Node<INPUT: InputGenerator + ChannelBuffer + Send, OUTPUT: WriteChannelTrait + 'static> {
    pub id: String,
    pub read_channel: ReadChannel<INPUT>,
    pub handler: Box<dyn Processor<INPUT = INPUT, OUTPUT = OUTPUT>>,
    pub work_queue: WorkQueue<INPUT::INPUT>,
    pub write_channel: TypedWriteChannel<OUTPUT>,
}

impl<
        INPUT: InputGenerator + ChannelBuffer + Send + 'static,
        OUTPUT: WriteChannelTrait + 'static,
    > Node<INPUT, OUTPUT>
{
    pub fn create(
        id: String,
        processor: Box<dyn Processor<INPUT = INPUT, OUTPUT = OUTPUT>>,
        read_channel: ReadChannel<INPUT>,
        writer_channel: OUTPUT,
    ) -> Node<INPUT, OUTPUT> {
        let id = id.clone();
        let write_channel = TypedWriteChannel {
            writer: Box::new(writer_channel),
        };
        let work_queue = read_channel.work_queue.as_ref().unwrap().clone();
        Node {
            handler: processor,
            read_channel,
            write_channel,
            work_queue,
            id,
        }
    }

    pub fn create_common(
        id: String,
        processor: Box<dyn Processor<INPUT = INPUT, OUTPUT = OUTPUT>>,
        block_channel_full: bool,
        channel_buffer_size: usize,
        process_buffer_size: usize,
        synchronizer_type: Box<dyn PacketSynchronizer>,
    ) -> Self {
        let write_channel = TypedWriteChannel {
            writer: Box::new(OUTPUT::create()),
        };

        let read_channel = ReadChannel::<INPUT>::create(
            block_channel_full,
            channel_buffer_size,
            process_buffer_size,
            synchronizer_type,
        );
        let work_queue = read_channel.work_queue.as_ref().unwrap().clone();

        Self {
            id,
            read_channel,
            work_queue,
            handler: processor,
            write_channel,
        }
    }
}

pub struct SourceNode<OUTPUT: WriteChannelTrait + 'static> {
    pub id: String,
    pub write_channel: TypedWriteChannel<OUTPUT>,
    pub handler: Box<dyn SourceProcessor<OUTPUT = OUTPUT>>,
}

impl<OUTPUT: WriteChannelTrait + 'static> SourceNode<OUTPUT> {
    pub fn create(
        id: String,
        processor: Box<dyn SourceProcessor<OUTPUT = OUTPUT>>,
        writer_channel: OUTPUT,
    ) -> SourceNode<OUTPUT> {
        let write_channel = TypedWriteChannel {
            writer: Box::new(writer_channel),
        };
        let id = id.clone();
        SourceNode {
            handler: processor,
            write_channel,
            id,
        }
    }

    pub fn create_common(id: String, processor: Box<dyn SourceProcessor<OUTPUT = OUTPUT>>) -> Self {
        let write_channel = TypedWriteChannel {
            writer: Box::new(OUTPUT::create()),
        };

        Self {
            id,
            handler: processor,
            write_channel,
        }
    }
}

pub struct TerminalNode<INPUT: InputGenerator + ChannelBuffer + Send> {
    pub id: String,
    pub read_channel: ReadChannel<INPUT>,
    pub handler: Box<dyn TerminalProcessor<INPUT = INPUT>>,
    pub work_queue: WorkQueue<INPUT::INPUT>,
}

impl<INPUT: InputGenerator + ChannelBuffer + Send + 'static> TerminalNode<INPUT> {
    pub fn create(
        id: String,
        processor: Box<dyn TerminalProcessor<INPUT = INPUT>>,
        read_channel: ReadChannel<INPUT>,
    ) -> TerminalNode<INPUT> {
        let id = id.clone();
        let work_queue = read_channel.work_queue.as_ref().unwrap().clone();
        TerminalNode {
            handler: processor,
            read_channel,
            work_queue,
            id,
        }
    }

    pub fn create_common(
        id: String,
        processor: Box<dyn TerminalProcessor<INPUT = INPUT>>,
        block_channel_full: bool,
        channel_buffer_size: usize,
        process_buffer_size: usize,
        synchronizer_type: Box<dyn PacketSynchronizer>,
    ) -> Self {
        let read_channel = ReadChannel::<INPUT>::create(
            block_channel_full,
            channel_buffer_size,
            process_buffer_size,
            synchronizer_type,
        );
        let work_queue = read_channel.work_queue.as_ref().unwrap().clone();

        Self {
            id,
            read_channel,
            handler: processor,
            work_queue,
        }
    }
}

impl<INPUT: InputGenerator + ChannelBuffer + Send, OUTPUT: WriteChannelTrait> fmt::Debug
    for Node<INPUT, OUTPUT>
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.id)
    }
}

pub trait SourceProcessor: Sync + Send {
    type OUTPUT: WriteChannelTrait;
    fn handle(
        &mut self,
        output: MutexGuard<TypedWriteChannel<Self::OUTPUT>>,
    ) -> Result<(), RustedPipeError>;
}

pub type ProcessorWriter<'a, T> = MutexGuard<'a, TypedWriteChannel<T>>;

pub trait Processor: Sync + Send {
    type INPUT: InputGenerator;
    type OUTPUT: WriteChannelTrait;

    fn handle(
        &mut self,
        input: <Self::INPUT as InputGenerator>::INPUT,
        output: ProcessorWriter<Self::OUTPUT>,
    ) -> Result<(), RustedPipeError>;
}

pub trait TerminalProcessor: Sync + Send {
    type INPUT: InputGenerator;
    fn handle(
        &mut self,
        input: <Self::INPUT as InputGenerator>::INPUT,
    ) -> Result<(), RustedPipeError>;
}
