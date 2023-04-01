use std::{fmt, sync::MutexGuard};

use crate::{
    channels::{
        read_channel::ReadChannel,
        read_channel::{ChannelBuffer, InputGenerator},
        typed_write_channel::{TypedWriteChannel, Writer},
    },
    RustedPipeError,
};

use crate::packet::work_queue::WorkQueue;

pub enum Nodes<INPUT: InputGenerator + ChannelBuffer + Send, OUTPUT: Writer + 'static> {
    TerminalHandler(Box<TerminalNode<INPUT>>),
    NodeHandler(Box<Node<INPUT, OUTPUT>>),
    SourceHandler(Box<SourceNode<OUTPUT>>),
}

pub enum Processors<INPUT: InputGenerator + ChannelBuffer, OUTPUT: Writer + 'static> {
    SourceProcessor(Box<dyn SourceProcessor<WRITE = OUTPUT>>),
    Processor(Box<dyn Processor<INPUT = INPUT, OUTPUT = OUTPUT>>),
    TerminalProcessor(Box<dyn TerminalProcessor<INPUT = INPUT>>),
}

/// PROCESSORS
pub struct Node<INPUT: InputGenerator + ChannelBuffer + Send, OUTPUT: Writer + 'static> {
    pub id: String,
    pub read_channel: ReadChannel<INPUT>,
    pub handler: Box<dyn Processor<INPUT = INPUT, OUTPUT = OUTPUT>>,
    pub work_queue: WorkQueue<INPUT::INPUT>,
    pub write_channel: TypedWriteChannel<OUTPUT>,
}

impl<INPUT: InputGenerator + ChannelBuffer + Send, OUTPUT: Writer + 'static> Node<INPUT, OUTPUT> {
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
}

pub struct SourceNode<WRITE: Writer + 'static> {
    pub id: String,
    pub write_channel: TypedWriteChannel<WRITE>,
    pub handler: Box<dyn SourceProcessor<WRITE = WRITE>>,
}

impl<WRITE: Writer + 'static> SourceNode<WRITE> {
    pub fn create(
        id: String,
        processor: Box<dyn SourceProcessor<WRITE = WRITE>>,
        writer_channel: WRITE,
    ) -> SourceNode<WRITE> {
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
}

pub struct TerminalNode<INPUT: InputGenerator + ChannelBuffer + Send> {
    pub id: String,
    pub read_channel: ReadChannel<INPUT>,
    pub handler: Box<dyn TerminalProcessor<INPUT = INPUT>>,
    pub work_queue: WorkQueue<INPUT::INPUT>,
}

impl<INPUT: InputGenerator + ChannelBuffer + Send> TerminalNode<INPUT> {
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
}

impl<INPUT: InputGenerator + ChannelBuffer + Send, OUTPUT: Writer> fmt::Debug
    for Node<INPUT, OUTPUT>
{
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

pub trait Processor: Sync + Send {
    type INPUT: InputGenerator;
    type OUTPUT: Writer;
    fn handle(
        &mut self,
        input: <Self::INPUT as InputGenerator>::INPUT,
        output: MutexGuard<TypedWriteChannel<Self::OUTPUT>>,
    ) -> Result<(), RustedPipeError>;

    fn id(&self) -> &String;
}

pub trait TerminalProcessor: Sync + Send {
    type INPUT: InputGenerator;
    fn handle(
        &mut self,
        input: <Self::INPUT as InputGenerator>::INPUT,
    ) -> Result<(), RustedPipeError>;

    fn id(&self) -> &String;
}
