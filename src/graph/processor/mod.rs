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
    Processor(Box<dyn Processor<INPUT, WRITE = OUTPUT>>),
    TerminalProcessor(Box<dyn TerminalProcessor<INPUT>>),
}

/// PROCESSORS
pub struct Node<INPUT: InputGenerator + ChannelBuffer + Send, OUTPUT: Writer + 'static> {
    pub id: String,
    pub read_channel: ReadChannel<INPUT>,
    pub handler: Box<dyn Processor<INPUT, WRITE = OUTPUT>>,
    pub work_queue: WorkQueue<INPUT::INPUT>,
    pub write_channel: TypedWriteChannel<OUTPUT>,
}

pub struct SourceNode<WRITE: Writer + 'static> {
    pub id: String,
    pub write_channel: TypedWriteChannel<WRITE>,
    pub handler: Box<dyn SourceProcessor<WRITE = WRITE>>,
}

pub struct TerminalNode<INPUT: InputGenerator + ChannelBuffer + Send> {
    pub id: String,
    pub read_channel: ReadChannel<INPUT>,
    pub handler: Box<dyn TerminalProcessor<INPUT>>,
    pub work_queue: WorkQueue<INPUT::INPUT>,
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

pub trait Processor<INPUT: InputGenerator>: Sync + Send {
    type WRITE: Writer;
    fn handle(
        &mut self,
        input: INPUT::INPUT,
        output: MutexGuard<TypedWriteChannel<Self::WRITE>>,
    ) -> Result<(), RustedPipeError>;

    fn id(&self) -> &String;
}

pub trait TerminalProcessor<INPUT: InputGenerator>: Sync + Send {
    fn handle(&mut self, input: INPUT::INPUT) -> Result<(), RustedPipeError>;

    fn id(&self) -> &String;
}
