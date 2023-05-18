/// This module holds the traits required to create Processors and Nodes.
///
/// A processor is class that implements a handle method for processing input data.
/// It's the main interface that needs to be implemented to interface with Rusted Pipe.
///
/// A node instead is what encapsulate a processor plus its input and output channels and it
/// forms the core element that is used by the graph for its data flow.
///
/// There are three different processor types (and nodes): TerminalProcessor, Processor, SourceProcessor.
///
/// TerminalProcessors do not have an output channel or type.
/// SourceProcessors do not have an input channel or type.
/// Processor has both.
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

/// A collection of the three node types that. Even though typed both INPUT and OUTPUT, some nodes
/// do not have those types. For those, use NoInput or NoOutput.
pub enum Nodes<INPUT: InputGenerator + ChannelBuffer + Send, OUTPUT: WriteChannelTrait + 'static> {
    TerminalNode(Box<TerminalNode<INPUT>>),
    Node(Box<Node<INPUT, OUTPUT>>),
    SourceNode(Box<SourceNode<OUTPUT>>),
}

/// A collection of the three processor types that. Even though typed both INPUT and OUTPUT, some nodes
/// do not have those types. For those, use NoInput or NoOutput.
pub enum Processors<INPUT: InputGenerator + ChannelBuffer, OUTPUT: WriteChannelTrait + 'static> {
    SourceProcessor(Box<dyn SourceProcessor<OUTPUT = OUTPUT>>),
    Processor(Box<dyn Processor<INPUT = INPUT, OUTPUT = OUTPUT>>),
    TerminalProcessor(Box<dyn TerminalProcessor<INPUT = INPUT>>),
}

/// Node processor structure. It expects an input and outputs some data.
pub struct Node<INPUT: InputGenerator + ChannelBuffer + Send, OUTPUT: WriteChannelTrait + 'static> {
    // Id of the node, important to differentiate instances of the same processor.
    pub id: String,
    // ReadChannel that reads input data.
    pub read_channel: ReadChannel<INPUT>,
    // Processor assigned to this node.
    pub handler: Box<dyn Processor<INPUT = INPUT, OUTPUT = OUTPUT>>,
    // Queue that holds the processor inputs as they are matched by the syncrhonizer.
    // This struct is shared with the ReadChannel that fills it with stuff to process.
    // Currently RustedPipe is sequential on each node and does not process data in parallel.
    pub work_queue: WorkQueue<INPUT::INPUT>,
    // WriteChannel to output data into the graph.
    pub write_channel: TypedWriteChannel<OUTPUT>,
}

impl<
        INPUT: InputGenerator + ChannelBuffer + Send + 'static,
        OUTPUT: WriteChannelTrait + 'static,
    > Node<INPUT, OUTPUT>
{
    /// Create a new Node
    ///
    /// * Arguments
    /// `id` - Id of the node.
    /// `processor` - A boxed instance of the processor that handles the data packets.
    /// `read_channel` - A ReadChannel object for input data.
    /// `write_channel` - A WriteChannel object for output data.
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

    /// A helper method for creating Nodes with the most common parameters instead
    /// of manually passing Read and Write channels.
    ///
    /// * Arguments
    /// `id` - Id of the node. It must be unique in the graph.
    /// `processor` - A boxed instance of the processor that handles the data packets.
    /// `block_channel_full` - If true, the ReadChannel will stop adding data to their buffers when full.
    /// Instead it would return an error and the graph will continue try to push the data until there is
    /// a free spot. In that case, the data will leave in the communication channel between WriteChannel and ReadChannel which is unbounded.
    /// This can easily cause OOM errors if not handled. Ideally you should avoid your buffers getting full.
    /// `channel_buffer_size` - The size of each of the buffers of the ReadChannel. Each
    /// channel of the ReadChannel will have the same size.
    /// `process_buffer_size` - The size of the work queue. It will drop stuff to process when full.
    /// `synchronizer_type` -  A synchronizer that matches data in the ReadChannel.
    /// `queue_monitor` -  True if queues should be monitored and available in Grafana.
    pub fn create_common(
        id: String,
        processor: Box<dyn Processor<INPUT = INPUT, OUTPUT = OUTPUT>>,
        block_channel_full: bool,
        channel_buffer_size: usize,
        process_buffer_size: usize,
        synchronizer_type: Box<dyn PacketSynchronizer>,
        queue_monitor: bool,
    ) -> Self {
        let write_channel = TypedWriteChannel {
            writer: Box::new(OUTPUT::create()),
        };

        let read_channel = ReadChannel::<INPUT>::create(
            &id,
            block_channel_full,
            channel_buffer_size,
            process_buffer_size,
            synchronizer_type,
            queue_monitor,
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

/// Source node structure. It is able to emit data without any input.
/// The graph calls this node any time it's not processing anything. Users
/// must do their own frame rate control inside this method.
pub struct SourceNode<OUTPUT: WriteChannelTrait + 'static> {
    // Id of the node, important to differentiate instances of the same processor.
    pub id: String,
    // WriteChannel to output data into the graph.
    pub write_channel: TypedWriteChannel<OUTPUT>,
    // Processor assigned to this node.
    pub handler: Box<dyn SourceProcessor<OUTPUT = OUTPUT>>,
}

impl<OUTPUT: WriteChannelTrait + 'static> SourceNode<OUTPUT> {
    /// Create a new SourceNode
    ///
    /// * Arguments
    /// `id` - Id of the node. It must be unique in the graph.
    /// `processor` - A boxed instance of the processor that handles the data packets.
    /// `write_channel` - A WriteChannel object for output data.
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

    /// A helper method for creating a SourceNode with the most common parameters instead
    /// of manually passing a ReadChannel.
    ///
    /// * Arguments
    /// `id` - Id of the node. It must be unique in the graph.
    /// `processor` - A boxed instance of the processor that handles the data packets.
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

/// Terminal node structure. This node expects data matched by its ReadChannel
/// but it's not able to output anything and has no WriteChannel.
pub struct TerminalNode<INPUT: InputGenerator + ChannelBuffer + Send> {
    // Id of the node, important to differentiate instances of the same processor.
    pub id: String,
    // ReadChannel that reads input data.
    pub read_channel: ReadChannel<INPUT>,
    // Processor assigned to this node.
    pub handler: Box<dyn TerminalProcessor<INPUT = INPUT>>,
    // Queue that holds the processor inputs as they are matched by the syncrhonizer.
    // This struct is shared with the ReadChannel that fills it with stuff to process.
    // Currently RustedPipe is sequential on each node and does not process data in parallel.
    pub work_queue: WorkQueue<INPUT::INPUT>,
}

impl<INPUT: InputGenerator + ChannelBuffer + Send + 'static> TerminalNode<INPUT> {
    /// Create a new Node
    ///
    /// * Arguments
    /// `id` - Id of the node.
    /// `processor` - A boxed instance of the processor that handles the data packets.
    /// `read_channel` - A ReadChannel object for input data.
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
    /// A helper method for creating Nodes with the most common parameters instead
    /// of manually passing Read and Write channels.
    ///
    /// * Arguments
    /// `id` - Id of the node. It must be unique in the graph.
    /// `processor` - A boxed instance of the processor that handles the data packets.
    /// `block_channel_full` - If true, the ReadChannel will stop adding data to their buffers when full.
    /// Instead it would return an error and the graph will continue try to push the data until there is
    /// a free spot. In that case, the data will leave in the communication channel between WriteChannel and ReadChannel which is unbounded.
    /// This can easily cause OOM errors if not handled. Ideally you should avoid your buffers getting full.
    /// `channel_buffer_size` - The size of each of the buffers of the ReadChannel. Each
    /// channel of the ReadChannel will have the same size.
    /// `process_buffer_size` - The size of the work queue. It will drop stuff to process when full.
    /// `synchronizer_type` -  A synchronizer that matches data in the ReadChannel.
    /// `queue_monitor` -  True if queues should be monitored and available in Grafana.
    pub fn create_common(
        id: String,
        processor: Box<dyn TerminalProcessor<INPUT = INPUT>>,
        block_channel_full: bool,
        channel_buffer_size: usize,
        process_buffer_size: usize,
        synchronizer_type: Box<dyn PacketSynchronizer>,
        queue_monitor: bool,
    ) -> Self {
        let read_channel = ReadChannel::<INPUT>::create(
            &id,
            block_channel_full,
            channel_buffer_size,
            process_buffer_size,
            synchronizer_type,
            queue_monitor,
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

/// SourceProcessor trait. This trait must be implemented for all those nodes
/// in your graph that produce data without any input. (ie. frame readers, sensor readers, etc.).
/// `handle` is called continuously by the Graph but sequentially, so once at the time.
/// Users must deal with frame rate limiting within this method if they want to alterate the fps of their
/// producer.
pub trait SourceProcessor: Sync + Send {
    /// Trait object that gives access to the output channels for writing.
    type OUTPUT: WriteChannelTrait;
    /// Used to produce data without any input. There is an output WriteChannel reference
    /// that allows the user to push data into one of the channels.
    ///
    /// * Arguments
    /// `output` - Reference to output channels for writing data into the graph. Connected nodes
    /// will receive this data and process it at need.
    fn handle(
        &mut self,
        output: MutexGuard<TypedWriteChannel<Self::OUTPUT>>,
    ) -> Result<(), RustedPipeError>;
}

/// A locked WriteChannel to allow writing data from a Processor.
pub type ProcessorWriter<'a, T> = MutexGuard<'a, TypedWriteChannel<T>>;

/// Processor trait that defines how to process data each time the ReadChannel has
/// matched some input data. It holds access to the read channels from which
/// the user can read the paired up data. It does not give access to the buffered data
/// but only to the data that has been paired up by the ReadChannel synchronizer.
///
/// It also holds a WriteChannel to output data within your `handle` computation.
pub trait Processor: Sync + Send {
    /// Trait object that gives access to the matched packet data.
    type INPUT: InputGenerator;
    /// Trait object that gives access to the output channels for writing.
    type OUTPUT: WriteChannelTrait;
    /// Called when data is matched. It allows the user to process the input data and outputs
    /// transformed data.
    ///
    /// * Arguments
    /// `input` - Reference to input channels for reading data from the ReadChannel.
    /// `output` - Reference to output channels for writing data into the graph. Connected nodes
    /// will receive this data and process it at need.
    fn handle(
        &mut self,
        input: <Self::INPUT as InputGenerator>::INPUT,
        output: ProcessorWriter<Self::OUTPUT>,
    ) -> Result<(), RustedPipeError>;
}

/// TerminalProcessor trait for data processing that produces no output. This can link your data
/// to a terminal UI, network connection or file system or any other sort of result generator.
pub trait TerminalProcessor: Sync + Send {
    /// Trait object that gives access to the matched packet data.
    type INPUT: InputGenerator;
    /// Called when data is matched. It allows the user to process the input data.
    ///
    /// * Arguments
    /// `input` - Reference to input channels for reading data from the ReadChannel.
    fn handle(
        &mut self,
        input: <Self::INPUT as InputGenerator>::INPUT,
    ) -> Result<(), RustedPipeError>;
}
