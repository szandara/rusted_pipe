# Rusted Pipe

## Overview

<img src="rusted.png" width="700">

## Key Concepts

### Processor
A computational transformation consuming one or more inputs and outputting one or more outputs. They can run for an arbitrary long period. However, for real time processing, the longest the computation the worse the result for the user will be.

There are 3 different processor types:
- SourceProcessor: it has no read channel and it's called continuously by the scheduler. SourceProcessors should perform their frame rate control to avoid proucing too much data.

- Processor: a processor that has a read channel and an output channel.

- TerminalProcessor: a processor that does not output anything. It only reads and consume data by producing some sort of output like an RTP video or saving to disk.

### Packet
A piece of data transported over the pipe. It can be typed or untyped.

### Data Version
Each packet contains a payload and a data version that is a timestamp in milliseconds.

### Node
A portion of the computational graph. A node is a wrapper around a Processor that embeds a read channel and an output channel.

### Graph
A collection of nodes linked together that process the incoming flow.

### Read Channel
A typed or untyped set of buffers which map to the expected inputs of the processor. A read channel can have up to 8 typed buffers or an arbitrary amounts if untyped.

### Write Channel
A typed or untyped set of buffers which map to an output of the processor. A write channel can have up to 8 typed outputs or an arbitrary amounts if untyped.

### Synchronizer
Each read channel has a sychrnonizer chosen by the user, to define a time synch strategy for its incoming data. Synchronizers constantly monitor the read channel buffers and output and schedule work for the node in case a match is found. While users can create their own synchronizers, rusted pipe already offers some common strategies.

## Contributing code
Send pull requests against the client packages in the Kubernetes main [repository](https://github.com/szandara/rustedpipe). 
