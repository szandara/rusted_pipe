# Rusted Pipe

Real time processing library for developing multithreaded ML pipelines, written in Rust.

# What is it?

This projects aims at simplifing the creation of inference pipelines. Such systems process data streams coming from 1 or more sources and pipe together several processing steps sequentially or in parallel depending on the goal to achieve. These processing steps form a computational graph that runs at high frame rate. Each node of the graph can have more than 1 input necessary for its computation. This software makes the data flow and synchronization a trivial problem for developers.

An example graph could be a pipeline for reading car plates in a vidoe feed, which could look like the following graph:

<img src="docs/graph.png" width="500">
Car detector and OCR can run in parallel as they work on the data independently but they produce at different speed. We can also run the sequential but the overall throughput would be slower.
Finally the result could look like this depending on the synchronization strategy (more on this below).

<img src="docs/synced.gif" width="500" height="320">

# What is it not?
Rusted pipe is not a data processing tool and does not solve other parallelism problems such as data parallelism. For that there are already powerful tools such as [Rayon](https://github.com/rayon-rs/rayon). In fact `Rayon` ca be used inside calculators. Similarly this tool is not a competitor of GStreamer. In the examples you will see how to integrate GStreamer with RustedPipe.

## Examples

Minimal example at https://github.com/szandara/rusted_pipe_examples/tree/master/your_first_pipeline.

Check out more pipeline examples at https://github.com/szandara/rusted_pipe_examples.

## Synchronization

Rusted Pipe already offers common syncrhonization strategies but also allows users to create their own. Out of the box Rusted pipe offers the following syncrhonizers:

- TimestampSychronizer: This synchronizer only matches tuple of data if their timestamp match exactly. It's suited for offline computations and data processing. It will try to process any incoming data. If one of the node drops a packat, it might hang the pipeline indefinitely. For that reason buffers should be big enough to account for slow processors.

- RealTimeSynchronizer: This synchronizer is more suited for real time computations. It deals with potential data loss from slow consumers dropping packets. There are three main variables to control the behavior depending on the user.
  - `wait_all`: Only outputs tuple if all buffers in the channel have a match. If false, processors might be called with only some of their read channel data. Processors should take into account lack of data.
  - `tolerance_ns`: Nano seconds of tolerance when matching tuples. 0 tolerance will only match exact versions.
  - `buffering`: Useful when dealing with slow consumers. It buffers data until all consumers have full tuple match and then continues processing. If out of sync is detected, it re-buffers.

To explain a bit better the problem of synchronization, let's take the graph explained above. Since all consumers produce data at different times, it's not trivial to make sure that all data is processed in a meaningful way. 

In this example there are 4 nodes running at different speed (on an M1 Apple CPU). This is an artifically bad case since consumers should strive for being fast to keep up with realtime. However, it explains how data is synched.

- A video producer running at 25 fps.
- A car deep learning model running at ~0.5 fps.
- An OCR tesseract model running at ~1 fps.
- A result renderer thath collects video images and inference results and generates an output.


| TimestampSychronizer (offline)      | RealTimeSynchronizer with buffering | RealTimeSynchronizer with wait_all |
| ----------- | ----------- | ----------- |
|<img src="docs/synced.gif" width="249" height="190"> | <img src="docs/buffered.gif" width="249" height="190"> | <img src="docs/wait_realtime.gif" width="249" height="190"> |

## Motivations

Roboticists often use something like ROS to create nodes which behave like services to process this data in parallel and move it to the next node in the computational graph. However, such message exchange suffer from serialization issues which has been tackled in different ways.

More frameworks exist that solve the same problem using in-memory buffers. Two notorious frameworks are GStreamer and Mediapipe (more in Alternatives). RustedPipe improves on top of those existing systems:
- Strict typing (pipeline fails at compile time).
- Rust memory management will make it hard to share invalid data.
- Comes with simplified time sycrhonization between nodes.
- 100% Rust written.

## Alternatives

Mediapipe has inspired a lot of this work, however, this library tries to overcome some limitations of it.
- Lack of strict typing. Mediapipe Calculator interface is `Process(CalculatorContext* cc)`, where a Calculator is a processing node of your computational graph. Data is casted to the expected type but there is no check at compile time.
- Mediapipe focuses on algorithmic distribution. Most of the code is bound to their calculator and algorithms and it's hard to integrate or extend mediapipe with your own calculators unless you work in the Bazel ecosystem.
- Mediapipe has no Rust support and has no compiler help (unlike Rust) for managing data exchange. One can easily mess up memory access by passing around references.

Gstreamer is not a direct competitor of Rusted Pipe but it's often mentioned as alternatives. Some have built similar tools on top of Gstreamer (ie. DeepStream or https://nnstreamer.ai/). While such libraries are powerful they suffer from usability issues:
- Nodes interfaces follow the same pattern as Mediapipe with no typing.
- In general creating your own processing units for GStreamer is complex and the learning curve is high. GStreamer pipeline compilation is relatively hard to understand and relies on OS system libraries to exists.
- While GStreamer has recently pushed for Rust support the usability has not change.


## Key Concepts

See [docs/README.md](docs/README.md)

## Contributing code
Send pull requests against the client packages in the Kubernetes main [repository](https://github.com/szandara/rustedpipe). 
