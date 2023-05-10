pub mod graph;
pub mod metrics;
pub mod processor;
pub mod runtime;

#[cfg(test)]
mod tests {
    use super::graph::link;
    use super::graph::Graph;
    use super::metrics::Metrics;
    use super::processor::SourceNode;
    use super::processor::SourceProcessor;
    use super::processor::TerminalNode;
    use super::processor::TerminalProcessor;
    use crate::channels::WriteChannelTrait;

    use std::thread;

    use crate::channels::read_channel::ReadChannel;
    use crate::channels::typed_write_channel::TypedWriteChannel;
    use crate::packet::work_queue::WorkQueue;
    use crate::RustedPipeError;
    use crossbeam::channel::unbounded;
    use crossbeam::channel::Receiver;
    use std::time::Duration;

    use crossbeam::channel::Sender;

    use crate::buffers::single_buffers::RtRingBuffer;
    use crate::buffers::synchronizers::timestamp::TimestampSynchronizer;
    use crate::channels::typed_read_channel::ReadChannel2;
    use crate::channels::typed_write_channel::WriteChannel1;

    use crate::packet::typed::ReadChannel2PacketSet;
    use crate::DataVersion;

    use crossbeam::channel::RecvTimeoutError;
    use std::sync::MutexGuard;

    use std::time::Instant;
    use std::time::{SystemTime, UNIX_EPOCH};

    struct TestNodeProducer {
        id: String,
        produce_time_ms: u64,
        counter: usize,
        max_packets: usize,
    }

    impl TestNodeProducer {
        fn new(id: String, produce_time_ms: u64, max_packets: usize) -> Self {
            TestNodeProducer {
                id,
                produce_time_ms,
                counter: 0,
                max_packets,
            }
        }
    }

    impl SourceProcessor for TestNodeProducer {
        type OUTPUT = WriteChannel1<String>;
        fn handle(
            &mut self,
            mut output_channel: MutexGuard<TypedWriteChannel<Self::OUTPUT>>,
        ) -> Result<(), RustedPipeError> {
            thread::sleep(Duration::from_millis(self.produce_time_ms));
            if self.counter == self.max_packets {
                return Err(RustedPipeError::EndOfStream());
            }

            let s = SystemTime::now();
            output_channel
                .writer
                .c1()
                .write(
                    "Test".to_string(),
                    &DataVersion {
                        timestamp_ns: self.counter as u128,
                    },
                )
                .unwrap();
            let e = SystemTime::now().duration_since(s).unwrap();
            println!(
                "P {}, Sending {} at {} in {}",
                self.id,
                self.counter,
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .expect("Time went backwards")
                    .as_nanos(),
                e.as_nanos()
            );
            self.counter += 1;
            Ok(())
        }
    }

    struct TestNodeConsumer {
        id: String,
        output: Sender<ReadChannel2PacketSet<String, String>>,
        counter: u32,
        consume_time_ms: u64,
    }
    impl TestNodeConsumer {
        fn new(
            output: Sender<ReadChannel2PacketSet<String, String>>,
            consume_time_ms: u64,
        ) -> Self {
            TestNodeConsumer {
                id: "consumer".to_string(),
                output,
                consume_time_ms,
                counter: 0,
            }
        }
    }

    impl TerminalProcessor for TestNodeConsumer {
        type INPUT = ReadChannel2<String, String>;
        fn handle(
            &mut self,
            input: ReadChannel2PacketSet<String, String>,
        ) -> Result<(), RustedPipeError> {
            println!(
                "Received {} at {}",
                self.counter,
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .expect("Time went backwards")
                    .as_millis()
            );
            self.counter += 1;
            self.output.send(input).unwrap();
            thread::sleep(Duration::from_millis(self.consume_time_ms));
            Ok(())
        }
    }

    fn setup_test() -> Graph {
        Graph::new(Metrics::no_metrics())
    }

    fn create_source_node(producer: TestNodeProducer) -> SourceNode<WriteChannel1<String>> {
        let write_channel1 = WriteChannel1::<String>::create();
        let write_channel = TypedWriteChannel {
            writer: Box::new(write_channel1),
        };
        let id = producer.id.clone();
        SourceNode {
            handler: Box::new(producer),
            write_channel,
            id,
        }
    }

    fn create_consumer_node(
        consumer: TestNodeConsumer,
        consumer_queue_strategy: WorkQueue<ReadChannel2PacketSet<String, String>>,
        buffer_size: usize,
        block_full: bool,
    ) -> TerminalNode<ReadChannel2<String, String>> {
        let synch_strategy = Box::new(TimestampSynchronizer::default());
        let read_channel2 = ReadChannel2::create(
            RtRingBuffer::<String>::new(buffer_size, block_full),
            RtRingBuffer::<String>::new(buffer_size, block_full),
        );
        let read_channel = ReadChannel::new(
            synch_strategy,
            Some(WorkQueue::<ReadChannel2PacketSet<String, String>>::default()),
            read_channel2,
        );

        let id = consumer.id.clone();
        TerminalNode {
            handler: Box::new(consumer),
            read_channel,
            work_queue: consumer_queue_strategy,
            id,
        }
    }

    fn setup_default_test(
        node0: TestNodeProducer,
        node1: TestNodeProducer,
        consume_time_ms: u64,
        consumer_queue_strategy: WorkQueue<ReadChannel2PacketSet<String, String>>,
    ) -> (Graph, Receiver<ReadChannel2PacketSet<String, String>>) {
        let mut node0 = create_source_node(node0);
        let mut node1 = create_source_node(node1);

        let (output, output_check) = unbounded();
        let process_terminal = TestNodeConsumer::new(output.clone(), consume_time_ms);
        let process_terminal =
            create_consumer_node(process_terminal, consumer_queue_strategy, 2000, false);

        link(
            node0.write_channel.writer.c1(),
            process_terminal.read_channel.channels.lock().unwrap().c1(),
        )
        .expect("Cannot link channels");

        link(
            node1.write_channel.writer.c1(),
            process_terminal.read_channel.channels.lock().unwrap().c2(),
        )
        .expect("Cannot link channels");

        let mut graph = setup_test();
        graph.start_source_node(node0);
        graph.start_source_node(node1);
        graph.start_terminal_node(process_terminal);

        (graph, output_check)
    }

    fn check_results(results: &Vec<ReadChannel2PacketSet<String, String>>, max_packets: usize) {
        assert_eq!(results.len(), max_packets);
        for (i, result) in results.iter().enumerate() {
            let data_0 = result.c1();
            let data_1 = result.c2();
            assert!(data_0.is_some(), "At packet {}", i);
            assert!(data_1.is_some(), "At packet {}", i);
            assert_eq!(*data_0.unwrap().data, "Test".to_string(), "At packet {}", i);
            assert_eq!(*data_1.unwrap().data, "Test".to_string(), "At packet {}", i);
        }
    }

    #[test]
    fn test_linked_nodes_can_send_and_receive_data() {
        let max_packets = 100;
        let mock_processing_time_ms = 3;

        let node0 = TestNodeProducer::new(
            "producer1".to_string(),
            mock_processing_time_ms,
            max_packets,
        );
        let node1 = TestNodeProducer::new(
            "producer2".to_string(),
            mock_processing_time_ms,
            max_packets,
        );

        let (graph, output_check) = setup_default_test(node0, node1, 0, WorkQueue::default());

        let mut results = Vec::with_capacity(max_packets);
        let deadline = Instant::now() + Duration::from_millis(700);

        for _ in 0..max_packets {
            let data = output_check.recv_deadline(deadline);
            if data.is_err() {
                break;
            }
            results.push(data.unwrap());
        }

        check_results(&results, max_packets);

        graph.stop(false, None);
    }

    #[test]
    fn test_graph_waits_for_data_if_stop_flag() {
        let max_packets = 100;
        let mock_processing_time_ms = 2;

        let node0 = TestNodeProducer::new(
            "producer1".to_string(),
            mock_processing_time_ms,
            max_packets,
        );
        let node1 = TestNodeProducer::new(
            "producer2".to_string(),
            mock_processing_time_ms,
            max_packets,
        );

        let (mut graph, output_check) = setup_default_test(node0, node1, 10, WorkQueue::default());

        // 1200ms = 12 ms * 100 packets. Receiver consume time is just approximated since the thread:sleep is not accurate and
        // there is some computation happening inside.
        graph.stop(true, Some(Duration::from_millis(1200)));

        let mut results = Vec::with_capacity(max_packets);
        let deadline = Instant::now() + Duration::from_millis(10);
        for _ in 0..max_packets {
            let data = output_check.recv_deadline(deadline);
            if data.is_err() {
                break;
            }
            results.push(data.unwrap());
        }
        check_results(&results, max_packets);
    }

    #[test]
    #[should_panic]
    fn test_graph_starting_same_node_id_panics() {
        let max_packets = 100;
        let mock_processing_time_ms = 2;

        let node0 = TestNodeProducer::new(
            "producer1".to_string(),
            mock_processing_time_ms,
            max_packets,
        );
        let node1 = TestNodeProducer::new(
            "producer2".to_string(),
            mock_processing_time_ms,
            max_packets,
        );

        let node0_clone = TestNodeProducer::new(
            "producer1".to_string(),
            mock_processing_time_ms,
            max_packets,
        );

        let (mut graph, _) = setup_default_test(node0, node1, 10, WorkQueue::default());
        let node0_clone = create_source_node(node0_clone);
        graph.start_source_node(node0_clone);
    }

    #[test]
    fn test_linked_nodes_can_send_and_receive_at_different_produce_speed() {
        let max_packets = 10;
        let mock_processing_time_ms = 3;
        let collection_time_ms: u64 = 400;

        let node0 = TestNodeProducer::new(
            "producer1".to_string(),
            mock_processing_time_ms * 10,
            max_packets,
        );
        let node1 = TestNodeProducer::new(
            "producer2".to_string(),
            mock_processing_time_ms,
            max_packets,
        );

        let (mut graph, output_check) = setup_default_test(node0, node1, 0, WorkQueue::default());

        let mut results = Vec::with_capacity(max_packets);
        let deadline = Instant::now() + Duration::from_millis(collection_time_ms);

        for _i in 0..max_packets {
            let data = output_check.recv_deadline(deadline);
            if data.is_err() {
                break;
            }
            results.push(data.unwrap());
        }

        check_results(&results, max_packets);
        graph.stop(false, None);
    }

    #[test]
    fn test_slow_consumers_data_is_dropped_real_time_queue() {
        let max_packets = 100;
        let mock_processing_time_ms = 4;
        let collection_time_ms: u64 = 600;

        let node0 = TestNodeProducer::new(
            "producer1".to_string(),
            mock_processing_time_ms,
            max_packets,
        );
        let node1 = TestNodeProducer::new(
            "producer2".to_string(),
            mock_processing_time_ms,
            max_packets,
        );

        let (mut graph, output_check) = setup_default_test(node0, node1, 100, WorkQueue::new(1));

        let mut results = Vec::with_capacity(max_packets);
        let deadline = Instant::now() + Duration::from_millis(collection_time_ms);

        let expected_versions: Vec<u128> = vec![8, 20, 30];
        for _i in 0..expected_versions.len() {
            let data = output_check.recv_deadline(deadline);
            if data.is_err() {
                eprintln!("Error receiving {:?}", data.err().unwrap());
                break;
            }
            results.push(data.unwrap());
        }

        check_results(&results, expected_versions.len());

        for (i, expected_version) in expected_versions.into_iter().enumerate() {
            let v1 = results[i].c1().unwrap().version.timestamp_ns;
            let v2 = results[i].c2().unwrap().version.timestamp_ns;
            let range = ((expected_version - 8)..(expected_version + 8)).collect::<Vec<u128>>();
            assert!(
                range.contains(&v1),
                "V1 {} not in expected_version {}",
                v1,
                expected_version
            );
            assert!(
                range.contains(&v2),
                "V2 {} not in expected_version {}",
                v2,
                expected_version
            );
        }
        graph.stop(false, None);
    }

    fn test_slow_consumers_blocks_if_configured(block_full: bool) {
        let max_packets = 10;
        let collection_time_ms: u64 = 50;

        // Very slow producer
        let node0 = TestNodeProducer::new("producer1".to_string(), 100, max_packets);
        let node1 = TestNodeProducer::new("producer2".to_string(), 5, max_packets);

        let mut node0 = create_source_node(node0);
        let mut node1 = create_source_node(node1);

        let (output, output_check) = unbounded();
        let process_terminal = TestNodeConsumer::new(output.clone(), 0);

        let process_terminal =
            create_consumer_node(process_terminal, WorkQueue::default(), 2, block_full);
        let mut graph = setup_test();

        let deadline = Instant::now() + Duration::from_millis(collection_time_ms);

        link(
            node0.write_channel.writer.c1(),
            process_terminal.read_channel.channels.lock().unwrap().c1(),
        )
        .expect("Cannot link channels");

        link(
            node1.write_channel.writer.c1(),
            process_terminal.read_channel.channels.lock().unwrap().c2(),
        )
        .expect("Cannot link channels");

        graph.start_source_node(node0);
        graph.start_source_node(node1);
        graph.start_terminal_node(process_terminal);

        assert_eq!(
            output_check.recv_deadline(deadline).err().unwrap(),
            RecvTimeoutError::Timeout
        );

        // If the queue does not block, the oldest message (timestamp 0) is overridden in the buffer
        // so when the slow producer sends its message it cannot be synced with the fast producer.
        let deadline = Instant::now() + Duration::from_millis(collection_time_ms);
        if block_full {
            assert!(output_check.recv_deadline(deadline).is_ok());
        } else {
            assert_eq!(
                output_check.recv_deadline(deadline).err().unwrap(),
                RecvTimeoutError::Timeout
            );
        }
    }

    macro_rules! param_test {
        ($name:ident, ($($block:ident),+)) => {
            $(
                paste::item! {
                    #[test]
                    fn [< $name _ $block >] () {
                        $name($block);
                    }
                }
            )+
        }
    }

    param_test!(test_slow_consumers_blocks_if_configured, (true, false));
}
