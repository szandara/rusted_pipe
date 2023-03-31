use graphviz_rust::dot_structures::Vertex;
use thiserror::Error;

use crate::channels::read_channel::ChannelBuffer;
use crate::channels::read_channel::InputGenerator;
use crate::channels::ChannelID;

use graphviz_rust::dot_structures;
use graphviz_rust::parse;
use std::any::Any;
use std::collections::HashMap;
use std::fs;

use super::graph::Graph;
use super::processor::Node;

#[derive(Debug, Error, PartialEq, Clone)]
pub enum FormatterError {
    #[error("Error trying to parse file {0:?}.")]
    ParsingError(String),
}

trait Reader: InputGenerator + ChannelBuffer + Send {}

struct AnonNode {
    node: Node<Box<dyn Reader>, Box<dyn Any>>,
}

#[derive(Default)]
pub struct NodeRegistry {
    nodes: HashMap<String, Box<dyn Fn() -> AnonNode>>,
}

impl NodeRegistry {
    pub fn insert(&mut self, key: String, factory: Box<dyn Fn() -> AnonNode>) {
        self.nodes.insert(key, factory);
    }
}

pub trait GraphFormatter {
    fn to_file(&self, graph: &Graph, output_file: String) -> Result<(), FormatterError>;
    fn from_file(&self, input_file: &str, registry: NodeRegistry) -> Result<Graph, FormatterError>;
}

#[derive(Debug, Default)]
pub struct DotFormatter {}

fn get_node_port_strings(v: &Vertex) -> (String, String) {
    match v {
        dot_structures::Vertex::N(id) => (
            id.0.to_string(),
            match id.1.as_ref().unwrap().0.as_ref().unwrap() {
                dot_structures::Id::Escaped(st) => {
                    let mut st_clone = st.clone();
                    st_clone.pop();
                    st_clone.remove(0);
                    st_clone
                }
                _ => todo!("Only escaped ports are supported, add \"\" quotes."),
            },
        ),
        dot_structures::Vertex::S(_) => todo!(),
    }
}

impl GraphFormatter for DotFormatter {
    fn to_file(&self, _: &Graph, _: String) -> Result<(), FormatterError> {
        panic!("Not implemented");
    }
    fn from_file(&self, input_file: &str, registry: NodeRegistry) -> Result<Graph, FormatterError> {
        let mut graph = Graph::new();
        let contents = fs::read_to_string(input_file)
            .expect(format!("File not found {}", input_file).as_str());
        let parsed_graph = parse(&contents.as_str())
            .or(Err(FormatterError::ParsingError(input_file.to_string())))?;

        match parsed_graph {
            dot_structures::Graph::Graph {
                id: _,
                strict: _,
                stmts: _,
            } => todo!(),
            dot_structures::Graph::DiGraph {
                id: _,
                strict: _,
                stmts,
            } => {
                for node in stmts.iter() {
                    match node {
                        dot_structures::Stmt::Node(node) => {
                            println!("Adding node {:?}", node.id.0.to_string());
                            let processor = registry.nodes.get(&node.id.0.to_string()).unwrap()();
                            graph.add_node(processor).unwrap();
                        }
                        _ => {}
                    }
                }
                for node in stmts.iter() {
                    match node {
                        dot_structures::Stmt::Edge(edge) => match &edge.ty {
                            dot_structures::EdgeTy::Pair(from_node_id, to_node_id) => {
                                println!("From {:?} -> {:?}", from_node_id, to_node_id);

                                let (from_node_id, from_port) = get_node_port_strings(from_node_id);
                                let (to_node_id, to_port) = get_node_port_strings(to_node_id);

                                graph
                                    .link(
                                        &from_node_id,
                                        &ChannelID::from(from_port.clone()),
                                        &to_node_id,
                                        &ChannelID::from(to_port.clone()),
                                    )
                                    .unwrap();
                            }
                            dot_structures::EdgeTy::Chain(_) => todo!(),
                        },
                        _ => {}
                    }
                }
            }
        }

        Ok(graph)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::MutexGuard;

    use super::*;
    use crate::{
        channels::{typed_write_channel::TypedWriteChannel, ChannelID},
        graph::processor::{Processor, SourceProcessor},
        DataVersion, RustedPipeError,
    };

    struct GenericNode {
        id: String,
    }

    impl SourceProcessor for GenericNode {
        type WRITE = String;

        fn handle(
            &mut self,
            output: MutexGuard<TypedWriteChannel<Self::WRITE>>,
        ) -> Result<(), RustedPipeError> {
            output
                .lock()
                .unwrap()
                .write(
                    &ChannelID::from(self.id.clone()),
                    self.id.clone(),
                    &DataVersion::from_now(),
                )
                .unwrap();
            Ok(())
        }

        fn id(&self) -> &String {
            &self.id
        }
    }

    fn test_registry() -> NodeRegistry {
        let mut nodes = NodeRegistry::default();
        nodes.insert(
            "source1".to_string(),
            Box::new(|| {
                new_node(
                    GenericNode {
                        id: "source1".to_string(),
                    },
                    WorkQueue::default(),
                )
            }),
        );
        nodes.insert(
            "source2".to_string(),
            Box::new(|| {
                new_node(
                    GenericNode {
                        id: "source2".to_string(),
                    },
                    WorkQueue::default(),
                )
            }),
        );
        nodes.insert(
            "mid1".to_string(),
            Box::new(|| {
                new_node(
                    GenericNode {
                        id: "mid1".to_string(),
                    },
                    WorkQueue::default(),
                )
            }),
        );
        nodes.insert(
            "sink".to_string(),
            Box::new(|| {
                new_node(
                    GenericNode {
                        id: "sink".to_string(),
                    },
                    WorkQueue::default(),
                )
            }),
        );
        nodes
    }

    #[test]
    fn test_read_graph_from_dot() {
        let parser = DotFormatter::default();
        let registry = test_registry();
        let graph = parser
            .from_file("src/graph/fixtures/dot_test.dot", registry)
            .unwrap();

        assert!(graph.nodes.contains_key("source1"));
        assert!(graph.nodes.contains_key("source2"));
        assert!(graph.nodes.contains_key("mid1"));
        assert!(graph.nodes.contains_key("sink"));
        println!(
            ":{:?}",
            graph
                .nodes
                .get("sink")
                .unwrap()
                .read_channel
                .available_channels()
        );
        assert!(graph
            .nodes
            .get("sink")
            .unwrap()
            .read_channel
            .available_channels()
            .contains(&ChannelID::from("source2")));
        assert!(graph
            .nodes
            .get("sink")
            .unwrap()
            .read_channel
            .available_channels()
            .contains(&ChannelID::from("mid1")));
        assert!(graph
            .nodes
            .get("mid1")
            .unwrap()
            .read_channel
            .available_channels()
            .contains(&ChannelID::from("source1")));
    }
}
