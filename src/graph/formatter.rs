use thiserror::Error;

use super::Graph;
use super::Processor;
use graphviz_rust::dot_structures;
use graphviz_rust::parse;
use std::fs;

#[derive(Debug, Error, PartialEq, Clone)]
pub enum FormatterError {
    #[error("Error trying to parse file {0:?}.")]
    ParsingError(String),
}

pub trait GraphFormatter {
    fn to_file(self, graph: &Graph, output_file: String) -> Result<(), FormatterError>;
    fn from_file(self, input_file: &str) -> Result<Graph, FormatterError>;
}

#[derive(Debug, Default)]
struct DotFormatter {}

impl GraphFormatter for DotFormatter {
    fn to_file(self, _: &Graph, _: String) -> Result<(), FormatterError> {
        panic!("Not implemented");
    }
    fn from_file(self, input_file: &str) -> Result<Graph, FormatterError> {
        let graph = Graph::new();
        let contents = fs::read_to_string(input_file)
            .expect(format!("File not found {}", input_file).as_str());
        let parsed_graph = parse(&contents.as_str())
            .or(Err(FormatterError::ParsingError(input_file.to_string())))?;

        match parsed_graph {
            dot_structures::Graph::Graph { id, strict, stmts } => todo!(),
            dot_structures::Graph::DiGraph { id, strict, stmts } => {
                for node in stmts {
                    match node {
                        dot_structures::Stmt::Node(node) => {}
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
    use std::collections::HashMap;

    use super::*;

    struct NodeRegistry {
        nodes: HashMap<String, Box<dyn Fn() -> Box<dyn Processor>>>,
    }

    fn test_registry() {}

    #[test]
    fn test_read_graph_from_dot() {
        let parser = DotFormatter::default();
        let graph = parser.from_file("src/graph/fixtures/dot_test.dot").unwrap();
        //assert!(false);
    }
}
