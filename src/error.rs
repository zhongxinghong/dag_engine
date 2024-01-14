pub type TaskError = Box<dyn std::error::Error + Send>;
pub type PanicError = Box<dyn std::any::Any + Send>;

#[derive(Debug)]
pub enum Error {
    InvalidNode{name: String},
    DuplicatedNode{name: String},
    NodeNotFound{name: String},
    InvalidEdge{from_node: String, to_node: String},
    DuplicatedEdge{from_node: String, to_node: String},
    CyclicGraphFound{ring: String},
    RuntimeFailed{node: String, err: TaskError},
    RuntimePanicked{node: String, err: PanicError},
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidNode{name} => {
                write!(f, "invalid node: {}", name)
            },
            Self::DuplicatedNode{name} => {
                write!(f, "duplicated node: {}", name)
            },
            Self::NodeNotFound{name} => {
                write!(f, "node not found: {}", name)
            },
            Self::InvalidEdge{from_node, to_node} => {
                write!(f, "invalid edge: {} -> {}", from_node, to_node)
            },
            Self::DuplicatedEdge{from_node, to_node} => {
                write!(f, "duplicated edge: {} -> {}", from_node, to_node)
            },
            Self::CyclicGraphFound{ring} => {
                write!(f, "found ring in graph: {}", ring)
            },
            Self::RuntimeFailed{node, err} => {
                write!(f, "run {} failed: {}", node, err)
            },
            Self::RuntimePanicked{node, err} => {
                if let Some(s) = err.downcast_ref::<String>() {
                    write!(f, "run {} panic: {}", node, s)
                } else {
                    write!(f, "run {} panic occurred", node)
                }
            },
        }
    }
}

impl std::error::Error for Error {

}
