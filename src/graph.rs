use std::collections::{HashMap, HashSet};

use crate::error::Error::{self, *};
use crate::error::TaskError;

pub type Task<C> = Box<dyn Fn(&C) -> Result<(), TaskError> + Send + Sync + 'static>;

pub(crate) struct Node<C> {
    pub index: usize,
    pub name: String,
    pub task: Task<C>,
    pub parent_count: usize,
    pub childrens: Vec<usize>,
    pub childrens_set: HashSet<usize>,
}

impl<C> Node<C> {
    fn new(index: usize, name: String, task: Task<C>) -> Node<C> {
        Node{
            index: index,
            name: name,
            task: task,
            parent_count: 0,
            childrens: vec![],
            childrens_set: HashSet::new(),
        }
    }
}

pub struct Graph<C> {
    pub(crate) nodes: Vec<Node<C>>,
    pub(crate) nodes_indices: HashMap<String, usize>,
}

impl<C> Graph<C> {
    pub fn new() -> Graph<C> {
        Graph{
            nodes: vec![],
            nodes_indices: HashMap::new(),
        }
    }

    pub fn add_node<F>(&mut self, name: &str, task: F) -> Result<(), Error>
        where
            F: Fn(&C) -> Result<(), TaskError> + Send + Sync + 'static
    {
        if name == "" {
            return Err(InvalidNode{name: name.to_string()});
        }
        if self.nodes_indices.contains_key(name) {
            return Err(DuplicatedNode{name: name.to_string()});
        }
        let index = self.nodes.len();
        let node = Node::new(index, name.to_string(), Box::new(task));
        self.nodes.push(node);
        self.nodes_indices.insert(name.to_string(), index);
        return Ok(());
    }

    pub fn add_edge(&mut self, from_node: &str, to_node: &str) -> Result<(), Error> {
        if from_node == "" || to_node == "" || from_node == to_node {
            return Err(InvalidEdge{
                from_node: from_node.to_string(),
                to_node: to_node.to_string(),
            });
        }
        let parent_index = *match self.nodes_indices.get(from_node) {
            Some(v) => v,
            None => return Err(NodeNotFound{name: from_node.to_string()}),
        };
        let child_index = *match self.nodes_indices.get(to_node) {
            Some(v) => v,
            None => return Err(NodeNotFound{name: to_node.to_string()}),
        };
        assert_ne!(parent_index, child_index);
        let ptr = self.nodes.as_mut_ptr();
        let parent = unsafe { ptr.add(parent_index).as_mut().unwrap() };
        let child = unsafe { ptr.add(child_index).as_mut().unwrap() };
        return Self::add_child(parent, child);
    }

    fn add_child(parent: &mut Node<C>, child: &mut Node<C>) -> Result<(), Error> {
        if !parent.childrens_set.insert(child.index) {
            return Err(DuplicatedEdge{
                from_node: parent.name.clone(),
                to_node: child.name.clone(),
            })
        }
        child.parent_count += 1;
        parent.childrens.push(child.index);
        return Ok(());
    }

    pub fn froze(mut self) -> Result<FrozenGraph<C>, Error> {
        let n_node = self.nodes.len();
        let root_task = |_: &C| -> Result<(), TaskError> {
            panic!("in ROOT node");
        };
        let mut root = Node::new(n_node, "$ROOT".to_string(), Box::new(root_task));

        let mut in_degrees: Vec<usize> = self.nodes.iter()
            .map(|node| node.parent_count).collect();
        let mut queue: Vec<usize> = Vec::with_capacity(n_node);
        let mut queue_i: usize = 0;

        for (index, in_degree) in in_degrees.iter().enumerate() {
            if *in_degree == 0 {
                queue.push(index);
                let child = &mut self.nodes[index];
                Self::add_child(&mut root, child).unwrap();
            }
        }
        while queue_i < queue.len() {
            let cursor = &self.nodes[queue[queue_i]];
            queue_i += 1;
            for child_index in cursor.childrens.iter() {
                let in_degree = &mut in_degrees[*child_index];
                *in_degree -= 1;
                if *in_degree == 0 {
                    queue.push(*child_index);
                }
            }
        }
        if queue_i < n_node {
            let mut ring = String::from("[");
            for (index, in_degree) in in_degrees.iter().enumerate() {
                if *in_degree > 0 {
                    if ring.len() > 1 {
                        ring.push_str(", ");
                    }
                    ring.push_str(&self.nodes[index].name);
                }
            }
            ring.push_str("]");
            return Err(CyclicGraphFound{ring: ring});
        }

        return Ok(FrozenGraph::new(self, root));
    }
}

pub struct FrozenGraph<C> {
    pub(crate) graph: Graph<C>,
    pub(crate) root: Node<C>,
}

impl<C> FrozenGraph<C> {
    fn new(graph: Graph<C>, root: Node<C>) -> FrozenGraph<C> {
        FrozenGraph{
            graph: graph,
            root: root,
        }
    }
}
