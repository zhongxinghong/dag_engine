use std::panic::{self, AssertUnwindSafe};
use std::sync::mpsc;
use std::thread::{self, Builder};

use crate::error::Error::{self, *};
use crate::error::{TaskError, PanicError};
use crate::graph::{Node, FrozenGraph};

pub struct Scheduler<C> {
    frozen: FrozenGraph<C>,
    sender: mpsc::Sender<RunningResult>,
    receiver: mpsc::Receiver<RunningResult>,
}

impl<C: Send + Sync> Scheduler<C> {
    pub fn new(frozen: FrozenGraph<C>) -> Scheduler<C> {
        let (sender, receiver) = mpsc::channel();
        return Scheduler{
            frozen: frozen,
            sender: sender,
            receiver: receiver,
        }
    }

    // TODO:
    //  - 1 thread for 1 task may not be very suitable for cases with
    //      lots of small tasks, consider reusing threads.
    //  - implement timeout control to prevent unfinishable tasks.
    //
    pub fn run(&self, ctx: &C) -> Result<(), Error> {
        let root = RunningNode::new(&self.frozen.root);
        let mut running_nodes: Vec<RunningNode> = self.frozen.graph.nodes.iter()
            .map(|node| RunningNode::new(node)).collect();

        return thread::scope(|s| -> Result<(), Error> {
            let mut cursor = &root;
            for _ in 0..running_nodes.len() {
                let parent = if cursor.index == root.index {
                    &self.frozen.root
                } else {
                    &self.frozen.graph.nodes[cursor.index]
                };
                for child_index in parent.childrens.iter() {
                    let index = *child_index;
                    let running_node = &mut running_nodes[index];
                    running_node.n_unfinished -= 1;
                    if running_node.n_unfinished > 0 {
                        continue;
                    }
                    let task = &self.frozen.graph.nodes[index].task;
                    let sender = &self.sender;
                    let f = move || {
                        let result = panic::catch_unwind(AssertUnwindSafe(|| {
                            return task(ctx);
                        }));
                        let message = match result {
                            Ok(v) => match v {
                                Ok(_) => RunningResult::Done{index},
                                Err(err) => RunningResult::Error{index, err},
                            },
                            Err(err) => RunningResult::Panic{index, err},
                        };
                        let _ = sender.send(message);
                    };
                    Builder::new()
                        .name(self.frozen.graph.nodes[index].name.clone())
                        .spawn_scoped(s, f)
                        .unwrap();
                }
                cursor = match self.receiver.recv().unwrap() {
                    RunningResult::Done{index} => &running_nodes[index],
                    RunningResult::Error{index, err} => return Err(RuntimeFailed{
                        node: self.frozen.graph.nodes[index].name.clone(),
                        err: err,
                    }),
                    RunningResult::Panic{index, err} => return Err(RuntimePanicked{
                        node: self.frozen.graph.nodes[index].name.clone(),
                        err: err,
                    }),
                };
            }
            return Ok(());
        });
    }
}

struct RunningNode {
    index: usize,
    n_unfinished: usize,
}

impl RunningNode {
    fn new<C>(node: &Node<C>) -> RunningNode {
        RunningNode{
            index: node.index,
            n_unfinished: node.parent_count,
        }
    }
}

enum RunningResult {
    Done{index: usize},
    Error{index: usize, err: TaskError},
    Panic{index: usize, err: PanicError},
}
