use dag_engine::{Error::*, TaskError, Graph};

fn dummy_task(_: &()) -> Result<(), TaskError> {
    Ok(())
}

#[test]
fn normal() {
    let mut g = Graph::new();
    g.add_node("A", dummy_task).unwrap();
    g.add_node("B", dummy_task).unwrap();
    g.add_node("C", dummy_task).unwrap();
    g.add_edge("A", "B").unwrap();
    g.add_edge("A", "C").unwrap();
    g.add_edge("B", "C").unwrap();
    g.froze().unwrap();
}

#[test]
fn invalid_task() {
    let mut g = Graph::new();
    g.add_node("A", dummy_task).unwrap();
    g.add_node("B", dummy_task).unwrap();
    assert!(g.add_node("", dummy_task).is_err_and(
        |e| if let InvalidNode{name} = e { name == "" } else { false }
    ));
}

#[test]
fn duplicated_task() {
    let mut g = Graph::new();
    g.add_node("A", dummy_task).unwrap();
    g.add_node("B", dummy_task).unwrap();
    assert!(g.add_node("B", dummy_task).is_err_and(
        |e| if let DuplicatedNode{name} = e { name == "B" } else { false }
    ));
}

#[test]
fn invalid_edge() {
    let mut g = Graph::new();
    g.add_node("A", dummy_task).unwrap();
    g.add_node("B", dummy_task).unwrap();
    g.add_edge("A", "B").unwrap();
    assert!(g.add_edge("A", "A").is_err_and(
        |e| if let InvalidEdge{from_node, to_node} = e {
            from_node == "A" && to_node == "A" } else { false }
    ));
    assert!(g.add_edge("", "A").is_err_and(
        |e| if let InvalidEdge{from_node, to_node} = e {
            from_node == "" && to_node == "A" } else { false }
    ));
    assert!(g.add_edge("A", "").is_err_and(
        |e| if let InvalidEdge{from_node, to_node} = e {
            from_node == "A" && to_node == "" } else { false }
    ));
    assert!(g.add_edge("A", "C").is_err_and(
        |e| if let NodeNotFound{name} = e { name == "C" } else { false }
    ));
    assert!(g.add_edge("C", "A").is_err_and(
        |e| if let NodeNotFound{name} = e { name == "C" } else { false }
    ));
    assert!(g.add_edge("A", "B").is_err_and(
        |e| if let DuplicatedEdge{from_node, to_node} = e {
            from_node == "A" && to_node == "B" } else { false }
    ));
}

#[test]
fn cyclic_graph() {
    let mut g = Graph::new();
    g.add_node("A", dummy_task).unwrap();
    g.add_node("B", dummy_task).unwrap();
    g.add_edge("A", "B").unwrap();
    g.add_edge("B", "A").unwrap();
    assert!(g.froze().is_err_and(
        |e| if let CyclicGraphFound{ring} = e {
            ring == "[A, B]" } else { false }
    ));
    let mut g = Graph::new();
    g.add_node("A", dummy_task).unwrap();
    g.add_node("B", dummy_task).unwrap();
    g.add_node("C", dummy_task).unwrap();
    g.add_edge("A", "B").unwrap();
    g.add_edge("B", "C").unwrap();
    g.add_edge("C", "A").unwrap();
    assert!(g.froze().is_err_and(
        |e| if let CyclicGraphFound{ring} = e {
            ring == "[A, B, C]" } else { false }
    ));
}