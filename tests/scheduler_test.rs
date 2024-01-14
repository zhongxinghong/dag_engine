use std::sync::Mutex;
use std::sync::atomic::{Ordering, AtomicU32, AtomicU64};
use std::thread;
use std::time::{Duration, Instant};

use dag_engine::{Error::*, TaskError, Task, Graph, Scheduler};
use rand::{SeedableRng, Rng};

struct SleepContext {
    n_run: AtomicU32,
    total_cost_ms: AtomicU64,
}

impl SleepContext {
    fn new() -> SleepContext {
        SleepContext{
            n_run: AtomicU32::new(0),
            total_cost_ms: AtomicU64::new(0),
        }
    }
}

fn sleep_task(duration_ms: u64) -> Task<SleepContext> {
    Box::new(move |ctx: &SleepContext| -> Result<(), TaskError> {
        let t0 = Instant::now();
        thread::sleep(Duration::from_millis(duration_ms));
        let t1 = Instant::now();
        let cost = (t1 - t0).as_millis() as u64;
        ctx.n_run.fetch_add(1, Ordering::Relaxed);
        ctx.total_cost_ms.fetch_add(cost, Ordering::Relaxed);
        Ok(())
    })
}

fn run_sleep(name: &str, g: Graph<SleepContext>) -> SleepContext {
    let s = Scheduler::new(g.froze().unwrap());
    let ctx = SleepContext::new();
    let t0 = Instant::now();
    s.run(&ctx).unwrap();
    let t1 = Instant::now();
    let cost_ms = (t1 - t0).as_millis();
    let total_cost_ms = ctx.total_cost_ms.load(Ordering::Relaxed);
    println!("{} cost: {} ms, total: {} ms", name, cost_ms, total_cost_ms);
    return ctx;
}

#[test]
fn sleep_linear() {
    let mut g = Graph::new();
    g.add_node("A1", sleep_task(20)).unwrap();
    g.add_node("A2", sleep_task(40)).unwrap();
    g.add_node("A3", sleep_task(60)).unwrap();
    g.add_node("B1", sleep_task(40)).unwrap();
    g.add_node("B2", sleep_task(60)).unwrap();
    g.add_node("B3", sleep_task(20)).unwrap();
    g.add_node("C1", sleep_task(60)).unwrap();
    g.add_node("C2", sleep_task(20)).unwrap();
    g.add_node("C3", sleep_task(40)).unwrap();
    g.add_edge("A1", "A2").unwrap();
    g.add_edge("A2", "A3").unwrap();
    g.add_edge("A3", "B1").unwrap();
    g.add_edge("B1", "B2").unwrap();
    g.add_edge("B2", "B3").unwrap();
    g.add_edge("B3", "C1").unwrap();
    g.add_edge("C1", "C2").unwrap();
    g.add_edge("C2", "C3").unwrap();
    let ctx = run_sleep("sleep_linear", g);
    assert_eq!(ctx.n_run.load(Ordering::Relaxed), 9);
}

#[test]
fn sleep_layer() {
    let mut g = Graph::new();
    g.add_node("A1", sleep_task(20)).unwrap();
    g.add_node("A2", sleep_task(40)).unwrap();
    g.add_node("A3", sleep_task(60)).unwrap();
    g.add_node("B1", sleep_task(40)).unwrap();
    g.add_node("B2", sleep_task(60)).unwrap();
    g.add_node("B3", sleep_task(20)).unwrap();
    g.add_node("C1", sleep_task(60)).unwrap();
    g.add_node("C2", sleep_task(20)).unwrap();
    g.add_node("C3", sleep_task(40)).unwrap();
    g.add_edge("A1", "B1").unwrap();
    g.add_edge("A1", "B2").unwrap();
    g.add_edge("A1", "B3").unwrap();
    g.add_edge("A2", "B1").unwrap();
    g.add_edge("A2", "B2").unwrap();
    g.add_edge("A2", "B3").unwrap();
    g.add_edge("A3", "B1").unwrap();
    g.add_edge("A3", "B2").unwrap();
    g.add_edge("A3", "B3").unwrap();
    g.add_edge("B1", "C1").unwrap();
    g.add_edge("B1", "C2").unwrap();
    g.add_edge("B1", "C3").unwrap();
    g.add_edge("B2", "C1").unwrap();
    g.add_edge("B2", "C2").unwrap();
    g.add_edge("B2", "C3").unwrap();
    g.add_edge("B3", "C1").unwrap();
    g.add_edge("B3", "C2").unwrap();
    g.add_edge("B3", "C3").unwrap();
    let ctx = run_sleep("sleep_layer", g);
    assert_eq!(ctx.n_run.load(Ordering::Relaxed), 9);
}

#[test]
fn sleep_dag() {
    let mut g = Graph::new();
    g.add_node("A1", sleep_task(20)).unwrap();
    g.add_node("A2", sleep_task(40)).unwrap();
    g.add_node("A3", sleep_task(60)).unwrap();
    g.add_node("B1", sleep_task(40)).unwrap();
    g.add_node("B2", sleep_task(60)).unwrap();
    g.add_node("B3", sleep_task(20)).unwrap();
    g.add_node("C1", sleep_task(60)).unwrap();
    g.add_node("C2", sleep_task(20)).unwrap();
    g.add_node("C3", sleep_task(40)).unwrap();
    g.add_edge("A1", "B1").unwrap();
    g.add_edge("A1", "B2").unwrap();
    g.add_edge("A1", "B3").unwrap();
    g.add_edge("A2", "B1").unwrap();
    g.add_edge("A2", "B3").unwrap();
    g.add_edge("A3", "B3").unwrap();
    g.add_edge("B1", "C2").unwrap();
    g.add_edge("B1", "C3").unwrap();
    g.add_edge("B2", "C2").unwrap();
    g.add_edge("B3", "C1").unwrap();
    g.add_edge("B3", "C2").unwrap();
    g.add_edge("B3", "C3").unwrap();
    let ctx = run_sleep("sleep_dag", g);
    assert_eq!(ctx.n_run.load(Ordering::Relaxed), 9);
}

struct ToposortContext {
    result: Mutex<Vec<String>>
}

impl ToposortContext {
    fn new() -> ToposortContext {
        ToposortContext{
            result: Mutex::new(vec![]),
        }
    }
}

fn toposort_task(name: &'static str) -> Task<ToposortContext> {
    Box::new(move |ctx: &ToposortContext| -> Result<(), TaskError> {
        ctx.result.lock().unwrap().push(name.to_string());
        Ok(())
    })
}

#[test]
fn toposort() {
    let mut g = Graph::new();
    g.add_node("A1", toposort_task("A1")).unwrap();
    g.add_node("A2", toposort_task("A2")).unwrap();
    g.add_node("A3", toposort_task("A3")).unwrap();
    g.add_node("B1", toposort_task("B1")).unwrap();
    g.add_node("B2", toposort_task("B2")).unwrap();
    g.add_node("B3", toposort_task("B3")).unwrap();
    g.add_node("C1", toposort_task("C1")).unwrap();
    g.add_node("C2", toposort_task("C2")).unwrap();
    g.add_node("C3", toposort_task("C3")).unwrap();
    g.add_edge("A1", "B1").unwrap();
    g.add_edge("A1", "B2").unwrap();
    g.add_edge("A1", "B3").unwrap();
    g.add_edge("A2", "B1").unwrap();
    g.add_edge("A2", "B2").unwrap();
    g.add_edge("A2", "B3").unwrap();
    g.add_edge("A3", "B1").unwrap();
    g.add_edge("A3", "B2").unwrap();
    g.add_edge("A3", "B3").unwrap();
    g.add_edge("B1", "C1").unwrap();
    g.add_edge("B1", "C2").unwrap();
    g.add_edge("B1", "C3").unwrap();
    g.add_edge("B2", "C1").unwrap();
    g.add_edge("B2", "C2").unwrap();
    g.add_edge("B2", "C3").unwrap();
    g.add_edge("B3", "C1").unwrap();
    g.add_edge("B3", "C2").unwrap();
    g.add_edge("B3", "C3").unwrap();

    let s = Scheduler::new(g.froze().unwrap());
    let ctx = ToposortContext::new();
    s.run(&ctx).unwrap();

    let result = ctx.result.into_inner().unwrap();
    // dbg!(result);
    assert_eq!(result.len(), 9);
    for (i, name) in result.iter().enumerate() {
        assert_eq!(&name[0..1], match i {
            0..=2 => "A",
            3..=5 => "B",
            6..=8 => "C",
            _ => panic!("{}", i),
        })
    }
}

struct ToposortRandomContext {
    result: Mutex<Vec<usize>>
}

impl ToposortRandomContext {
    fn new() -> ToposortRandomContext {
        ToposortRandomContext{
            result: Mutex::new(vec![])
        }
    }
}

fn toposort_random_task(id: usize) -> Task<ToposortRandomContext> {
    Box::new(move |ctx: &ToposortRandomContext| -> Result<(), TaskError> {
        ctx.result.lock().unwrap().push(id);
        Ok(())
    })
}

#[test]
fn toposort_random() {
    let mut g = Graph::new();
    let mut rng = rand::rngs::StdRng::seed_from_u64(42);
    let n_node: usize = 128;
    let mut n_edge: usize = 0;
    for i in 0..n_node {
        let si = i.to_string();
        g.add_node(&si, toposort_random_task(i)).unwrap();
        if i > 0 {
            g.add_edge(&(i - 1).to_string(), &si).unwrap();
            n_edge += 1;
        }
        if i > 1 {
            for j in 0..i-1 {
                let k: u32 = rng.gen();
                if k % 16 == 0 {
                    g.add_edge(&j.to_string(), &si).unwrap();
                    n_edge += 1;
                }
            }
        }
    }
    assert_eq!(n_edge, 622);

    let s = Scheduler::new(g.froze().unwrap());
    let ctx = ToposortRandomContext::new();
    let t0 = Instant::now();
    s.run(&ctx).unwrap();
    let t1 = Instant::now();
    println!("toposort_random cost: {} ms", (t1 - t0).as_millis());

    let result = ctx.result.into_inner().unwrap();
    for i in 0..n_node {
        assert_eq!(i, result[i]);
    }
}

struct FailedContext {
    n_run: AtomicU32
}

impl FailedContext {
    fn new() -> FailedContext {
        FailedContext{
            n_run: AtomicU32::new(0),
        }
    }
}

#[derive(Debug)]
struct FailedError {
    reason: String,
}

impl std::fmt::Display for FailedError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.reason)
    }
}

impl std::error::Error for FailedError {

}

fn failed_task(reason: &'static str) -> Task<FailedContext> {
    Box::new(move |ctx: &FailedContext| -> Result<(), TaskError> {
        ctx.n_run.fetch_add(1, Ordering::Relaxed);
        match reason {
            "" => Ok(()),
            s => Err(Box::new(FailedError{reason: s.to_string()})),
        }
    })
}

#[test]
fn failed() {
    let mut g = Graph::new();
    g.add_node("A", failed_task("")).unwrap();
    g.add_node("B", failed_task("")).unwrap();
    g.add_node("C", failed_task("C")).unwrap();
    g.add_node("D", failed_task("")).unwrap();
    g.add_edge("A", "B").unwrap();
    g.add_edge("B", "C").unwrap();
    g.add_edge("C", "D").unwrap();

    let s = Scheduler::new(g.froze().unwrap());
    let ctx = FailedContext::new();
    assert!(s.run(&ctx).is_err_and(|e| -> bool {
        if let RuntimeFailed{node, err} = e {
            if node == "C" {
                if let Some(e) = err.downcast_ref::<FailedError>() {
                    return e.reason == node;
                }
            }
        }
        return false;
    }));
    assert_eq!(ctx.n_run.load(Ordering::Relaxed), 3);
}

struct PanickedContext {
    n_run: AtomicU32
}

impl PanickedContext {
    fn new() -> PanickedContext {
        PanickedContext{
            n_run: AtomicU32::new(0),
        }
    }
}

fn panicked_task(reason: &'static str) -> Task<PanickedContext> {
    Box::new(move |ctx: &PanickedContext| -> Result<(), TaskError> {
        ctx.n_run.fetch_add(1, Ordering::Relaxed);
        return match reason {
            "" => Ok(()),
            s => panic!("{}", s),
        }
    })
}

#[test]
fn panicked() {
    let mut g = Graph::new();
    g.add_node("A", panicked_task("")).unwrap();
    g.add_node("B", panicked_task("")).unwrap();
    g.add_node("C1", panicked_task("C1")).unwrap();
    g.add_node("C2", panicked_task("")).unwrap();
    g.add_node("D", panicked_task("")).unwrap();
    g.add_edge("A", "B").unwrap();
    g.add_edge("B", "C1").unwrap();
    g.add_edge("B", "C2").unwrap();
    g.add_edge("C1", "D").unwrap();
    g.add_edge("C2", "D").unwrap();

    let s = Scheduler::new(g.froze().unwrap());
    let ctx = PanickedContext::new();
    assert!(s.run(&ctx).is_err_and(|e| -> bool {
        if let RuntimePanicked{node, err} = e {
            if node == "C1" {
                if let Some(s) = err.downcast_ref::<String>() {
                    return s == &node;
                }
            }
            // dbg!(&node, &err);
        }
        return false;
    }));
    let n_run = ctx.n_run.load(Ordering::Relaxed);
    assert!(3 <= n_run && n_run <= 4);
    // dbg!(n_run);

    let mut g = Graph::new();
    g.add_node("A", panicked_task("")).unwrap();
    g.add_node("B", panicked_task("")).unwrap();
    g.add_node("C1", panicked_task("C1")).unwrap();
    g.add_node("C2", panicked_task("C2")).unwrap();
    g.add_node("C3", panicked_task("C3")).unwrap();
    g.add_node("D", panicked_task("")).unwrap();
    g.add_edge("A", "B").unwrap();
    g.add_edge("B", "C1").unwrap();
    g.add_edge("B", "C2").unwrap();
    g.add_edge("B", "C3").unwrap();
    g.add_edge("C1", "D").unwrap();
    g.add_edge("C2", "D").unwrap();
    g.add_edge("C3", "D").unwrap();

    let s = Scheduler::new(g.froze().unwrap());
    let ctx = PanickedContext::new();
    assert!(s.run(&ctx).is_err_and(|e| -> bool {
        if let RuntimePanicked{node, err} = e {
            if node.starts_with("C") {
                if let Some(s) = err.downcast_ref::<String>() {
                    return s == &node;
                }
            }
            // dbg!(&node, &err);
        }
        return false;
    }));
    let n_run = ctx.n_run.load(Ordering::Relaxed);
    assert!(3 <= n_run && n_run <= 5);
    // dbg!(n_run);
}
