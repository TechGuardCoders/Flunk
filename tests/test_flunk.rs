//! Rust port of tests/test_flunk.py + tests/test_chaos_pool.py (1:1).
//! Offline tests: detector thresholds, router state machine, RTO/RPO
//! accounting, and fleet chaos per-node injection. No external network.

use flunk::{
    ChaosNode, ChaosPool, RegionServer, Router, FAILURE_THRESHOLD, RECOVERY_THRESHOLD,
};
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

// ------------------------------------------------- test_flunk.py fakes/helpers

fn router_with_probes(probes: Vec<bool>) -> Router {
    let regions: HashMap<String, Arc<RegionServer>> = HashMap::new();
    let mut r = Router::new(regions, "a");
    let mut q = VecDeque::from(probes);
    // emulate Python: r.probes["a"] = deque([...], maxlen=10)
    while q.len() > 10 {
        q.pop_front();
    }
    r.probes_insert("a", q);
    r
}

// ------------------------------------------------------- test_flunk.py (4)

/// 3 consecutive misses -> DOWN
#[test]
fn test_failure_detector_threshold() {
    let mut r = router_with_probes(vec![true, true, false, false, false]);
    assert!(!r.is_up("a"), "3 consecutive misses -> DOWN");
}

/// intermittent misses (not consecutive) stay UP
#[test]
fn test_flapping_does_not_trip() {
    let mut r = router_with_probes(vec![true, false, true, false, true]);
    assert!(r.is_up("a"), "intermittent misses (not consecutive) stay UP");
}

/// 3 misses -> DOWN; 1 hit after down is not recovery; 3 consecutive hits -> recovered
#[test]
fn test_recovery_needs_consecutive_hits() {
    let mut r = router_with_probes(vec![false, false, false]);
    assert!(!r.is_up("a"), "3 misses -> DOWN");
    r.probes_insert("a", VecDeque::from(vec![false, false, false, true]));
    assert!(!r.is_up("a"), "1 hit after down is not recovery");
    r.probes_insert("a", VecDeque::from(vec![false, false, false, true, true, true]));
    assert!(r.is_up("a"), "3 consecutive hits -> recovered");
}

/// Real RegionServers on ephemeral ports; kill -> failover to standby;
/// heal -> recovered primary takes traffic back.
#[test]
fn test_router_fails_over_and_back() {
    let a = Arc::new(RegionServer::new("region-a", 0, true, 0.01).unwrap());
    let b = Arc::new(RegionServer::new("region-b", 0, true, 0.01).unwrap());
    let mut regions = HashMap::new();
    regions.insert("a".to_string(), Arc::clone(&a));
    regions.insert("b".to_string(), Arc::clone(&b));
    let mut r = Router::new(regions, "a");
    for _ in 0..FAILURE_THRESHOLD {
        r.probe("a");
    }
    r.route("{\"q\": 1}");
    // kill a, probe until detected
    a.kill();
    for _ in 0..FAILURE_THRESHOLD {
        r.probe("a");
    }
    let out = r.route("{\"q\": 2}");
    assert!(out.ok && out.region == "region-b", "traffic must move to standby");
    a.heal();
    for _ in 0..RECOVERY_THRESHOLD {
        r.probe("a");
    }
    r.route("{\"q\": 3}"); // first route flips state back to active (served from b once)
    let out = r.route("{\"q\": 4}");
    assert_eq!(out.region, "region-a", "recovered primary takes traffic back");
    assert_ne!(a.port(), 0, "ephemeral port 0 must resolve to the OS-assigned port");
    assert_ne!(b.port(), 0, "ephemeral port 0 must resolve to the OS-assigned port");
}

// ---------------------------------------------------- test_chaos_pool.py (4)

/// FakeNode: object with kill()/heal()/healthy (mirrors the Python fake).
struct FakeNode {
    healthy: Mutex<bool>,
}

impl FakeNode {
    fn new() -> FakeNode {
        FakeNode { healthy: Mutex::new(true) }
    }
}

impl ChaosNode for FakeNode {
    fn kill(&self) {
        *self.healthy.lock().unwrap() = false;
    }
    fn heal(&self) {
        *self.healthy.lock().unwrap() = true;
    }
    fn is_healthy(&self) -> bool {
        *self.healthy.lock().unwrap()
    }
}

fn pool_of(nodes: Vec<(&str, Box<dyn ChaosNode>)>) -> ChaosPool {
    let map: HashMap<String, Box<dyn ChaosNode>> =
        nodes.into_iter().map(|(n, b)| (n.to_string(), b)).collect();
    ChaosPool::new(map)
}

/// per-node injection attributed to the right node labels
#[test]
fn test_per_node_injection_attributed() {
    let mut pool = pool_of(vec![
        ("glm-head", Box::new(FakeNode::new())),
        ("tensorfold", Box::new(FakeNode::new())),
        ("sushi", Box::new(FakeNode::new())),
    ]);
    pool.inject("tensorfold", "kill");
    pool.inject("sushi", "kill");
    assert_eq!(pool.down_nodes(), vec!["sushi".to_string(), "tensorfold".to_string()]);
    let nodes: Vec<&str> = pool.history.iter().map(|e| e.node.as_str()).collect();
    assert_eq!(nodes, vec!["tensorfold", "sushi"]);
    assert!(pool.history.iter().all(|e| e.action == "kill"));
}

/// heal clears down set; unknown node must panic (Python: assert raises)
#[test]
fn test_heal_and_unknown_node_rejected() {
    let mut pool = pool_of(vec![
        ("a", Box::new(FakeNode::new())),
        ("b", Box::new(FakeNode::new())),
    ]);
    pool.inject("a", "kill");
    assert_eq!(pool.down_nodes(), vec!["a".to_string()]);
    pool.inject("a", "heal");
    assert!(pool.down_nodes().is_empty());
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        pool.inject("ghost", "kill");
    }));
    assert!(result.is_err(), "unknown node must raise");
}

/// schedule runs in order
#[test]
fn test_schedule_runs_in_order() {
    let mut pool = pool_of(vec![
        ("n1", Box::new(FakeNode::new())),
        ("n2", Box::new(FakeNode::new())),
    ]);
    pool.run_schedule(&[(0.0, "n1", "kill"), (0.0, "n2", "kill"), (0.0, "n1", "heal")]);
    assert_eq!(pool.down_nodes(), vec!["n2".to_string()]);
    let actions: Vec<&str> = pool.history.iter().map(|e| e.action.as_str()).collect();
    assert_eq!(actions, vec!["kill", "kill", "heal"]);
}

/// Real RegionServer objects work as pool nodes (kill/heal/healthy).
#[test]
fn test_region_servers_fit_the_pool_interface() {
    let a = Box::new(RegionServer::new("region-a", 0, true, 0.01).unwrap());
    let b = Box::new(RegionServer::new("region-b", 0, true, 0.01).unwrap());
    let mut pool = pool_of(vec![("a", a as Box<dyn ChaosNode>), ("b", b as Box<dyn ChaosNode>)]);
    pool.inject("a", "kill");
    assert_eq!(pool.down_nodes(), vec!["a".to_string()]);
    pool.inject("a", "heal");
    assert!(pool.down_nodes().is_empty());
    // Drop runs RegionServer::stop() (Python: finally: a.stop(); b.stop()).
}
