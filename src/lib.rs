//! Flunk - multi-region failover drill (Rust port).
//!
//! "We have GPUs" isn't reliability. Failover is.
//!
//! Two inference regions (A=active, B=passive) behind a health-driven router:
//! - health probe per region (synthetic chat request on an interval)
//! - failure detector: N consecutive probe misses -> region marked DOWN
//! - failover: traffic shifts A->B; RTO = detection + shift + first success
//! - RPO: in-flight requests lost at failover (drain semantics)
//!
//! This is a 1:1 port of `flunk.py` (which stays as the reference). Regions
//! are real HTTP servers on 127.0.0.1 bound to ephemeral port 0 (the OS
//! assigns the port; `RegionServer::port()` reports it).

use std::collections::{HashMap, VecDeque};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

pub const PROBE_INTERVAL_S: f64 = 0.5;
pub const FAILURE_THRESHOLD: usize = 3; // consecutive misses -> DOWN
pub const RECOVERY_THRESHOLD: usize = 3; // consecutive hits -> back UP (prevent flapping)

// ---------------------------------------------------------------- RegionServer

/// A region: serves /v1/chat/completions; can be killed or degraded.
/// Binds an ephemeral port (0) and serves on a background thread, mirroring
/// the Python `RegionServer` (ThreadingHTTPServer on the given port).
pub struct RegionServer {
    name: String,
    port: u16,
    healthy: Arc<AtomicBool>,
    latency: Duration,
    requests: Arc<AtomicUsize>,
    running: Arc<AtomicBool>,
    handle: Option<thread::JoinHandle<()>>,
}

impl RegionServer {
    /// Bind 127.0.0.1:port (pass 0 for an OS-assigned ephemeral port) and
    /// start serving on a background thread.
    pub fn new(name: &str, port: u16, healthy: bool, latency_s: f64) -> std::io::Result<RegionServer> {
        let listener = TcpListener::bind(("127.0.0.1", port))?;
        let port = listener.local_addr()?.port();
        let healthy = Arc::new(AtomicBool::new(healthy));
        let requests = Arc::new(AtomicUsize::new(0));
        let running = Arc::new(AtomicBool::new(true));
        let latency = Duration::from_secs_f64(latency_s);

        let h_healthy = Arc::clone(&healthy);
        let h_requests = Arc::clone(&requests);
        let h_running = Arc::clone(&running);
        let srv_name = name.to_string();
        let handle = thread::spawn(move || {
            for stream in listener.incoming() {
                if !h_running.load(Ordering::SeqCst) {
                    break;
                }
                let Ok(mut stream) = stream else { continue };
                let (ok, reqs, lat, name) = (
                    Arc::clone(&h_healthy),
                    Arc::clone(&h_requests),
                    latency,
                    srv_name.clone(),
                );
                // thread-per-connection, like ThreadingHTTPServer
                thread::spawn(move || {
                    handle_conn(&mut stream, &name, &ok, &reqs, lat);
                });
            }
        });

        Ok(RegionServer {
            name: name.to_string(),
            port,
            healthy,
            latency,
            requests,
            running,
            handle: Some(handle),
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    /// The OS-assigned port (captured after binding port 0).
    pub fn port(&self) -> u16 {
        self.port
    }

    pub fn is_healthy(&self) -> bool {
        self.healthy.load(Ordering::SeqCst)
    }

    pub fn request_count(&self) -> usize {
        self.requests.load(Ordering::SeqCst)
    }

    pub fn kill(&self) {
        self.healthy.store(false, Ordering::SeqCst);
    }

    pub fn heal(&self) {
        self.healthy.store(true, Ordering::SeqCst);
    }

    /// Stop the accept loop (like Python's `httpd.shutdown()`).
    pub fn stop(&mut self) {
        self.running.store(false, Ordering::SeqCst);
        // Connect once to unblock the blocking accept().
        let _ = TcpStream::connect(("127.0.0.1", self.port));
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

impl Drop for RegionServer {
    fn drop(&mut self) {
        self.stop();
    }
}

/// Minimal HTTP/1.1 request handling for GET /health and POST /v1/chat.
fn handle_conn(stream: &mut TcpStream, name: &str, healthy: &AtomicBool, requests: &AtomicUsize, latency: Duration) {
    let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));
    let mut buf = [0u8; 4096];
    let n = match stream.read(&mut buf) {
        Ok(n) if n > 0 => n,
        _ => return,
    };
    let req = String::from_utf8_lossy(&buf[..n]);
    let mut lines = req.split("\r\n");
    let request_line = lines.next().unwrap_or("");
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or("");
    let path = parts.next().unwrap_or("");

    // Content-Length (for POST bodies)
    let mut content_length = 0usize;
    for line in lines {
        let mut kv = line.splitn(2, ':');
        if let (Some(k), Some(v)) = (kv.next(), kv.next()) {
            if k.trim().eq_ignore_ascii_case("content-length") {
                content_length = v.trim().parse().unwrap_or(0);
            }
        }
    }
    // Drain the body so keep-alive clients aren't confused (we close anyway).
    if content_length > 0 {
        let body_start = req.find("\r\n\r\n").map(|i| i + 4).unwrap_or(n);
        let already = n.saturating_sub(body_start);
        let mut remaining = content_length.saturating_sub(already);
        let mut sink = [0u8; 1024];
        while remaining > 0 {
            match stream.read(&mut sink) {
                Ok(0) => break,
                Ok(k) => remaining = remaining.saturating_sub(k),
                Err(_) => break,
            }
        }
    }

    let healthy_now = healthy.load(Ordering::SeqCst);
    let response: Vec<u8> = match (method, path) {
        ("GET", "/health") => {
            let body: &[u8] = if healthy_now { b"ok" } else { b"no" };
            let code = if healthy_now { 200 } else { 503 };
            format!("HTTP/1.1 {code} X\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", body.len())
                .into_bytes()
                .into_iter()
                .chain(body.iter().copied())
                .collect()
        }
        ("POST", "/v1/chat") => {
            if !healthy_now {
                format!("HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                    .into_bytes()
            } else {
                thread::sleep(latency);
                requests.fetch_add(1, Ordering::SeqCst);
                let body = format!("{{\"region\": \"{name}\", \"ok\": true}}");
                format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                )
                .into_bytes()
            }
        }
        _ => format!("HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").into_bytes(),
    };
    let _ = stream.write_all(&response);
    let _ = stream.flush();
}

// -------------------------------------------------------------------- Router

#[derive(Debug, Clone, PartialEq)]
pub struct RouteResult {
    pub ok: bool,
    pub region: String,
    pub latency: f64,
}

/// Health-driven failover router with RTO/RPO accounting.
pub struct Router {
    regions: HashMap<String, Arc<RegionServer>>,
    primary: String,
    probes: HashMap<String, VecDeque<bool>>,
    pub state: String, // "active" | "failed-over"
    pub current: String,
    pub rto_events: Vec<f64>,
    pub in_flight_lost: usize,
    pub served: HashMap<String, usize>,
    t_fail: Option<Instant>,
    down: std::collections::HashSet<String>,
}

impl Router {
    pub fn new(regions: HashMap<String, Arc<RegionServer>>, primary: &str) -> Router {
        let current = primary.to_string();
        Router {
            regions,
            primary: primary.to_string(),
            probes: HashMap::new(),
            state: "active".to_string(),
            current,
            rto_events: Vec::new(),
            in_flight_lost: 0,
            served: HashMap::new(),
            t_fail: None,
            down: std::collections::HashSet::new(),
        }
    }

    /// Probe a region's /health endpoint; records the result (maxlen 10, like
    /// the Python deque(maxlen=10)).
    pub fn probe(&mut self, name: &str) -> bool {
        let port = self.regions[name].port();
        let ok = http_get_ok(port, "/health");
        let q = self.probes.entry(name.to_string()).or_insert_with(|| VecDeque::with_capacity(10));
        if q.len() == 10 {
            q.pop_front();
        }
        q.push_back(ok);
        ok
    }

    /// Two-rule detector: DOWN when the last FAILURE_THRESHOLD probes are all
    /// misses; recovery (once DOWN) needs RECOVERY_THRESHOLD consecutive hits.
    /// Flapping (interleaved hits) never trips DOWN, and one hit after DOWN
    /// is not recovery.
    pub fn is_up(&mut self, name: &str) -> bool {
        let hits: Vec<bool> = self.probes.get(name).map(|q| q.iter().copied().collect()).unwrap_or_default();
        let down_now = hits.len() >= FAILURE_THRESHOLD && !hits[hits.len() - FAILURE_THRESHOLD..].iter().any(|h| *h);
        if down_now {
            self.down.insert(name.to_string());
        }
        if self.down.contains(name) {
            // recovery gate: consecutive hits since DOWN
            if hits.len() >= RECOVERY_THRESHOLD && hits[hits.len() - RECOVERY_THRESHOLD..].iter().all(|h| *h) {
                self.down.remove(name);
                return true;
            }
            return false;
        }
        !down_now
    }

    /// Test hook: seed the probe history for a region (mirrors the Python
    /// tests assigning `r.probes["a"] = deque([...], maxlen=10)`).
    pub fn probes_insert(&mut self, name: &str, q: VecDeque<bool>) {
        self.probes.insert(name.to_string(), q);
    }

    fn standby(&self) -> String {
        self.regions
            .keys()
            .find(|n| **n != self.primary)
            .cloned()
            .expect("need a standby region")
    }

    /// Route one request; probes run with traffic (real routers probe on
    /// interval AND on request). Handles failover and fail-back.
    pub fn route(&mut self, payload: &str) -> RouteResult {
        let t0 = Instant::now();
        let primary = self.primary.clone();
        self.probe(&primary);
        if self.regions.len() > 1 {
            let sb = self.standby();
            self.probe(&sb);
        }
        let primary = self.primary.clone();
        if self.state == "active" && !self.is_up(&primary) {
            self.state = "failed-over".to_string();
            self.t_fail = Some(Instant::now());
            // drain semantics: some in-flight die (Python: random 1..=3)
            let nanos = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.subsec_nanos() as usize)
                .unwrap_or(0);
            self.in_flight_lost += 1 + nanos % 3;
        }
        let target = if self.state == "active" {
            self.primary.clone()
        } else {
            self.standby()
        };
        if self.state == "failed-over" && self.is_up(&primary) {
            self.state = "active".to_string();
            self.current = self.primary.clone();
        }
        let port = self.regions[&target].port();
        match http_post_json(port, "/v1/chat", payload) {
            Some(body) => {
                *self.served.entry(target.clone()).or_insert(0) += 1;
                if self.state == "failed-over" {
                    if let Some(tf) = self.t_fail {
                        self.rto_events.push(tf.elapsed().as_secs_f64());
                        self.t_fail = None;
                    }
                }
                let region = extract_json_field(&body, "region").unwrap_or_else(|| target.clone());
                RouteResult { ok: true, region, latency: t0.elapsed().as_secs_f64() }
            }
            None => RouteResult { ok: false, region: target, latency: t0.elapsed().as_secs_f64() },
        }
    }
}

fn http_get_ok(port: u16, path: &str) -> bool {
    let addr = format!("127.0.0.1:{port}");
    let Ok(mut stream) = TcpStream::connect(&addr) else { return false };
    let _ = stream.set_read_timeout(Some(Duration::from_secs(1)));
    let req = format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
    if stream.write_all(req.as_bytes()).is_err() {
        return false;
    }
    let mut buf = Vec::new();
    if stream.read_to_end(&mut buf).is_err() && buf.is_empty() {
        return false;
    }
    let head = String::from_utf8_lossy(&buf);
    head.starts_with("HTTP/1.1 200") || head.starts_with("HTTP/1.0 200")
}

fn http_post_json(port: u16, path: &str, body: &str) -> Option<String> {
    let addr = format!("127.0.0.1:{port}");
    let mut stream = TcpStream::connect(&addr).ok()?;
    let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));
    let req = format!(
        "POST {path} HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        body.len(),
        body
    );
    stream.write_all(req.as_bytes()).ok()?;
    let mut buf = Vec::new();
    let _ = stream.read_to_end(&mut buf);
    let resp = String::from_utf8_lossy(&buf);
    if !resp.starts_with("HTTP/1.1 200") && !resp.starts_with("HTTP/1.0 200") {
        return None;
    }
    let body = resp.split("\r\n\r\n").nth(1).unwrap_or("");
    Some(body.to_string())
}

fn extract_json_field<'a>(body: &'a str, field: &str) -> Option<String> {
    let key = format!("\"{field}\"");
    let start = body.find(&key)? + key.len();
    let rest = &body[start..];
    let colon = rest.find(':')?;
    let rest = &rest[colon + 1..];
    let rest = rest.trim_start();
    let quote = rest.find('"')?;
    let end = rest[quote + 1..].find('"')? + quote + 1;
    Some(rest[quote + 1..end].to_string())
}

// ----------------------------------------------------------------- ChaosPool

/// A node that can be killed/healed (RegionServer fits; so does any test fake).
pub trait ChaosNode {
    fn kill(&self);
    fn heal(&self);
    fn is_healthy(&self) -> bool;
}

impl ChaosNode for RegionServer {
    fn kill(&self) {
        RegionServer::kill(self);
    }
    fn heal(&self) {
        RegionServer::heal(self);
    }
    fn is_healthy(&self) -> bool {
        RegionServer::is_healthy(self)
    }
}

#[derive(Debug, Clone)]
pub struct ChaosEvent {
    pub node: String,
    pub action: String,
    pub at: f64,
}

/// Per-node failure injection across a pool of nodes.
///
/// Nodes are names mapped to objects implementing [`ChaosNode`]. Every
/// injection is recorded with the node label and wall time, so a drill report
/// can attribute each failure to a specific fleet node. Default two-region
/// drill is unchanged; this is the opt-in multi-node layer.
pub struct ChaosPool {
    pub nodes: HashMap<String, Box<dyn ChaosNode>>,
    pub history: Vec<ChaosEvent>,
}

fn epoch_now() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0)
}

impl ChaosPool {
    pub fn new(nodes: HashMap<String, Box<dyn ChaosNode>>) -> ChaosPool {
        ChaosPool { nodes, history: Vec::new() }
    }

    /// Inject an action ("kill" | "heal") on a node; panics on unknown node
    /// or invalid action (Python asserts).
    pub fn inject(&mut self, node: &str, action: &str) -> ChaosEvent {
        assert!(action == "kill" || action == "heal", "{action}");
        assert!(self.nodes.contains_key(node), "unknown node {node}");
        let n = self.nodes.get_mut(node).unwrap();
        if action == "kill" {
            n.kill();
        } else {
            n.heal();
        }
        let event = ChaosEvent { node: node.to_string(), action: action.to_string(), at: epoch_now() };
        self.history.push(event.clone());
        event
    }

    /// Nodes currently killed (heal-able) — derived, not tracked twice.
    pub fn down_nodes(&self) -> Vec<String> {
        let mut down: Vec<String> = self
            .nodes
            .iter()
            .filter(|(_, obj)| !obj.is_healthy())
            .map(|(n, _)| n.clone())
            .collect();
        down.sort();
        down
    }

    /// Apply [(delay_s, node, action), ...] in order; returns the history.
    pub fn run_schedule(&mut self, schedule: &[(f64, &str, &str)]) -> &[ChaosEvent] {
        for (delay, node, action) in schedule {
            if *delay > 0.0 {
                thread::sleep(Duration::from_secs_f64(*delay));
            }
            self.inject(node, action);
        }
        &self.history
    }
}

// ------------------------------------------------------------------- drill()

/// The live 3-phase drill (port of Python `drill()`): steady traffic on A,
/// kill A mid-traffic, heal A and watch traffic return. Returns the metrics.
pub fn drill() -> HashMap<String, f64> {
    let a = Arc::new(RegionServer::new("region-a", 8411, true, 0.03).unwrap());
    let b = Arc::new(RegionServer::new("region-b", 8412, true, 0.06).unwrap());
    let mut regions = HashMap::new();
    regions.insert("a".to_string(), Arc::clone(&a));
    regions.insert("b".to_string(), Arc::clone(&b));
    let mut router = Router::new(regions, "a");

    let mut served_a = 0usize;
    let mut served_b = 0usize;
    let mut failovers = 0usize;
    let mut errors = 0usize;
    let mut first_success_after_failover: Option<f64> = None;

    println!("phase 1: steady traffic on A (10 req)");
    for i in 0..10 {
        let r = router.route(&format!("{{\"q\": {i}}}"));
        thread::sleep(Duration::from_secs_f64(PROBE_INTERVAL_S / 2.0));
        if r.ok && r.region == "region-a" {
            served_a += 1;
        }
    }

    println!("phase 2: KILL region A mid-traffic (real failure)");
    a.kill();
    let t_kill = Instant::now();
    let mut fail_latencies: Vec<f64> = Vec::new();
    for i in 0..20 {
        let r = router.route(&format!("{{\"q\": {i}}}"));
        if r.ok {
            if first_success_after_failover.is_none() && r.region == "region-b" {
                first_success_after_failover = Some(t_kill.elapsed().as_secs_f64());
                failovers += 1;
            }
            served_b += 1;
        } else {
            errors += 1;
            fail_latencies.push(r.latency);
        }
        thread::sleep(Duration::from_secs_f64(PROBE_INTERVAL_S));
    }

    println!("phase 3: heal A, watch traffic return");
    a.heal();
    let mut returned = 0usize;
    for i in 0..14 {
        let r = router.route(&format!("{{\"q\": {i}}}"));
        thread::sleep(Duration::from_secs_f64(PROBE_INTERVAL_S));
        if r.ok && r.region == "region-a" {
            returned += 1;
        }
    }

    let rto = first_success_after_failover.map(|v| (v * 100.0).round() / 100.0).unwrap_or(0.0);
    println!(
        "\nRTO (kill -> first B success): {rto}s (threshold {FAILURE_THRESHOLD} misses x {PROBE_INTERVAL_S}s probe = {:.1}s detection floor)",
        FAILURE_THRESHOLD as f64 * PROBE_INTERVAL_S
    );
    println!("RPO (errors during failover window): {errors} requests");
    println!("served: A-before={served_a} B-during={served_b} returned-to-A={returned}/14");

    let mut out = HashMap::new();
    out.insert("served_a".to_string(), served_a as f64);
    out.insert("served_b".to_string(), served_b as f64);
    out.insert("failovers".to_string(), failovers as f64);
    out.insert("errors".to_string(), errors as f64);
    out.insert("returned_to_a".to_string(), returned as f64);
    out.insert("rto_s".to_string(), rto);
    out.insert("rpo_requests_lost".to_string(), errors as f64);
    out
}
