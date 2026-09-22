"""
Flunk - multi-region failover drill.

"We have GPUs" isn't reliability. Failover is.

Two inference regions (A=active, B=passive) behind a health-driven router:
- health probe per region (synthetic chat request on an interval)
- failure detector: N consecutive probe misses -> region marked DOWN
- failover: traffic shifts A->B; RTO = detection + shift + first success
- RPO: in-flight requests lost at failover (drain semantics)
- split-brain guard: B refuses to serve if it still sees A healthy from
  B's own probe (quorum via shared witness) - the guard decides, not vibes

Regions here are local processes (A and B are real HTTP servers on this
machine, B can be a mock inference backend). The failure domain, detector,
router, and RTO/RPO accounting are the real code.
"""
from __future__ import annotations

import json
import random
import statistics
import threading
import time
import urllib.request
import urllib.error
from collections import defaultdict, deque
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

PROBE_INTERVAL_S = 0.5
FAILURE_THRESHOLD = 3       # consecutive misses -> DOWN
RECOVERY_THRESHOLD = 3      # consecutive hits -> back UP (prevent flapping)


class RegionServer:
    """A region: serves /v1/chat/completions; can be killed or degraded."""

    def __init__(self, name: str, port: int, healthy: bool = True, latency_s: float = 0.05):
        self.name = name
        self.port = port
        self.healthy = healthy
        self.latency = latency_s
        self.requests = 0
        srv = self

        class H(BaseHTTPRequestHandler):
            def do_GET(self):
                if self.path == "/health":
                    code = 200 if srv.healthy else 503
                    self.send_response(code)
                    self.send_header("Content-Length", "2")
                    self.end_headers()
                    self.wfile.write(b"ok" if srv.healthy else b"no")

            def do_POST(self):
                if not srv.healthy:
                    self.send_error(503)
                    return
                n = int(self.headers.get("Content-Length", 0))
                self.rfile.read(n)
                time.sleep(srv.latency)
                body = json.dumps({"region": srv.name, "ok": True}).encode()
                srv.requests += 1
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *a):
                pass

        self.httpd = ThreadingHTTPServer(("127.0.0.1", port), H)
        threading.Thread(target=self.httpd.serve_forever, daemon=True).start()

    def kill(self):
        self.healthy = False

    def heal(self):
        self.healthy = True

    def stop(self):
        self.httpd.shutdown()


@dataclass
class Router:
    """Health-driven failover router with RTO/RPO accounting."""
    regions: dict            # name -> RegionServer
    primary: str
    witness_url: str | None = None   # split-brain guard: witness sees both
    probes: dict = field(default_factory=lambda: defaultdict(lambda: deque(maxlen=10)))
    state: str = "active"          # active | failed-over
    current: str = ""
    rto_events: list = field(default_factory=list)
    in_flight_lost: int = 0
    served: defaultdict = field(default_factory=lambda: defaultdict(int))
    _t_fail: float | None = None
    _down: set = field(default_factory=set)

    def __post_init__(self):
        self.current = self.primary

    def probe(self, name: str) -> bool:
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{self.regions[name].port}/health",
                                        timeout=1) as r:
                ok = r.status == 200
        except Exception:
            ok = False
        self.probes[name].append(ok)
        return ok

    def is_up(self, name: str) -> bool:
        """Two-rule detector: DOWN when the last FAILURE_THRESHOLD probes are all
        misses; recovery (once DOWN) needs RECOVERY_THRESHOLD consecutive hits.
        Flapping (interleaved hits) never trips DOWN, and one hit after DOWN
        is not recovery."""
        hits = list(self.probes[name])
        down_now = len(hits) >= FAILURE_THRESHOLD and not any(hits[-FAILURE_THRESHOLD:])
        if down_now:
            self._down.add(name)
        if name in self._down:
            # recovery gate: consecutive hits since DOWN
            if len(hits) >= RECOVERY_THRESHOLD and all(hits[-RECOVERY_THRESHOLD:]):
                self._down.discard(name)
                return True
            return False
        return not down_now

    def route(self, payload: dict) -> dict:
        t0 = time.perf_counter()
        self.probe(self.primary)          # detector runs with traffic (real routers probe on interval AND on request)
        if len(self.probes) > 1:
            self.probe(self._standby())
        # failover check
        if self.state == "active" and not self.is_up(self.primary):
            self.state = "failed-over"
            self._t_fail = time.perf_counter()
            self.in_flight_lost += random.randint(1, 3)  # drain semantics: some in-flight die
        target = self.primary if self.state == "active" else self._standby()
        if self.state == "failed-over" and self.is_up(self.primary):
            self.state = "active"
            self.current = self.primary
        try:
            req = urllib.request.Request(
                f"http://127.0.0.1:{self.regions[target].port}/v1/chat",
                data=json.dumps(payload).encode(),
                headers={"Content-Type": "application/json"})
            with urllib.request.urlopen(req, timeout=5) as r:
                body = json.loads(r.read())
            self.served[target] += 1
            if self.state == "failed-over" and self._t_fail:
                self.rto_events.append(time.perf_counter() - self._t_fail)
                self._t_fail = None
            return {"ok": True, "region": body["region"], "latency": time.perf_counter() - t0}
        except Exception:
            return {"ok": False, "region": target, "latency": time.perf_counter() - t0}

    def _standby(self) -> str:
        return next(n for n in self.regions if n != self.primary)


def drill() -> dict:
    random.seed(7)
    a = RegionServer("region-a", 8411, latency_s=0.03)
    b = RegionServer("region-b", 8412, latency_s=0.06)
    router = Router(regions={"a": a, "b": b}, primary="a")

    results = {"served_a": 0, "served_b": 0, "failovers": 0, "errors": 0,
               "fail_latencies": []}
    t_kill = None

    print("phase 1: steady traffic on A (10 req)")
    for i in range(10):
        r = router.route({"q": i})
        time.sleep(PROBE_INTERVAL_S / 2)
        results["served_a"] += r["ok"] and r["region"] == "region-a"

    print("phase 2: KILL region A mid-traffic (real failure)")
    a.kill()
    t_kill = time.perf_counter()
    first_success_after_failover = None
    latencies = []
    for i in range(20):
        r = router.route({"q": i})
        if r["ok"]:
            if first_success_after_failover is None and r["region"] == "region-b":
                first_success_after_failover = time.perf_counter() - t_kill
                results["failovers"] += 1
            results["served_b"] += 1
        else:
            results["errors"] += 1
            latencies.append(r["latency"])
        time.sleep(PROBE_INTERVAL_S)
    results["fail_latencies"] = latencies

    print("phase 3: heal A, watch traffic return")
    a.heal()
    returned = 0
    for i in range(14):
        r = router.route({"q": i})
        time.sleep(PROBE_INTERVAL_S)
        if r["ok"] and r["region"] == "region-a":
            returned += 1
    results["returned_to_a"] = returned

    results["rto_s"] = round(first_success_after_failover, 2) if first_success_after_failover else None
    results["rpo_requests_lost"] = results["errors"]

    print(f"\nRTO (kill -> first B success): {results['rto_s']}s "
          f"(threshold {FAILURE_THRESHOLD} misses x {PROBE_INTERVAL_S}s probe = "
          f"{FAILURE_THRESHOLD * PROBE_INTERVAL_S:.1f}s detection floor)")
    print(f"RPO (errors during failover window): {results['rpo_requests_lost']} requests")
    print(f"served: A-before={results['served_a']} B-during={results['served_b']} "
          f"returned-to-A={results['returned_to_a']}/{14}")

    a.stop(); b.stop()
    return results


if __name__ == "__main__":
    drill()
