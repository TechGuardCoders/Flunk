"""Offline tests: detector thresholds, router state machine, RTO/RPO accounting."""
import sys
from pathlib import Path
from collections import deque

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from flunk import Router, RegionServer, FAILURE_THRESHOLD, RECOVERY_THRESHOLD  # noqa: E402


class FakeRegion:
    def __init__(self, port):
        self.port = port
        self.healthy = True
        self.requests = 0


def test_failure_detector_threshold():
    r = Router(regions={}, primary="a")
    r.probes["a"] = deque([True, True, False, False, False], maxlen=10)
    assert r.is_up("a") is False, "3 consecutive misses -> DOWN"


def test_flapping_does_not_trip():
    r = Router(regions={}, primary="a")
    r.probes["a"] = deque([True, False, True, False, True], maxlen=10)
    assert r.is_up("a") is True, "intermittent misses (not consecutive) stay UP"


def test_recovery_needs_consecutive_hits():
    r = Router(regions={}, primary="a")
    r.probes["a"] = deque([False, False, False], maxlen=10)
    assert r.is_up("a") is False, "3 misses -> DOWN"
    r.probes["a"] = deque([False, False, False, True], maxlen=10)
    assert r.is_up("a") is False, "1 hit after down is not recovery"
    r.probes["a"] = deque([False, False, False, True, True, True], maxlen=10)
    assert r.is_up("a") is True, "3 consecutive hits -> recovered"


def test_router_fails_over_and_back():
    a = RegionServer("region-a", 8421, latency_s=0.01)
    b = RegionServer("region-b", 8422, latency_s=0.01)
    r = Router(regions={"a": a, "b": b}, primary="a")
    for _ in range(FAILURE_THRESHOLD):
        r.probe("a")
    r.route({"q": 1})
    # kill a, probe until detected
    a.kill()
    for _ in range(FAILURE_THRESHOLD):
        r.probe("a")
    out = r.route({"q": 2})
    assert out["ok"] and out["region"] == "region-b", "traffic must move to standby"
    a.heal()
    for _ in range(RECOVERY_THRESHOLD):
        r.probe("a")
    r.route({"q": 3})   # first route flips state back to active (served from b once)
    out = r.route({"q": 4})
    assert out["region"] == "region-a", "recovered primary takes traffic back"
    a.stop(); b.stop()
