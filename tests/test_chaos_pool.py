"""Fleet chaos tests: per-node injection across a pool. No external network."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from flunk import ChaosPool, RegionServer  # noqa: E402


class FakeNode:
    def __init__(self):
        self.healthy = True

    def kill(self):
        self.healthy = False

    def heal(self):
        self.healthy = True


def test_per_node_injection_attributed():
    pool = ChaosPool(nodes={"glm-head": FakeNode(), "tensorfold": FakeNode(),
                            "sushi": FakeNode()})
    pool.inject("tensorfold", "kill")
    pool.inject("sushi", "kill")
    assert pool.down_nodes() == ["sushi", "tensorfold"]
    assert [e["node"] for e in pool.history] == ["tensorfold", "sushi"]
    assert all(e["action"] == "kill" for e in pool.history)


def test_heal_and_unknown_node_rejected():
    pool = ChaosPool(nodes={"a": FakeNode(), "b": FakeNode()})
    pool.inject("a", "kill")
    assert pool.down_nodes() == ["a"]
    pool.inject("a", "heal")
    assert pool.down_nodes() == []
    try:
        pool.inject("ghost", "kill")
        raise AssertionError("unknown node must raise")
    except AssertionError as e:
        assert "ghost" in str(e)


def test_schedule_runs_in_order():
    pool = ChaosPool(nodes={"n1": FakeNode(), "n2": FakeNode()})
    pool.run_schedule([(0, "n1", "kill"), (0, "n2", "kill"), (0, "n1", "heal")])
    assert pool.down_nodes() == ["n2"]
    assert [e["action"] for e in pool.history] == ["kill", "kill", "heal"]


def test_region_servers_fit_the_pool_interface():
    """Real RegionServer objects work as pool nodes (kill/heal/healthy)."""
    a = RegionServer("region-a", 8431, latency_s=0.01)
    b = RegionServer("region-b", 8432, latency_s=0.01)
    try:
        pool = ChaosPool(nodes={"a": a, "b": b})
        pool.inject("a", "kill")
        assert pool.down_nodes() == ["a"]
        pool.inject("a", "heal")
        assert pool.down_nodes() == []
    finally:
        a.stop(); b.stop()
