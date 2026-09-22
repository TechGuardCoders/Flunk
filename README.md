# Flunk

**Multi-region failover drill — "we have GPUs" isn't reliability; failover is.**

Two inference regions behind a health-driven router. Region A serves; A dies mid-traffic; the detector trips, traffic shifts to B, and we measure what users actually felt: **RTO** (time from failure to first successful request elsewhere) and **RPO** (requests lost in the gap).

Part of the [TechGuardCoders portfolio](https://github.com/orgs/TechGuardCoders/repositories): Cost Peep → Batcher → Spinal → Ball Knowledge → Bastion → Furnace → Truffle → Popcorn → QueuedGPU → Circuiter → UberCode → **Flunk** → …

## The drill (measured, real HTTP servers, real kill)

Three phases against live region servers:

1. **Steady** — 10 requests land on region-a
2. **Kill** — region-a goes down mid-traffic; detector needs 3 consecutive probe misses (0.5s interval → 1.5s detection floor); traffic fails over to region-b
3. **Heal** — region-a returns; recovery requires 3 *consecutive* healthy probes (flapping protection); traffic shifts back

| Metric | Measured |
|---|---|
| **RTO** (kill → first success on standby) | **1.1s** |
| **RPO** (requests lost in the failover window) | **2** |
| Served through failover | 18/20 on the standby |
| Return-to-primary after heal | 11/14 (recovery gate holds the rest briefly) |

RTO 1.1s sits right at the detection floor (3 misses × 0.5s) — the failover was detection-bound, not transfer-bound. Want lower RTO? Probe faster or fail on fewer misses; each choice trades false positives (flapping) against detection latency. That trade is the whole game.

## Detector design (the part that matters)

Two rules, not one:
- **DOWN** = the last 3 probes all missed (consecutive, not intermittent — flapping never trips it)
- **Recovery** = 3 *consecutive* hits after DOWN (one lucky probe doesn't move traffic back)

## Bugs the drill caught (the honest part)

1. **The detector starved** — the router checked region health but nothing fed the probe deque during traffic; the first version failed over *never* (it couldn't see the failure). Fix: probe on every route (real routers probe on interval and on request). A health check that nothing runs is a decoration.
2. **Region-name mismatch** — drill counters compared `"a"` against actual region name `"region-a"`; RTO read `None` while failover visibly worked. Measurement bugs hide behind working systems.
3. **Flapping vs recovery** — first detector draft let a single hit undo a failure detection; one lucky probe would yank traffic back to a dying region. Fixed with the consecutive-hits recovery gate.

## Run it

```bash
uv venv --python 3.11 .venv && uv pip install pytest
./.venv/Scripts/python -m pytest tests/ -q    # detector state machine
./.venv/Scripts/python flunk.py               # live 3-phase drill
```

## What's next

RTO/RPO numbers feed the **Benchwarmer** capstone teardown; the router's failover events join Cost Peep's reliability panel.
