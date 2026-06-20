"""M9 — reader CPU vs poll_timeout_ms (the cost side of the poll tradeoff).

A lower ``poll_timeout_ms`` cuts propagation/barrier latency (M1/M2) but wakes the
reader loop more often. This measures the idle reader's CPU fraction over a fixed
window across the poll sweep, so the latency wins of a small poll can be weighed
against their CPU cost.

Idle is measured cleanly (the reader loop is the only activity in-process). A
true under-load reader-CPU number needs the load generated from a *separate*
process to avoid counting the writer's CPU — that arrives with the cross-process
writer (plan §12.3); flagged here rather than reported misleadingly.

Run with ``uv run --group bench pytest benchmarks/test_reader_cpu.py``;
``KTABLES_BENCH_PROFILE`` selects quick | full | soak (default quick).
"""

from __future__ import annotations

import asyncio
import time

import psutil
import pytest

from benchmarks._harness import identity_codec
from benchmarks.conftest import PROFILE
from ktables import KafkaTable

pytestmark = pytest.mark.benchmark


_POLL_SWEEP = {"quick": [20, 200], "full": [20, 50, 100, 200, 500], "soak": [200]}
_WINDOW_S = {"quick": 2.0, "full": 3.0, "soak": 2.0}
_PROFILE_POLLS = _POLL_SWEEP.get(PROFILE, _POLL_SWEEP["quick"])


@pytest.mark.parametrize("poll_ms", _PROFILE_POLLS, ids=[f"poll{p}" for p in _PROFILE_POLLS])
async def test_reader_cpu_idle(poll_ms: int, bench_topic, bootstrap: str, bench_results: dict) -> None:
    window = _WINDOW_S.get(PROFILE, 2.0)
    topic = await bench_topic(partitions=1)
    table: KafkaTable[bytes] = KafkaTable(bootstrap_servers=bootstrap, topic=topic, value_decoder=identity_codec, poll_timeout_ms=poll_ms, ensure_topic=False)
    proc = psutil.Process()
    async with table:
        assert await table.barrier(timeout=30)  # quiescent and caught up
        cpu_before = proc.cpu_times()
        wall_before = time.perf_counter()
        await asyncio.sleep(window)  # only the idle reader loop runs during this window
        cpu_after = proc.cpu_times()
        wall = time.perf_counter() - wall_before

    cpu_seconds = (cpu_after.user + cpu_after.system) - (cpu_before.user + cpu_before.system)
    cpu_fraction = cpu_seconds / wall
    bench_results["metrics"].setdefault("reader_cpu_idle", []).append(
        {
            "poll_timeout_ms": poll_ms,
            "window_s": window,
            "cpu_fraction": round(cpu_fraction, 5),
            "approx_wakeups_per_s": round(1000 / poll_ms, 1),
        }
    )
    print(f"\n[reader_cpu_idle poll={poll_ms}] cpu_fraction={cpu_fraction:.4%} (~{round(1000 / poll_ms)} wakeups/s)")
