"""Experiment (M2b): idle barrier() latency vs fetch_max_wait_ms / poll_timeout_ms.

Proves that idle barrier() latency is ≈ max(fetch_max_wait_ms, poll_timeout_ms):
barrier()'s end_offsets() snapshot waits behind the consumer's in-flight fetch
long-poll (~fetch_max_wait_ms), and the reader resolves it on its next getmany()
(~poll_timeout_ms). Measures both the end-to-end barrier latency AND end_offsets()
in isolation — if end_offsets tracks fetch_max_wait_ms, the mechanism is confirmed.

Uses the public ``fetch_max_wait_ms`` constructor parameter (no monkeypatch). Not
marked ``benchmark``, so the suite runs skip it; run explicitly:

    uv run --group bench pytest benchmarks/test_experiment_barrier_fetch_wait.py -s -q
"""

from __future__ import annotations

import time

import pytest

from benchmarks._harness import LatencyRecorder, identity_codec
from ktables import KafkaTable

# (fetch_max_wait_ms, poll_timeout_ms): sweep fmw at the default poll, then drop both.
_CELLS = [(500, 200), (100, 200), (50, 200), (10, 200), (10, 20)]


@pytest.mark.parametrize(("fmw", "poll"), _CELLS, ids=[f"fmw{f}-poll{p}" for f, p in _CELLS])
async def test_idle_barrier_and_end_offsets_vs_fetch_max_wait(fmw: int, poll: int, bench_topic, bootstrap) -> None:
    topic = await bench_topic(partitions=1)
    table: KafkaTable[bytes] = KafkaTable(
        bootstrap_servers=bootstrap, topic=topic, value_decoder=identity_codec,
        poll_timeout_ms=poll, fetch_max_wait_ms=fmw, ensure_topic=False,
    )
    barrier_rec, endoff_rec = LatencyRecorder(), LatencyRecorder()

    async with table:
        assert await table.barrier(timeout=30), "initial catch-up timed out"
        tps = sorted(table._consumer.assignment(), key=lambda tp: tp.partition)  # type: ignore[union-attr]
        for _ in range(30):
            start = time.perf_counter()
            assert await table.barrier(timeout=30)
            barrier_rec.record(time.perf_counter() - start)
            # end_offsets() in isolation, under the same idle reader (barrier's snapshot step).
            start = time.perf_counter()
            await table._consumer.end_offsets(tps)  # type: ignore[union-attr]
            endoff_rec.record(time.perf_counter() - start)

    b, e = barrier_rec.summary(), endoff_rec.summary()
    print(
        f"\n[fmw={fmw:>3} poll={poll:>3}]  end_offsets p50={e.p50_us / 1000:>6.1f}ms"
        f"  |  barrier p50={b.p50_us / 1000:>6.1f}ms p99={b.p99_us / 1000:>6.1f}ms"
    )
