"""M2 — barrier() latency (the on-demand read-your-own-writes primitive).

Closed-loop service-time measurement: time each ``await table.barrier()`` call.
Scenarios (plan §6): idle caught-up + after-burst (the backlog sweep, B=0 vs B>0),
concurrent barriers, and under churn.

Hypothesis (plan §6, corrected in review): idle barrier mean ≈ end_offsets RTT +
poll_timeout_ms/2; after-burst/churn ≈ RTT + fetch latency, largely independent of
poll_timeout_ms (the background fetcher wakes the in-flight getmany early on data).

Run with ``uv run --group bench pytest benchmarks/test_barrier.py``;
``KTABLES_BENCH_PROFILE`` selects quick | full | soak (default quick).
"""

from __future__ import annotations

import asyncio
import time

import pytest

from benchmarks._harness import LatencyRecorder, identity_codec, summary_to_dict
from benchmarks.conftest import PROFILE
from ktables import KafkaTable, KafkaTableWriter

pytestmark = pytest.mark.benchmark

_PAYLOAD = b"x" * 64  # barrier reads offsets, not values — payload size is not a factor (§6)


# Cells are (poll_timeout_ms, backlog_B, partitions); B=0 is the idle floor,
# B>0 is after-burst. Full sweeps one factor at a time (§6).
_POLL_SWEEP = (20, 50, 100, 200, 500)
_BACKLOG_SWEEP = (10, 100, 1000)
_PARTITION_SWEEP = (1, 4, 16)
_DEFAULT_POLL = 200


def _full_cells() -> list[tuple[int, int, int]]:
    cells: dict[tuple[int, int, int], None] = {}
    for poll in _POLL_SWEEP:
        cells[(poll, 0, 1)] = None  # idle floor vs poll_timeout_ms
    for backlog in _BACKLOG_SWEEP:
        cells[(_DEFAULT_POLL, backlog, 1)] = None  # after-burst vs backlog
    for parts in _PARTITION_SWEEP:
        cells[(_DEFAULT_POLL, 100, parts)] = None  # vs partition count
    return list(cells)


_CELLS: dict[str, list[tuple[int, int, int]]] = {
    "quick": [(200, 0, 1), (200, 100, 1)],
    "full": _full_cells(),
    "soak": [(200, 0, 1)],
}
# Idle barriers are cheap (just calls); after-burst barriers each produce B records,
# so they use fewer samples to stay tractable.
_IDLE_N = {"quick": 100, "full": 1000, "soak": 100}
_BURST_N = {"quick": 30, "full": 200, "soak": 30}

_PROFILE_CELLS = _CELLS.get(PROFILE, _CELLS["quick"])
_CELL_IDS = [f"poll{poll}-B{backlog}-parts{parts}" for poll, backlog, parts in _PROFILE_CELLS]


def _record_row(bench_results: dict, scenario: str, params: dict, summary) -> None:
    bench_results["metrics"].setdefault("barrier", []).append({"scenario": scenario, **params, **summary_to_dict(summary)})
    print(
        f"\n[barrier {scenario} {params}] n={summary.count} "
        f"p50={summary.p50_us / 1000:.2f}ms p99={summary.p99_us / 1000:.2f}ms max={summary.max_us / 1000:.2f}ms"
    )


@pytest.mark.parametrize(("poll_ms", "backlog", "partitions"), _PROFILE_CELLS, ids=_CELL_IDS)
async def test_barrier_latency(poll_ms: int, backlog: int, partitions: int, bench_topic, bootstrap: str, bench_results: dict) -> None:
    samples = (_IDLE_N if backlog == 0 else _BURST_N).get(PROFILE, 30)
    topic = await bench_topic(partitions=partitions)
    writer: KafkaTableWriter[bytes] = KafkaTableWriter(bootstrap_servers=bootstrap, topic=topic, value_encoder=identity_codec, ensure_topic=False)
    table: KafkaTable[bytes] = KafkaTable(bootstrap_servers=bootstrap, topic=topic, value_decoder=identity_codec, poll_timeout_ms=poll_ms, ensure_topic=False)
    recorder = LatencyRecorder()

    async with writer, table:
        assert await table.barrier(timeout=30), "initial catch-up barrier timed out"
        for s in range(samples):
            if backlog:
                # Pipeline the burst so a real backlog exists when barrier() is called.
                await asyncio.gather(*(writer.set(f"b{s}-{j}", _PAYLOAD) for j in range(backlog)))
            start = time.perf_counter()
            ok = await table.barrier(timeout=30)
            elapsed = time.perf_counter() - start
            assert ok, "barrier returned False"
            recorder.record(elapsed)

    summary = recorder.summary()
    assert summary.count == samples and summary.dropped == 0
    assert summary.p50_us <= summary.p99_us <= summary.max_us
    scenario = "idle" if backlog == 0 else "after_burst"
    _record_row(bench_results, scenario, {"poll_timeout_ms": poll_ms, "backlog": backlog, "partitions": partitions}, summary)


async def test_barrier_concurrent(bench_topic, bootstrap: str, bench_results: dict) -> None:
    """K barriers issued together (asyncio.gather): each should resolve in roughly
    the same time (the reader resolves them all in one sweep)."""
    concurrency = 8
    rounds = {"quick": 20, "full": 100, "soak": 20}.get(PROFILE, 20)
    topic = await bench_topic(partitions=1)
    writer: KafkaTableWriter[bytes] = KafkaTableWriter(bootstrap_servers=bootstrap, topic=topic, value_encoder=identity_codec, ensure_topic=False)
    table: KafkaTable[bytes] = KafkaTable(bootstrap_servers=bootstrap, topic=topic, value_decoder=identity_codec, ensure_topic=False)
    recorder = LatencyRecorder()

    async def timed_barrier() -> float:
        start = time.perf_counter()
        ok = await table.barrier(timeout=30)
        assert ok
        return time.perf_counter() - start

    async with writer, table:
        assert await table.barrier(timeout=30)
        for r in range(rounds):
            await asyncio.gather(*(writer.set(f"c{r}-{j}", _PAYLOAD) for j in range(10)))
            for elapsed in await asyncio.gather(*(timed_barrier() for _ in range(concurrency))):
                recorder.record(elapsed)

    summary = recorder.summary()
    assert summary.count == rounds * concurrency and summary.dropped == 0
    _record_row(bench_results, "concurrent", {"concurrency": concurrency, "rounds": rounds}, summary)


async def test_barrier_under_churn(bench_topic, bootstrap: str, bench_results: dict) -> None:
    """A background writer churns continuously while barriers are measured —
    each barrier must drain whatever landed before its call."""
    samples = _IDLE_N.get(PROFILE, 100)
    topic = await bench_topic(partitions=1)
    writer: KafkaTableWriter[bytes] = KafkaTableWriter(bootstrap_servers=bootstrap, topic=topic, value_encoder=identity_codec, ensure_topic=False)
    table: KafkaTable[bytes] = KafkaTable(bootstrap_servers=bootstrap, topic=topic, value_decoder=identity_codec, ensure_topic=False)
    recorder = LatencyRecorder()

    async with writer, table:
        assert await table.barrier(timeout=30)
        stop = asyncio.Event()

        async def churn() -> None:
            i = 0
            while not stop.is_set():
                await writer.set(f"churn{i}", _PAYLOAD)
                i += 1

        churn_task = asyncio.create_task(churn())
        try:
            for _ in range(samples):
                start = time.perf_counter()
                ok = await table.barrier(timeout=30)
                assert ok
                recorder.record(time.perf_counter() - start)
        finally:
            stop.set()
            await churn_task

    summary = recorder.summary()
    assert summary.count == samples and summary.dropped == 0
    _record_row(bench_results, "churn", {}, summary)
