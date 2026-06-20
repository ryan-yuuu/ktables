"""M1 — write→read propagation latency (publish → visible in the reader's dict).

This module currently implements the **same-process baseline** (writer and reader
in one event loop), per the plan's topology sequencing (§12.3): build the
same-process path first; the cross-process writer for load/throughput cells comes
next. Latency is measured by the ``on_set`` hook (``PropagationProbe``), never by
read-polling.

Run with ``uv run --group bench pytest benchmarks/test_propagation.py`` and pick a
profile via ``KTABLES_BENCH_PROFILE`` (quick | full | soak; default quick).
"""

from __future__ import annotations

import multiprocessing
import time
from queue import Empty as QueueEmpty

import pytest
from aiokafka import AIOKafkaProducer

from benchmarks._harness import (
    BASELINE_PRODUCER_KWARGS,
    PropagationProbe,
    RawPropagationBaseline,
    assert_samples_accounted,
    run_open_loop,
    stamping_encoder,
    summary_to_dict,
    wait_until,
)
from benchmarks._writer_proc import writer_main
from benchmarks.conftest import PROFILE
from ktables import KafkaTableWriter

# If draining the residual backlog after sending finishes takes longer than this,
# the reader could not keep up — the cell is saturated regardless of generator lag.
_DRAIN_BACKLOG_LIMIT_S = 5.0

pytestmark = pytest.mark.benchmark

# Cells are (poll_timeout_ms, payload_bytes, partitions). Per plan §6 the full
# profile sweeps poll_timeout_ms (at the default payload) and payload (at the
# default poll) — not the full cross-product — plus one multi-partition cell.
_POLL_SWEEP = (20, 50, 100, 200, 500)
_PAYLOAD_SWEEP = (100, 1024, 16384)
_DEFAULT_POLL = 200
_DEFAULT_PAYLOAD = 1024


def _full_cells() -> list[tuple[int, int, int]]:
    cells: dict[tuple[int, int, int], None] = {}  # dict preserves order + dedups
    for poll in _POLL_SWEEP:
        cells[(poll, _DEFAULT_PAYLOAD, 1)] = None
    for payload in _PAYLOAD_SWEEP:
        cells[(_DEFAULT_POLL, payload, 1)] = None
    cells[(_DEFAULT_POLL, _DEFAULT_PAYLOAD, 4)] = None
    return list(cells)


_CELLS: dict[str, list[tuple[int, int, int]]] = {
    "quick": [(_DEFAULT_POLL, _DEFAULT_PAYLOAD, 1)],
    "full": _full_cells(),
    "soak": [(_DEFAULT_POLL, _DEFAULT_PAYLOAD, 1)],
}
# (warm-up records discarded, measured records recorded). Full uses a p99-grade
# sample budget (plan §4.8); quick is a fast smoke.
_COUNTS: dict[str, tuple[int, int]] = {
    "quick": (50, 500),
    "full": (500, 10_000),
    "soak": (50, 500),
}

_PROFILE_CELLS = _CELLS.get(PROFILE, _CELLS["quick"])
_CELL_IDS = [f"poll{poll}-payload{payload}-parts{parts}" for poll, payload, parts in _PROFILE_CELLS]


def _skips(stats) -> int:
    return stats.keyless_records + stats.key_decode_errors + stats.value_decode_errors


@pytest.mark.parametrize(("poll_ms", "payload", "partitions"), _PROFILE_CELLS, ids=_CELL_IDS)
async def test_propagation_same_process_baseline(
    poll_ms: int,
    payload: int,
    partitions: int,
    bench_topic,
    bootstrap: str,
    bench_results: dict,
) -> None:
    warmup, measured = _COUNTS.get(PROFILE, _COUNTS["quick"])
    topic = await bench_topic(partitions=partitions)
    writer: KafkaTableWriter[int] = KafkaTableWriter(
        bootstrap_servers=bootstrap, topic=topic, value_encoder=stamping_encoder(payload), ensure_topic=False
    )
    probe = PropagationProbe(bootstrap_servers=bootstrap, topic=topic, poll_timeout_ms=poll_ms, ensure_topic=False)

    async with writer, probe.table:
        # Warm up with recording off (connections, metadata, first fetch, broker warmth).
        for i in range(warmup):
            await writer.set(f"w{i}", i)
        assert await probe.table.barrier(timeout=30), "warm-up barrier timed out"

        skips_before = _skips(probe.table.stats)
        probe.recording = True
        for i in range(measured):
            await writer.set(f"m{i}", i)
        assert await probe.table.barrier(timeout=30), "measurement barrier timed out"
        probe.recording = False

    skipped = _skips(probe.table.stats) - skips_before
    assert_samples_accounted(sent=measured, stamped=probe.stamped, skipped=skipped)

    summary = probe.recorder.summary()
    assert summary.count == measured - skipped
    assert summary.dropped == 0, "latency exceeded the histogram ceiling"
    assert summary.min_us > 0
    assert summary.p50_us <= summary.p99_us <= summary.max_us  # monotonic percentiles

    bench_results["metrics"].setdefault("propagation_same_process", []).append(
        {
            "topology": "same_process",
            "poll_timeout_ms": poll_ms,
            "payload_bytes": payload,
            "partitions": partitions,
            "warmup": warmup,
            **summary_to_dict(summary),
        }
    )
    print(
        f"\n[propagation same_process poll={poll_ms} payload={payload} parts={partitions}] "
        f"n={summary.count} p50={summary.p50_us / 1000:.2f}ms "
        f"p99={summary.p99_us / 1000:.2f}ms max={summary.max_us / 1000:.2f}ms"
    )


@pytest.mark.parametrize(("poll_ms", "payload", "partitions"), _PROFILE_CELLS, ids=_CELL_IDS)
async def test_propagation_raw_kafka_baseline(
    poll_ms: int,
    payload: int,
    partitions: int,
    bench_topic,
    bootstrap: str,
    bench_results: dict,
) -> None:
    """M7 — the same publish→consume latency through a bare aiokafka producer +
    consumer (no ktables). Subtract from the same-process M1 cell to isolate
    ktables' overhead over raw Kafka."""
    warmup, measured = _COUNTS.get(PROFILE, _COUNTS["quick"])
    encode = stamping_encoder(payload)
    topic = await bench_topic(partitions=partitions)
    producer = AIOKafkaProducer(bootstrap_servers=bootstrap, **BASELINE_PRODUCER_KWARGS)
    baseline = RawPropagationBaseline(bootstrap_servers=bootstrap, topic=topic, poll_timeout_ms=poll_ms)

    await producer.start()
    await baseline.start()
    try:
        for i in range(warmup):
            await producer.send_and_wait(topic, value=encode(i), key=f"w{i}".encode())
        assert await wait_until(lambda: baseline.consumed >= warmup, timeout=30), "baseline warm-up drain timed out"

        baseline.recording = True
        for i in range(measured):
            await producer.send_and_wait(topic, value=encode(i), key=f"m{i}".encode())
        assert await wait_until(lambda: baseline.stamped >= measured, timeout=30), "baseline measurement drain timed out"
        baseline.recording = False
    finally:
        await baseline.stop()
        await producer.stop()

    assert_samples_accounted(sent=measured, stamped=baseline.stamped)
    summary = baseline.recorder.summary()
    assert summary.count == measured and summary.dropped == 0
    assert summary.p50_us <= summary.p99_us <= summary.max_us

    bench_results["metrics"].setdefault("propagation_raw_baseline", []).append(
        {"topology": "raw_aiokafka", "poll_timeout_ms": poll_ms, "payload_bytes": payload, "partitions": partitions, **summary_to_dict(summary)}
    )
    print(
        f"\n[propagation raw_baseline poll={poll_ms} payload={payload} parts={partitions}] "
        f"n={summary.count} p50={summary.p50_us / 1000:.2f}ms p99={summary.p99_us / 1000:.2f}ms"
    )


# -- open-loop propagation under sustained load (same-process vs cross-process) ----
#
# Per the per-cell topology policy (§4.2): the same load (open-loop at rate R) is
# measured with the writer in-process and in a separate process, and the
# same-vs-cross delta reported — that delta IS the co-location bias, measured not
# assumed. Open-loop cells use coordinated-omission correction (the probe records
# via record_open_loop), so the HDR sample count exceeds the record count.

_OPEN_LOOP_PAYLOAD = 1024
_OPEN_LOOP_CELLS: dict[str, list[tuple[str, int]]] = {
    "quick": [("same_process", 1000), ("cross_process", 1000)],
    "full": [(topo, rate) for topo in ("same_process", "cross_process") for rate in (1000, 5000)],
    "soak": [("same_process", 1000)],
}
_OPEN_LOOP_COUNT = {"quick": 1000, "full": 5000, "soak": 1000}
_PROFILE_OPEN_LOOP = _OPEN_LOOP_CELLS.get(PROFILE, _OPEN_LOOP_CELLS["quick"])
_OPEN_LOOP_IDS = [f"{topo}-rate{rate}" for topo, rate in _PROFILE_OPEN_LOOP]


@pytest.mark.parametrize(("topology", "rate"), _PROFILE_OPEN_LOOP, ids=_OPEN_LOOP_IDS)
async def test_propagation_open_loop(topology: str, rate: int, bench_topic, bootstrap: str, bench_results: dict) -> None:
    count = _OPEN_LOOP_COUNT.get(PROFILE, 1000)
    period = 1.0 / rate
    topic = await bench_topic(partitions=1)
    probe = PropagationProbe(
        bootstrap_servers=bootstrap, topic=topic, poll_timeout_ms=_DEFAULT_POLL, ensure_topic=False, expected_interval=period
    )

    async with probe.table:
        assert await probe.table.barrier(timeout=30), "reader catch-up timed out"
        probe.recording = True
        if topology == "same_process":
            saturation = await _drive_same_process(bootstrap, topic, rate, count, probe)
        else:
            saturation = await _drive_cross_process(bootstrap, topic, rate, count, probe)
        probe.recording = False

    assert_samples_accounted(sent=count, stamped=probe.stamped)
    summary = probe.recorder.summary()
    assert summary.count >= count and summary.dropped == 0  # CO correction adds synthetic samples

    # Saturated cells (generator fell behind, or — same-process — the reader couldn't
    # drain) have understated latencies, so quarantine them under a separate metric
    # key that compare.py never matches against the trustworthy cells.
    metric = "propagation_open_loop_saturated" if saturation["saturated"] else "propagation_open_loop"
    if saturation["saturated"]:
        print(f"\n[WARNING] {topology} rate={rate} SATURATED (max_lag={saturation['max_lag_s'] * 1000:.1f}ms) — quarantined")
    bench_results["metrics"].setdefault(metric, []).append(
        {"topology": topology, "rate_hz": rate, "records": count, "saturated": saturation["saturated"], "max_lag_ms": round(saturation["max_lag_s"] * 1000, 2), **summary_to_dict(summary)}
    )
    print(
        f"\n[propagation open_loop {topology} rate={rate}] records={count} "
        f"p50={summary.p50_us / 1000:.2f}ms p99={summary.p99_us / 1000:.2f}ms max={summary.max_us / 1000:.2f}ms saturated={saturation['saturated']}"
    )


async def _drive_same_process(bootstrap: str, topic: str, rate: int, count: int, probe: PropagationProbe) -> dict:
    """In-process open-loop writer (shares the reader's event loop)."""
    writer: KafkaTableWriter[int] = KafkaTableWriter(
        bootstrap_servers=bootstrap, topic=topic, value_encoder=stamping_encoder(_OPEN_LOOP_PAYLOAD), ensure_topic=False
    )
    async with writer:
        run = await run_open_loop(rate_hz=rate, count=count, send=lambda i: writer.set(f"m{i}", i))
        drain_start = time.perf_counter()  # all records sent; time the residual drain
        assert await wait_until(lambda: probe.stamped >= count, timeout=60), "same-process drain timed out"
        backlog_grew = (time.perf_counter() - drain_start) > _DRAIN_BACKLOG_LIMIT_S
    return run.assess(backlog_grew=backlog_grew).as_summary()


async def _drive_cross_process(bootstrap: str, topic: str, rate: int, count: int, probe: PropagationProbe) -> dict:
    """Separate-process open-loop writer (the reader's loop runs unperturbed). The
    child reports the generator's saturation verdict; the parent cannot cleanly
    separate the child's send phase from drain (spawn latency confounds wall-time),
    so reader-backlog detection is left to the same-process cells."""
    ctx = multiprocessing.get_context("spawn")
    result_queue = ctx.Queue()
    proc = ctx.Process(target=writer_main, args=(bootstrap, topic, float(rate), count, _OPEN_LOOP_PAYLOAD, "m", result_queue))
    proc.start()
    try:
        assert await wait_until(lambda: probe.stamped >= count, timeout=60), "cross-process drain timed out"
        try:
            return result_queue.get(timeout=10)
        except QueueEmpty:
            raise AssertionError(f"cross-process writer exited without a verdict (exitcode={proc.exitcode})") from None
    finally:
        proc.join(timeout=10)
        if proc.is_alive():
            proc.terminate()
            proc.join(timeout=5)
        result_queue.close()
