"""M3/M4 — write latency and throughput (the writer in isolation; no reader).

M3 write latency: time each ``await writer.set()`` (publish→ack) sequentially.
M4 throughput: sustained records/s with a sweep of pipelined in-flight depth
(depth 1 == naive sequential), plus the per-record latency observed at that depth.

Caveat baked into the README: single-node Redpanda, RF=1, so ``acks=all`` is a
local write and these understate a replicated cluster.

Run with ``uv run --group bench pytest benchmarks/test_write.py``;
``KTABLES_BENCH_PROFILE`` selects quick | full | soak (default quick).
"""

from __future__ import annotations

import asyncio
import time

import pytest
from aiokafka import AIOKafkaProducer

from benchmarks._harness import BASELINE_PRODUCER_KWARGS, LatencyRecorder, identity_codec, summary_to_dict
from benchmarks.conftest import PROFILE
from ktables import KafkaTableWriter

pytestmark = pytest.mark.benchmark

_PAYLOAD_SWEEP = (100, 1024, 16384)
_PARTITION_SWEEP = (1, 4)
_DEFAULT_PAYLOAD = 1024


# -- M3 write latency --------------------------------------------------------


def _latency_cells() -> list[tuple[int, int, bool]]:
    # (payload_bytes, partitions, enable_idempotence). Per §6: sweep payload and
    # partitions with idempotence on; idempotence-off is a single comparison cell.
    cells: dict[tuple[int, int, bool], None] = {}
    for payload in _PAYLOAD_SWEEP:
        cells[(payload, 1, True)] = None
    for parts in _PARTITION_SWEEP:
        cells[(_DEFAULT_PAYLOAD, parts, True)] = None
    cells[(_DEFAULT_PAYLOAD, 1, False)] = None
    return list(cells)


_LATENCY_CELLS: dict[str, list[tuple[int, int, bool]]] = {
    "quick": [(_DEFAULT_PAYLOAD, 1, True)],
    "full": _latency_cells(),
    "soak": [(_DEFAULT_PAYLOAD, 1, True)],
}
_LATENCY_N = {"quick": 1000, "full": 5000, "soak": 1000}
_PROFILE_LATENCY_CELLS = _LATENCY_CELLS.get(PROFILE, _LATENCY_CELLS["quick"])
_LATENCY_IDS = [f"payload{p}-parts{parts}-idem{idem}" for p, parts, idem in _PROFILE_LATENCY_CELLS]


@pytest.mark.parametrize(("payload", "partitions", "idempotence"), _PROFILE_LATENCY_CELLS, ids=_LATENCY_IDS)
async def test_write_latency(payload: int, partitions: int, idempotence: bool, bench_topic, bootstrap: str, bench_results: dict) -> None:
    n = _LATENCY_N.get(PROFILE, 1000)
    warmup = 50
    value = b"x" * payload
    topic = await bench_topic(partitions=partitions)
    writer: KafkaTableWriter[bytes] = KafkaTableWriter(
        bootstrap_servers=bootstrap, topic=topic, value_encoder=identity_codec, ensure_topic=False, enable_idempotence=idempotence
    )
    recorder = LatencyRecorder()
    async with writer:
        for i in range(warmup):
            await writer.set(f"w{i}", value)
        start = time.perf_counter()
        for i in range(n):
            t0 = time.perf_counter()
            await writer.set(f"k{i}", value)
            recorder.record(time.perf_counter() - t0)
        sequential_rps = n / (time.perf_counter() - start)

    summary = recorder.summary()
    assert summary.count == n and summary.dropped == 0
    bench_results["metrics"].setdefault("write_latency", []).append(
        {
            "payload_bytes": payload,
            "partitions": partitions,
            "enable_idempotence": idempotence,
            "sequential_rps": round(sequential_rps),
            **summary_to_dict(summary),
        }
    )
    print(
        f"\n[write_latency payload={payload} parts={partitions} idem={idempotence}] "
        f"n={n} p50={summary.p50_us / 1000:.2f}ms p99={summary.p99_us / 1000:.2f}ms "
        f"seq_throughput={round(sequential_rps)}/s"
    )


# -- M4 throughput -----------------------------------------------------------

_DEPTHS = {"quick": [64], "full": [1, 8, 64, 256], "soak": [64]}
_THROUGHPUT_TOTAL = {"quick": 2000, "full": 20_000, "soak": 2000}
_PROFILE_DEPTHS = _DEPTHS.get(PROFILE, _DEPTHS["quick"])


async def _send_timed(writer: KafkaTableWriter[bytes], key: str, value: bytes, recorder: LatencyRecorder) -> None:
    t0 = time.perf_counter()
    await writer.set(key, value)
    recorder.record(time.perf_counter() - t0)


@pytest.mark.parametrize("depth", _PROFILE_DEPTHS, ids=[f"depth{d}" for d in _PROFILE_DEPTHS])
async def test_write_throughput(depth: int, bench_topic, bootstrap: str, bench_results: dict) -> None:
    total = _THROUGHPUT_TOTAL.get(PROFILE, 2000)
    value = b"x" * _DEFAULT_PAYLOAD
    topic = await bench_topic(partitions=1)
    writer: KafkaTableWriter[bytes] = KafkaTableWriter(bootstrap_servers=bootstrap, topic=topic, value_encoder=identity_codec, ensure_topic=False)
    recorder = LatencyRecorder()
    async with writer:
        for i in range(50):  # warm up
            await writer.set(f"w{i}", value)
        start = time.perf_counter()
        base = 0
        while base < total:
            chunk = min(depth, total - base)
            await asyncio.gather(*(_send_timed(writer, f"k{base + j}", value, recorder) for j in range(chunk)))
            base += chunk
        throughput = total / (time.perf_counter() - start)

    summary = recorder.summary()
    assert summary.count == total and summary.dropped == 0
    bench_results["metrics"].setdefault("write_throughput", []).append(
        {"in_flight_depth": depth, "payload_bytes": _DEFAULT_PAYLOAD, "throughput_rps": round(throughput), **summary_to_dict(summary)}
    )
    print(
        f"\n[write_throughput depth={depth}] total={total} throughput={round(throughput)}/s "
        f"latency_p50={summary.p50_us / 1000:.2f}ms latency_p99={summary.p99_us / 1000:.2f}ms"
    )


async def test_write_latency_raw_baseline(bench_topic, bootstrap: str, bench_results: dict) -> None:
    """M7 — write latency through a bare aiokafka producer (idempotence-matched).
    Subtract from the default M3 cell to isolate KafkaTableWriter's overhead."""
    n = _LATENCY_N.get(PROFILE, 1000)
    value = b"x" * _DEFAULT_PAYLOAD
    topic = await bench_topic(partitions=1)
    producer = AIOKafkaProducer(bootstrap_servers=bootstrap, **BASELINE_PRODUCER_KWARGS)
    recorder = LatencyRecorder()
    await producer.start()
    try:
        for i in range(50):
            await producer.send_and_wait(topic, value=value, key=f"w{i}".encode())
        for i in range(n):
            t0 = time.perf_counter()
            await producer.send_and_wait(topic, value=value, key=f"k{i}".encode())
            recorder.record(time.perf_counter() - t0)
    finally:
        await producer.stop()

    summary = recorder.summary()
    assert summary.count == n and summary.dropped == 0
    bench_results["metrics"].setdefault("write_latency_raw_baseline", []).append(
        {"payload_bytes": _DEFAULT_PAYLOAD, "partitions": 1, "enable_idempotence": True, **summary_to_dict(summary)}
    )
    print(f"\n[write_latency raw_baseline] n={n} p50={summary.p50_us / 1000:.2f}ms p99={summary.p99_us / 1000:.2f}ms")
