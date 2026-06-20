"""M5 — cold-start / time-to-usable-view (start() to caught_up).

Preload a topic with N records, then time a fresh table's ``start()``. The total
wall-clock is decomposed into connect/metadata + ``catch_up_seconds`` (the reader's
own replay timer) + up to one poll of gate-detection slack. Variants: decode cost
(raw bytes vs json.loads vs pydantic), flat vs grouped (codec + index overhead),
and catch-up while a writer keeps producing (the gate must still terminate).

Run with ``uv run --group bench pytest benchmarks/test_catchup.py``;
``KTABLES_BENCH_PROFILE`` selects quick | full | soak (default quick).
"""

from __future__ import annotations

import asyncio
import json
import time
from collections.abc import Callable
from datetime import datetime, timezone

import pytest
from pydantic import AwareDatetime, BaseModel

from benchmarks._harness import identity_codec, preload
from benchmarks.conftest import PROFILE
from ktables import GroupedKafkaTable, GroupedKafkaTableWriter, KafkaTable, KafkaTableWriter

pytestmark = pytest.mark.benchmark


class ServiceRecord(BaseModel):
    service_id: str
    endpoint: str
    revision: int
    published_at: AwareDatetime


def _raw_encoder(seq: int) -> bytes:
    return b"x" * 256


def _json_encoder(seq: int) -> bytes:
    return json.dumps({"seq": seq, "pad": "x" * 200}).encode()


def _pydantic_encoder(seq: int) -> bytes:
    return ServiceRecord(
        service_id=f"svc{seq}", endpoint="http://svc.local:8080", revision=seq, published_at=datetime.now(tz=timezone.utc)
    ).model_dump_json().encode()


# codec name -> (value_encoder taking the seq, value_decoder)
_CODECS: dict[str, tuple[Callable[[int], bytes], Callable[[bytes], object]]] = {
    "raw": (_raw_encoder, identity_codec),
    "json": (_json_encoder, json.loads),
    "pydantic": (_pydantic_encoder, ServiceRecord.model_validate_json),
}

_FLAT_CELLS: dict[str, list[tuple[int, str]]] = {
    "quick": [(1000, "raw")],
    "full": [(1000, "raw"), (10_000, "raw"), (100_000, "raw"), (10_000, "json"), (10_000, "pydantic")],
    "soak": [(1000, "raw")],
}
_GROUPED_N: dict[str, list[int]] = {"quick": [1000], "full": [1000, 10_000, 100_000], "soak": [1000]}
_LIVE_N = {"quick": 2000, "full": 20_000, "soak": 2000}

_PROFILE_FLAT = _FLAT_CELLS.get(PROFILE, _FLAT_CELLS["quick"])
_PROFILE_GROUPED = _GROUPED_N.get(PROFILE, _GROUPED_N["quick"])


def _record_catchup(bench_results: dict, metric: str, params: dict, total_s: float, stats) -> None:
    row = {
        **params,
        "total_start_ms": round(total_s * 1000, 3),
        "catch_up_ms": round((stats.catch_up_seconds or 0.0) * 1000, 3),
        "connect_metadata_ms": round((total_s - (stats.catch_up_seconds or 0.0)) * 1000, 3),
        "replayed_at_catch_up": stats.replayed_at_catch_up,
    }
    bench_results["metrics"].setdefault(metric, []).append(row)
    replay_rps = stats.replayed_at_catch_up / stats.catch_up_seconds if stats.catch_up_seconds else 0
    print(f"\n[{metric} {params}] total={row['total_start_ms']}ms catch_up={row['catch_up_ms']}ms replay={round(replay_rps)}/s")


@pytest.mark.parametrize(("n", "codec"), _PROFILE_FLAT, ids=[f"n{n}-{c}" for n, c in _PROFILE_FLAT])
async def test_catchup_flat(n: int, codec: str, bench_topic, bootstrap: str, bench_results: dict) -> None:
    encoder, decoder = _CODECS[codec]
    topic = await bench_topic(partitions=1)
    writer: KafkaTableWriter[int] = KafkaTableWriter(bootstrap_servers=bootstrap, topic=topic, value_encoder=encoder, ensure_topic=False)
    async with writer:
        await preload(writer, ((f"k{i}", i) for i in range(n)))

    table: KafkaTable[object] = KafkaTable(bootstrap_servers=bootstrap, topic=topic, value_decoder=decoder, ensure_topic=False)
    start = time.perf_counter()
    await table.start()
    total = time.perf_counter() - start
    try:
        assert table.status == "caught_up", f"expected caught_up, got {table.status}"
        assert len(table) == n
        _record_catchup(bench_results, "catchup_flat", {"n": n, "codec": codec}, total, table.stats)
    finally:
        await table.stop()


@pytest.mark.parametrize("n", _PROFILE_GROUPED, ids=[f"n{n}" for n in _PROFILE_GROUPED])
async def test_catchup_grouped(n: int, bench_topic, bootstrap: str, bench_results: dict) -> None:
    # Raw decoder isolates the grouped layer's key-codec + index cost from value decode.
    topic = await bench_topic(partitions=1)
    writer: GroupedKafkaTableWriter[bytes] = GroupedKafkaTableWriter(
        bootstrap_servers=bootstrap, topic=topic, value_encoder=identity_codec, ensure_topic=False
    )
    async with writer:
        await preload(writer, (("g", f"m{i}", b"x" * 256) for i in range(n)))

    table: GroupedKafkaTable[bytes] = GroupedKafkaTable(bootstrap_servers=bootstrap, topic=topic, value_decoder=identity_codec, ensure_topic=False)
    start = time.perf_counter()
    await table.start()
    total = time.perf_counter() - start
    try:
        assert table.status == "caught_up"
        assert table.member_count("g") == n
        _record_catchup(bench_results, "catchup_grouped", {"n": n}, total, table.stats)
    finally:
        await table.stop()


async def test_catchup_under_live_writes(bench_topic, bootstrap: str, bench_results: dict) -> None:
    """The catch-up gate snapshots end offsets at start; a writer producing during
    catch-up must not stop start() from terminating (it catches up to the snapshot,
    later writes are live)."""
    n = _LIVE_N.get(PROFILE, 2000)
    topic = await bench_topic(partitions=1)
    writer: KafkaTableWriter[int] = KafkaTableWriter(bootstrap_servers=bootstrap, topic=topic, value_encoder=_raw_encoder, ensure_topic=False)
    table: KafkaTable[object] = KafkaTable(bootstrap_servers=bootstrap, topic=topic, value_decoder=identity_codec, ensure_topic=False)
    async with writer:
        await preload(writer, ((f"k{i}", i) for i in range(n)))

        stop = asyncio.Event()

        async def churn() -> None:
            i = 0
            while not stop.is_set():
                try:
                    await writer.set(f"live{i}", i)
                except Exception:
                    return  # a writer error during shutdown must not mask a start() failure
                i += 1

        churn_task = asyncio.create_task(churn())
        try:
            start = time.perf_counter()
            await table.start()
            total = time.perf_counter() - start
        finally:
            stop.set()
            churn_task.cancel()
            await asyncio.gather(churn_task, return_exceptions=True)

    try:
        assert table.started  # terminated rather than chasing the live tail forever
        assert table.status in ("caught_up", "degraded")
        bench_results["metrics"].setdefault("catchup_live", []).append(
            {"preloaded": n, "status": table.status, "total_start_ms": round(total * 1000, 3), "replayed_at_catch_up": table.stats.replayed_at_catch_up}
        )
        print(f"\n[catchup_live preloaded={n}] status={table.status} total={round(total * 1000, 3)}ms")
    finally:
        await table.stop()
