"""M8 — table memory footprint (the library holds the whole topic in RAM).

Reports three figures per cell:

- ``dict_bytes`` — a direct ``sys.getsizeof`` walk of the materialized dict(s),
  the cleanest table-only number (excludes the consumer / fetch buffers).
- ``traced_bytes`` — ``tracemalloc`` total after population + ``gc.collect()``;
  includes consumer overhead, so its per-key value is inflated at small N and
  converges to the dict cost as N grows.
- ``rss_delta_bytes`` — process RSS delta; coarsest, includes everything.

Flat vs grouped exposes the grouped layer's second copy (inner ``_data`` AND the
nested ``_index``). Run with ``uv run --group bench pytest benchmarks/test_memory.py``;
``KTABLES_BENCH_PROFILE`` selects quick | full | soak (default quick).
"""

from __future__ import annotations

import gc
import sys
import tracemalloc

import psutil
import pytest

from benchmarks._harness import identity_codec, preload
from benchmarks.conftest import PROFILE
from ktables import GroupedKafkaTable, GroupedKafkaTableWriter, KafkaTable, KafkaTableWriter

pytestmark = pytest.mark.benchmark

# (n_keys, payload_bytes). N is swept to amortize fixed overhead; extend to 1_000_000
# for a dedicated run (plan §10 — heavy on a laptop, so it is not in the default full).
_FLAT_CELLS: dict[str, list[tuple[int, int]]] = {
    "quick": [(10_000, 256)],
    "full": [(10_000, 256), (100_000, 256), (10_000, 1024), (10_000, 16384)],
    "soak": [(10_000, 256)],
}
_GROUPED_N: dict[str, list[int]] = {"quick": [10_000], "full": [10_000, 100_000], "soak": [10_000]}
_PROFILE_FLAT = _FLAT_CELLS.get(PROFILE, _FLAT_CELLS["quick"])
_PROFILE_GROUPED = _GROUPED_N.get(PROFILE, _GROUPED_N["quick"])


def _flat_dict_footprint(data: dict) -> int:
    """sys.getsizeof of a flat dict: its table plus every key and value object."""
    return sys.getsizeof(data) + sum(sys.getsizeof(k) + sys.getsizeof(v) for k, v in data.items())


@pytest.mark.parametrize(("n", "payload"), _PROFILE_FLAT, ids=[f"n{n}-payload{p}" for n, p in _PROFILE_FLAT])
async def test_memory_flat(n: int, payload: int, bench_topic, bootstrap: str, bench_results: dict) -> None:
    value = b"x" * payload
    topic = await bench_topic(partitions=1)
    writer: KafkaTableWriter[bytes] = KafkaTableWriter(bootstrap_servers=bootstrap, topic=topic, value_encoder=identity_codec, ensure_topic=False)
    async with writer:
        await preload(writer, ((f"k{i}", value) for i in range(n)))

    proc = psutil.Process()
    gc.collect()
    rss_before = proc.memory_info().rss
    tracemalloc.start()
    table: KafkaTable[bytes] = KafkaTable(bootstrap_servers=bootstrap, topic=topic, value_decoder=identity_codec, ensure_topic=False)
    await table.start()
    try:
        assert await table.barrier(timeout=60)
        assert len(table) == n
        gc.collect()
        traced_current, _ = tracemalloc.get_traced_memory()
        rss_delta = proc.memory_info().rss - rss_before
        dict_bytes = _flat_dict_footprint(table._data)
    finally:
        tracemalloc.stop()
        await table.stop()

    _record_memory(bench_results, "memory_flat", {"n": n, "payload_bytes": payload}, n, dict_bytes, traced_current, rss_delta)


@pytest.mark.parametrize("n", _PROFILE_GROUPED, ids=[f"n{n}" for n in _PROFILE_GROUPED])
async def test_memory_grouped(n: int, bench_topic, bootstrap: str, bench_results: dict) -> None:
    value = b"x" * 256
    topic = await bench_topic(partitions=1)
    writer: GroupedKafkaTableWriter[bytes] = GroupedKafkaTableWriter(
        bootstrap_servers=bootstrap, topic=topic, value_encoder=identity_codec, ensure_topic=False
    )
    async with writer:
        await preload(writer, (("g", f"m{i}", value) for i in range(n)))

    proc = psutil.Process()
    gc.collect()
    rss_before = proc.memory_info().rss
    tracemalloc.start()
    table: GroupedKafkaTable[bytes] = GroupedKafkaTable(bootstrap_servers=bootstrap, topic=topic, value_decoder=identity_codec, ensure_topic=False)
    await table.start()
    try:
        assert await table.barrier(timeout=60)
        assert table.member_count("g") == n
        gc.collect()
        traced_current, _ = tracemalloc.get_traced_memory()
        rss_delta = proc.memory_info().rss - rss_before
        # Grouped holds the topic twice: the inner flat dict AND the nested index.
        index = table._index
        dict_bytes = _flat_dict_footprint(table._table._data) + sys.getsizeof(index) + sum(
            sys.getsizeof(group) + _flat_dict_footprint(members) for group, members in index.items()
        )
    finally:
        tracemalloc.stop()
        await table.stop()

    _record_memory(bench_results, "memory_grouped", {"n": n, "payload_bytes": 256}, n, dict_bytes, traced_current, rss_delta)


def _record_memory(bench_results: dict, metric: str, params: dict, n: int, dict_bytes: int, traced: int, rss_delta: int) -> None:
    row = {
        **params,
        "dict_bytes": dict_bytes,
        "dict_bytes_per_key": round(dict_bytes / n, 1),
        "traced_bytes": traced,
        "traced_bytes_per_key": round(traced / n, 1),
        "rss_delta_bytes": rss_delta,
        "rss_delta_bytes_per_key": round(rss_delta / n, 1),
    }
    bench_results["metrics"].setdefault(metric, []).append(row)
    print(f"\n[{metric} {params}] dict={dict_bytes // 1024}KiB ({row['dict_bytes_per_key']} B/key) traced={traced // 1024}KiB rss_delta={rss_delta // 1024}KiB")
