"""M6 — in-memory read microbenchmarks (pytest-benchmark), validating the
documented O(·) read complexities.

These operations are pure-CPU and never touch the broker, so the table's internal
state is populated **directly** (the standard, noise-free way to microbenchmark
in-memory reads) rather than via Redpanda — the broker contributes nothing to what
is measured here. The macro suite (test_propagation/barrier/write/catchup/...) is
where the real broker is exercised.

Read with: min for the O(1) lookups (least-noisy estimate of true cost), median +
IQR for the O(N) operations (snapshot/codec allocate, so GC/allocator is part of
the real cost). Suggested invocation::

    uv run --group bench pytest benchmarks/test_reads_micro.py \\
        --benchmark-disable-gc --benchmark-json results/micro.json
"""

from __future__ import annotations

import pytest

from ktables import GroupedKafkaTable, KafkaTable, LengthPrefixedKeyCodec

pytestmark = pytest.mark.benchmark

_BOOTSTRAP = "localhost:9092"  # never connects — these tables are never start()ed


def _flat_table(n: int) -> KafkaTable[bytes]:
    table: KafkaTable[bytes] = KafkaTable(bootstrap_servers=_BOOTSTRAP, topic="micro", value_decoder=bytes)
    table._started = True  # bypass start(); reads only walk _data
    table._data = {f"k{i}": b"v" for i in range(n)}
    return table


def _grouped_table(num_groups: int, members_per_group: int) -> GroupedKafkaTable[bytes]:
    table: GroupedKafkaTable[bytes] = GroupedKafkaTable(bootstrap_servers=_BOOTSTRAP, topic="micro", value_decoder=bytes)
    table._table._started = True
    table._index = {f"g{g}": {f"m{m}": b"v" for m in range(members_per_group)} for g in range(num_groups)}
    return table


# -- O(1) point lookups: time must stay flat as the table grows ---------------


@pytest.mark.parametrize("total", [1_000, 100_000], ids=["n1k", "n100k"])
def test_get_member_is_o1(benchmark, total: int) -> None:
    table = _grouped_table(num_groups=10, members_per_group=total // 10)
    result = benchmark.pedantic(table.get_member, args=("g0", "m0"), rounds=100, iterations=2000, warmup_rounds=5)
    assert result == b"v"


@pytest.mark.parametrize("total", [1_000, 100_000], ids=["n1k", "n100k"])
def test_has_member_is_o1(benchmark, total: int) -> None:
    table = _grouped_table(num_groups=10, members_per_group=total // 10)
    assert benchmark.pedantic(table.has_member, args=("g0", "m0"), rounds=100, iterations=2000, warmup_rounds=5) is True


# -- O(output) group reads ----------------------------------------------------


@pytest.mark.parametrize("members", [10, 100, 1000], ids=lambda m: f"members{m}")
def test_members_is_o_group(benchmark, members: int) -> None:
    table = _grouped_table(num_groups=10, members_per_group=members)
    out = benchmark(table.members, "g0")
    assert len(out) == members


@pytest.mark.parametrize("num_groups", [100, 1000, 10_000], ids=lambda g: f"groups{g}")
def test_groups_is_o_groupcount(benchmark, num_groups: int) -> None:
    table = _grouped_table(num_groups=num_groups, members_per_group=1)
    out = benchmark(table.groups)
    assert len(out) == num_groups


# -- O(N) whole-view snapshots ------------------------------------------------


@pytest.mark.parametrize("n", [1_000, 10_000, 100_000], ids=lambda n: f"n{n}")
def test_flat_snapshot_is_o_n(benchmark, n: int) -> None:
    table = _flat_table(n)
    out = benchmark(table.snapshot)
    assert len(out) == n


@pytest.mark.parametrize("n", [1_000, 10_000, 100_000], ids=lambda n: f"n{n}")
def test_grouped_snapshot_is_o_n(benchmark, n: int) -> None:
    table = _grouped_table(num_groups=max(1, n // 100), members_per_group=100)
    out = benchmark(table.snapshot)
    assert sum(len(members) for members in out.values()) == table.member_count("g0") * len(out)


# -- composite-key codec ------------------------------------------------------


def test_codec_encode(benchmark) -> None:
    codec = LengthPrefixedKeyCodec()
    out = benchmark.pedantic(codec.encode, args=("billing", "host-a"), rounds=100, iterations=5000, warmup_rounds=5)
    assert out == "7:billinghost-a"


def test_codec_decode(benchmark) -> None:
    codec = LengthPrefixedKeyCodec()
    out = benchmark.pedantic(codec.decode, args=("7:billinghost-a",), rounds=100, iterations=5000, warmup_rounds=5)
    assert out == ("billing", "host-a")
