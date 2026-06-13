"""Tests for ktables — pure unit tests plus broker-backed integration tests.

The integration tests (everything using the ``topic`` fixture) need a Kafka
broker on localhost:9092 and are skipped, loudly, when none is reachable:

    docker run -d --name ktables-test-kafka -p 9092:9092 apache/kafka:3.9.0
    pytest tests
"""

from __future__ import annotations

import asyncio
import time
import uuid
from datetime import datetime, timezone

import pytest
from aiokafka.admin import AIOKafkaAdminClient
from aiokafka.errors import IllegalStateError, KafkaError
from pydantic import AwareDatetime, BaseModel

from ktables import KafkaTable, KafkaTableWriter, ViewStats

BOOTSTRAP = "localhost:9092"


class ServiceRecord(BaseModel):
    """Test payload: a service-registry-style advertisement."""

    schema_version: int = 1
    service_id: str
    endpoint: str
    revision: int
    published_at: AwareDatetime


def make_record(service_id: str, revision: int) -> ServiceRecord:
    return ServiceRecord(
        service_id=service_id,
        endpoint=f"http://{service_id}.local:8080",
        revision=revision,
        published_at=datetime.now(tz=timezone.utc),
    )


def make_table(topic: str) -> KafkaTable[ServiceRecord]:
    return KafkaTable.json(bootstrap_servers=BOOTSTRAP, topic=topic, model=ServiceRecord)


def make_writer(topic: str) -> KafkaTableWriter[ServiceRecord]:
    return KafkaTableWriter.json(bootstrap_servers=BOOTSTRAP, topic=topic, model=ServiceRecord)


async def eventually(predicate, timeout: float = 5.0, interval: float = 0.005) -> bool:
    """Poll until ``predicate()`` is true — publish→visible is eventually
    consistent (no read-your-own-writes), so tests must never bare-read."""
    deadline = time.perf_counter() + timeout
    while time.perf_counter() < deadline:
        if predicate():
            return True
        await asyncio.sleep(interval)
    return predicate()


# ---------------------------------------------------------------------------
# Pure unit tests (no broker)
# ---------------------------------------------------------------------------


class TestConstruction:
    def test_rejects_empty_topic_and_servers(self) -> None:
        with pytest.raises(ValueError):
            KafkaTable(bootstrap_servers="", topic="t", value_decoder=bytes)
        with pytest.raises(ValueError):
            KafkaTable(bootstrap_servers="b", topic="", value_decoder=bytes)
        with pytest.raises(ValueError):
            KafkaTableWriter(bootstrap_servers="b", topic="", value_encoder=bytes)

    def test_rejects_non_positive_catchup_timeout(self) -> None:
        with pytest.raises(ValueError):
            KafkaTable(bootstrap_servers="b", topic="t", value_decoder=bytes, catchup_timeout=0)

    def test_rejects_non_callable_codecs(self) -> None:
        with pytest.raises(TypeError):
            KafkaTable(bootstrap_servers="b", topic="t", value_decoder="nope")  # type: ignore[arg-type]
        with pytest.raises(TypeError):
            KafkaTableWriter(bootstrap_servers="b", topic="t", value_encoder="nope")  # type: ignore[arg-type]


class TestUnstartedGuards:
    def test_reads_raise_before_start(self) -> None:
        table = make_table("unit.never.started")
        for access in (lambda: table["k"], lambda: len(table), lambda: list(table), table.snapshot):
            with pytest.raises(RuntimeError, match="not started"):
                access()
        assert table.status == "unstarted"

    def test_writer_requires_start(self) -> None:
        writer = make_writer("unit.never.started")
        with pytest.raises(RuntimeError, match="not started"):
            writer._require_producer()


class TestResourceHandleSemantics:
    def test_equality_is_identity_not_contents(self) -> None:
        # Mapping injects contents-__eq__; KafkaTable must override back to
        # identity — two tables are not "the same table" because contents match.
        a, b = make_table("unit.eq"), make_table("unit.eq")
        assert a == a
        assert a != b

    def test_instances_are_hashable(self) -> None:
        # Mapping sets __hash__ = None; the override must restore hashability.
        assert len({make_table("unit.hash"), make_table("unit.hash")}) == 2

    def test_repr_is_compact(self) -> None:
        text = repr(make_table("unit.repr"))
        assert "unit.repr" in text and "unstarted" in text
        assert "value_decoder" not in text  # no dataclass-style field dump


class TestViewStats:
    def test_snapshot_is_frozen(self) -> None:
        stats = ViewStats()
        with pytest.raises(AttributeError):
            stats.records_applied = 5  # type: ignore[misc]


class TestBarrierLifecycle:
    """Lifecycle guards — no broker needed (the guards run before any I/O)."""

    async def test_barrier_on_unstarted_table_raises(self) -> None:
        table = make_table("unit.never.started")
        with pytest.raises(RuntimeError, match="not started"):
            await table.barrier()

    async def test_barrier_after_stop_raises_stopped_not_unstarted(self) -> None:
        # The post-stop state is exactly _started=True with _consumer=None;
        # reproduce it directly so this stays a pure unit test. barrier() must
        # raise the *stopped* message, not the *not-started* one.
        table = make_table("unit.stopped")
        table._started = True
        assert table._consumer is None
        with pytest.raises(RuntimeError, match="stopped"):
            await table.barrier()


# ---------------------------------------------------------------------------
# Integration tests (broker required; skipped when unreachable)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def broker_available() -> bool:
    async def probe() -> bool:
        admin = AIOKafkaAdminClient(bootstrap_servers=BOOTSTRAP)
        try:
            await admin.start()
            await admin.close()
            return True
        except Exception:
            return False

    return asyncio.run(probe())


@pytest.fixture
async def topic(broker_available: bool):
    if not broker_available:
        pytest.skip(f"no Kafka broker reachable at {BOOTSTRAP}")
    name = f"ktables.test.{uuid.uuid4().hex[:8]}"
    yield name
    admin = AIOKafkaAdminClient(bootstrap_servers=BOOTSTRAP)
    await admin.start()
    try:
        await admin.delete_topics([name])
    finally:
        await admin.close()


async def test_reader_first_creates_topic_and_absorbs_writes_as_live_updates(topic: str) -> None:
    # No bring-up order: the reader ensures the topic, gates instantly on the
    # empty log, then sees the late writer's records without a restart.
    async with make_table(topic) as table:
        assert table.status == "caught_up"
        assert len(table) == 0
        async with make_writer(topic) as writer:
            await writer.set("alpha", make_record("alpha", 1))
            assert await eventually(lambda: "alpha" in table)


async def test_double_start_raises(topic: str) -> None:
    async with make_table(topic) as table:
        with pytest.raises(RuntimeError, match="already started"):
            await table.start()


async def test_cold_start_replays_lww_over_uncompacted_history(topic: str) -> None:
    services, revs = ["alpha", "beta", "gamma"], 40
    async with make_writer(topic) as writer:
        for rev in range(1, revs + 1):
            for svc in services:
                await writer.set(svc, make_record(svc, rev))

        async with make_table(topic) as table:
            # Catch-up gate: complete as of start-time end offsets. The topic
            # is too young for compaction, so this proves LWW over full history.
            assert table.status == "caught_up"
            assert sorted(table.snapshot()) == sorted(services)
            assert all(table[s].revision == revs for s in services)
            assert table.stats.replayed_at_catch_up >= len(services) * revs

            # Live update.
            await writer.set("alpha", make_record("alpha", revs + 1))
            assert await eventually(lambda: table["alpha"].revision == revs + 1)

            # Tombstone deletes.
            await writer.delete("beta")
            assert await eventually(lambda: "beta" not in table)

            # Poison record: skipped, previous value retained, reader survives.
            await writer._require_producer().send_and_wait(topic, value=b"{not json", key=b"alpha")
            assert await eventually(lambda: table.stats.value_decode_errors >= 1)
            assert table["alpha"].revision == revs + 1
            assert table.status == "caught_up"


async def test_restart_rehydrates_identically(topic: str) -> None:
    async with make_writer(topic) as writer:
        await writer.set("alpha", make_record("alpha", 2))
        await writer.set("beta", make_record("beta", 1))
        await writer.delete("beta")

    async with make_table(topic) as first:
        snapshot_one = {k: v.revision for k, v in first.snapshot().items()}

    async with make_table(topic) as second:
        snapshot_two = {k: v.revision for k, v in second.snapshot().items()}

    assert snapshot_one == snapshot_two == {"alpha": 2}


# ---------------------------------------------------------------------------
# barrier() — the on-demand read-your-own-writes primitive (broker required)
# ---------------------------------------------------------------------------


def _unreachable_end_offsets(real):
    """Wrap a consumer's real ``end_offsets`` so it reports targets the reader
    can never reach — used to hold a barrier in its wait phase."""

    async def inflated(partitions, *args, **kwargs):
        offsets = await real(partitions)
        return {tp: off + 10_000 for tp, off in offsets.items()}

    return inflated


async def test_barrier_makes_prior_writes_immediately_visible(topic: str) -> None:
    # The core RYOW guarantee: after barrier() returns True, every record acked
    # before the call is visible *immediately* — no eventually(). This is the
    # test that would fail under a racy position-polling design.
    async with make_table(topic) as table, make_writer(topic) as writer:
        keys = [f"svc{i}" for i in range(10)]
        for k in keys:
            await writer.set(k, make_record(k, 1))
        assert await table.barrier() is True
        for k in keys:
            assert table[k].revision == 1
        assert table._barriers == []


async def test_barrier_makes_tombstones_immediately_visible(topic: str) -> None:
    async with make_table(topic) as table, make_writer(topic) as writer:
        await writer.set("k", make_record("k", 1))
        assert await table.barrier() is True
        assert "k" in table
        await writer.delete("k")
        assert await table.barrier() is True
        assert "k" not in table


async def test_barrier_on_idle_caught_up_table_returns_true(topic: str) -> None:
    async with make_table(topic) as table:
        assert table.status == "caught_up"
        assert await table.barrier(timeout=5) is True


async def test_concurrent_barriers_all_resolve_and_see_their_writes(topic: str) -> None:
    async with make_table(topic) as table, make_writer(topic) as writer:
        keys = [f"k{i}" for i in range(10)]
        for k in keys:
            await writer.set(k, make_record(k, 1))
        results = await asyncio.gather(*(table.barrier(timeout=10) for _ in range(5)))
        assert results == [True] * 5
        for k in keys:
            assert k in table
        assert table._barriers == []


async def test_barrier_targets_call_time_offsets_not_later_writes(topic: str) -> None:
    # barrier() snapshots the end offsets at call time, so it returns True even
    # while new writes keep arriving — it does not wait for post-call records.
    # If it waited for "all writes ever", the churn would make it time out.
    async with make_table(topic) as table, make_writer(topic) as writer:
        await writer.set("pre", make_record("pre", 1))
        stop = asyncio.Event()

        async def churn() -> None:
            i = 0
            while not stop.is_set():
                await writer.set(f"post{i}", make_record(f"post{i}", 1))
                i += 1

        churn_task = asyncio.create_task(churn())
        try:
            assert await table.barrier(timeout=10) is True
            assert table["pre"].revision == 1  # the pre-call write is visible
        finally:
            stop.set()
            await churn_task


async def test_barrier_resolves_across_multiple_poll_iterations(topic: str, monkeypatch: pytest.MonkeyPatch) -> None:
    # Throttle the reader to one record per poll so the barrier's target offset
    # is reached only after many reader-loop iterations — exercising the
    # across-iterations sweep, not single-shot resolution.
    table = KafkaTable.json(bootstrap_servers=BOOTSTRAP, topic=topic, model=ServiceRecord, poll_timeout_ms=20)
    async with table, make_writer(topic) as writer:
        real_getmany = table._consumer.getmany  # type: ignore[union-attr]

        async def one_at_a_time(*args, **kwargs):
            kwargs["max_records"] = 1
            return await real_getmany(*args, **kwargs)

        monkeypatch.setattr(table._consumer, "getmany", one_at_a_time)
        n = 12
        for i in range(n):
            await writer.set(f"svc{i}", make_record(f"svc{i}", 1))
        assert await table.barrier(timeout=10) is True
        assert all(f"svc{i}" in table for i in range(n))
        assert table.stats.records_applied >= n  # applied across many iterations
        assert table._barriers == []


async def test_barrier_times_out_to_false_without_leaking(topic: str, monkeypatch: pytest.MonkeyPatch) -> None:
    async with make_table(topic) as table:
        monkeypatch.setattr(table._consumer, "end_offsets", _unreachable_end_offsets(table._consumer.end_offsets))
        assert await table.barrier(timeout=0.5) is False
        assert table.status == "caught_up"  # table itself stays healthy
        assert table._barriers == []  # no leak: the barrier self-pruned


async def test_barrier_returns_false_on_snapshot_error(topic: str, monkeypatch: pytest.MonkeyPatch) -> None:
    async with make_table(topic) as table:
        # (a) broker error while snapshotting → False, never raised, never registered.
        async def boom(partitions, *args, **kwargs):
            raise KafkaError("induced ListOffsets failure")

        monkeypatch.setattr(table._consumer, "end_offsets", boom)
        assert await table.barrier(timeout=5) is False
        assert table._barriers == []
        assert table.status == "caught_up"

        # (b) snapshot that hangs past the timeout → also False (whole-call budget).
        async def hang(partitions, *args, **kwargs):
            await asyncio.sleep(3600)

        monkeypatch.setattr(table._consumer, "end_offsets", hang)
        assert await table.barrier(timeout=0.3) is False
        assert table._barriers == []


async def test_barrier_returns_false_promptly_on_reader_death(topic: str, monkeypatch: pytest.MonkeyPatch) -> None:
    async with make_table(topic) as table:
        # Hold the barrier in its wait phase…
        monkeypatch.setattr(table._consumer, "end_offsets", _unreachable_end_offsets(table._consumer.end_offsets))
        bar = asyncio.create_task(table.barrier(timeout=30))
        assert await eventually(lambda: len(table._barriers) == 1)

        # …then kill the reader: the next poll raises a non-retriable error.
        async def boom(*args, **kwargs):
            raise RuntimeError("induced reader death")

        monkeypatch.setattr(table._consumer, "getmany", boom)
        # Must unblock via the _failed event well before its own 30s timeout.
        assert await asyncio.wait_for(bar, timeout=5) is False
        assert table.status == "failed"
        assert table._barriers == []


async def test_barrier_returns_false_when_stop_races_the_wait(topic: str, monkeypatch: pytest.MonkeyPatch) -> None:
    # Regression guard: a timeout=None barrier racing shutdown must not hang.
    table = make_table(topic)
    await table.start()
    try:
        monkeypatch.setattr(table._consumer, "end_offsets", _unreachable_end_offsets(table._consumer.end_offsets))
        bar = asyncio.create_task(table.barrier(timeout=None))
        assert await eventually(lambda: len(table._barriers) == 1)
        await table.stop()
        assert await asyncio.wait_for(bar, timeout=5) is False
    finally:
        if table._consumer is not None:
            await table.stop()


async def test_barrier_returns_false_immediately_when_reader_already_dead(topic: str, monkeypatch: pytest.MonkeyPatch) -> None:
    # The reader-already-dead fast-path (distinct from death *during* the wait):
    # barrier() must short-circuit to False at the _failure check, before it
    # snapshots end offsets or registers — so nothing is ever queued.
    async with make_table(topic) as table:
        async def boom(*args, **kwargs):
            raise RuntimeError("induced reader death")

        monkeypatch.setattr(table._consumer, "getmany", boom)
        assert await eventually(lambda: table.status == "failed")
        assert await table.barrier(timeout=5) is False
        assert table._barriers == []  # never registered — short-circuited


async def test_barrier_survives_transient_position_unavailability(topic: str, monkeypatch: pytest.MonkeyPatch) -> None:
    # A metadata blip can momentarily empty the assignment under group_id=None,
    # making consumer.position() raise IllegalStateError. The reader must
    # tolerate it (not die) and the barrier must still resolve once positions
    # are readable again. Without the catch, the first raise kills the reader
    # and the barrier returns False.
    async with make_table(topic) as table, make_writer(topic) as writer:
        await writer.set("k", make_record("k", 1))
        real_position = table._consumer.position
        calls = {"n": 0}

        async def flaky_position(tp):
            calls["n"] += 1
            if calls["n"] <= 2:  # simulate the transient-shrink window
                raise IllegalStateError(f"Partition {tp} is not assigned")
            return await real_position(tp)

        monkeypatch.setattr(table._consumer, "position", flaky_position)
        assert await table.barrier(timeout=10) is True
        assert table.status != "failed"  # reader survived the transient raise
        assert table._barriers == []


async def test_barrier_accounts_for_decode_skipped_records(topic: str) -> None:
    # The docstring guarantees that on True, every acked record is visible "or
    # counted in stats as decode-skipped". A poison value sits below the
    # barrier's target: the reader advances past it (counting the skip), so the
    # barrier still resolves and the good record after it shows immediately.
    # (Keyless records aren't covered here: a compacted topic rejects null-key
    # records broker-side, so that path can't arise on a table-shaped topic.)
    async with make_table(topic) as table, make_writer(topic) as writer:
        await writer._require_producer().send_and_wait(topic, value=b"{not json", key=b"poison")
        await writer.set("good", make_record("good", 1))
        assert await table.barrier() is True
        # Immediately, no eventually(): the skip is accounted for, not lost.
        assert table.stats.value_decode_errors >= 1
        assert table["good"].revision == 1
        assert "poison" not in table
        assert table._barriers == []
