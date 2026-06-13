"""Tests for ktables — pure unit tests plus broker-backed integration tests.

The integration tests (everything using the ``topic`` fixture) need a Kafka
broker on localhost:9092 and are skipped, loudly, when none is reachable:

    docker run -d --name ktables-test-kafka -p 9092:9092 apache/kafka:3.9.0
    pytest tests
"""

from __future__ import annotations

import asyncio
import contextlib
import time
import types
import uuid
from datetime import datetime, timezone

import pytest
from aiokafka import AIOKafkaConsumer, TopicPartition
from aiokafka.admin import AIOKafkaAdminClient
from aiokafka.errors import IllegalStateError, KafkaError, TopicAlreadyExistsError
from pydantic import AwareDatetime, BaseModel

from ktables import KafkaTable, KafkaTableWriter, ViewStats
from ktables.kafka_table import ensure_topic

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


# ---------------------------------------------------------------------------
# ensure_topic — validation, idempotency, error propagation (mocked admin)
# ---------------------------------------------------------------------------
#
# The broker reachable in CI does NOT reliably raise TopicAlreadyExistsError on
# a rapid second create_topics (KRaft acks the create before it propagates), so
# the already-exists / re-raise branches can't be driven by a real double-call —
# they're exercised against a fake admin client instead.


class _FakeAdmin:
    """Stand-in for AIOKafkaAdminClient: ``create_topics`` does whatever
    ``on_create`` dictates, and ``close`` records that the ``finally`` ran."""

    def __init__(self, on_create) -> None:
        self._on_create = on_create
        self.closed = False

    async def start(self) -> None:
        pass

    async def create_topics(self, topics) -> None:
        self._on_create()

    async def close(self) -> None:
        self.closed = True


class TestEnsureTopic:
    async def test_rejects_non_positive_partitions_and_rf(self) -> None:
        # Validation runs before any admin client is constructed — no broker.
        with pytest.raises(ValueError, match=">= 1"):
            await ensure_topic(BOOTSTRAP, "t", num_partitions=0)
        with pytest.raises(ValueError, match=">= 1"):
            await ensure_topic(BOOTSTRAP, "t", replication_factor=0)

    async def test_already_exists_returns_false_and_closes(self, monkeypatch: pytest.MonkeyPatch) -> None:
        def already_exists() -> None:
            raise TopicAlreadyExistsError()

        admin = _FakeAdmin(already_exists)
        monkeypatch.setattr("ktables.kafka_table.AIOKafkaAdminClient", lambda **kw: admin)
        assert await ensure_topic(BOOTSTRAP, "t") is False
        assert admin.closed  # finally: the client is always closed

    async def test_unexpected_error_is_reraised_and_closes(self, monkeypatch: pytest.MonkeyPatch) -> None:
        def boom() -> None:
            raise RuntimeError("create_topics failed")

        admin = _FakeAdmin(boom)
        monkeypatch.setattr("ktables.kafka_table.AIOKafkaAdminClient", lambda **kw: admin)
        with pytest.raises(RuntimeError, match="create_topics failed"):
            await ensure_topic(BOOTSTRAP, "t")
        assert admin.closed  # finally still runs on the re-raise path


# ---------------------------------------------------------------------------
# Read-API surface & introspection — private-state driven, no broker
# ---------------------------------------------------------------------------


class TestIntrospectionUnit:
    def test_topic_failure_and_is_caught_up_properties(self) -> None:
        table = make_table("unit.props")
        assert table.topic == "unit.props"
        assert table.failure is None
        assert table.is_caught_up is False

    def test_iter_over_started_table_yields_keys(self) -> None:
        table = make_table("unit.iter")
        table._started = True  # bypass start(); __iter__ just walks _data
        table._data = {"a": 1, "b": 2}  # type: ignore[dict-item]  # values irrelevant to iteration
        assert sorted(table) == ["a", "b"]

    def test_status_reports_loading_then_degraded(self) -> None:
        table = make_table("unit.status")
        table._started = True
        assert table.status == "loading"
        table._timed_out = True
        assert table.status == "degraded"

    async def test_wait_until_caught_up_returns_false_when_reader_dead(self) -> None:
        table = make_table("unit.deadwait")
        table._failure = RuntimeError("reader already dead")
        assert await table.wait_until_caught_up(timeout=0.1) is False


# ---------------------------------------------------------------------------
# _on_reader_done — the task done-callback, called directly
# ---------------------------------------------------------------------------


class TestReaderDoneCallbackUnit:
    def test_clean_completion_records_no_failure(self) -> None:
        table = make_table("unit.donecb")

        class _Task:
            def cancelled(self) -> bool:
                return False

            def exception(self) -> BaseException | None:
                return None

        table._on_reader_done(_Task())  # type: ignore[arg-type]
        assert table.failure is None
        assert table.status == "unstarted"

    def test_cancelled_task_is_ignored(self) -> None:
        table = make_table("unit.donecancel")

        class _Task:
            def cancelled(self) -> bool:
                return True

            def exception(self) -> BaseException | None:
                raise AssertionError("exception() must not be read for a cancelled task")

        table._on_reader_done(_Task())  # type: ignore[arg-type]
        assert table.failure is None


# ---------------------------------------------------------------------------
# stop() — idempotency & teardown edge paths, no broker
# ---------------------------------------------------------------------------


class TestStopLifecycleUnit:
    async def test_stop_on_unstarted_table_is_noop(self) -> None:
        table = make_table("unit.stop")
        await table.stop()  # task is None and consumer is None — both short-circuits
        assert table._task is None and table._consumer is None

    async def test_stop_cancels_pending_and_skips_already_done_barriers(self) -> None:
        table = make_table("unit.stopbar")
        loop = asyncio.get_running_loop()
        done: asyncio.Future[None] = loop.create_future()
        done.set_result(None)
        pending: asyncio.Future[None] = loop.create_future()
        table._barriers = [({}, done), ({}, pending)]
        await table.stop()
        assert pending.cancelled()  # pending barrier is cancelled to unblock its waiter
        assert not done.cancelled()  # already-done barrier is left alone
        assert table._barriers == []

    async def test_stop_swallows_consumer_stop_failure(self) -> None:
        table = make_table("unit.stoperr")

        class _BadConsumer:
            async def stop(self) -> None:
                raise RuntimeError("consumer.stop blew up")

        table._consumer = _BadConsumer()  # type: ignore[assignment]
        await table.stop()  # the error is logged, not raised, and the handle is cleared
        assert table._consumer is None


# ---------------------------------------------------------------------------
# _apply — keyless and undecodable-key skips, no broker
# ---------------------------------------------------------------------------


def _fake_record(*, key: bytes | None, value: bytes | None, partition: int = 0, offset: int = 0):
    """A minimal stand-in for ConsumerRecord — _apply only reads these four."""
    return types.SimpleNamespace(key=key, value=value, partition=partition, offset=offset)


class TestApplyUnit:
    def test_keyless_record_is_skipped_and_counted(self) -> None:
        table = make_table("unit.apply.keyless")
        table._apply(_fake_record(key=None, value=b"{}"))  # type: ignore[arg-type]
        assert table.stats.keyless_records == 1
        assert len(table._data) == 0

    def test_undecodable_key_is_skipped_and_counted(self) -> None:
        def bad_key(b: bytes) -> str:
            raise ValueError("undecodable key")

        table = KafkaTable(bootstrap_servers=BOOTSTRAP, topic="unit.apply.badkey", value_decoder=bytes, key_decoder=bad_key)
        table._apply(_fake_record(key=b"\xff\xfe", value=b"x"))  # type: ignore[arg-type]
        assert table.stats.key_decode_errors == 1
        assert len(table._data) == 0


# ---------------------------------------------------------------------------
# Reader loop — gate extension & barrier sweep, driven via a stub consumer
# ---------------------------------------------------------------------------


class _StubConsumer:
    """The slice of AIOKafkaConsumer the reader loop touches. ``getmany`` yields
    (so the test's poller can observe state and the loop never starves); the
    log is empty and all positions sit at the gate, so the table latches fast."""

    def __init__(self, assignment: set[TopicPartition]) -> None:
        self._assignment = assignment

    async def getmany(self, timeout_ms: int):
        await asyncio.sleep(0)
        return {}

    def assignment(self) -> set[TopicPartition]:
        return set(self._assignment)

    async def end_offsets(self, partitions):
        return {tp: 0 for tp in partitions}

    async def position(self, tp: TopicPartition) -> int:
        return 0


async def _run_reader_until(table, consumer, tps, end_offsets, predicate) -> None:
    """Drive ``table._run`` as a task until ``predicate()`` holds, then cancel."""
    task = asyncio.create_task(table._run(consumer, tps, dict(end_offsets), 0.0))
    try:
        assert await eventually(predicate), "reader-loop predicate never held"
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


class TestReaderLoopUnit:
    async def test_gate_extends_to_late_assigned_partitions(self) -> None:
        # The start-time gate knows only p0; the loop discovers p1 in the live
        # assignment and extends the gate to it before latching.
        table = make_table("unit.expand")
        table._started = True
        p0 = TopicPartition("unit.expand", 0)
        p1 = TopicPartition("unit.expand", 1)
        consumer = _StubConsumer({p0, p1})
        await _run_reader_until(table, consumer, [p0], {p0: 0}, lambda: table.is_caught_up)
        assert table.is_caught_up

    async def test_gate_tolerates_transient_assignment_shrink(self) -> None:
        # The gate spans p0 & p1 but the loop momentarily sees only p0: the
        # "new partitions" set is empty, so the gate is left as-is (no re-fetch).
        table = make_table("unit.shrink")
        table._started = True
        p0 = TopicPartition("unit.shrink", 0)
        p1 = TopicPartition("unit.shrink", 1)
        consumer = _StubConsumer({p0})
        await _run_reader_until(table, consumer, [p0, p1], {p0: 0, p1: 0}, lambda: table.is_caught_up)
        assert table.is_caught_up

    async def test_sweep_drops_cancelled_and_already_done_barriers(self) -> None:
        # A cancelled barrier is dropped (continue); an already-resolved barrier
        # whose target is met is dropped without a redundant set_result.
        table = make_table("unit.sweep")
        table._started = True
        p0 = TopicPartition("unit.sweep", 0)
        loop = asyncio.get_running_loop()
        cancelled: asyncio.Future[None] = loop.create_future()
        cancelled.cancel()
        done: asyncio.Future[None] = loop.create_future()
        done.set_result(None)
        table._barriers = [({p0: 0}, cancelled), ({p0: 0}, done)]
        consumer = _StubConsumer({p0})
        await _run_reader_until(table, consumer, [p0], {p0: 0}, lambda: table._barriers == [])
        assert cancelled.cancelled() and done.done()


# ---------------------------------------------------------------------------
# start() edge paths — real start() with induced broker conditions
# ---------------------------------------------------------------------------


class TestStartEdgePaths:
    async def test_start_skips_ensure_topic_when_disabled(self, topic: str) -> None:
        await ensure_topic(BOOTSTRAP, topic)  # pre-create: the table won't ensure it
        table = KafkaTable.json(bootstrap_servers=BOOTSTRAP, topic=topic, model=ServiceRecord, ensure_topic=False)
        async with table:
            assert table.status == "caught_up"

    async def test_start_raises_and_cleans_up_when_no_partitions(self, topic: str, monkeypatch: pytest.MonkeyPatch) -> None:
        # An empty assignment after consumer.start() means the topic is missing
        # (or ensure was masked): start() must raise and tear the consumer down.
        monkeypatch.setattr(AIOKafkaConsumer, "assignment", lambda self: set())
        table = make_table(topic)
        with pytest.raises(RuntimeError, match="no partitions assigned"):
            await table.start()
        assert table._consumer is None  # except-BaseException cleanup ran

    async def test_start_raises_when_reader_dies_during_catchup(self, topic: str, monkeypatch: pytest.MonkeyPatch) -> None:
        async def boom(self, *args, **kwargs):
            raise RuntimeError("induced reader death")

        monkeypatch.setattr(AIOKafkaConsumer, "getmany", boom)
        table = make_table(topic)
        with pytest.raises(RuntimeError, match="died during catch-up"):
            await table.start()
        assert table.status == "failed"

    async def test_start_degrades_when_catchup_times_out(self, topic: str, monkeypatch: pytest.MonkeyPatch) -> None:
        # Inflate the start-time end offsets so the gate is never reachable; with
        # a tight catchup_timeout, start() returns DEGRADED rather than crashing.
        real = AIOKafkaConsumer.end_offsets

        async def inflated(self, partitions, *args, **kwargs):
            offsets = await real(self, partitions)
            return {tp: off + 10_000 for tp, off in offsets.items()}

        monkeypatch.setattr(AIOKafkaConsumer, "end_offsets", inflated)
        table = KafkaTable.json(bootstrap_servers=BOOTSTRAP, topic=topic, model=ServiceRecord, catchup_timeout=0.5)
        async with table:
            assert table.status == "degraded"


# ---------------------------------------------------------------------------
# KafkaTableWriter — repr, lifecycle guards, ensure-topic toggle
# ---------------------------------------------------------------------------


class TestWriterUnit:
    def test_repr_reflects_unstarted_state(self) -> None:
        writer = make_writer("unit.wrepr")
        text = repr(writer)
        assert "unit.wrepr" in text and "started=False" in text

    async def test_double_start_raises(self) -> None:
        writer = make_writer("unit.wstart")
        writer._producer = object()  # type: ignore[assignment]  # simulate a started producer
        with pytest.raises(RuntimeError, match="already started"):
            await writer.start()

    async def test_stop_on_unstarted_writer_is_noop(self) -> None:
        writer = make_writer("unit.wstop")
        await writer.stop()  # producer is None — short-circuits
        assert writer._producer is None


class TestWriterEnsureTopicDisabled:
    async def test_start_skips_ensure_topic_when_disabled(self, topic: str) -> None:
        await ensure_topic(BOOTSTRAP, topic)  # pre-create: the writer won't ensure it
        writer = KafkaTableWriter.json(bootstrap_servers=BOOTSTRAP, topic=topic, model=ServiceRecord, ensure_topic=False)
        async with writer:
            await writer.set("k", make_record("k", 1))
