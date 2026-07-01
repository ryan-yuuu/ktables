"""Tests for ktables — pure unit tests plus broker-backed integration tests.

The integration tests (everything using the ``bootstrap``/``topic`` fixtures)
run against a real Redpanda broker that testcontainers spins up automatically
(Docker required). They are auto-marked ``integration`` — see tests/conftest.py:

    uv run pytest                       # full suite (needs Docker)
    uv run pytest -m "not integration"  # unit suite only (no Docker)
"""

from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import logging
import time
import types
import uuid
from datetime import datetime, timezone

import pytest
from aiokafka import AIOKafkaConsumer, TopicPartition
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.admin.config_resource import ConfigResource, ConfigResourceType
from aiokafka.errors import (
    IllegalStateError,
    InvalidConfigurationError,
    KafkaConnectionError,
    KafkaError,
    TopicAlreadyExistsError,
    TopicAuthorizationFailedError,
    UnknownTopicOrPartitionError,
)
from pydantic import AwareDatetime, BaseModel

from ktables import KafkaTable, KafkaTableWriter, ViewStats
from ktables.kafka_table import (
    EnsureTopicResult,
    TopicConfigMismatchError,
    _check_topic_policy,
    _explicit_overrides,
    _policy_from_entries,
    _raise_for_code,
    _reconcile_policy,
    _requires_compaction,
    _satisfies_compaction,
    _split_policy,
    _try_create_topic,
    ensure_topic,
)

# Placeholder address for pure-unit tests that construct a table/writer but
# never start() it (so they never connect). Integration tests get the real
# broker address from the ``bootstrap`` fixture / the table_factory/writer_factory
# fixtures instead.
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


# Unit-only builders (never connect — see BOOTSTRAP above). Integration tests
# use the table_factory/writer_factory fixtures, which bind the live broker.
def make_table(topic: str) -> KafkaTable[ServiceRecord]:
    return KafkaTable.json(bootstrap_servers=BOOTSTRAP, topic=topic, model=ServiceRecord)


def make_writer(topic: str) -> KafkaTableWriter[ServiceRecord]:
    return KafkaTableWriter.json(bootstrap_servers=BOOTSTRAP, topic=topic, model=ServiceRecord)


async def eventually(predicate, timeout: float = 5.0, interval: float = 0.005) -> bool:
    """Poll until ``predicate()`` is true — publish-to-visible is eventually
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

    def test_rejects_non_callable_hooks(self) -> None:
        with pytest.raises(TypeError, match="callable"):
            KafkaTable(bootstrap_servers="b", topic="t", value_decoder=bytes, on_set="nope")  # type: ignore[arg-type]
        with pytest.raises(TypeError, match="callable"):
            KafkaTable(bootstrap_servers="b", topic="t", value_decoder=bytes, on_delete="nope")  # type: ignore[arg-type]


class TestFetchMaxWait:
    """fetch_max_wait_ms tunes the consumer's fetch long-poll — a dominant term in
    barrier() latency on a quiet table (alongside poll_timeout_ms)."""

    def test_defaults_to_aiokafka_500ms(self) -> None:
        assert make_table("unit.fmw")._fetch_max_wait_ms == 500

    def test_custom_value_is_stored(self) -> None:
        table = KafkaTable.json(bootstrap_servers=BOOTSTRAP, topic="unit.fmw", model=ServiceRecord, fetch_max_wait_ms=10)
        assert table._fetch_max_wait_ms == 10


class TestEnableIdempotence:
    """Idempotence is opt-in: the producer defaults to at-least-once (may
    duplicate/reorder). Turning it on implies acks=all — registry-grade
    durability (see the KafkaTableWriter class docstring)."""

    def test_defaults_to_false(self) -> None:
        assert make_writer("unit.idem")._enable_idempotence is False

    def test_explicit_true_is_honored(self) -> None:
        writer = KafkaTableWriter(bootstrap_servers="b", topic="t", value_encoder=bytes, enable_idempotence=True)
        assert writer._enable_idempotence is True


def _capture_producer(sink: dict) -> type:
    """A stand-in AIOKafkaProducer that records its construction kwargs."""

    class _FakeProducer:
        def __init__(self, **kwargs: object) -> None:
            sink.update(kwargs)

        async def start(self) -> None:
            pass

        async def stop(self) -> None:
            pass

    return _FakeProducer


class TestAcks:
    """acks is an opt-in producer knob: unset (None) defers to aiokafka's
    default (acks=1, or acks=all under enable_idempotence); a set value passes
    straight through to the producer."""

    def test_defaults_to_none(self) -> None:
        assert make_writer("unit.acks")._acks is None

    def test_explicit_value_is_stored(self) -> None:
        writer = KafkaTableWriter(bootstrap_servers="b", topic="t", value_encoder=bytes, acks="all")
        assert writer._acks == "all"

    async def test_set_acks_reaches_the_producer(self, monkeypatch: pytest.MonkeyPatch) -> None:
        captured: dict[str, object] = {}
        monkeypatch.setattr("ktables.kafka_table.AIOKafkaProducer", _capture_producer(captured))
        writer = KafkaTableWriter(bootstrap_servers="b", topic="t", value_encoder=bytes, ensure_topic=False, acks="all")
        await writer.start()
        assert captured["acks"] == "all"

    async def test_unset_acks_is_omitted_from_producer(self, monkeypatch: pytest.MonkeyPatch) -> None:
        captured: dict[str, object] = {}
        monkeypatch.setattr("ktables.kafka_table.AIOKafkaProducer", _capture_producer(captured))
        writer = KafkaTableWriter(bootstrap_servers="b", topic="t", value_encoder=bytes, ensure_topic=False)
        await writer.start()
        assert "acks" not in captured


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

    def test_started_is_false_before_start(self) -> None:
        assert make_table("unit.never.started").started is False


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
# Integration tests (broker required; Redpanda auto-started via testcontainers)
# ---------------------------------------------------------------------------


@pytest.fixture
def table_factory(bootstrap: str):
    """Build a ServiceRecord KafkaTable bound to the session Redpanda broker."""

    def _make(topic: str, **kwargs: object) -> KafkaTable[ServiceRecord]:
        return KafkaTable.json(bootstrap_servers=bootstrap, topic=topic, model=ServiceRecord, **kwargs)

    return _make


@pytest.fixture
def writer_factory(bootstrap: str):
    """Build a ServiceRecord KafkaTableWriter bound to the session Redpanda broker."""

    def _make(topic: str, **kwargs: object) -> KafkaTableWriter[ServiceRecord]:
        return KafkaTableWriter.json(bootstrap_servers=bootstrap, topic=topic, model=ServiceRecord, **kwargs)

    return _make


@pytest.fixture
async def topic(bootstrap: str):
    name = f"ktables.test.{uuid.uuid4().hex[:8]}"
    yield name
    admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap)
    await admin.start()
    try:
        await admin.delete_topics([name])
    finally:
        await admin.close()


async def test_reader_first_creates_topic_and_absorbs_writes_as_live_updates(topic: str, table_factory, writer_factory) -> None:
    # No bring-up order: the reader ensures the topic, gates instantly on the
    # empty log, then sees the late writer's records without a restart.
    async with table_factory(topic) as table:
        assert table.status == "caught_up"
        assert len(table) == 0
        async with writer_factory(topic) as writer:
            await writer.set("alpha", make_record("alpha", 1))
            assert await eventually(lambda: "alpha" in table)


async def test_double_start_raises(topic: str, table_factory, writer_factory) -> None:
    async with table_factory(topic) as table:
        with pytest.raises(RuntimeError, match="already started"):
            await table.start()


async def test_fetch_max_wait_ms_reaches_the_consumer(topic: str, table_factory, writer_factory) -> None:
    # The knob must actually plumb through to the AIOKafkaConsumer — it gates the
    # fetch long-poll, the dominant term in idle barrier() latency.
    table = table_factory(topic, fetch_max_wait_ms=10)
    async with table:
        assert table._consumer._fetch_max_wait_ms == 10


async def test_cold_start_replays_lww_over_uncompacted_history(topic: str, table_factory, writer_factory) -> None:
    services, revs = ["alpha", "beta", "gamma"], 40
    async with writer_factory(topic) as writer:
        for rev in range(1, revs + 1):
            for svc in services:
                await writer.set(svc, make_record(svc, rev))

        async with table_factory(topic) as table:
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


async def test_restart_rehydrates_identically(topic: str, table_factory, writer_factory) -> None:
    async with writer_factory(topic) as writer:
        await writer.set("alpha", make_record("alpha", 2))
        await writer.set("beta", make_record("beta", 1))
        await writer.delete("beta")

    async with table_factory(topic) as first:
        snapshot_one = {k: v.revision for k, v in first.snapshot().items()}

    async with table_factory(topic) as second:
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


async def test_barrier_makes_prior_writes_immediately_visible(topic: str, table_factory, writer_factory) -> None:
    # The core RYOW guarantee: after barrier() returns True, every record acked
    # before the call is visible *immediately* — no eventually(). This is the
    # test that would fail under a racy position-polling design.
    async with table_factory(topic) as table, writer_factory(topic) as writer:
        keys = [f"svc{i}" for i in range(10)]
        for k in keys:
            await writer.set(k, make_record(k, 1))
        assert await table.barrier() is True
        for k in keys:
            assert table[k].revision == 1
        assert table._barriers == []


async def test_barrier_makes_tombstones_immediately_visible(topic: str, table_factory, writer_factory) -> None:
    async with table_factory(topic) as table, writer_factory(topic) as writer:
        await writer.set("k", make_record("k", 1))
        assert await table.barrier() is True
        assert "k" in table
        await writer.delete("k")
        assert await table.barrier() is True
        assert "k" not in table


async def test_barrier_on_idle_caught_up_table_returns_true(topic: str, table_factory, writer_factory) -> None:
    async with table_factory(topic) as table:
        assert table.status == "caught_up"
        assert await table.barrier(timeout=5) is True


async def test_concurrent_barriers_all_resolve_and_see_their_writes(topic: str, table_factory, writer_factory) -> None:
    async with table_factory(topic) as table, writer_factory(topic) as writer:
        keys = [f"k{i}" for i in range(10)]
        for k in keys:
            await writer.set(k, make_record(k, 1))
        results = await asyncio.gather(*(table.barrier(timeout=10) for _ in range(5)))
        assert results == [True] * 5
        for k in keys:
            assert k in table
        assert table._barriers == []


async def test_barrier_targets_call_time_offsets_not_later_writes(topic: str, table_factory, writer_factory) -> None:
    # barrier() snapshots the end offsets at call time, so it returns True even
    # while new writes keep arriving — it does not wait for post-call records.
    # If it waited for "all writes ever", the churn would make it time out.
    async with table_factory(topic) as table, writer_factory(topic) as writer:
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


async def test_barrier_resolves_across_multiple_poll_iterations(topic: str, table_factory, writer_factory, monkeypatch: pytest.MonkeyPatch) -> None:
    # Throttle the reader to one record per poll so the barrier's target offset
    # is reached only after many reader-loop iterations — exercising the
    # across-iterations sweep, not single-shot resolution.
    table = table_factory(topic, poll_timeout_ms=20)
    async with table, writer_factory(topic) as writer:
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


async def test_barrier_times_out_to_false_without_leaking(topic: str, table_factory, writer_factory, monkeypatch: pytest.MonkeyPatch) -> None:
    async with table_factory(topic) as table:
        monkeypatch.setattr(table._consumer, "end_offsets", _unreachable_end_offsets(table._consumer.end_offsets))
        assert await table.barrier(timeout=0.5) is False
        assert table.status == "caught_up"  # table itself stays healthy
        assert table._barriers == []  # no leak: the barrier self-pruned


async def test_barrier_returns_false_on_snapshot_error(topic: str, table_factory, writer_factory, monkeypatch: pytest.MonkeyPatch) -> None:
    async with table_factory(topic) as table:
        # (a) broker error while snapshotting yields False, never raised, never registered.
        async def boom(partitions, *args, **kwargs):
            raise KafkaError("induced ListOffsets failure")

        monkeypatch.setattr(table._consumer, "end_offsets", boom)
        assert await table.barrier(timeout=5) is False
        assert table._barriers == []
        assert table.status == "caught_up"

        # (b) snapshot that hangs past the timeout, also False (whole-call budget).
        async def hang(partitions, *args, **kwargs):
            await asyncio.sleep(3600)

        monkeypatch.setattr(table._consumer, "end_offsets", hang)
        assert await table.barrier(timeout=0.3) is False
        assert table._barriers == []


async def test_barrier_returns_false_promptly_on_reader_death(topic: str, table_factory, writer_factory, monkeypatch: pytest.MonkeyPatch) -> None:
    async with table_factory(topic) as table:
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


async def test_barrier_returns_false_when_stop_races_the_wait(topic: str, table_factory, writer_factory, monkeypatch: pytest.MonkeyPatch) -> None:
    # Regression guard: a timeout=None barrier racing shutdown must not hang.
    table = table_factory(topic)
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


async def test_barrier_returns_false_immediately_when_reader_already_dead(topic: str, table_factory, writer_factory, monkeypatch: pytest.MonkeyPatch) -> None:
    # The reader-already-dead fast-path (distinct from death *during* the wait):
    # barrier() must short-circuit to False at the _failure check, before it
    # snapshots end offsets or registers — so nothing is ever queued.
    async with table_factory(topic) as table:
        async def boom(*args, **kwargs):
            raise RuntimeError("induced reader death")

        monkeypatch.setattr(table._consumer, "getmany", boom)
        assert await eventually(lambda: table.status == "failed")
        assert await table.barrier(timeout=5) is False
        assert table._barriers == []  # never registered — short-circuited


async def test_barrier_survives_transient_position_unavailability(topic: str, table_factory, writer_factory, monkeypatch: pytest.MonkeyPatch) -> None:
    # A metadata blip can momentarily empty the assignment under group_id=None,
    # making consumer.position() raise IllegalStateError. The reader must
    # tolerate it (not die) and the barrier must still resolve once positions
    # are readable again. Without the catch, the first raise kills the reader
    # and the barrier returns False.
    async with table_factory(topic) as table, writer_factory(topic) as writer:
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


async def test_barrier_accounts_for_decode_skipped_records(topic: str, table_factory, writer_factory) -> None:
    # The docstring guarantees that on True, every acked record is visible "or
    # counted in stats as decode-skipped". A poison value sits below the
    # barrier's target: the reader advances past it (counting the skip), so the
    # barrier still resolves and the good record after it shows immediately.
    # (Keyless records aren't covered here: a compacted topic rejects null-key
    # records broker-side, so that path can't arise on a table-shaped topic.)
    async with table_factory(topic) as table, writer_factory(topic) as writer:
        await writer._require_producer().send_and_wait(topic, value=b"{not json", key=b"poison")
        await writer.set("good", make_record("good", 1))
        assert await table.barrier() is True
        # Immediately, no eventually(): the skip is accounted for, not lost.
        assert table.stats.value_decode_errors >= 1
        assert table["good"].revision == 1
        assert "poison" not in table
        assert table._barriers == []


# ---------------------------------------------------------------------------
# Pure policy helpers (no broker) — used by the on_policy_mismatch machinery
# ---------------------------------------------------------------------------


class TestPublicExports:
    @pytest.mark.parametrize(
        "name",
        ["PolicyMismatchAction", "EnsureTopicOutcome", "EnsureTopicResult", "TopicConfigMismatchError"],
    )
    def test_reconciliation_symbols_are_public(self, name: str) -> None:
        import ktables

        assert name in ktables.__all__
        assert hasattr(ktables, name)


class TestSplitPolicy:
    @pytest.mark.parametrize(
        "raw",
        ["compact", "compact,delete", "delete,compact", "delete, compact", " compact "],
    )
    def test_compact_recognized_regardless_of_order_or_whitespace(self, raw: str) -> None:
        assert "compact" in _split_policy(raw)

    def test_delete_only_has_no_compact(self) -> None:
        assert "compact" not in _split_policy("delete")


class TestRequiresCompaction:
    @pytest.mark.parametrize("expected", [None, "", "delete"])
    def test_non_compacting_declarations_do_not_require_compaction(self, expected) -> None:
        assert _requires_compaction(expected) is False

    @pytest.mark.parametrize("expected", ["compact", "compact,delete"])
    def test_compacting_declarations_require_compaction(self, expected: str) -> None:
        assert _requires_compaction(expected) is True


class TestSatisfiesCompaction:
    @pytest.mark.parametrize("actual", ["compact", "compact,delete", "delete,compact"])
    def test_policies_containing_compact_satisfy(self, actual: str) -> None:
        assert _satisfies_compaction(actual) is True

    def test_delete_only_does_not_satisfy(self) -> None:
        assert _satisfies_compaction("delete") is False


class TestExplicitOverrides:
    """describe_configs entry shapes (verified against aiokafka 0.14.0):
    v1+ entry = (name, value, read_only, config_source:int, is_sensitive, synonyms)
                where config_source == 1 means TOPIC_CONFIG (an explicit override);
    v0  entry = (name, value, read_only, is_default:bool, is_sensitive).
    """

    def test_v1_keeps_only_topic_source_writable_set_overrides(self) -> None:
        entries = [
            ("retention.ms", "1000", False, 1, False, []),  # topic override -> kept
            ("segment.bytes", "99", False, 1, False, []),  # topic override -> kept
            ("max.message.bytes", "1048588", False, 5, False, []),  # default source -> dropped
            ("flush.ms", "0", True, 1, False, []),  # read_only -> dropped
            ("some.null", None, False, 1, False, []),  # value None -> dropped
        ]
        assert _explicit_overrides(entries) == {"retention.ms": "1000", "segment.bytes": "99"}

    def test_v0_keeps_only_non_default_writable_overrides(self) -> None:
        entries = [
            ("retention.ms", "1000", False, False, False),  # is_default False -> kept
            ("max.message.bytes", "1048588", False, True, False),  # is_default True -> dropped
            ("flush.ms", "0", True, False, False),  # read_only -> dropped
        ]
        assert _explicit_overrides(entries) == {"retention.ms": "1000"}


class TestTopicConfigMismatchError:
    def test_stores_fields_and_names_them_in_message(self) -> None:
        err = TopicConfigMismatchError("my-topic", "compact", "delete")
        assert err.topic == "my-topic"
        assert err.expected == "compact"
        assert err.actual == "delete"
        text = str(err)
        assert "my-topic" in text
        assert "compact" in text
        assert "delete" in text

    def test_is_a_plain_exception_not_under_a_ktables_base(self) -> None:
        # DX decision: no KTablesError base; the error stands alone on Exception.
        assert issubclass(TopicConfigMismatchError, Exception)
        assert TopicConfigMismatchError.__mro__[1] is Exception


class TestEnsureTopicResult:
    def test_carries_outcome_and_policy(self) -> None:
        result = EnsureTopicResult(outcome="reconciled", policy="delete")
        assert result.outcome == "reconciled"
        assert result.policy == "delete"

    def test_is_frozen(self) -> None:
        result = EnsureTopicResult(outcome="created", policy=None)
        with pytest.raises(dataclasses.FrozenInstanceError):
            result.outcome = "verified"  # type: ignore[misc]

    def test_uses_slots_no_instance_dict(self) -> None:
        assert not hasattr(EnsureTopicResult(outcome="skipped", policy=None), "__dict__")

    def test_equality_by_value(self) -> None:
        assert EnsureTopicResult("verified", "compact") == EnsureTopicResult("verified", "compact")
        assert EnsureTopicResult("verified", "compact") != EnsureTopicResult("mismatch", "delete")


class TestRaiseForCode:
    def test_zero_is_noop(self) -> None:
        assert _raise_for_code(0, "ignored") is None

    def test_nonzero_raises_mapped_exception_with_broker_message(self) -> None:
        with pytest.raises(InvalidConfigurationError, match="bad config value"):
            _raise_for_code(40, "bad config value")

    def test_none_message_does_not_render_as_none(self) -> None:
        with pytest.raises(TopicAuthorizationFailedError) as excinfo:
            _raise_for_code(29, None)
        assert "None" not in str(excinfo.value)


class _FakeAdminClient:
    """Faithful stand-in for AIOKafkaAdminClient, mirroring the real response
    shapes (verified against aiokafka 0.13.0 and 0.14.0):

    * ``create_topics``  -> obj with ``.topic_errors = [(name, code[, message])]``
    * ``describe_configs`` -> ``[obj(resources=[(code, msg, type, name, entries)])]``
    * ``alter_configs``  -> ``[obj(resources=[(code, msg, type, name)])]``

    Each method returns its configured response, or raises its configured
    exception. Call counts and the last ``alter_configs`` payload are recorded.
    """

    def __init__(
        self,
        *,
        create=None,
        describe=None,
        alter=None,
        start_exc=None,
    ) -> None:
        self._create = create
        self._describe = describe
        self._alter = alter
        self._start_exc = start_exc
        self.closed = False
        self.create_calls = 0
        self.describe_calls = 0
        self.alter_calls = 0
        self.altered_configs: dict | None = None

    @staticmethod
    def _resolve(spec):
        if isinstance(spec, Exception):
            raise spec
        return spec

    async def start(self) -> None:
        if self._start_exc is not None:
            raise self._start_exc

    async def create_topics(self, new_topics) -> object:
        self.create_calls += 1
        return self._resolve(self._create)

    async def describe_configs(self, config_resources, include_synonyms=False) -> object:
        self.describe_calls += 1
        return self._resolve(self._describe)

    async def alter_configs(self, config_resources) -> object:
        self.alter_calls += 1
        self.altered_configs = dict(config_resources[0].configs)
        return self._resolve(self._alter)

    async def close(self) -> None:
        self.closed = True


def _create_response(topic_errors):
    return types.SimpleNamespace(topic_errors=topic_errors)


def _describe_response(entries, *, code=0, msg=""):
    """A describe_configs return: list[Response], each with .resources of
    (error_code, error_message, resource_type, resource_name, config_entries)."""
    return [types.SimpleNamespace(resources=[(code, msg, 2, "t", entries)])]


def _alter_response(*, code=0, msg=""):
    """An alter_configs return: list[Response], each .resources of
    (error_code, error_message, resource_type, resource_name)."""
    return [types.SimpleNamespace(resources=[(code, msg, 2, "t")])]


_OVERRIDE_ENTRIES = [
    ("cleanup.policy", "delete", False, 1, False, []),
    ("retention.ms", "1234567", False, 1, False, []),
    ("segment.bytes", "10485760", False, 1, False, []),
]


def _entries_with_policy(policy, overrides=None):
    entries = [("cleanup.policy", policy, False, 1, False, [])]
    for key, value in (overrides or {}).items():
        entries.append((key, value, False, 1, False, []))
    return entries


class TestTryCreateTopic:
    async def test_code_zero_means_created_and_logs(self, caplog: pytest.LogCaptureFixture) -> None:
        admin = _FakeAdminClient(create=_create_response([("t", 0, "")]))
        with caplog.at_level(logging.INFO, logger="ktables.kafka_table"):
            created = await _try_create_topic(admin, "t", 1, 1, {"cleanup.policy": "compact"})
        assert created is True
        assert any("created topic" in r.message for r in caplog.records)

    async def test_code_36_means_exists_and_does_not_log_created(self, caplog: pytest.LogCaptureFixture) -> None:
        # Bug #1 regression: a real broker returns 36 in-band, NOT by raising.
        admin = _FakeAdminClient(create=_create_response([("t", 36, "The topic has already been created")]))
        with caplog.at_level(logging.INFO, logger="ktables.kafka_table"):
            created = await _try_create_topic(admin, "t", 1, 1, {"cleanup.policy": "compact"})
        assert created is False
        assert not any("created topic" in r.message for r in caplog.records)

    async def test_other_nonzero_code_raises_with_broker_message(self) -> None:
        admin = _FakeAdminClient(create=_create_response([("t", 29, "Not authorized to access topics: [t]")]))
        with pytest.raises(TopicAuthorizationFailedError, match="Not authorized"):
            await _try_create_topic(admin, "t", 1, 1, {"cleanup.policy": "compact"})

    async def test_v0_topic_errors_tuple_without_message(self) -> None:
        # CreateTopicsResponse v0 element is (name, code) — no message field.
        admin = _FakeAdminClient(create=_create_response([("t", 36)]))
        assert await _try_create_topic(admin, "t", 1, 1, {"cleanup.policy": "compact"}) is False

    async def test_defensive_topic_already_exists_exception_means_exists(self) -> None:
        admin = _FakeAdminClient(create=TopicAlreadyExistsError())
        assert await _try_create_topic(admin, "t", 1, 1, {"cleanup.policy": "compact"}) is False

    async def test_empty_topic_errors_raises_unconfirmed(self) -> None:
        admin = _FakeAdminClient(create=_create_response([]))
        with pytest.raises(RuntimeError, match="cannot confirm"):
            await _try_create_topic(admin, "t", 1, 1, {"cleanup.policy": "compact"})


class TestPolicyFromEntries:
    def test_returns_cleanup_policy_value(self) -> None:
        entries = [
            ("retention.ms", "1000", False, 1, False, []),
            ("cleanup.policy", "compact,delete", False, 1, False, []),
        ]
        assert _policy_from_entries(entries) == "compact,delete"

    def test_returns_none_when_policy_absent(self) -> None:
        assert _policy_from_entries([("retention.ms", "1000", False, 1, False, [])]) is None

    def test_returns_none_for_empty(self) -> None:
        assert _policy_from_entries([]) is None


class TestReconcilePolicy:
    async def test_merges_compact_over_preserved_overrides_in_one_alter(self) -> None:
        admin = _FakeAdminClient(alter=_alter_response())
        await _reconcile_policy(admin, "t", "compact", _OVERRIDE_ENTRIES)
        assert admin.alter_calls == 1
        assert admin.altered_configs == {
            "cleanup.policy": "compact",
            "retention.ms": "1234567",
            "segment.bytes": "10485760",
        }

    async def test_success_logs_preserved_override_count(self, caplog: pytest.LogCaptureFixture) -> None:
        admin = _FakeAdminClient(alter=_alter_response())
        with caplog.at_level(logging.INFO, logger="ktables.kafka_table"):
            await _reconcile_policy(admin, "t", "compact", _OVERRIDE_ENTRIES)
        msgs = [r.message for r in caplog.records if "reconciled topic" in r.message]
        assert len(msgs) == 1
        assert "preserved 2 override" in msgs[0]

    @pytest.mark.parametrize(
        "code,exc",
        [
            (29, TopicAuthorizationFailedError),  # ACL denial
            (3, UnknownTopicOrPartitionError),  # topic vanished mid-flight
            (40, InvalidConfigurationError),  # §5.4: a preserved override rejected on write
        ],
    )
    async def test_inband_alter_rejection_raises_and_does_not_log_success(
        self, code: int, exc: type[Exception], caplog: pytest.LogCaptureFixture
    ) -> None:
        admin = _FakeAdminClient(alter=_alter_response(code=code, msg="rejected"))
        with caplog.at_level(logging.INFO, logger="ktables.kafka_table"):
            with pytest.raises(exc, match="rejected"):
                await _reconcile_policy(admin, "t", "compact", _OVERRIDE_ENTRIES)
        assert not any("reconciled topic" in r.message for r in caplog.records)

    async def test_empty_alter_response_raises_unconfirmed(self) -> None:
        # A degenerate broker that echoes no resource must not be reported as success.
        admin = _FakeAdminClient(alter=[types.SimpleNamespace(resources=[])])
        with pytest.raises(RuntimeError, match="cannot confirm reconcile"):
            await _reconcile_policy(admin, "t", "compact", _OVERRIDE_ENTRIES)
        assert admin.alter_calls == 1


class TestCheckTopicPolicyVerifiedAndMismatch:
    async def test_compact_is_verified_without_alter_or_retention_log(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        admin = _FakeAdminClient(describe=_describe_response(_entries_with_policy("compact")))
        with caplog.at_level(logging.INFO, logger="ktables.kafka_table"):
            result = await _check_topic_policy(admin, "t", "compact", "warn")
        assert result == EnsureTopicResult("verified", "compact")
        assert admin.alter_calls == 0
        assert not any("retention" in r.message for r in caplog.records)

    async def test_compact_delete_is_verified_with_retention_info(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        admin = _FakeAdminClient(describe=_describe_response(_entries_with_policy("compact,delete")))
        with caplog.at_level(logging.INFO, logger="ktables.kafka_table"):
            result = await _check_topic_policy(admin, "t", "compact", "warn")
        assert result == EnsureTopicResult("verified", "compact,delete")
        retention = [r for r in caplog.records if "retention" in r.message]
        assert len(retention) == 1
        assert retention[0].levelno == logging.INFO

    async def test_mismatch_warn_logs_warning_and_returns_mismatch(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        admin = _FakeAdminClient(describe=_describe_response(_entries_with_policy("delete")))
        with caplog.at_level(logging.INFO, logger="ktables.kafka_table"):
            result = await _check_topic_policy(admin, "t", "compact", "warn")
        assert result == EnsureTopicResult("mismatch", "delete")
        assert admin.alter_calls == 0
        warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert len(warnings) == 1
        assert "compact" in warnings[0].message  # the required policy is interpolated

    async def test_mismatch_raise_raises_with_fields(self) -> None:
        admin = _FakeAdminClient(describe=_describe_response(_entries_with_policy("delete")))
        with pytest.raises(TopicConfigMismatchError) as excinfo:
            await _check_topic_policy(admin, "t", "compact", "raise")
        assert (excinfo.value.topic, excinfo.value.expected, excinfo.value.actual) == ("t", "compact", "delete")

    async def test_mismatch_reconcile_alters_and_returns_pre_reconcile_policy(self) -> None:
        admin = _FakeAdminClient(
            describe=_describe_response(_entries_with_policy("delete", {"retention.ms": "1234567"})),
            alter=_alter_response(),
        )
        result = await _check_topic_policy(admin, "t", "compact", "reconcile")
        assert result == EnsureTopicResult("reconciled", "delete")  # policy is the PRE-reconcile value
        assert admin.alter_calls == 1
        assert admin.altered_configs == {"cleanup.policy": "compact", "retention.ms": "1234567"}

    async def test_reconcile_to_plain_compact_logs_no_retention_caveat(self, caplog: pytest.LogCaptureFixture) -> None:
        admin = _FakeAdminClient(describe=_describe_response(_entries_with_policy("delete")), alter=_alter_response())
        with caplog.at_level(logging.INFO, logger="ktables.kafka_table"):
            await _check_topic_policy(admin, "t", "compact", "reconcile")
        assert not any("retention" in r.message for r in caplog.records)

    async def test_reconcile_to_compact_delete_logs_retention_caveat(self, caplog: pytest.LogCaptureFixture) -> None:
        # §5.3: the eviction caveat fires on the effective post-action policy too.
        admin = _FakeAdminClient(describe=_describe_response(_entries_with_policy("delete")), alter=_alter_response())
        with caplog.at_level(logging.INFO, logger="ktables.kafka_table"):
            result = await _check_topic_policy(admin, "t", "compact,delete", "reconcile")
        assert result.outcome == "reconciled"
        assert admin.altered_configs["cleanup.policy"] == "compact,delete"
        retention = [r for r in caplog.records if "retention" in r.message]
        assert len(retention) == 1 and retention[0].levelno == logging.INFO


class TestCheckTopicPolicyUnverifiable:
    @pytest.mark.parametrize("code", [29, 3])
    async def test_inband_describe_error_warn_is_info_unreadable(
        self, code: int, caplog: pytest.LogCaptureFixture
    ) -> None:
        admin = _FakeAdminClient(describe=_describe_response([], code=code, msg="denied"))
        with caplog.at_level(logging.INFO, logger="ktables.kafka_table"):
            result = await _check_topic_policy(admin, "t", "compact", "warn")
        assert result == EnsureTopicResult("unreadable", None)
        assert not any(r.levelno == logging.WARNING for r in caplog.records)
        assert any("could not verify" in r.message and r.levelno == logging.INFO for r in caplog.records)

    async def test_inband_describe_error_raise_propagates(self) -> None:
        admin = _FakeAdminClient(describe=_describe_response([], code=29, msg="denied"))
        with pytest.raises(TopicAuthorizationFailedError):
            await _check_topic_policy(admin, "t", "compact", "raise")

    async def test_inband_describe_error_reconcile_propagates(self) -> None:
        admin = _FakeAdminClient(describe=_describe_response([], code=3, msg="missing"))
        with pytest.raises(UnknownTopicOrPartitionError, match="missing"):
            await _check_topic_policy(admin, "t", "compact", "reconcile")

    async def test_flat_list_describe_response_fails_loudly(self) -> None:
        # A mock returning a bare list of tuples (no .resources wrapper) must FAIL —
        # pinning that the list[Response] -> .resources indirection is load-bearing,
        # so a "simplified" mock can't pass while diverging from the real broker.
        admin = _FakeAdminClient(describe=[(0, "", 2, "t", _entries_with_policy("compact"))])
        with pytest.raises(AttributeError):
            await _check_topic_policy(admin, "t", "compact", "warn")

    @pytest.mark.parametrize("exc", [KafkaError("broker down"), asyncio.TimeoutError()])
    async def test_describe_raises_warn_is_info_unreadable(
        self, exc: Exception, caplog: pytest.LogCaptureFixture
    ) -> None:
        admin = _FakeAdminClient(describe=exc)
        with caplog.at_level(logging.INFO, logger="ktables.kafka_table"):
            result = await _check_topic_policy(admin, "t", "compact", "warn")
        assert result == EnsureTopicResult("unreadable", None)
        assert not any(r.levelno == logging.WARNING for r in caplog.records)

    async def test_describe_raises_raise_propagates(self) -> None:
        admin = _FakeAdminClient(describe=KafkaError("broker down"))
        with pytest.raises(KafkaError):
            await _check_topic_policy(admin, "t", "compact", "raise")

    async def test_non_kafka_error_propagates_even_under_warn(self) -> None:
        # Guard against an over-broad except swallowing real bugs.
        admin = _FakeAdminClient(describe=ValueError("boom"))
        with pytest.raises(ValueError, match="boom"):
            await _check_topic_policy(admin, "t", "compact", "warn")

    async def test_absent_policy_warn_is_info_unreadable(self, caplog: pytest.LogCaptureFixture) -> None:
        admin = _FakeAdminClient(describe=_describe_response([("retention.ms", "1000", False, 1, False, [])]))
        with caplog.at_level(logging.INFO, logger="ktables.kafka_table"):
            result = await _check_topic_policy(admin, "t", "compact", "warn")
        assert result == EnsureTopicResult("unreadable", None)
        assert not any(r.levelno == logging.WARNING for r in caplog.records)

    @pytest.mark.parametrize("action", ["raise", "reconcile"])
    async def test_absent_policy_strict_modes_raise_runtimeerror(self, action: str) -> None:
        admin = _FakeAdminClient(describe=_describe_response([("retention.ms", "1000", False, 1, False, [])]))
        with pytest.raises(RuntimeError, match="could not read cleanup.policy"):
            await _check_topic_policy(admin, "t", "compact", action)

    async def test_unknown_action_raises_and_does_not_reconcile(self) -> None:
        # Defense in depth: an action that somehow reached the dispatch must NOT
        # silently fall through to a broker mutation.
        admin = _FakeAdminClient(describe=_describe_response(_entries_with_policy("delete")), alter=_alter_response())
        with pytest.raises(ValueError, match="unexpected on_policy_mismatch"):
            await _check_topic_policy(admin, "t", "compact", "bogus")
        assert admin.alter_calls == 0


# ---------------------------------------------------------------------------
# ensure_topic — validation, idempotency, error propagation (mocked admin)
# ---------------------------------------------------------------------------
#
# The broker reachable in CI does NOT reliably raise TopicAlreadyExistsError on
# a rapid second create_topics (KRaft acks the create before it propagates), so
# the already-exists / re-raise branches can't be driven by a real double-call —
# they're exercised against a fake admin client instead.


def _install_admin(monkeypatch: pytest.MonkeyPatch, admin: _FakeAdminClient) -> None:
    monkeypatch.setattr("ktables.kafka_table.AIOKafkaAdminClient", lambda **kw: admin)


_EXISTS = [("t", 36, "The topic has already been created")]


class TestEnsureTopic:
    async def test_rejects_non_positive_partitions_and_rf(self) -> None:
        # Validation runs before any admin client is constructed — no broker.
        with pytest.raises(ValueError, match=">= 1"):
            await ensure_topic(BOOTSTRAP, "t", num_partitions=0)
        with pytest.raises(ValueError, match=">= 1"):
            await ensure_topic(BOOTSTRAP, "t", replication_factor=0)

    async def test_rejects_unknown_on_policy_mismatch_before_connecting(self) -> None:
        # The standalone primitive must fail fast on a typo — never fall through
        # to a broker mutation. Validation precedes any admin client.
        with pytest.raises(ValueError, match="on_policy_mismatch"):
            await ensure_topic(BOOTSTRAP, "t", on_policy_mismatch="bogus")

    async def test_created_returns_created_outcome_without_checking(self, monkeypatch: pytest.MonkeyPatch) -> None:
        admin = _FakeAdminClient(create=_create_response([("t", 0, "")]))
        _install_admin(monkeypatch, admin)
        result = await ensure_topic(BOOTSTRAP, "t")
        assert result == EnsureTopicResult("created", None)
        assert admin.describe_calls == 0  # nothing to check on a fresh create
        assert admin.closed

    async def test_ignore_mode_skips_without_describe(self, monkeypatch: pytest.MonkeyPatch) -> None:
        admin = _FakeAdminClient(create=_create_response(_EXISTS))
        _install_admin(monkeypatch, admin)
        result = await ensure_topic(BOOTSTRAP, "t", on_policy_mismatch="ignore")
        assert result == EnsureTopicResult("skipped", None)
        assert admin.describe_calls == 0

    async def test_non_compacting_declared_policy_skips_without_describe(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        admin = _FakeAdminClient(create=_create_response(_EXISTS))
        _install_admin(monkeypatch, admin)
        result = await ensure_topic(BOOTSTRAP, "t", topic_configs={"cleanup.policy": "delete"})
        assert result == EnsureTopicResult("skipped", None)
        assert admin.describe_calls == 0

    async def test_existing_compacting_topic_is_verified(self, monkeypatch: pytest.MonkeyPatch) -> None:
        admin = _FakeAdminClient(
            create=_create_response(_EXISTS),
            describe=_describe_response(_entries_with_policy("compact")),
        )
        _install_admin(monkeypatch, admin)
        result = await ensure_topic(BOOTSTRAP, "t")
        assert result == EnsureTopicResult("verified", "compact")

    async def test_default_warn_on_mismatch_logs_one_warning_no_alter(
        self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        admin = _FakeAdminClient(
            create=_create_response(_EXISTS),
            describe=_describe_response(_entries_with_policy("delete")),
        )
        _install_admin(monkeypatch, admin)
        with caplog.at_level(logging.INFO, logger="ktables.kafka_table"):
            result = await ensure_topic(BOOTSTRAP, "t")  # default on_policy_mismatch="warn"
        assert result == EnsureTopicResult("mismatch", "delete")
        assert admin.alter_calls == 0
        assert len([r for r in caplog.records if r.levelno == logging.WARNING]) == 1

    async def test_reconcile_flips_and_returns_reconciled(self, monkeypatch: pytest.MonkeyPatch) -> None:
        admin = _FakeAdminClient(
            create=_create_response(_EXISTS),
            describe=_describe_response(_entries_with_policy("delete", {"retention.ms": "1234567"})),
            alter=_alter_response(),
        )
        _install_admin(monkeypatch, admin)
        result = await ensure_topic(BOOTSTRAP, "t", on_policy_mismatch="reconcile")
        assert result == EnsureTopicResult("reconciled", "delete")
        assert admin.altered_configs == {"cleanup.policy": "compact", "retention.ms": "1234567"}
        assert admin.closed

    async def test_start_failure_propagates_and_closes(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # Bug #2: start() is inside try/finally, so a failed connect still closes.
        admin = _FakeAdminClient(start_exc=KafkaConnectionError("broker down"))
        _install_admin(monkeypatch, admin)
        with pytest.raises(KafkaConnectionError):
            await ensure_topic(BOOTSTRAP, "t")
        assert admin.closed

    async def test_raise_mode_mismatch_raises_and_closes(self, monkeypatch: pytest.MonkeyPatch) -> None:
        admin = _FakeAdminClient(
            create=_create_response(_EXISTS),
            describe=_describe_response(_entries_with_policy("delete")),
        )
        _install_admin(monkeypatch, admin)
        with pytest.raises(TopicConfigMismatchError):
            await ensure_topic(BOOTSTRAP, "t", on_policy_mismatch="raise")
        assert admin.closed  # finally still runs on the policy-assertion raise path

    async def test_create_error_is_reraised_and_closes(self, monkeypatch: pytest.MonkeyPatch) -> None:
        admin = _FakeAdminClient(create=_create_response([("t", 29, "Not authorized")]))
        _install_admin(monkeypatch, admin)
        with pytest.raises(TopicAuthorizationFailedError, match="Not authorized"):
            await ensure_topic(BOOTSTRAP, "t")
        assert admin.closed


def _make_table(**kw):
    return KafkaTable(bootstrap_servers="b", topic="t", value_decoder=bytes, **kw)


def _make_writer(**kw):
    return KafkaTableWriter(bootstrap_servers="b", topic="t", value_encoder=bytes, **kw)


class TestOnPolicyMismatchValidation:
    @pytest.mark.parametrize("make", [_make_table, _make_writer])
    def test_rejects_unknown_action(self, make) -> None:
        with pytest.raises(ValueError, match="on_policy_mismatch"):
            make(on_policy_mismatch="bogus")

    @pytest.mark.parametrize("make", [_make_table, _make_writer])
    @pytest.mark.parametrize("action", ["raise", "reconcile"])
    def test_active_action_with_ensure_topic_false_raises(self, make, action: str) -> None:
        with pytest.raises(ValueError, match="ensure_topic=True"):
            make(ensure_topic=False, on_policy_mismatch=action)

    @pytest.mark.parametrize("make", [_make_table, _make_writer])
    @pytest.mark.parametrize("action", ["ignore", "warn"])
    def test_passive_action_with_ensure_topic_false_is_allowed(self, make, action: str) -> None:
        make(ensure_topic=False, on_policy_mismatch=action)  # constructs cleanly, no raise


class TestStartForwardsOnPolicyMismatch:
    """start() must thread on_policy_mismatch into ensure_topic — otherwise the
    whole table/writer surface silently degrades to the default. A sentinel raised
    by the patched ensure_topic aborts start() before it touches a broker."""

    class _Sentinel(Exception):
        pass

    async def test_kafka_table_start_forwards(self, monkeypatch: pytest.MonkeyPatch) -> None:
        captured: dict = {}

        async def fake_ensure(*args, **kwargs):
            captured.update(kwargs)
            raise self._Sentinel()

        monkeypatch.setattr("ktables.kafka_table.ensure_topic", fake_ensure)
        table = _make_table(on_policy_mismatch="raise")
        with pytest.raises(self._Sentinel):
            await table.start()
        assert captured["on_policy_mismatch"] == "raise"

    async def test_kafka_table_writer_start_forwards(self, monkeypatch: pytest.MonkeyPatch) -> None:
        captured: dict = {}

        async def fake_ensure(*args, **kwargs):
            captured.update(kwargs)
            raise self._Sentinel()

        monkeypatch.setattr("ktables.kafka_table.ensure_topic", fake_ensure)
        writer = _make_writer(on_policy_mismatch="reconcile")
        with pytest.raises(self._Sentinel):
            await writer.start()
        assert captured["on_policy_mismatch"] == "reconcile"

    async def test_start_tolerates_ensure_topic_result_return(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # start() must DISCARD ensure_topic's EnsureTopicResult, not branch on it.
        # ensure_topic returns the new type; the consumer ctor then aborts via the
        # sentinel — reaching it proves start() accepted the return and proceeded.
        async def fake_ensure(*args, **kwargs):
            return EnsureTopicResult("mismatch", "delete")

        def boom_consumer(*args, **kwargs):
            raise self._Sentinel()

        monkeypatch.setattr("ktables.kafka_table.ensure_topic", fake_ensure)
        monkeypatch.setattr("ktables.kafka_table.AIOKafkaConsumer", boom_consumer)
        table = _make_table(on_policy_mismatch="warn")
        with pytest.raises(self._Sentinel):
            await table.start()


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


class TestApplyHooks:
    """on_set/on_delete fire on _apply's two branches, never on a rejected
    record; _apply does not wrap a raising hook (fail-loud). All broker-free."""

    @staticmethod
    def _bytes_table(**kw: object) -> KafkaTable[bytes]:
        return KafkaTable(bootstrap_servers=BOOTSTRAP, topic="unit.hooks", value_decoder=bytes, **kw)

    def test_on_set_fires_with_key_and_decoded_value(self) -> None:
        sets: list[tuple[str, bytes]] = []
        table = self._bytes_table(on_set=lambda k, v: sets.append((k, v)))
        table._apply(_fake_record(key=b"billing", value=b"payload"))  # type: ignore[arg-type]
        assert sets == [("billing", b"payload")]
        assert table._data["billing"] == b"payload"

    def test_on_delete_fires_with_key_on_tombstone(self) -> None:
        sets: list[tuple[str, bytes]] = []
        deletes: list[str] = []
        table = self._bytes_table(on_set=lambda k, v: sets.append((k, v)), on_delete=deletes.append)
        table._apply(_fake_record(key=b"billing", value=None))  # type: ignore[arg-type]
        assert deletes == ["billing"]
        assert sets == []

    def test_on_set_fires_with_none_value_not_on_delete(self) -> None:
        # Option-B guard: a value_decoder returning None for a NON-null record is
        # an upsert-to-None, never a tombstone — on_set fires, on_delete does not.
        sets: list[tuple[str, object]] = []
        deletes: list[str] = []
        table = KafkaTable(
            bootstrap_servers=BOOTSTRAP, topic="unit.hooks.none", value_decoder=lambda b: None,
            on_set=lambda k, v: sets.append((k, v)), on_delete=deletes.append,
        )
        table._apply(_fake_record(key=b"k", value=b"null"))  # type: ignore[arg-type]
        assert sets == [("k", None)]
        assert deletes == []

    def test_hooks_not_called_on_keyless_record(self) -> None:
        sets: list[object] = []
        deletes: list[object] = []
        table = self._bytes_table(on_set=lambda k, v: sets.append((k, v)), on_delete=deletes.append)
        table._apply(_fake_record(key=None, value=b"x"))  # type: ignore[arg-type]
        assert sets == [] and deletes == []

    def test_hooks_not_called_on_key_decode_error(self) -> None:
        def bad_key(b: bytes) -> str:
            raise ValueError("undecodable key")

        sets: list[object] = []
        deletes: list[object] = []
        table = KafkaTable(
            bootstrap_servers=BOOTSTRAP, topic="unit.hooks.badkey", value_decoder=bytes, key_decoder=bad_key,
            on_set=lambda k, v: sets.append((k, v)), on_delete=deletes.append,
        )
        table._apply(_fake_record(key=b"\xff", value=b"x"))  # type: ignore[arg-type]
        assert sets == [] and deletes == []

    def test_on_set_not_called_on_value_decode_error(self) -> None:
        def bad_value(b: bytes) -> object:
            raise ValueError("undecodable value")

        sets: list[object] = []
        table = KafkaTable(
            bootstrap_servers=BOOTSTRAP, topic="unit.hooks.badval", value_decoder=bad_value,
            on_set=lambda k, v: sets.append((k, v)),
        )
        table._apply(_fake_record(key=b"k", value=b"x"))  # type: ignore[arg-type]
        assert sets == []
        assert table.stats.value_decode_errors == 1

    def test_apply_does_not_wrap_a_raising_hook(self) -> None:
        # The deliberate fail-loud decision: _apply must NOT swallow a hook
        # exception; a genuine observer bug surfaces (and kills the reader).
        def boom(k: str, v: bytes) -> None:
            raise RuntimeError("observer bug")

        table = self._bytes_table(on_set=boom)
        with pytest.raises(RuntimeError, match="observer bug"):
            table._apply(_fake_record(key=b"k", value=b"v"))  # type: ignore[arg-type]


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
    async def test_start_skips_ensure_topic_when_disabled(self, bootstrap, topic: str, table_factory, writer_factory) -> None:
        await ensure_topic(bootstrap, topic)  # pre-create: the table won't ensure it
        table = table_factory(topic, ensure_topic=False)
        async with table:
            assert table.status == "caught_up"

    async def test_start_raises_and_cleans_up_when_no_partitions(self, topic: str, table_factory, writer_factory, monkeypatch: pytest.MonkeyPatch) -> None:
        # An empty assignment after consumer.start() means the topic is missing
        # (or ensure was masked): start() must raise and tear the consumer down.
        monkeypatch.setattr(AIOKafkaConsumer, "assignment", lambda self: set())
        table = table_factory(topic)
        with pytest.raises(RuntimeError, match="no partitions assigned"):
            await table.start()
        assert table._consumer is None  # except-BaseException cleanup ran

    async def test_start_raises_when_reader_dies_during_catchup(self, topic: str, table_factory, writer_factory, monkeypatch: pytest.MonkeyPatch) -> None:
        async def boom(self, *args, **kwargs):
            raise RuntimeError("induced reader death")

        monkeypatch.setattr(AIOKafkaConsumer, "getmany", boom)
        table = table_factory(topic)
        with pytest.raises(RuntimeError, match="died during catch-up"):
            await table.start()
        assert table.status == "failed"

    async def test_start_degrades_when_catchup_times_out(self, topic: str, table_factory, writer_factory, monkeypatch: pytest.MonkeyPatch) -> None:
        # Inflate the start-time end offsets so the gate is never reachable; with
        # a tight catchup_timeout, start() returns DEGRADED rather than crashing.
        real = AIOKafkaConsumer.end_offsets

        async def inflated(self, partitions, *args, **kwargs):
            offsets = await real(self, partitions)
            return {tp: off + 10_000 for tp, off in offsets.items()}

        monkeypatch.setattr(AIOKafkaConsumer, "end_offsets", inflated)
        table = table_factory(topic, catchup_timeout=0.5)
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
    async def test_start_skips_ensure_topic_when_disabled(self, bootstrap, topic: str, table_factory, writer_factory) -> None:
        await ensure_topic(bootstrap, topic)  # pre-create: the writer won't ensure it
        writer = writer_factory(topic, ensure_topic=False)
        async with writer:
            await writer.set("k", make_record("k", 1))


async def test_raising_on_set_hook_kills_the_reader(topic: str, table_factory, writer_factory) -> None:
    # End-to-end: a hook that raises in the reader loop is NOT swallowed. The
    # unit test pins _apply's no-wrap decision; this confirms the consequence —
    # the reader task dies and the failure surfaces as status 'failed'.
    def boom(key: str, value: object) -> None:
        raise RuntimeError("observer bug")

    async with writer_factory(topic) as writer, table_factory(topic, on_set=boom) as table:
        await writer.set("k", make_record("k", 1))
        assert await eventually(lambda: table.status == "failed")
        assert isinstance(table.failure, RuntimeError)


# ---------------------------------------------------------------------------
# on_policy_mismatch — integration (real Redpanda: pre-create, then reconcile)
# ---------------------------------------------------------------------------


async def _precreate_topic(bootstrap: str, topic: str, policy: str, **extra: str) -> None:
    admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap)
    await admin.start()
    try:
        await admin.create_topics(
            [
                NewTopic(
                    name=topic,
                    num_partitions=1,
                    replication_factor=1,
                    topic_configs={"cleanup.policy": policy, **extra},
                )
            ]
        )
    finally:
        await admin.close()


async def _read_config(bootstrap: str, topic: str, key: str) -> str | None:
    admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap)
    await admin.start()
    try:
        resps = await admin.describe_configs([ConfigResource(ConfigResourceType.TOPIC, topic)])
        for r in resps:
            for res in r.resources:
                for entry in res[4]:
                    if entry[0] == key:
                        return entry[1]
    finally:
        await admin.close()
    return None


class TestOnPolicyMismatchIntegration:
    async def test_reconcile_flips_delete_to_compact_preserving_overrides(self, bootstrap, topic) -> None:
        await _precreate_topic(bootstrap, topic, "delete", **{"retention.ms": "1234567", "segment.bytes": "10485760"})
        result = await ensure_topic(bootstrap, topic, on_policy_mismatch="reconcile")
        assert result == EnsureTopicResult("reconciled", "delete")
        assert "compact" in _split_policy(await _read_config(bootstrap, topic, "cleanup.policy"))
        assert await _read_config(bootstrap, topic, "retention.ms") == "1234567"
        assert await _read_config(bootstrap, topic, "segment.bytes") == "10485760"

    async def test_reconcile_is_idempotent(self, bootstrap, topic) -> None:
        await _precreate_topic(bootstrap, topic, "delete")
        first = await ensure_topic(bootstrap, topic, on_policy_mismatch="reconcile")
        assert first.outcome == "reconciled"
        # Already compacting now: a second run verifies and does NOT alter again.
        second = await ensure_topic(bootstrap, topic, on_policy_mismatch="reconcile")
        assert second.outcome == "verified"
        assert "compact" in _split_policy(second.policy)

    async def test_compact_delete_accepted_by_all_active_modes(self, bootstrap, topic) -> None:
        await _precreate_topic(bootstrap, topic, "compact,delete")
        # Pin the broker's literal returned policy string against the set-split logic.
        assert _split_policy(await _read_config(bootstrap, topic, "cleanup.policy")) == {"compact", "delete"}
        for mode in ("warn", "raise", "reconcile"):
            result = await ensure_topic(bootstrap, topic, on_policy_mismatch=mode)
            assert result.outcome == "verified"
            assert "compact" in _split_policy(result.policy)

    async def test_existing_delete_raise_mode_raises(self, bootstrap, topic) -> None:
        await _precreate_topic(bootstrap, topic, "delete")
        with pytest.raises(TopicConfigMismatchError):
            await ensure_topic(bootstrap, topic, on_policy_mismatch="raise")

    async def test_table_and_writer_start_raise_on_mismatch(self, bootstrap, topic) -> None:
        await _precreate_topic(bootstrap, topic, "delete")
        table = KafkaTable(bootstrap_servers=bootstrap, topic=topic, value_decoder=bytes, on_policy_mismatch="raise")
        with pytest.raises(TopicConfigMismatchError):
            await table.start()
        writer = KafkaTableWriter(bootstrap_servers=bootstrap, topic=topic, value_encoder=bytes, on_policy_mismatch="raise")
        with pytest.raises(TopicConfigMismatchError):
            await writer.start()

    async def test_table_starts_under_warn_despite_mismatch(
        self, bootstrap, topic, caplog: pytest.LogCaptureFixture
    ) -> None:
        await _precreate_topic(bootstrap, topic, "delete")
        table = KafkaTable(bootstrap_servers=bootstrap, topic=topic, value_decoder=bytes, on_policy_mismatch="warn")
        with caplog.at_level(logging.WARNING, logger="ktables.kafka_table"):
            async with table:  # start() runs real ensure_topic, logs WARNING, then connects
                assert table.status == "caught_up"
        # warn (not ignore): the mismatch must have produced exactly one WARNING.
        warnings = [r for r in caplog.records if r.levelno == logging.WARNING and topic in r.message]
        assert len(warnings) == 1

    async def test_concurrent_reader_and_writer_reconcile_converges(self, bootstrap, topic) -> None:
        await _precreate_topic(bootstrap, topic, "delete", **{"retention.ms": "1234567"})
        table = KafkaTable(bootstrap_servers=bootstrap, topic=topic, value_decoder=bytes, on_policy_mismatch="reconcile")
        writer = KafkaTableWriter(bootstrap_servers=bootstrap, topic=topic, value_encoder=bytes, on_policy_mismatch="reconcile")
        async with table, writer:
            pass
        assert "compact" in _split_policy(await _read_config(bootstrap, topic, "cleanup.policy"))
        assert await _read_config(bootstrap, topic, "retention.ms") == "1234567"
