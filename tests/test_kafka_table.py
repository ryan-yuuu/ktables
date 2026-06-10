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
