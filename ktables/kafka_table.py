"""KafkaTable — materialize a Kafka topic into an in-memory dict.

A generic, self-contained "GlobalKTable at home": every process that runs a
table replays the topic from the beginning into a local last-write-wins dict
keyed by the Kafka message key, then keeps consuming for live updates. A
record with a null value is a tombstone and deletes the key.

Verified design facts (traced through aiokafka 0.13.0 source during review):
- ``group_id=None`` + constructor topic: assignment is populated synchronously
  inside ``consumer.start()`` (NoGroupCoordinator) — no race with our seek.
- The catch-up gate (positions vs a start-time ``end_offsets`` snapshot)
  survives compaction holes and transaction control markers: the fetcher
  advances ``position()`` past compacted-away offsets and control batches.
- Partition assignment is NOT fixed at start: NoGroupCoordinator listens for
  metadata changes and auto-assigns new partitions (they replay from earliest).
  While still catching up, the gate extends itself to newly seen partitions;
  after the latch, new partitions' records simply arrive as live updates.
- Broker restarts recover transparently (no group/session to lose). A
  NON-retriable reader error (e.g. authorization) kills the reader task: that
  failure is captured, logged loudly, and surfaced via ``status``/``failure``.

Consistency contract (the four guarantees):
1. After ``start()``/``async with``: complete as of the start-time end offsets
   (unless ``status == "degraded"`` — catch-up timed out, view may be partial).
2. Thereafter: eventually consistent; publish→visible is typically a few ms.
3. Contents are stable between *your* awaits (single event loop; only the
   reader task mutates). ``snapshot()`` for copies you hold across awaits.
4. NO read-your-own-writes: after ``await writer.set(k, v)``, a local
   ``table.get(k)`` may return the old value until the broker round trip.

See README.md for usage. Integration tests need a Kafka broker on
localhost:9092: ``pytest tests``.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import TYPE_CHECKING, Generic, Literal, Protocol, TypeVar

if TYPE_CHECKING:
    # Annotation-only (PEP 563 lazy annotations): no runtime import, so no
    # typing_extensions runtime dependency on Python 3.10.
    from typing_extensions import Self

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, ConsumerRecord, TopicPartition
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import TopicAlreadyExistsError

logger = logging.getLogger(__name__)

V = TypeVar("V")

DEFAULT_TOPIC_CONFIGS: Mapping[str, str] = MappingProxyType({"cleanup.policy": "compact"})

TableStatus = Literal["unstarted", "loading", "caught_up", "degraded", "failed"]
"""``degraded``: catch-up timed out; serving possibly-partial data (loudly logged).
``failed``: the reader task died; contents are frozen at the last applied state."""


class SupportsJsonModel(Protocol):
    """The pydantic-v2 JSON surface the ``.json()`` presets rely on."""

    def model_dump_json(self) -> str: ...

    @classmethod
    def model_validate_json(cls, json_data: str | bytes) -> Self: ...


JsonT = TypeVar("JsonT", bound=SupportsJsonModel)


async def ensure_topic(
    bootstrap_servers: str,
    topic: str,
    *,
    num_partitions: int = 1,
    replication_factor: int = 1,
    topic_configs: Mapping[str, str] | None = None,
) -> bool:
    """Idempotently create ``topic`` with an explicit config.

    Returns True if this call created it, False if it already existed.
    CreateTopics is atomic broker-side, so reader and writer racing to ensure
    the same topic is benign — one wins, the other no-ops. This is the
    EXPLICIT creation path; relying on broker auto-create is the bug (default
    configs: cleanup.policy=delete), which this helper exists to make
    unnecessary.

    Any error other than already-exists (ACL denial, replication factor >
    available brokers, broker unreachable) is logged with context and
    re-raised — callers own retry/permission policy. The defaults
    (1 partition, RF=1) are DEV-grade; production registries want RF>=3 with
    min.insync.replicas=2 alongside an acks=all writer.
    """
    if num_partitions < 1 or replication_factor < 1:
        raise ValueError(f"num_partitions and replication_factor must be >= 1, got {num_partitions}/{replication_factor}")
    admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap_servers)
    await admin.start()
    try:
        await admin.create_topics(
            [
                NewTopic(
                    name=topic,
                    num_partitions=num_partitions,
                    replication_factor=replication_factor,
                    topic_configs=dict(topic_configs) if topic_configs is not None else dict(DEFAULT_TOPIC_CONFIGS),
                )
            ]
        )
        logger.info("created topic %s (partitions=%d, rf=%d)", topic, num_partitions, replication_factor)
        return True
    except TopicAlreadyExistsError:
        logger.debug("topic %s already exists", topic)
        return False
    except Exception:
        logger.exception(
            "ensure_topic failed for topic=%s on %s (not an already-exists). If this process should not create topics, construct with ensure_topic=False.",  # noqa: E501
            topic,
            bootstrap_servers,
        )
        raise
    finally:
        await admin.close()


def _utf8_decode(b: bytes) -> str:
    return b.decode("utf-8")


def _utf8_encode(s: str) -> bytes:
    return s.encode("utf-8")


@dataclass(frozen=True, slots=True)
class ViewStats:
    """An immutable point-in-time snapshot of the reader's counters."""

    records_applied: int = 0
    tombstones_applied: int = 0
    keyless_records: int = 0
    key_decode_errors: int = 0
    value_decode_errors: int = 0
    catch_up_seconds: float | None = None
    replayed_at_catch_up: int = 0


class _LiveStats:
    """Internal mutable counters; ``freeze()`` produces the public snapshot."""

    __slots__ = tuple(ViewStats.__dataclass_fields__)

    def __init__(self) -> None:
        self.records_applied = 0
        self.tombstones_applied = 0
        self.keyless_records = 0
        self.key_decode_errors = 0
        self.value_decode_errors = 0
        self.catch_up_seconds: float | None = None
        self.replayed_at_catch_up = 0

    def freeze(self) -> ViewStats:
        return ViewStats(**{f: getattr(self, f) for f in self.__slots__})


class KafkaTable(Mapping[str, V]):
    """An IMMUTABLE Mapping materialized from a Kafka topic, LWW per key.

    Honest dict-likeness: read-only ``Mapping`` only (``table[k]``, ``k in
    table``, iteration, ``.get``) — deliberately NOT ``MutableMapping``; writes
    go through :class:`KafkaTableWriter`, because there is no
    read-your-own-writes (a just-published record is visible only after the
    broker round trip).

    A running table is a resource handle, not a value: equality is identity
    (two tables with momentarily equal contents are not "the same table").
    Not thread-safe; single event loop only. Reads before ``start()`` raise.
    """

    def __init__(
        self,
        *,
        bootstrap_servers: str,
        topic: str,
        value_decoder: Callable[[bytes], V],
        key_decoder: Callable[[bytes], str] = _utf8_decode,
        catchup_timeout: float = 30.0,
        poll_timeout_ms: int = 200,
        ensure_topic: bool = True,
        topic_configs: Mapping[str, str] | None = None,
    ) -> None:
        if not bootstrap_servers or not topic:
            raise ValueError("bootstrap_servers and topic must be non-empty")
        if catchup_timeout <= 0:
            raise ValueError("catchup_timeout must be > 0")
        if not callable(value_decoder) or not callable(key_decoder):
            raise TypeError("value_decoder and key_decoder must be callable")
        self._bootstrap_servers = bootstrap_servers
        self._topic = topic
        self._value_decoder = value_decoder
        self._key_decoder = key_decoder
        self._catchup_timeout = catchup_timeout
        self._poll_timeout_ms = poll_timeout_ms
        # Explicit ensure on start (idempotent, correct config) — lets reader
        # or writer come up first in dev with no ordering. Disable on
        # locked-down clusters where the app lacks topic-create ACLs; the free
        # module-level ensure_topic() remains the deploy-time primitive.
        self._ensure_topic = ensure_topic
        self._topic_configs = dict(topic_configs) if topic_configs is not None else dict(DEFAULT_TOPIC_CONFIGS)

        self._live = _LiveStats()
        self._caught_up = asyncio.Event()
        self._failed = asyncio.Event()  # wakes catch-up waiters on reader death
        self._data: dict[str, V] = {}
        self._task: asyncio.Task[None] | None = None
        self._consumer: AIOKafkaConsumer | None = None
        self._started = False
        self._timed_out = False
        self._failure: BaseException | None = None

    # -- read API (Mapping) ----------------------------------------------------

    # Mapping injects contents-based __eq__ and sets __hash__ = None; a running
    # table is a resource handle, so restore identity semantics explicitly
    # (two tables with momentarily equal contents are not "the same table").
    def __eq__(self, other: object) -> bool:
        return self is other

    def __hash__(self) -> int:
        return id(self)

    def _require_started(self) -> None:
        if not self._started:
            raise RuntimeError("table not started — use 'async with table:' or call start()")

    def __getitem__(self, key: str) -> V:
        self._require_started()
        return self._data[key]

    def __iter__(self) -> Iterator[str]:
        self._require_started()
        return iter(self._data)

    def __len__(self) -> int:
        self._require_started()
        return len(self._data)

    def snapshot(self) -> dict[str, V]:
        self._require_started()
        return dict(self._data)

    def __repr__(self) -> str:
        return f"<KafkaTable topic={self._topic!r} status={self.status} size={len(self._data)}>"

    @classmethod
    def json(cls, *, bootstrap_servers: str, topic: str, model: type[JsonT], **kwargs: object) -> Self:
        """Preset for pydantic-v2-shaped models (anything satisfying
        :class:`SupportsJsonModel`); pydantic itself is NOT a dependency."""
        return KafkaTable(bootstrap_servers=bootstrap_servers, topic=topic, value_decoder=model.model_validate_json, **kwargs)  # type: ignore[arg-type]

    # -- introspection ---------------------------------------------------------

    @property
    def topic(self) -> str:
        return self._topic

    @property
    def stats(self) -> ViewStats:
        """A frozen snapshot of the reader's counters (the live ones keep moving)."""
        return self._live.freeze()

    @property
    def failure(self) -> BaseException | None:
        """The exception that killed the reader task, if it died. See ``status``."""
        return self._failure

    @property
    def status(self) -> TableStatus:
        if self._failure is not None:
            return "failed"
        if not self._started:
            return "unstarted"
        if self._caught_up.is_set():
            return "caught_up"
        if self._timed_out:
            return "degraded"
        return "loading"

    @property
    def is_caught_up(self) -> bool:
        return self._caught_up.is_set()

    async def wait_until_caught_up(self, timeout: float | None = None) -> bool:
        """Wait for replay to reach the start-time end offsets; True if reached.

        Returns False on timeout OR if the reader has died (check ``status``).
        """
        if self._failure is not None:
            return False
        # Wait on catch-up OR reader death, whichever first — a death must wake
        # this immediately, not burn the rest of the timeout as false-degraded.
        # No shield: cancelling waiters on timeout is correct and harmless —
        # catch-up progress lives in the reader task, not here.
        caught = asyncio.ensure_future(self._caught_up.wait())
        failed = asyncio.ensure_future(self._failed.wait())
        try:
            await asyncio.wait({caught, failed}, timeout=timeout, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for fut in (caught, failed):
                fut.cancel()
        return self._caught_up.is_set() and self._failure is None

    # -- lifecycle -----------------------------------------------------------

    async def start(self) -> None:
        """Start the reader and wait (bounded) for catch-up.

        Raises on double-start, on a missing topic, and if the reader dies
        during catch-up. On catch-up *timeout* the table stays usable and
        keeps consuming (``status == "degraded"``, loudly logged) — mirrors a
        boot-gate policy of serve-degraded rather than crash-loop.

        After a reader-death failure the table counts as started: reads serve
        the frozen last-applied state (possibly empty) rather than raising
        "not started" — gate on ``status``/``failure`` for liveness decisions.
        """
        if self._started:
            raise RuntimeError(f"KafkaTable for topic={self._topic!r} already started")
        if self._ensure_topic:
            await ensure_topic(self._bootstrap_servers, self._topic, topic_configs=self._topic_configs)
        # Pass the topic to the constructor: with group_id=None, aiokafka's
        # NoGroupCoordinator assigns ALL partitions synchronously inside
        # start() — no group coordinator, no commits, no rebalance, full
        # replay every boot. (Manual assign()+partitions_for_topic() is a
        # trap: stale partition-cache for fresh topics.)
        consumer = AIOKafkaConsumer(
            self._topic,
            bootstrap_servers=self._bootstrap_servers,
            group_id=None,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
        )
        await consumer.start()
        self._consumer = consumer
        try:
            tps = sorted(consumer.assignment(), key=lambda tp: tp.partition)
            if not tps:
                raise RuntimeError(
                    f"topic {self._topic!r}: no partitions assigned. Topic missing — or, if ensure_topic ran, "
                    "check the ensure_topic log above for a masked creation error (e.g. ACLs)."
                )
            # Belt-and-suspenders over auto_offset_reset="earliest": makes the
            # replay-from-zero intent explicit and unconditional. Do not
            # "simplify" away — reset-to-earliest only applies on the
            # no-valid-position path.
            await consumer.seek_to_beginning(*tps)
            # Gate target: high-water marks at start. Records past this
            # snapshot are "live updates", not catch-up.
            end_offsets: dict[TopicPartition, int] = await consumer.end_offsets(tps)
        except BaseException:
            await consumer.stop()
            self._consumer = None
            raise
        self._started = True
        task = asyncio.create_task(self._run(consumer, tps, end_offsets, time.perf_counter()), name=f"kafka-table:{self._topic}")
        task.add_done_callback(self._on_reader_done)
        self._task = task
        if not await self.wait_until_caught_up(timeout=self._catchup_timeout):
            if self._failure is not None:
                # Reader died during boot: fail start() loudly, cleaned up.
                failure = self._failure
                await self.stop()
                raise RuntimeError(f"KafkaTable reader for topic={self._topic!r} died during catch-up") from failure
            self._timed_out = True
            logger.error(
                "table for topic=%s NOT caught up after %.1fs (applied=%d so far); continuing DEGRADED — data may be incomplete",
                self._topic,
                self._catchup_timeout,
                self._live.records_applied,
            )

    def _on_reader_done(self, task: asyncio.Task[None]) -> None:
        if task.cancelled():
            return
        exc = task.exception()
        if exc is not None:
            self._failure = exc
            self._failed.set()
            logger.error(
                "KafkaTable reader for topic=%s DIED (%s); table is FROZEN at last applied state (size=%d, status=failed)",
                self._topic,
                type(exc).__name__,
                len(self._data),
                exc_info=exc,
            )

    async def stop(self) -> None:
        task, self._task = self._task, None
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception:
                # Already recorded + logged by _on_reader_done; never let a
                # dead reader turn teardown into a throwing call that masks
                # the caller's own exception and skips consumer cleanup.
                pass
        consumer, self._consumer = self._consumer, None
        if consumer is not None:
            try:
                await consumer.stop()
            except Exception:
                logger.exception("consumer.stop() failed for topic=%s during teardown", self._topic)

    async def __aenter__(self) -> Self:
        await self.start()
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.stop()

    # -- reader loop ----------------------------------------------------------

    def _apply(self, record: ConsumerRecord) -> None:
        where = f"topic={self._topic} partition={record.partition} offset={record.offset}"
        key_bytes = record.key
        if key_bytes is None:
            self._live.keyless_records += 1
            logger.warning("keyless record skipped (%s) — producer is writing unkeyed records to a keyed table topic", where)
            return
        try:
            key = self._key_decoder(key_bytes)
        except Exception:
            self._live.key_decode_errors += 1
            logger.exception("undecodable key skipped (%s)", where)
            return
        if record.value is None:  # null value = tombstone (b"" is NOT a tombstone)
            self._data.pop(key, None)
            self._live.tombstones_applied += 1
            return
        try:
            self._data[key] = self._value_decoder(record.value)
        except Exception:
            # Poison tolerance: a bad record must not kill the table; the
            # previous good value for the key (if any) is retained.
            self._live.value_decode_errors += 1
            logger.exception("undecodable value skipped (key=%s, %s)", key, where)
            return
        self._live.records_applied += 1

    async def _run(
        self,
        consumer: AIOKafkaConsumer,
        tps: list[TopicPartition],
        end_offsets: dict[TopicPartition, int],
        started: float,
    ) -> None:
        # Any exception escaping this loop is captured by _on_reader_done
        # (status="failed", loud log). Transient broker outages do NOT raise —
        # aiokafka retries internally and getmany just returns empty batches.
        while True:
            batches = await consumer.getmany(timeout_ms=self._poll_timeout_ms)
            for records in batches.values():
                for record in records:
                    self._apply(record)
            if not self._caught_up.is_set():
                # NoGroupCoordinator auto-assigns NEW partitions on metadata
                # change (verified in aiokafka source: "Partition changes will
                # be noticed by metadata update and assigned"). While catching
                # up, extend the gate to cover them; after the latch, their
                # replay simply arrives as live updates.
                current = consumer.assignment()
                if current and set(tps) != current:
                    new = sorted(current - set(tps), key=lambda tp: tp.partition)
                    if new:
                        end_offsets.update(await consumer.end_offsets(new))
                        tps = sorted(current, key=lambda tp: tp.partition)
                positions = [await consumer.position(tp) for tp in tps]
                if all(pos >= end_offsets[tp] for pos, tp in zip(positions, tps, strict=True)):
                    self._live.catch_up_seconds = time.perf_counter() - started
                    self._live.replayed_at_catch_up = self._live.records_applied + self._live.tombstones_applied
                    self._caught_up.set()


class KafkaTableWriter(Generic[V]):
    """Writer counterpart of :class:`KafkaTable`: keyed upserts + tombstones.

    ``set(key, value)`` publishes the encoded value under the key (the table's
    LWW upsert); ``delete(key)`` publishes a null-value tombstone. A periodic
    re-``set`` of the same value is a heartbeat — no separate API needed.

    Registry-grade durability by default: ``enable_idempotence=True`` (which
    implies ``acks=all``), so a leader failover can't drop an acked record and
    producer retries can't duplicate or reorder. Opt out for throwaway data.

    The key encoder must be deterministic and stable across processes and
    versions — on a multi-partition topic, per-key LWW ordering holds only if
    a key always hashes to the same partition.
    """

    def __init__(
        self,
        *,
        bootstrap_servers: str,
        topic: str,
        value_encoder: Callable[[V], bytes],
        key_encoder: Callable[[str], bytes] = _utf8_encode,
        ensure_topic: bool = True,
        topic_configs: Mapping[str, str] | None = None,
        enable_idempotence: bool = True,
    ) -> None:
        if not bootstrap_servers or not topic:
            raise ValueError("bootstrap_servers and topic must be non-empty")
        if not callable(value_encoder) or not callable(key_encoder):
            raise TypeError("value_encoder and key_encoder must be callable")
        self._bootstrap_servers = bootstrap_servers
        self._topic = topic
        self._value_encoder = value_encoder
        self._key_encoder = key_encoder
        self._ensure_topic = ensure_topic
        self._topic_configs = dict(topic_configs) if topic_configs is not None else dict(DEFAULT_TOPIC_CONFIGS)
        self._enable_idempotence = enable_idempotence
        self._producer: AIOKafkaProducer | None = None

    def __repr__(self) -> str:
        return f"<KafkaTableWriter topic={self._topic!r} started={self._producer is not None}>"

    @classmethod
    def json(cls, *, bootstrap_servers: str, topic: str, model: type[JsonT] | None = None, **kwargs: object) -> Self:
        """Preset for pydantic-v2-shaped values (encodes via ``model_dump_json``).

        ``model`` is typing/documentation-only; it is not used at runtime.
        """

        def encode(v: JsonT) -> bytes:
            return v.model_dump_json().encode()

        return KafkaTableWriter(bootstrap_servers=bootstrap_servers, topic=topic, value_encoder=encode, **kwargs)  # type: ignore[arg-type]

    async def start(self) -> None:
        if self._producer is not None:
            raise RuntimeError(f"KafkaTableWriter for topic={self._topic!r} already started")
        if self._ensure_topic:
            await ensure_topic(self._bootstrap_servers, self._topic, topic_configs=self._topic_configs)
        producer = AIOKafkaProducer(bootstrap_servers=self._bootstrap_servers, enable_idempotence=self._enable_idempotence)
        await producer.start()
        self._producer = producer

    async def stop(self) -> None:
        producer, self._producer = self._producer, None
        if producer is not None:
            await producer.stop()

    async def __aenter__(self) -> Self:
        await self.start()
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.stop()

    def _require_producer(self) -> AIOKafkaProducer:
        if self._producer is None:
            raise RuntimeError("writer not started — use 'async with writer:' or call start()")
        return self._producer

    async def set(self, key: str, value: V) -> None:
        """Upsert: publish ``value`` under ``key`` (awaits broker ack).

        Note: no read-your-own-writes — a table in this same process sees this
        record only after the broker round trip (~ms), not synchronously.
        """
        await self._require_producer().send_and_wait(self._topic, value=self._value_encoder(value), key=self._key_encoder(key))

    async def delete(self, key: str) -> None:
        """Tombstone: publish a null value under ``key`` (awaits broker ack)."""
        await self._require_producer().send_and_wait(self._topic, value=None, key=self._key_encoder(key))
