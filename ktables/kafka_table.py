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
   ``table.get(k)`` may return the old value until the broker round trip —
   unless you ``await table.barrier()`` first, the on-demand freshness/RYOW
   primitive (it proves visibility of every record acked before the call, on
   the partitions assigned at call time).

See README.md for usage. Integration tests spin up Redpanda automatically via
testcontainers (Docker required): ``pytest`` — or ``pytest -m "not integration"``
for the broker-free unit suite.
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
    # Annotation-only (lazy annotations): no runtime typing_extensions
    # dependency on 3.10.
    from typing_extensions import Self

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, ConsumerRecord, TopicPartition
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import IllegalStateError, KafkaError, TopicAlreadyExistsError

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
        fetch_max_wait_ms: int = 500,
        ensure_topic: bool = True,
        topic_configs: Mapping[str, str] | None = None,
        on_set: Callable[[str, V], None] | None = None,
        on_delete: Callable[[str], None] | None = None,
    ) -> None:
        if not bootstrap_servers or not topic:
            raise ValueError("bootstrap_servers and topic must be non-empty")
        if catchup_timeout <= 0:
            raise ValueError("catchup_timeout must be > 0")
        if not callable(value_decoder) or not callable(key_decoder):
            raise TypeError("value_decoder and key_decoder must be callable")
        if (on_set is not None and not callable(on_set)) or (on_delete is not None and not callable(on_delete)):
            raise TypeError("on_set and on_delete must be callable")
        self._bootstrap_servers = bootstrap_servers
        self._topic = topic
        self._value_decoder = value_decoder
        self._key_decoder = key_decoder
        # Optional apply observers for derived views (e.g. GroupedKafkaTable):
        # on_set(key, value) fires on an applied value record, on_delete(key) on
        # a tombstone. Synchronous, fired inside _apply after the dict mutation;
        # must not raise (a raise kills the reader). See grouped_table.py.
        self._on_set = on_set
        self._on_delete = on_delete
        self._catchup_timeout = catchup_timeout
        self._poll_timeout_ms = poll_timeout_ms
        # The consumer's fetch long-poll. On a *quiet* table, idle barrier() latency
        # is ~max(fetch_max_wait_ms, poll_timeout_ms): barrier()'s end_offsets() waits
        # behind the in-flight fetch (~fetch_max_wait_ms) and the reader resolves it on
        # its next getmany() (~poll_timeout_ms). Lower BOTH to minimize barrier latency;
        # the cost is more frequent fetches/wakeups (broker traffic + reader CPU). The
        # default mirrors aiokafka's.
        self._fetch_max_wait_ms = fetch_max_wait_ms
        # Idempotent ensure on start: reader or writer may come up first.
        # Disable where the app lacks topic-create ACLs (see ensure_topic()).
        self._ensure_topic = ensure_topic
        self._topic_configs = dict(topic_configs) if topic_configs is not None else dict(DEFAULT_TOPIC_CONFIGS)

        self._live = _LiveStats()
        self._caught_up = asyncio.Event()
        self._failed = asyncio.Event()  # wakes catch-up/barrier waiters on reader death
        self._data: dict[str, V] = {}
        self._task: asyncio.Task[None] | None = None
        self._consumer: AIOKafkaConsumer | None = None
        self._started = False
        self._timed_out = False
        self._failure: BaseException | None = None
        # Pending barrier()s: (call-time end-offset targets, future resolved
        # once positions reach them). Read and resolved only by the reader loop
        # (positions must be read there — see barrier()); each barrier()
        # self-prunes its own entry in a finally.
        self._barriers: list[tuple[dict[TopicPartition, int], asyncio.Future[None]]] = []

    # -- read API (Mapping) ----------------------------------------------------

    # Mapping injects contents-__eq__ and __hash__=None; a table is a
    # resource handle, so restore identity semantics.
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

    @property
    def started(self) -> bool:
        """True once ``start()`` has begun the reader; stays True after ``stop()``
        or reader death (the single lifecycle predicate derived views guard on)."""
        return self._started

    async def wait_until_caught_up(self, timeout: float | None = None) -> bool:
        """Wait for replay to reach the start-time end offsets; True if reached.

        Returns False on timeout OR if the reader has died (check ``status``).
        """
        if self._failure is not None:
            return False
        # Race catch-up against reader death so a death wakes this
        # immediately. No shield: cancelling these waiters is harmless.
        caught = asyncio.ensure_future(self._caught_up.wait())
        failed = asyncio.ensure_future(self._failed.wait())
        try:
            await asyncio.wait({caught, failed}, timeout=timeout, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for fut in (caught, failed):
                fut.cancel()
        return self._caught_up.is_set() and self._failure is None

    async def barrier(self, timeout: float | None = None) -> bool:
        """Wait until the table reflects everything published before this call.

        Snapshots the topic's end offsets now (over the partitions assigned at
        call time) and waits until the reader has consumed AND applied every
        record below them. On ``True``, every record whose publish was
        broker-acked before this call was invoked — on a call-time-assigned
        partition — is visible in the mapping (or counted in ``stats`` as
        keyless/decode-skipped). This is the on-demand read-your-own-writes
        primitive: ``await writer.set(k, v); await table.barrier(); table[k]``
        is then guaranteed.

        ``timeout`` bounds the whole call (the end-offset snapshot plus the
        wait); ``None`` waits indefinitely. Returns ``False`` on timeout,
        reader death, ``stop()`` racing the wait, or a broker error/timeout
        while snapshotting the end offsets — every "couldn't prove it" path is
        a ``False``, never an exception. Raises ``RuntimeError`` only on
        lifecycle misuse (table never started, or already stopped).

        Runtime partition expansion is out of scope: the guarantee covers the
        partitions assigned when ``barrier()`` was called.
        """
        self._require_started()
        if self._consumer is None:
            raise RuntimeError("table stopped — barrier() cannot prove freshness on a stopped table")
        if self._failure is not None:
            return False
        consumer = self._consumer
        loop = asyncio.get_running_loop()
        deadline = None if timeout is None else loop.time() + timeout
        tps = sorted(consumer.assignment(), key=lambda tp: tp.partition)
        # Snapshot the call-time end offsets ourselves. end_offsets is a pure
        # ListOffsets request/response: it touches no fetch buffers, positions,
        # or subscription state, so it is safe to call from this coroutine
        # while the reader task runs getmany on the same consumer. position(),
        # which the reader's catch-up logic mutates state for and which can
        # block ~request_timeout_ms, stays reader-loop-only. Bound the snapshot
        # by the remaining budget (end_offsets is otherwise capped only by the
        # consumer's request_timeout_ms, ~40s, unrelated to our timeout) and
        # map a broker failure to False — an unanswerable ListOffsets is an
        # environmental condition, same family as timeout/reader-death.
        try:
            snapshot_budget = None if deadline is None else max(0.0, deadline - loop.time())
            targets: dict[TopicPartition, int] = await asyncio.wait_for(consumer.end_offsets(tps), timeout=snapshot_budget)
        except (asyncio.TimeoutError, KafkaError):
            logger.warning(
                "barrier() could not snapshot end offsets for topic=%s (broker error/timeout); returning False",
                self._topic,
            )
            return False
        fut: asyncio.Future[None] = loop.create_future()
        self._barriers.append((targets, fut))
        failed = asyncio.ensure_future(self._failed.wait())
        try:
            wait_budget = None if deadline is None else max(0.0, deadline - loop.time())
            await asyncio.wait({fut, failed}, timeout=wait_budget, return_when=asyncio.FIRST_COMPLETED)
        finally:
            failed.cancel()
            if not fut.done():
                fut.cancel()
            # Self-prune: drop our own entry however the await exits (resolved,
            # timed out, caller-cancelled, reader death). Self-pruning keeps the
            # list clean even after a reader death, when the reader can no
            # longer sweep. No await in this block — see the reader loop's
            # invariant comment.
            self._barriers = [(t, f) for t, f in self._barriers if f is not fut]
        return fut.done() and not fut.cancelled() and self._failure is None

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
        # Constructor topic + group_id=None: all partitions are assigned
        # synchronously inside start() — no group, no commits, no rebalance.
        # (Manual assign()+partitions_for_topic() hits a stale partition
        # cache on fresh topics.)
        consumer = AIOKafkaConsumer(
            self._topic,
            bootstrap_servers=self._bootstrap_servers,
            group_id=None,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            fetch_max_wait_ms=self._fetch_max_wait_ms,
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
            # Keep: auto_offset_reset only covers the no-valid-position
            # path; this makes replay-from-zero unconditional.
            await consumer.seek_to_beginning(*tps)
            # Gate target: HWM at start; later records are live updates.
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
                # Logged by _on_reader_done; re-raising here would mask
                # caller errors and skip consumer cleanup.
                pass
        # A cancelled reader task never resolves pending barriers, so wake them
        # ourselves: a timeout=None barrier racing shutdown would otherwise hang
        # forever. Each woken barrier() returns False and self-prunes; clearing
        # here makes the list empty immediately regardless.
        for _, fut in self._barriers:
            if not fut.done():
                fut.cancel()
        self._barriers.clear()
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
            if self._on_delete is not None:
                self._on_delete(key)
            return
        try:
            value = self._value_decoder(record.value)
        except Exception:
            # Poison tolerance: keep the prior value, never kill the reader.
            self._live.value_decode_errors += 1
            logger.exception("undecodable value skipped (key=%s, %s)", key, where)
            return
        # value MAY be None (a decoder that maps bytes to None) — that is a real
        # value, NOT a tombstone: tombstones are record.value is None (above).
        self._data[key] = value
        self._live.records_applied += 1
        if self._on_set is not None:
            self._on_set(key, value)

    async def _run(
        self,
        consumer: AIOKafkaConsumer,
        tps: list[TopicPartition],
        end_offsets: dict[TopicPartition, int],
        started: float,
    ) -> None:
        # Escaping exceptions are captured by _on_reader_done (status
        # "failed"); transient outages don't raise — getmany returns empty.
        while True:
            batches = await consumer.getmany(timeout_ms=self._poll_timeout_ms)
            for records in batches.values():
                for record in records:
                    self._apply(record)
            if not self._caught_up.is_set():
                # aiokafka auto-assigns new partitions on metadata change;
                # pre-latch, extend the gate — post-latch they arrive as
                # live updates.
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
            if self._barriers:
                # Read positions only for partitions that some pending barrier
                # targets, in the same coroutine that runs _apply and after it,
                # so "positions reached => records applied" holds by
                # construction. Assignment is grow-only under group_id=None
                # EXCEPT for transient shrinks: a metadata blip can momentarily
                # empty the assignment, making position() raise
                # IllegalStateError. Catch it and omit that tp (its barrier
                # defers via the .get(tp, -1) default below) rather than letting
                # a blip aiokafka self-heals from kill the reader for good.
                needed = {tp for targets, _ in self._barriers for tp in targets}
                barrier_positions: dict[TopicPartition, int] = {}
                for tp in needed:
                    try:
                        barrier_positions[tp] = await consumer.position(tp)
                    except IllegalStateError:
                        pass  # transiently unassigned: defer barriers needing tp
                # INVARIANT: the authoritative read is the `for` loop below, and
                # there is no await between it and the rebind, so the
                # read-modify-write of self._barriers is atomic w.r.t. the event
                # loop. The awaits above are the only yield points: a barrier
                # appended (or self-pruned) during them is still seen by the for
                # loop, and if its target is absent from barrier_positions it
                # defers via the .get(tp, -1) default below.
                still_pending: list[tuple[dict[TopicPartition, int], asyncio.Future[None]]] = []
                for targets, fut in self._barriers:
                    if fut.cancelled():
                        continue  # timed-out / caller-cancelled: drop, never re-add
                    # .get(tp, -1): a target tp can be absent from
                    # barrier_positions — a barrier appended during the awaits
                    # above, or a tp whose position() raised and was skipped; -1
                    # fails the >= check and defers, never resolving on missing data.
                    if all(barrier_positions.get(tp, -1) >= off for tp, off in targets.items()):
                        if not fut.done():
                            fut.set_result(None)
                    else:
                        still_pending.append((targets, fut))
                self._barriers = still_pending


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
