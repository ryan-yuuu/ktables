"""Composite-key codec and grouped table for ktables.

A *grouped table* materializes a single compacted topic as a nested
``group → {member → value}`` view, where each ``(group, member)`` pair is an
independent compaction key. That one-key-per-member layout is what makes a
multi-writer registry race-free: no two writers ever share a key, so there is no
read-modify-write and no lost update (the standard alternative — a single key
whose value is the whole collection — loses updates under concurrent writers).
The nested "collection per group" is reconstructed on read, in memory, in each
consumer.

This module's :class:`CompositeKeyCodec` is the injective ``(group, member) ↔
flat key`` mapping the convention rests on, with :class:`LengthPrefixedKeyCodec`
as the collision-proof default. :class:`GroupedKafkaTable` (reader) and
:class:`GroupedKafkaTableWriter` (writer) compose the base
:class:`~ktables.kafka_table.KafkaTable` / ``KafkaTableWriter`` over that codec,
maintaining the nested view via an incremental index. See README.md for usage.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Generic, Protocol, TypeVar

if TYPE_CHECKING:
    # Annotation-only (lazy annotations): no runtime typing_extensions
    # dependency on 3.10. Mirrors kafka_table.py.
    from typing_extensions import Self

from ktables.kafka_table import KafkaTable, KafkaTableWriter, SupportsJsonModel, TableStatus, ViewStats

logger = logging.getLogger(__name__)

V = TypeVar("V")
JsonT = TypeVar("JsonT", bound=SupportsJsonModel)


class CompositeKeyCodec(Protocol):
    """Lossless, injective encoding of a ``(group, member)`` pair into one flat key.

    INJECTIVITY is the load-bearing invariant: distinct ``(group, member)`` pairs
    must never encode to the same key, or one member would compact away another.
    ``decode`` is the inverse on this codec's own outputs and returns ``None`` for
    any key it did not produce — a foreign key on a shared topic, or malformed
    input — so the reader skips it (counted) instead of crashing. Implementations
    must be deterministic and STABLE across processes and versions (a key must
    always encode/partition identically everywhere).
    """

    def encode(self, group: str, member: str) -> str: ...

    def decode(self, key: str) -> tuple[str, str] | None: ...


@dataclass(frozen=True, slots=True)
class LengthPrefixedKeyCodec:
    """Default codec: ``f"{len(group)}{separator}{group}{member}"`` (separator ``:``).

    Injective for all UTF-8-encodable string content, with no reserved characters
    and no escaping: the decimal length prefix is digits-only (``str(int)`` never
    emits a sign, leading zero, or separator), so the FIRST separator
    unambiguously ends it; ``group`` is then delimited by that length — not by any
    character — so any content (the separator, control chars, digits) round-trips.
    Lone surrogates are the one exclusion: they are not UTF-8-encodable and fail
    at the writer's byte layer, not here.
    """

    separator: str = ":"

    def encode(self, group: str, member: str) -> str:
        return f"{len(group)}{self.separator}{group}{member}"

    def decode(self, key: str) -> tuple[str, str] | None:
        head, sep, rest = key.partition(self.separator)
        # Reject foreign keys: no separator, or a non-ASCII-decimal prefix
        # (isascii() guards against unicode digits, signs, and underscores that
        # int() would otherwise accept).
        if not sep or not head.isascii() or not head.isdigit():
            return None
        # Bound the int() parse BEFORE it runs: a real length prefix can never
        # have more digits than the content it measures (+1 covers the "0:"
        # empty-group case). This caps parse cost at O(len(rest)) regardless of
        # sys.int_max_str_digits, so a million-digit foreign prefix can't stall.
        if len(head) > len(rest) + 1:
            return None
        n = int(head)
        # str(n) != head rejects non-canonical prefixes (leading zeros, e.g.
        # "01:xy") that encode() never emits — tightening foreign-key detection.
        if str(n) != head or n > len(rest):
            return None
        return rest[:n], rest[n:]


DEFAULT_KEY_CODEC: CompositeKeyCodec = LengthPrefixedKeyCodec()


class GroupedKafkaTable(Generic[V]):
    """A nested ``group → {member → value}`` view over a compacted topic.

    Composes (never subclasses) :class:`~ktables.kafka_table.KafkaTable`: the
    inner table materializes the flat ``encode(group, member) → value`` topic,
    and ``on_set``/``on_delete`` hooks maintain an incremental nested index so
    grouped reads are O(output) — point lookups O(1), single-group O(|group|),
    the whole view one O(N) ``snapshot()``. All the base table's guarantees
    carry through by delegation: catch-up gating, ``barrier()`` read-your-own-
    writes, ``degraded``/``failed`` status, and poison tolerance.

    Reads are synchronous and return point-in-time copies (value objects shared
    by reference), exactly like ``KafkaTable``. Not thread-safe; single event
    loop only. Reads before ``start()`` raise. See README.md for usage.
    """

    def __init__(
        self,
        *,
        bootstrap_servers: str,
        topic: str,
        value_decoder: Callable[[bytes], V],
        key_codec: CompositeKeyCodec = DEFAULT_KEY_CODEC,
        catchup_timeout: float = 30.0,
        poll_timeout_ms: int = 200,
        ensure_topic: bool = True,
        topic_configs: Mapping[str, str] | None = None,
    ) -> None:
        self._codec = key_codec
        self._index: dict[str, dict[str, V]] = {}
        self._foreign_key_count: int = 0
        # key_decoder is fixed at UTF-8 (not exposed): the codec owns the
        # str↔(group, member) layer. on_set/on_delete are our own index wiring.
        self._table: KafkaTable[V] = KafkaTable(
            bootstrap_servers=bootstrap_servers,
            topic=topic,
            value_decoder=value_decoder,
            catchup_timeout=catchup_timeout,
            poll_timeout_ms=poll_timeout_ms,
            ensure_topic=ensure_topic,
            topic_configs=topic_configs,
            on_set=self._index_set,
            on_delete=self._index_delete,
        )

    @classmethod
    def json(
        cls,
        *,
        bootstrap_servers: str,
        topic: str,
        model: type[JsonT],
        key_codec: CompositeKeyCodec = DEFAULT_KEY_CODEC,
        **kwargs: object,
    ) -> Self:
        """Preset for pydantic-v2-shaped models (anything satisfying
        :class:`~ktables.kafka_table.SupportsJsonModel`)."""
        return cls(  # type: ignore[arg-type]
            bootstrap_servers=bootstrap_servers,
            topic=topic,
            value_decoder=model.model_validate_json,
            key_codec=key_codec,
            **kwargs,
        )

    def __repr__(self) -> str:
        return f"<GroupedKafkaTable topic={self._table.topic!r} status={self._table.status} groups={len(self._index)}>"

    # -- index maintenance (the on_set/on_delete handlers) --------------------

    def _decode_or_count(self, key: str) -> tuple[str, str] | None:
        """Decode the composite key; count + skip foreign keys (``decode → None``,
        or ``decode`` raised). Shared by both hooks so foreign-key tolerance is
        identical on set and delete. (Distinct from the base reader's
        ``key_decode_errors``, which counts undecodable key *bytes* one layer down.)"""
        try:
            decoded = self._codec.decode(key)
        except Exception:  # a codec contract violation is treated as a foreign key
            self._foreign_key_count += 1
            logger.exception("key_codec.decode raised for key=%r on topic=%s; skipped", key, self._table.topic)
            return None
        if decoded is None:  # foreign key (not our scheme)
            self._foreign_key_count += 1
        return decoded

    def _index_set(self, key: str, value: V) -> None:
        decoded = self._decode_or_count(key)
        if decoded is None:
            return
        group, member = decoded
        self._index.setdefault(group, {})[member] = value

    def _index_delete(self, key: str) -> None:
        decoded = self._decode_or_count(key)
        if decoded is None:
            return
        group, member = decoded
        members = self._index.get(group)
        if members is not None:
            members.pop(member, None)
            if not members:  # last member gone → group vanishes
                del self._index[group]

    # -- read API (synchronous, point-in-time copies) -------------------------

    def _require_started(self) -> None:
        if not self._table.started:
            raise RuntimeError("table not started — use 'async with table:' or call start()")

    def get_member(self, group: str, member: str) -> V | None:
        """The value for one member, or None if absent (mirrors ``dict.get``)."""
        self._require_started()
        members = self._index.get(group)
        return None if members is None else members.get(member)

    def has_member(self, group: str, member: str) -> bool:
        self._require_started()
        members = self._index.get(group)
        return members is not None and member in members

    def members(self, group: str) -> dict[str, V]:
        """A point-in-time ``{member: value}`` copy for one group (empty if absent)."""
        self._require_started()
        return dict(self._index.get(group, {}))

    def member_count(self, group: str) -> int:
        self._require_started()
        members = self._index.get(group)
        return 0 if members is None else len(members)

    def has_group(self, group: str) -> bool:
        self._require_started()
        return group in self._index

    def groups(self) -> set[str]:
        self._require_started()
        return set(self._index)

    def snapshot(self) -> dict[str, dict[str, V]]:
        """A point-in-time nested copy of the whole view (one O(N) pass)."""
        self._require_started()
        return {group: dict(members) for group, members in self._index.items()}

    # -- introspection (delegated, plus the grouped-layer counter) ------------

    @property
    def topic(self) -> str:
        return self._table.topic

    @property
    def status(self) -> TableStatus:
        return self._table.status

    @property
    def stats(self) -> ViewStats:
        return self._table.stats

    @property
    def failure(self) -> BaseException | None:
        return self._table.failure

    @property
    def is_caught_up(self) -> bool:
        return self._table.is_caught_up

    @property
    def started(self) -> bool:
        return self._table.started

    @property
    def foreign_key_count(self) -> int:
        """Records skipped because their key is not a usable (group, member) —
        ``decode → None`` or ``decode`` raised. The grouped analog of
        ``ViewStats.key_decode_errors``.

        Detects only *shape-invalid* foreign keys: a foreign key that happens to
        match the codec's shape decodes to a real ``(group, member)`` and silently
        joins that group — it is NOT counted here and is undetectable in-band, so
        run a grouped table on a dedicated topic."""
        return self._foreign_key_count

    # -- lifecycle / freshness (delegated to the inner table) -----------------

    async def start(self) -> None:
        await self._table.start()

    async def stop(self) -> None:
        await self._table.stop()

    async def __aenter__(self) -> Self:
        await self.start()
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.stop()

    async def barrier(self, timeout: float | None = None) -> bool:
        return await self._table.barrier(timeout)

    async def wait_until_caught_up(self, timeout: float | None = None) -> bool:
        return await self._table.wait_until_caught_up(timeout)


class GroupedKafkaTableWriter(Generic[V]):
    """Writer counterpart of :class:`GroupedKafkaTable`: race-free per-member
    upserts and tombstones over the flat composite-key topic.

    Composes (never subclasses) :class:`~ktables.kafka_table.KafkaTableWriter`.
    Its sole logic is composite-key encoding — ``set(group, member, value)``
    publishes ``value`` under ``encode(group, member)`` (a per-member LWW upsert)
    and ``delete(group, member)`` tombstones that one key. Because every
    ``(group, member)`` is a distinct key, independent writers never share a key:
    no read-modify-write, no lost update. Registry-grade durability
    (``enable_idempotence`` ⇒ ``acks=all``) and lifecycle are the inner writer's.
    """

    def __init__(
        self,
        *,
        bootstrap_servers: str,
        topic: str,
        value_encoder: Callable[[V], bytes],
        key_codec: CompositeKeyCodec = DEFAULT_KEY_CODEC,
        ensure_topic: bool = True,
        topic_configs: Mapping[str, str] | None = None,
        enable_idempotence: bool = True,
    ) -> None:
        self._codec = key_codec
        self._topic = topic
        # key_encoder is fixed at UTF-8 (not exposed): the codec owns the
        # str↔(group, member) layer (mirrors GroupedKafkaTable).
        self._writer: KafkaTableWriter[V] = KafkaTableWriter(
            bootstrap_servers=bootstrap_servers,
            topic=topic,
            value_encoder=value_encoder,
            ensure_topic=ensure_topic,
            topic_configs=topic_configs,
            enable_idempotence=enable_idempotence,
        )

    @classmethod
    def json(
        cls,
        *,
        bootstrap_servers: str,
        topic: str,
        model: type[JsonT] | None = None,
        key_codec: CompositeKeyCodec = DEFAULT_KEY_CODEC,
        **kwargs: object,
    ) -> Self:
        """Preset encoding via ``model_dump_json()`` (``model`` is typing-only)."""

        def encode(value: JsonT) -> bytes:
            return value.model_dump_json().encode()

        return cls(  # type: ignore[arg-type]
            bootstrap_servers=bootstrap_servers,
            topic=topic,
            value_encoder=encode,
            key_codec=key_codec,
            **kwargs,
        )

    def __repr__(self) -> str:
        return f"<GroupedKafkaTableWriter topic={self._topic!r}>"

    async def set(self, group: str, member: str, value: V) -> None:
        """Upsert one member (LWW per member); awaits broker ack."""
        await self._writer.set(self._codec.encode(group, member), value)

    async def delete(self, group: str, member: str) -> None:
        """Tombstone one member; awaits broker ack."""
        await self._writer.delete(self._codec.encode(group, member))

    async def start(self) -> None:
        await self._writer.start()

    async def stop(self) -> None:
        await self._writer.stop()

    async def __aenter__(self) -> Self:
        await self.start()
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.stop()
