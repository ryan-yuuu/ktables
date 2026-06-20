"""Tests for ktables.grouped_table — pure unit tests plus broker-backed integration tests.

Mirrors tests/test_kafka_table.py: integration tests (those using the
``bootstrap``/``topic`` fixtures) run against a real Redpanda broker that
testcontainers spins up automatically (Docker required) and are auto-marked
``integration`` — see tests/conftest.py. The pure-unit tests here (codec,
projection, construction guards) need no broker:

    uv run pytest                       # full suite (needs Docker)
    uv run pytest -m "not integration"  # unit suite only (no Docker)
"""

from __future__ import annotations

import asyncio
import time
import uuid
from collections.abc import Mapping

import pytest
from aiokafka import AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient
from hypothesis import given
from hypothesis import strategies as st
from pydantic import BaseModel

from ktables import KafkaTableWriter
from ktables.grouped_table import (
    DEFAULT_KEY_CODEC,
    CompositeKeyCodec,
    GroupedKafkaTable,
    GroupedKafkaTableWriter,
    LengthPrefixedKeyCodec,
)

# Placeholder address for pure-unit tests that construct but never start() (so
# they never connect). Integration tests bind the live broker via fixtures.
BOOTSTRAP = "localhost:9092"

# UTF-8-encodable text (no lone surrogates): the codec's str↔(group, member)
# layer sits beneath the writer's UTF-8 byte layer.
_TEXT = st.text(st.characters(codec="utf-8"))


async def _eventually(predicate, timeout: float = 5.0, interval: float = 0.01) -> bool:
    """Poll until ``predicate()`` holds (or timeout). Used to await an
    asynchronously-induced reader death without a flaky fixed sleep."""
    deadline = time.perf_counter() + timeout
    while time.perf_counter() < deadline:
        if predicate():
            return True
        await asyncio.sleep(interval)
    return predicate()


def _build_index(flat: Mapping[str, object], codec: CompositeKeyCodec) -> dict[str, dict[str, object]]:
    """Reference projection (the equivalence oracle): the index, by definition, equals this
    pure batch function over the inner table's flat mapping. It is the
    equivalence oracle for the incremental maintainer (TestIndexProjection,
    Phase 3). It stores every decoded key's value **including ``None``** — no
    ``value is None`` special-case — which is exactly why it agrees with the
    two-hook maintainer (``on_set`` stores; only ``on_delete`` removes)."""
    out: dict[str, dict[str, object]] = {}
    for key, value in flat.items():
        try:
            decoded = codec.decode(key)
        except Exception:
            continue
        if decoded is None:
            continue
        group, member = decoded
        out.setdefault(group, {})[member] = value
    return out


class Endpoint(BaseModel):
    """Test payload: a member's own advertised value (value-agnostic feature)."""

    url: str


# ---------------------------------------------------------------------------
# Phase 1 — composite-key codec (pure unit, no broker)
# ---------------------------------------------------------------------------


class TestCodec:
    # Adversarial round-trip vectors: empty fields, separator inside fields,
    # the classic ("1","2:x") vs ("12",":x") near-collision, digits, unicode,
    # control chars, long fields.
    ROUND_TRIP_VECTORS = [
        ("", ""),
        ("", "abc"),
        ("abc", ""),
        ("billing", "host-a"),
        ("a:b", "c:d"),
        (":x", "y"),
        ("1", "2:x"),
        ("12", ":x"),
        ("123", "456"),
        ("héllo", "wörld"),
        ("\x1f\x00", "\t\n"),
        ("x" * 1000, "y" * 1000),
    ]

    @pytest.mark.parametrize("group,member", ROUND_TRIP_VECTORS)
    def test_round_trip(self, group: str, member: str) -> None:
        codec = LengthPrefixedKeyCodec()
        assert codec.decode(codec.encode(group, member)) == (group, member)

    def test_default_key_codec_round_trips(self) -> None:
        assert DEFAULT_KEY_CODEC.decode(DEFAULT_KEY_CODEC.encode("g", "m")) == ("g", "m")

    @given(group=_TEXT, member=_TEXT)
    def test_round_trip_property(self, group: str, member: str) -> None:
        # Round-trip is the discriminating injectivity check: an information-
        # losing codec (e.g. one that drops the length prefix) fails this on the
        # first ambiguous input. Four-independent-draws "injectivity" almost
        # never draws a colliding pair, so it cannot falsify non-injectivity.
        codec = LengthPrefixedKeyCodec()
        assert codec.decode(codec.encode(group, member)) == (group, member)

    @given(pairs=st.lists(st.tuples(_TEXT, _TEXT), max_size=50))
    def test_injective_no_two_distinct_pairs_share_an_encoding(self, pairs: list[tuple[str, str]]) -> None:
        # Encoding a *batch* makes collisions reachable: if any two distinct
        # pairs encoded equal, the encoded set would be smaller than the pair set.
        codec = LengthPrefixedKeyCodec()
        distinct = set(pairs)
        encoded = {codec.encode(group, member) for group, member in distinct}
        assert len(encoded) == len(distinct)

    @pytest.mark.parametrize(
        "key",
        [
            "billing",   # no separator
            "abc:def",   # non-decimal prefix
            "٣:x",       # unicode digit — isascii() rejects (int() would accept)
            "+1:x",      # sign
            "1_0:x",     # underscore — isdigit() rejects (int() would accept "10")
            " 1:x",      # leading space
            "01:xy",     # non-canonical leading zero
            "00:xy",     # non-canonical
            "99:short",  # length prefix exceeds remaining content
            "",          # empty
            ":x",        # empty prefix
        ],
    )
    def test_foreign_key_rejected(self, key: str) -> None:
        assert LengthPrefixedKeyCodec().decode(key) is None

    def test_decode_dos_guard_rejects_huge_prefix(self) -> None:
        # A million-digit prefix is rejected by the length pre-check *before*
        # int() ever parses it — independent of sys.int_max_str_digits.
        assert LengthPrefixedKeyCodec().decode("9" * 1_000_000 + ":x") is None

    def test_empty_strings_are_valid_and_distinct(self) -> None:
        codec = LengthPrefixedKeyCodec()
        assert codec.encode("", "") == "0:"
        assert codec.decode("0:") == ("", "")
        # ("", "") must not collide with ("0", "")
        assert codec.encode("", "") != codec.encode("0", "")


# ---------------------------------------------------------------------------
# Phase 3 — GroupedKafkaTable reader: construction, guards, projection (no broker)
# ---------------------------------------------------------------------------

# Placeholder-bound builder (never started — never connects). Integration tests
# use the broker-bound fixtures below.
def _grouped(topic: str, **kwargs: object) -> GroupedKafkaTable[Endpoint]:
    return GroupedKafkaTable.json(bootstrap_servers=BOOTSTRAP, topic=topic, model=Endpoint, **kwargs)


class TestGroupedConstruction:
    def test_json_builds_without_connecting(self) -> None:
        table = _grouped("unit.grouped.json")
        assert table.status == "unstarted"
        assert table.started is False
        assert table.foreign_key_count == 0

    def test_repr_includes_topic_and_status(self) -> None:
        text = repr(_grouped("unit.grouped.repr"))
        assert "unit.grouped.repr" in text
        assert "unstarted" in text

    @pytest.mark.parametrize("reserved", ["on_set", "on_delete", "key_decoder"])
    def test_rejects_reserved_reader_kwargs(self, reserved: str) -> None:
        # value_decoder is REQUIRED (provided); the reserved kwargs are not
        # parameters and must raise a clean unexpected-keyword TypeError.
        with pytest.raises(TypeError):
            GroupedKafkaTable(
                bootstrap_servers=BOOTSTRAP, topic="unit.grouped", value_decoder=bytes,
                **{reserved: (lambda *a: None)},  # type: ignore[arg-type]
            )

    def test_fetch_max_wait_ms_forwarded_to_inner_table(self) -> None:
        # The grouped table forwards the knob to the KafkaTable it composes.
        default = GroupedKafkaTable(bootstrap_servers=BOOTSTRAP, topic="unit.grouped.fmw", value_decoder=bytes)
        custom = GroupedKafkaTable(bootstrap_servers=BOOTSTRAP, topic="unit.grouped.fmw", value_decoder=bytes, fetch_max_wait_ms=10)
        assert default._table._fetch_max_wait_ms == 500
        assert custom._table._fetch_max_wait_ms == 10


class TestGroupedUnstartedGuards:
    def test_reads_raise_before_start(self) -> None:
        table = _grouped("unit.grouped.guard")
        accesses = [
            lambda: table.get_member("g", "m"),
            lambda: table.has_member("g", "m"),
            lambda: table.members("g"),
            lambda: table.member_count("g"),
            lambda: table.has_group("g"),
            table.groups,
            table.snapshot,
        ]
        for access in accesses:
            with pytest.raises(RuntimeError, match="not started"):
                access()

    def test_introspection_readable_before_start(self) -> None:
        table = _grouped("unit.grouped.introspect")
        assert table.topic == "unit.grouped.introspect"
        assert table.status == "unstarted"
        assert table.started is False
        assert table.foreign_key_count == 0
        assert table.failure is None
        assert table.is_caught_up is False
        assert table.stats.records_applied == 0


class TestIndexProjection:
    """The index maintainers, broker-free: drive _index_set/_index_delete and
    inspect _index directly (no started state, no broker)."""

    @staticmethod
    def _table(**kwargs: object) -> GroupedKafkaTable[object]:
        return GroupedKafkaTable(bootstrap_servers=BOOTSTRAP, topic="unit.grouped", value_decoder=bytes, **kwargs)

    def test_index_set_upserts_into_nested_dict(self) -> None:
        table = self._table()
        c = DEFAULT_KEY_CODEC
        table._index_set(c.encode("billing", "a"), "v1")
        table._index_set(c.encode("billing", "b"), "v2")
        table._index_set(c.encode("search", "c"), "v3")
        assert table._index == {"billing": {"a": "v1", "b": "v2"}, "search": {"c": "v3"}}

    def test_index_set_overwrites_member(self) -> None:
        table = self._table()
        c = DEFAULT_KEY_CODEC
        table._index_set(c.encode("g", "m"), "old")
        table._index_set(c.encode("g", "m"), "new")
        assert table._index == {"g": {"m": "new"}}

    def test_index_set_stores_none_as_a_real_value(self) -> None:
        # None via on_set is a real value, NOT a delete (option-B).
        table = self._table()
        table._index_set(DEFAULT_KEY_CODEC.encode("g", "m"), None)
        assert table._index == {"g": {"m": None}}

    def test_index_delete_removes_member(self) -> None:
        table = self._table()
        c = DEFAULT_KEY_CODEC
        table._index_set(c.encode("g", "m1"), "v1")
        table._index_set(c.encode("g", "m2"), "v2")
        table._index_delete(c.encode("g", "m1"))
        assert table._index == {"g": {"m2": "v2"}}

    def test_index_delete_of_last_member_drops_group(self) -> None:
        table = self._table()
        c = DEFAULT_KEY_CODEC
        table._index_set(c.encode("g", "m"), "v")
        table._index_delete(c.encode("g", "m"))
        assert table._index == {}

    def test_index_delete_of_absent_member_in_empty_index_is_noop(self) -> None:
        table = self._table()
        table._index_delete(DEFAULT_KEY_CODEC.encode("g", "missing"))
        assert table._index == {}

    def test_index_delete_of_absent_member_keeps_a_populated_group(self) -> None:
        # group exists but member doesn't: members.pop is a no-op and the
        # non-empty group survives (distinct from the last-member-drops-group path).
        table = self._table()
        c = DEFAULT_KEY_CODEC
        table._index_set(c.encode("g", "m"), "v")
        table._index_delete(c.encode("g", "other"))
        assert table._index == {"g": {"m": "v"}}

    def test_foreign_key_skipped_and_counted_on_both_paths(self) -> None:
        table = self._table()
        table._index_set("not-our-scheme", "v")  # decode -> None
        table._index_delete("also-foreign")
        assert table._index == {}
        assert table.foreign_key_count == 2

    def test_codec_decode_raising_is_counted_and_skipped(self) -> None:
        class RaisingCodec:
            def encode(self, group: str, member: str) -> str:
                return f"{group}|{member}"

            def decode(self, key: str) -> tuple[str, str] | None:
                raise ValueError("boom")

        table = self._table(key_codec=RaisingCodec())
        table._index_set("anything", "v")
        assert table._index == {}
        assert table.foreign_key_count == 1

    @given(
        events=st.lists(
            st.tuples(
                st.sampled_from(["set", "delete"]),
                st.text(max_size=4),                          # group
                st.text(max_size=4),                          # member
                st.one_of(st.none(), st.text(max_size=4)),    # value (None included)
            ),
            max_size=40,
        )
    )
    def test_incremental_index_matches_batch_oracle(self, events: list[tuple[str, str, str, object]]) -> None:
        # The central invariant: incremental maintenance equals _build_index over
        # the flat state, after EVERY event — including None-valued upserts.
        table = self._table()
        codec = DEFAULT_KEY_CODEC
        flat: dict[str, object] = {}
        for op, group, member, value in events:
            key = codec.encode(group, member)
            if op == "set":
                flat[key] = value
                table._index_set(key, value)
            else:
                flat.pop(key, None)
                table._index_delete(key)
            assert table._index == _build_index(flat, codec)


# ---------------------------------------------------------------------------
# Phase 4 — GroupedKafkaTableWriter: construction, guards, encode wiring (no broker)
# ---------------------------------------------------------------------------


def _grouped_writer(topic: str, **kwargs: object) -> GroupedKafkaTableWriter[Endpoint]:
    return GroupedKafkaTableWriter.json(bootstrap_servers=BOOTSTRAP, topic=topic, model=Endpoint, **kwargs)


class TestGroupedWriter:
    def test_json_builds_and_repr_shows_topic(self) -> None:
        assert "unit.gw.repr" in repr(_grouped_writer("unit.gw.repr"))

    def test_rejects_reserved_key_encoder_kwarg(self) -> None:
        # value_encoder is REQUIRED (provided); key_encoder is reserved (byte
        # layer fixed at UTF-8) and must raise unexpected-keyword TypeError.
        with pytest.raises(TypeError):
            GroupedKafkaTableWriter(
                bootstrap_servers=BOOTSTRAP, topic="unit.gw", value_encoder=bytes,
                key_encoder=(lambda s: s.encode()),  # type: ignore[call-arg]
            )

    async def test_set_before_start_raises(self) -> None:
        with pytest.raises(RuntimeError, match="not started"):
            await _grouped_writer("unit.gw").set("g", "m", Endpoint(url="http://x"))

    async def test_delete_before_start_raises(self) -> None:
        with pytest.raises(RuntimeError, match="not started"):
            await _grouped_writer("unit.gw").delete("g", "m")

    async def test_set_sends_under_the_composite_key(self, monkeypatch: pytest.MonkeyPatch) -> None:
        writer = _grouped_writer("unit.gw")
        calls: list[tuple[str, object]] = []

        async def record(key: str, value: object) -> None:
            calls.append((key, value))

        monkeypatch.setattr(writer._writer, "set", record)
        value = Endpoint(url="http://x")
        await writer.set("billing", "host-a", value)
        assert calls == [(DEFAULT_KEY_CODEC.encode("billing", "host-a"), value)]

    async def test_delete_tombstones_the_composite_key(self, monkeypatch: pytest.MonkeyPatch) -> None:
        writer = _grouped_writer("unit.gw")
        calls: list[str] = []

        async def record(key: str) -> None:
            calls.append(key)

        monkeypatch.setattr(writer._writer, "delete", record)
        await writer.delete("billing", "host-a")
        assert calls == [DEFAULT_KEY_CODEC.encode("billing", "host-a")]


# ---------------------------------------------------------------------------
# Integration tests (broker required; Redpanda auto-started via testcontainers)
# ---------------------------------------------------------------------------


@pytest.fixture
def grouped_table_factory(bootstrap: str):
    """Build an Endpoint GroupedKafkaTable bound to the session Redpanda broker."""

    def _make(topic: str, **kwargs: object) -> GroupedKafkaTable[Endpoint]:
        return GroupedKafkaTable.json(bootstrap_servers=bootstrap, topic=topic, model=Endpoint, **kwargs)

    return _make


@pytest.fixture
def base_writer_factory(bootstrap: str):
    """Seed the topic via a plain KafkaTableWriter using codec-encoded keys, so
    the reader is exercised independently of the grouped writer (Phase 4)."""

    def _make(topic: str, **kwargs: object) -> KafkaTableWriter[Endpoint]:
        return KafkaTableWriter.json(bootstrap_servers=bootstrap, topic=topic, model=Endpoint, **kwargs)

    return _make


@pytest.fixture
async def topic(bootstrap: str):
    name = f"ktables.test.grouped.{uuid.uuid4().hex[:8]}"
    yield name
    admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap)
    await admin.start()
    try:
        await admin.delete_topics([name])
    finally:
        await admin.close()


class TestGroupedReads:
    async def test_reads_reflect_seeded_members_after_barrier(self, topic, grouped_table_factory, base_writer_factory) -> None:
        codec = DEFAULT_KEY_CODEC
        async with base_writer_factory(topic) as writer:
            await writer.set(codec.encode("billing", "a"), Endpoint(url="http://a"))
            await writer.set(codec.encode("billing", "b"), Endpoint(url="http://b"))
            await writer.set(codec.encode("search", "c"), Endpoint(url="http://c"))
        async with grouped_table_factory(topic) as table:
            assert await table.barrier()
            assert await table.wait_until_caught_up()
            assert table.status == "caught_up"
            assert table.get_member("billing", "a") == Endpoint(url="http://a")
            assert table.get_member("billing", "z") is None
            assert table.has_member("billing", "b") is True
            assert table.has_member("billing", "z") is False
            assert table.members("billing") == {"a": Endpoint(url="http://a"), "b": Endpoint(url="http://b")}
            assert table.member_count("billing") == 2
            assert table.member_count("nope") == 0
            assert table.has_group("search") is True
            assert table.has_group("nope") is False
            assert table.groups() == {"billing", "search"}
            assert table.snapshot() == {
                "billing": {"a": Endpoint(url="http://a"), "b": Endpoint(url="http://b")},
                "search": {"c": Endpoint(url="http://c")},
            }

    async def test_members_and_snapshot_copy_containers_but_share_values(self, topic, grouped_table_factory, base_writer_factory) -> None:
        codec = DEFAULT_KEY_CODEC
        async with base_writer_factory(topic) as writer:
            await writer.set(codec.encode("g", "m"), Endpoint(url="http://m"))
        async with grouped_table_factory(topic) as table:
            assert await table.barrier()
            # (a) The returned CONTAINERS are independent copies: mutating the
            # returned dicts must not affect the table's view.
            members = table.members("g")
            members["injected"] = Endpoint(url="http://evil")
            snap = table.snapshot()
            snap["g"]["injected2"] = Endpoint(url="http://evil2")
            snap["new_group"] = {}
            assert table.members("g") == {"m": Endpoint(url="http://m")}
            assert table.groups() == {"g"}
            # (b) The VALUE objects are shared by reference (documented contract):
            # mutating a returned value IS visible on a later read. Locks the
            # zero-copy-of-values semantics against an accidental deep copy.
            member = table.get_member("g", "m")
            assert member is not None
            member.url = "mutated"
            assert table.get_member("g", "m").url == "mutated"


@pytest.fixture
def grouped_writer_factory(bootstrap: str):
    """Build an Endpoint GroupedKafkaTableWriter bound to the session broker."""

    def _make(topic: str, **kwargs: object) -> GroupedKafkaTableWriter[Endpoint]:
        return GroupedKafkaTableWriter.json(bootstrap_servers=bootstrap, topic=topic, model=Endpoint, **kwargs)

    return _make


class TestGroupedWriteRead:
    """End-to-end through the grouped writer + reader against the real broker."""

    async def test_write_barrier_read_round_trip(self, topic, grouped_table_factory, grouped_writer_factory) -> None:
        async with grouped_writer_factory(topic) as writer, grouped_table_factory(topic) as table:
            await writer.set("billing", "a", Endpoint(url="http://a"))
            await writer.set("billing", "b", Endpoint(url="http://b"))
            assert await table.barrier()
            assert table.members("billing") == {"a": Endpoint(url="http://a"), "b": Endpoint(url="http://b")}

    async def test_tombstone_removes_member_then_group(self, topic, grouped_table_factory, grouped_writer_factory) -> None:
        async with grouped_writer_factory(topic) as writer, grouped_table_factory(topic) as table:
            await writer.set("g", "m1", Endpoint(url="http://1"))
            await writer.set("g", "m2", Endpoint(url="http://2"))
            assert await table.barrier()
            assert table.member_count("g") == 2

            await writer.delete("g", "m1")
            assert await table.barrier()
            assert table.members("g") == {"m2": Endpoint(url="http://2")}
            assert table.has_group("g")

            await writer.delete("g", "m2")  # last member → group vanishes
            assert await table.barrier()
            assert not table.has_group("g")
            assert table.groups() == set()

    async def test_same_member_id_across_groups_is_independent(self, topic, grouped_table_factory, grouped_writer_factory) -> None:
        async with grouped_writer_factory(topic) as writer, grouped_table_factory(topic) as table:
            await writer.set("g1", "shared", Endpoint(url="http://1"))
            await writer.set("g2", "shared", Endpoint(url="http://2"))
            assert await table.barrier()
            assert table.get_member("g1", "shared") == Endpoint(url="http://1")
            assert table.get_member("g2", "shared") == Endpoint(url="http://2")

    async def test_two_writers_same_group_do_not_clobber(self, topic, grouped_table_factory, grouped_writer_factory) -> None:
        async with grouped_writer_factory(topic) as w1, grouped_writer_factory(topic) as w2, grouped_table_factory(topic) as table:
            await w1.set("g", "from-w1", Endpoint(url="http://1"))
            await w2.set("g", "from-w2", Endpoint(url="http://2"))
            assert await table.barrier()
            assert table.members("g") == {
                "from-w1": Endpoint(url="http://1"),
                "from-w2": Endpoint(url="http://2"),
            }

    async def test_reads_after_stop_serve_the_frozen_view(self, topic, grouped_table_factory, grouped_writer_factory) -> None:
        async with grouped_writer_factory(topic) as writer:
            await writer.set("g", "m", Endpoint(url="http://m"))
        table = grouped_table_factory(topic)
        await table.start()
        assert await table.barrier()
        assert table.get_member("g", "m") == Endpoint(url="http://m")
        await table.stop()
        # started stays True after stop(): reads serve the frozen view (parity
        # with base KafkaTable).
        assert table.get_member("g", "m") == Endpoint(url="http://m")
        assert table.groups() == {"g"}

    async def test_reads_on_a_failed_table_serve_the_frozen_view(self, topic, grouped_table_factory, grouped_writer_factory, monkeypatch: pytest.MonkeyPatch) -> None:
        async with grouped_writer_factory(topic) as writer:
            await writer.set("g", "m", Endpoint(url="http://m"))
        async with grouped_table_factory(topic) as table:
            assert await table.barrier()
            assert table.get_member("g", "m") == Endpoint(url="http://m")

            # Kill the inner reader: the next poll raises a non-retriable error.
            async def boom(*args: object, **kwargs: object) -> None:
                raise RuntimeError("induced reader death")

            monkeypatch.setattr(table._table._consumer, "getmany", boom)
            assert await _eventually(lambda: table.status == "failed")
            assert table.failure is not None
            # started stays True after death → reads serve the frozen index,
            # they do not raise (the documented reads-on-failed path).
            assert table.get_member("g", "m") == Endpoint(url="http://m")
            assert table.groups() == {"g"}


class TestGroupedForeignKeys:
    """Foreign keys on a shared topic — see the dedicated-topic note in the README."""

    async def test_non_scheme_key_is_skipped_and_counted(self, topic, bootstrap, grouped_table_factory) -> None:
        async with grouped_table_factory(topic) as table:  # ensures topic + starts
            producer = AIOKafkaProducer(bootstrap_servers=bootstrap, enable_idempotence=True)
            await producer.start()
            try:
                await producer.send_and_wait(topic, key=b"not-our-scheme", value=b'{"url":"http://x"}')
            finally:
                await producer.stop()
            assert await table.barrier()
            assert table.groups() == set()
            assert table.foreign_key_count >= 1

    async def test_shape_matching_foreign_key_pollutes_a_group(self, topic, bootstrap, grouped_table_factory) -> None:
        # A foreign key matching the scheme's shape decodes to a real
        # (group, member) and pollutes that group — NOT counted (decode succeeds).
        async with grouped_table_factory(topic) as table:
            producer = AIOKafkaProducer(bootstrap_servers=bootstrap, enable_idempotence=True)
            await producer.start()
            try:
                key = DEFAULT_KEY_CODEC.encode("billing", "x").encode("utf-8")
                await producer.send_and_wait(topic, key=key, value=b'{"url":"http://x"}')
            finally:
                await producer.stop()
            assert await table.barrier()
            assert table.get_member("billing", "x") == Endpoint(url="http://x")
            assert table.foreign_key_count == 0


class TestPublicExports:
    def test_grouped_names_are_exported_from_ktables(self) -> None:
        import ktables

        for name in (
            "GroupedKafkaTable",
            "GroupedKafkaTableWriter",
            "CompositeKeyCodec",
            "LengthPrefixedKeyCodec",
            "DEFAULT_KEY_CODEC",
        ):
            assert hasattr(ktables, name), f"{name} not importable from ktables"
            assert name in ktables.__all__, f"{name} missing from ktables.__all__"
