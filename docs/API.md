# ktables API reference

The complete public API. For background, usage, and the consistency contract,
see the [README](../README.md).

## `KafkaTable[V]` — read-only `Mapping[str, V]`

| Member | Description |
|---|---|
| `KafkaTable(*, bootstrap_servers, topic, value_decoder, key_decoder=utf-8, catchup_timeout=30.0, poll_timeout_ms=200, fetch_max_wait_ms=500, ensure_topic=True, topic_configs=None)` | Construct (does not connect). |
| `KafkaTable.json(*, bootstrap_servers, topic, model, **kwargs)` | Preset wiring `model.model_validate_json` as the decoder. |
| `start()` / `stop()` / `async with` | Lifecycle. `start()` raises on double-start, missing topic, or reader death during catch-up; on catch-up *timeout* it serves degraded. |
| `table[key]`, `key in table`, `iter`, `len`, `.get(key, default=None)` | Mapping reads. Raise `RuntimeError` before `start()`. |
| `snapshot()` | Shallow-copy dict, safe to hold across awaits. |
| `status` | `"unstarted" \| "loading" \| "caught_up" \| "degraded" \| "failed"`. |
| `failure` | Exception that killed the reader, else `None`. |
| `is_caught_up` / `wait_until_caught_up(timeout=None)` | Catch-up gate; the wait returns `False` on timeout or reader death. |
| `barrier(timeout=None)` | On-demand read-your-own-writes: `True` once every write acked before the call is visible; `False` on timeout/reader death/`stop()`/broker-snapshot error. Raises only on lifecycle misuse. On a **quiet** table its latency is ≈ `max(fetch_max_wait_ms, poll_timeout_ms)` (~1 ms while the table is actively consuming); lower both knobs to minimize it. |
| `stats` | Frozen `ViewStats` snapshot (see below). |

Equality is **identity** and instances are hashable: a running table is a
resource handle, not a value.

## `KafkaTableWriter[V]`

| Member | Description |
|---|---|
| `KafkaTableWriter(*, bootstrap_servers, topic, value_encoder, key_encoder=utf-8, ensure_topic=True, topic_configs=None, enable_idempotence=True)` | Construct. Idempotence implies `acks=all` (registry-grade durability); opt out for throwaway data. |
| `KafkaTableWriter.json(*, bootstrap_servers, topic, model=None, **kwargs)` | Preset encoding via `model_dump_json()` (`model` is typing-only). |
| `set(key, value)` | Keyed upsert; awaits broker ack. Re-`set` periodically as a heartbeat. |
| `delete(key)` | Publishes a null-value tombstone; awaits broker ack. |
| `start()` / `stop()` / `async with` | Lifecycle; `set`/`delete` before start raise `RuntimeError`. |

The key encoder must be deterministic and stable across processes — on a
multi-partition topic, per-key ordering holds only if a key always hashes to
the same partition.

## `GroupedKafkaTable[V]` — grouped read view

| Member | Description |
|---|---|
| `GroupedKafkaTable(*, bootstrap_servers, topic, value_decoder, key_codec=DEFAULT_KEY_CODEC, catchup_timeout=30.0, poll_timeout_ms=200, fetch_max_wait_ms=500, ensure_topic=True, topic_configs=None)` | Construct (does not connect). |
| `GroupedKafkaTable.json(*, bootstrap_servers, topic, model, key_codec=DEFAULT_KEY_CODEC, **kwargs)` | Preset wiring `model.model_validate_json`. |
| `get_member(group, member) -> V \| None` / `has_member(group, member) -> bool` | Point lookups — O(1). |
| `members(group) -> dict[str, V]` | One group's `{member: value}` map (a copy) — O(\|group\|). |
| `member_count(group) -> int` / `has_group(group) -> bool` / `groups() -> set[str]` | Group-level reads. |
| `snapshot() -> dict[str, dict[str, V]]` | The whole nested view (a copy) — one O(N) pass. |
| `foreign_key_count` | Count of records skipped because their key isn't a `(group, member)` of this codec. |
| `start()` / `stop()` / `async with` / `barrier()` / `wait_until_caught_up()` / `status` / `stats` / `failure` / `is_caught_up` / `started` / `topic` | Delegated to the inner `KafkaTable` — identical semantics. Data reads raise `RuntimeError` before `start()`. |

## `GroupedKafkaTableWriter[V]`

| Member | Description |
|---|---|
| `GroupedKafkaTableWriter(*, bootstrap_servers, topic, value_encoder, key_codec=DEFAULT_KEY_CODEC, ensure_topic=True, topic_configs=None, enable_idempotence=True)` | Construct. |
| `GroupedKafkaTableWriter.json(*, bootstrap_servers, topic, model=None, key_codec=DEFAULT_KEY_CODEC, **kwargs)` | Preset encoding via `model_dump_json()`. |
| `set(group, member, value)` | Upsert one member (LWW per member); awaits broker ack. |
| `delete(group, member)` | Tombstone one member; awaits broker ack. |
| `start()` / `stop()` / `async with` | Lifecycle. |

The reader and writer fix the key byte-layer at UTF-8 and expose only
`key_codec` (the `(group, member)` ↔ key layer); both run on a dedicated topic.

## Composite-key codec

| Member | Description |
|---|---|
| `CompositeKeyCodec` | Protocol: `encode(group, member) -> str` and `decode(key) -> tuple[str, str] \| None` (`None` = a foreign key, skipped on read). Must be injective and stable across processes. |
| `LengthPrefixedKeyCodec(separator=":")` | Default codec: `f"{len(group)}{separator}{group}{member}"` — injective for all content, no escaping. |
| `DEFAULT_KEY_CODEC` | The default singleton (`LengthPrefixedKeyCodec()`). |

## Module level

| Member | Description |
|---|---|
| `ensure_topic(bootstrap_servers, topic, *, num_partitions=1, replication_factor=1, topic_configs=None) -> bool` | Idempotent explicit create; `True` if this call created it. Defaults are dev-grade — production registries want RF≥3, `min.insync.replicas=2`, `acks=all`. |
| `DEFAULT_TOPIC_CONFIGS` | `{"cleanup.policy": "compact"}` (read-only mapping). |
| `ViewStats` | Frozen counters: `records_applied`, `tombstones_applied`, `keyless_records`, `key_decode_errors`, `value_decode_errors`, `catch_up_seconds`, `replayed_at_catch_up`. |
| `SupportsJsonModel` | Protocol the `.json()` presets require (`model_dump_json` / `model_validate_json`). |
| `TableStatus` | The `status` literal type. |
