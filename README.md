# ktables

[![PyPI](https://img.shields.io/pypi/v/ktables)](https://pypi.org/project/ktables/)
[![Tests](https://github.com/ryan-yuuu/ktables/actions/workflows/test.yml/badge.svg)](https://github.com/ryan-yuuu/ktables/actions/workflows/test.yml)
![Python 3.10+](https://img.shields.io/badge/python-3.10%2B-blue)
[![License: MIT](https://img.shields.io/badge/license-MIT-green)](LICENSE)

Materialize a Kafka topic into an in-memory, compacted dict — a GlobalKTable for asyncio Python.

Every process that opens a `KafkaTable` replays the topic from the beginning
into a local read-only mapping, then keeps consuming for live updates; a
`KafkaTableWriter` maintains the topic with keyed upserts and tombstones.
Built for small, broadly-needed reference data — service registries,
capability advertisements, feature flags, config maps — not for large or
high-churn state.

<br>

## Table of Contents

- [Background](#background)
- [Install](#install)
- [Usage](#usage)
- [Consistency contract](#consistency-contract)
- [API](#api)
- [Contributing](#contributing)
- [License](#license)

<br>

## Background

Kafka Streams (JVM) has two table abstractions over changelog topics: the
partition-sharded `KTable`, where each application instance holds a slice of
the keys, and `GlobalKTable`, where every instance bootstraps and maintains a
full local copy — the right shape for lookup data that any instance may need
at any moment. The Python ecosystem has several maintained stream-processing
frameworks, but all of them implement only the sharded shape, with
framework-owned changelog topics and their own process runtimes. ktables fills
the gap with just the global-table piece, as a plain asyncio library over
`aiokafka`: your topic, your message format, your event loop.

The Kafka semantics the implementation relies on (group-less consumers,
catch-up gating against end offsets, compaction independence) are documented,
with provenance, in the module docstring of
[`kafka_table.py`](./ktables/kafka_table.py).

<br>

## Install

```sh
pip install ktables
```

Requires Python 3.10+. Pydantic is **not** required — the `.json()` presets
accept any class with pydantic-v2's JSON methods.

<br>

## Usage

Maintain a registry from one service:

```python
from ktables import KafkaTableWriter

writer = KafkaTableWriter.json(
    bootstrap_servers="localhost:9092", topic="my.registry", model=ServiceRecord
)
async with writer:
    await writer.set("billing", record)      # upsert (broker-acked)
    ...
    await writer.delete("billing")           # tombstone: removes the key
```

Consume it from any other process:

```python
from ktables import KafkaTable

table = KafkaTable.json(
    bootstrap_servers="localhost:9092", topic="my.registry", model=ServiceRecord
)
async with table:               # replays the topic; returns once caught up
    record = table.get("billing")
    if table.status != "caught_up":   # "degraded": catch-up timed out
        ...
```

Non-pydantic payloads: construct directly with your own codecs —
`KafkaTable(..., value_decoder=bytes_to_value)` /
`KafkaTableWriter(..., value_encoder=value_to_bytes)`.

### Removing a key on clean shutdown

There is deliberately no `delete_on_close` option (shutdown-time deletion is
application policy, and no library can promise it on a crash). Compose it:

```python
async with writer:
    await writer.set(my_key, my_record)
    try:
        ...  # serve
    finally:
        await writer.delete(my_key)   # acked before the producer stops
```

### Locked-down clusters

Both classes ensure their topic exists at start (idempotent create,
compacted). If the application lacks topic-create ACLs, pass
`ensure_topic=False` and create the topic out-of-band (the module-level
`ensure_topic()` function is the deploy-time primitive).

<br>

## Consistency contract

`KafkaTable` is eventually consistent. Precisely:

1. When `start()` / `async with` returns, contents are complete as of the
   topic's end offsets at start time — unless `status == "degraded"`
   (catch-up timed out; data may be partial).
2. Thereafter, updates appear within milliseconds of the broker write — but
   **there is no read-your-own-writes**: after `await writer.set(k, v)`, a
   table in the same process may briefly still return the old value.
3. Contents are stable between your awaits (single event loop; only the
   reader task mutates). Use `snapshot()` for a copy held across awaits.
4. Correctness does not depend on broker-side compaction: last-write-wins
   over the full log yields the same dict; compaction only bounds replay time.

A tombstone is a record with a **null** value (`b""` is data, not a tombstone).
If the background reader dies (non-retriable error, e.g. authorization),
contents freeze at the last applied state: `status` becomes `"failed"` and
`failure` holds the exception — gate liveness decisions on `status`, never on
reads alone. Transient broker outages do not kill the reader; it resumes.

<br>

## API

### `KafkaTable[V]` — read-only `Mapping[str, V]`

| Member | Description |
|---|---|
| `KafkaTable(*, bootstrap_servers, topic, value_decoder, key_decoder=utf-8, catchup_timeout=30.0, poll_timeout_ms=200, ensure_topic=True, topic_configs=None)` | Construct (does not connect). |
| `KafkaTable.json(*, bootstrap_servers, topic, model, **kwargs)` | Preset wiring `model.model_validate_json` as the decoder. |
| `start()` / `stop()` / `async with` | Lifecycle. `start()` raises on double-start, missing topic, or reader death during catch-up; on catch-up *timeout* it serves degraded. |
| `table[key]`, `key in table`, `iter`, `len`, `.get(key, default=None)` | Mapping reads. Raise `RuntimeError` before `start()`. |
| `snapshot()` | Shallow-copy dict, safe to hold across awaits. |
| `status` | `"unstarted" \| "loading" \| "caught_up" \| "degraded" \| "failed"`. |
| `failure` | Exception that killed the reader, else `None`. |
| `is_caught_up` / `wait_until_caught_up(timeout=None)` | Catch-up gate; the wait returns `False` on timeout or reader death. |
| `stats` | Frozen `ViewStats` snapshot (see below). |

Equality is **identity** and instances are hashable: a running table is a
resource handle, not a value.

### `KafkaTableWriter[V]`

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

### Module level

| Member | Description |
|---|---|
| `ensure_topic(bootstrap_servers, topic, *, num_partitions=1, replication_factor=1, topic_configs=None) -> bool` | Idempotent explicit create; `True` if this call created it. Defaults are dev-grade — production registries want RF≥3, `min.insync.replicas=2`, `acks=all`. |
| `DEFAULT_TOPIC_CONFIGS` | `{"cleanup.policy": "compact"}` (read-only mapping). |
| `ViewStats` | Frozen counters: `records_applied`, `tombstones_applied`, `keyless_records`, `key_decode_errors`, `value_decode_errors`, `catch_up_seconds`, `replayed_at_catch_up`. |
| `SupportsJsonModel` | Protocol the `.json()` presets require (`model_dump_json` / `model_validate_json`). |
| `TableStatus` | The `status` literal type. |

<br>

## Contributing

Questions and bug reports are welcome as issues, and PRs are accepted. The
repo is developed with [uv](https://docs.astral.sh/uv/); please run the test
suite before submitting:

```sh
$ uv run pytest tests
```

Unit tests always run; integration tests need a Kafka broker on
`localhost:9092` and skip otherwise
(`docker run -d -p 9092:9092 apache/kafka:3.9.0`).

<br>

## License

[MIT](LICENSE)
