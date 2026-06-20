# ktables

[![PyPI](https://img.shields.io/pypi/v/ktables)](https://pypi.org/project/ktables/)
[![CI](https://github.com/ryan-yuuu/ktables/actions/workflows/ci.yml/badge.svg)](https://github.com/ryan-yuuu/ktables/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/ryan-yuuu/ktables/python-coverage-comment-action-data/endpoint.json)](https://github.com/ryan-yuuu/ktables/tree/python-coverage-comment-action-data)
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
- [Performance](#performance)
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

### Grouped tables

A **grouped table** is a nested `{group: {member: value}}` view over a single
compacted topic, where each `(group, member)` pair is its own compaction key.
It is the race-free way to model a **multi-writer registry**: many independent
processes each announce their own entry under a shared group, with no
read-modify-write and no lost updates (every writer owns its own key). The
"collection per group" is reconstructed in memory on read.

```python
from ktables import GroupedKafkaTable, GroupedKafkaTableWriter

# Each process announces its own member under a shared group:
async with GroupedKafkaTableWriter.json(
    bootstrap_servers="localhost:9092", topic="services", model=ServiceRecord
) as writer:
    await writer.set("billing", "host-a", record)   # upsert one member
    ...
    await writer.delete("billing", "host-a")         # tombstone one member

# Any reader sees the whole group:
async with GroupedKafkaTable.json(
    bootstrap_servers="localhost:9092", topic="services", model=ServiceRecord
) as table:
    if await table.barrier():                  # read-your-own-writes, on demand
        table.get_member("billing", "host-a")  # one member          — O(1)
        table.members("billing")               # {member: value} map — O(group)
        table.groups()                         # all group ids
        table.snapshot()                       # whole nested view    — one O(N) pass
```

Reads are O(output). To iterate every group use `snapshot()` — **not**
`for g in table.groups(): table.members(g)`, which re-scans per group.

**Use a dedicated topic.** The composite-key codec is injective, but it cannot
distinguish your keys from a third party's identically-shaped keys on a shared
topic (a foreign key matching the scheme would be read as a real member). Give a
grouped table its own topic.

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

### On-demand read-your-own-writes: `barrier()`

When you need guarantee #2 *now* — e.g. you just published a record and must
read it back consistently — `await table.barrier()` closes the gap:

```python
await writer.set("billing", record)
if await table.barrier():        # waits until the table has caught up
    record = table["billing"]    # guaranteed visible
```

`barrier()` snapshots the topic's end offsets at call time and returns once the
reader has consumed **and applied** every record below them (on the partitions
assigned at the call), so every write acked before the call is then visible. It
returns a `bool`, never raising for environmental conditions: `True` once fresh;
`False` on `timeout` (bounds the whole call), reader death, `stop()` racing the
wait, or a broker error while snapshotting. It raises `RuntimeError` only on
lifecycle misuse (table never started, or already stopped). Runtime partition
expansion is out of scope — the guarantee covers the call-time assignment.

**Barrier latency.** On a table that is actively consuming, `barrier()` resolves
in ~1 ms. On a **quiet** table it is slower — its latency is approximately
`max(fetch_max_wait_ms, poll_timeout_ms)`: the end-offset snapshot waits behind the
consumer's in-flight fetch long-poll (`fetch_max_wait_ms`, default 500 ms) and the
reader then resolves it on its next poll (`poll_timeout_ms`, default 200 ms). To
minimize barrier latency on quiet tables, lower **both** knobs (e.g.
`fetch_max_wait_ms=10, poll_timeout_ms=20` takes the idle barrier from ~500 ms to
~30 ms) — at the cost of more frequent fetches and reader wake-ups (broker traffic
and CPU). Leave the defaults unless fast read-your-own-writes on idle tables matters.

A tombstone is a record with a **null** value (`b""` is data, not a tombstone).
If the background reader dies (non-retriable error, e.g. authorization),
contents freeze at the last applied state: `status` becomes `"failed"` and
`failure` holds the exception — gate liveness decisions on `status`, never on
reads alone. Transient broker outages do not kill the reader; it resumes.

<br>

## API

The complete API reference — every class, method, and module-level export —
lives in **[docs/API.md](docs/API.md)**: `KafkaTable` / `KafkaTableWriter`, the
grouped `GroupedKafkaTable` / `GroupedKafkaTableWriter`, the composite-key codec,
and the module-level helpers (`ensure_topic`, `ViewStats`, `SupportsJsonModel`,
`TableStatus`, `DEFAULT_TOPIC_CONFIGS`).

<br>

## Performance

ktables adds negligible latency over the raw Kafka client — propagation (publish to
read) and write latency (publish to ack) are within measurement noise of bare
`aiokafka`, and reads are in-memory `dict` operations. The one tunable cost is `barrier()` on a *quiet* table; see the
[Consistency contract](#consistency-contract) for the
`max(fetch_max_wait_ms, poll_timeout_ms)` barrier-latency model.

For measured numbers and perf tuning:

- **[Performance report](benchmarks/REPORT.md)** — propagation, `barrier()`, write
  latency/throughput, catch-up, memory, and reader-CPU results, the raw-Kafka
  baseline delta, run-to-run stability, and tuning guidance.
- **[Benchmark suite](benchmarks/README.md)** — how to run it (profiles, the
  testcontainers Redpanda broker) and reproduce the numbers.

<br>

## Contributing

Questions and bug reports are welcome as issues, and PRs are accepted. The
repo is developed with [uv](https://docs.astral.sh/uv/); please run the test
suite before submitting:

```sh
uv run pytest tests
```

The integration tests spin up a Redpanda broker automatically via
[testcontainers](https://testcontainers.com/) (Docker required). Run only the
broker-free unit suite with `uv run pytest -m "not integration"`.

<br>

## License

[MIT](LICENSE)
