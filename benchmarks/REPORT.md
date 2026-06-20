# ktables performance report

A point-in-time run of the `full` benchmark profile. Numbers are **comparative**
(for tuning guidance and regression tracking), **not** production SLAs — see
[Caveats](#caveats--threats-to-validity). To reproduce, see
[How to reproduce](#how-to-reproduce); methodology lives in
[`benchmarks/README.md`](./README.md) and the test plan in
`notes/ktables-benchmark-test-plan.md`.

## Run metadata

| | |
|---|---|
| Profile | `full` |
| Date | 2026-06-19 |
| Duration | 49 min (80 benchmarks) |
| Host | Apple M3 Pro — 11-core (5 performance + 6 efficiency), 36 GB RAM (macOS, arm64) |
| Broker | Redpanda v25.3.15 — **single node, RF=1, loopback** (testcontainers) |
| Runtime | Python 3.12.7 · aiokafka 0.14.0 · ktables 0.3.0 |

## Executive summary

1. **`barrier()` on a quiet table costs ≈ `max(fetch_max_wait_ms, poll_timeout_ms)`** — by default ~500 ms (gated by aiokafka's fetch long-poll), dropping to **~1 ms** under churn. Lowering *both* knobs takes the idle barrier to ~30 ms (proven below; `fetch_max_wait_ms` is now exposed on `KafkaTable`).
2. **ktables adds ~0 ms over raw aiokafka** for propagation and writes (≤0.2 ms p99, only on large payloads). You pay for Kafka, not for ktables.
3. **Same-process co-location inflates the propagation tail 4–5×** under load and caps sustainable throughput; the cross-process topology does not.
4. **Write throughput peaks at in-flight depth ≈ 64 (~18k/s)** and *regresses* beyond it.
5. **Decode cost dominates per-record CPU** and is user-controlled; pydantic-v2 (480k rec/s) beats stdlib `json.loads` (404k rec/s); raw bytes 689k rec/s.
6. **Grouped tables cost ~17–21% more RAM than flat** (the index duplicates keys/structure but *shares* values) — *not* 2×.
7. **Every documented O(·) read complexity holds.**

## Methodology

- **Instrumentation.** Propagation (M1) is timed via the `on_set` hook fired inside
  `_apply` — the instant a record becomes visible to reads — using system-wide
  `CLOCK_MONOTONIC` (comparable across the cross-process writer). `barrier()`, write
  latency, throughput, catch-up, and reader CPU are timed directly around the call.
  Percentiles come from **HdrHistogram** (1 µs … 600 s, 3 significant figures);
  out-of-range samples are counted, never silently dropped.
- **Coordinated omission.** The **open-loop** M1 cells are CO-corrected
  (`record_corrected_value`, expected interval = the send period). **Closed-loop**
  metrics (sequential M1, M2 barrier, M3 write) measure service time and use plain
  recording. Open-loop cells that could not sustain the target rate are flagged
  *saturated* and quarantined out of the trustworthy results.
- **Warm-up + sample budget** (full profile). Warm-up records/iterations are
  discarded before measurement; sent vs stamped samples are reconciled (a lost
  sample fails the cell).

  | metric | warm-up | measured (per cell) | note |
  |---|---:|---|---|
  | M1 propagation / M7 baseline | 500 | 10,000 | sequential, closed-loop |
  | M1 open-loop | — | 5,000 records | CO-corrected |
  | M2 barrier idle / after-burst / churn | 1 | 1,000 / 200 / 1,000 | |
  | M2 concurrent | 1 | 800 (100 rounds × 8) | |
  | M3 write latency | 50 | 5,000 | |
  | M4 throughput | 50 | 20,000 | |
  | M2b experiment | 1 | 30 | |
  | **M5 catch-up · M8 memory · M9 CPU** | — | **1 (single-shot)** | run-to-run noise only |
  | M6 reads | pytest-benchmark | auto-calibrated rounds | min for O(1), median for O(N) |

  Latency metrics (M1–M3) therefore have tight within-run estimates; M5/M8/M9 are a
  single measurement per cell, so their only noise estimate is run-to-run (below).

## Full results

### M1 — propagation latency (publish to visible)

Same-process, sequential writes; stamped via the `on_set` hook (`CLOCK_MONOTONIC`).

| poll (ms) | payload | partitions | p50 | p99 | p99.9 | max |
|---:|---:|---:|---:|---:|---:|---:|
| 20 | 1 KB | 1 | 0.64 | 0.91 | 1.30 | 2.53 |
| 50 | 1 KB | 1 | 0.64 | 0.91 | 1.34 | 2.32 |
| 100 | 1 KB | 1 | 0.65 | 0.95 | 1.47 | 3.21 |
| 200 | 1 KB | 1 | 0.64 | 0.91 | 1.27 | 1.86 |
| 500 | 1 KB | 1 | 0.65 | 0.93 | 1.46 | 2.58 |
| 200 | 100 B | 1 | 0.64 | 0.96 | 1.55 | 4.08 |
| 200 | 16 KB | 1 | 0.69 | 1.47 | 2.68 | 5.22 |
| 200 | 1 KB | 4 | 0.67 | 1.03 | 1.77 | 2.64 |

*(ms.)* Propagation is **flat across `poll_timeout_ms`** — the background fetcher
(`fetch_min_bytes=1`) wakes the poll early on data.

### M7 — raw-aiokafka baseline and ktables overhead

| poll | payload | parts | ktables p50 / p99 | raw p50 / p99 | Δ p50 / Δ p99 |
|---:|---:|---:|---:|---:|---:|
| 20 | 1 KB | 1 | 0.64 / 0.91 | 0.65 / 0.97 | −0.01 / −0.07 |
| 200 | 1 KB | 1 | 0.64 / 0.91 | 0.63 / 0.92 | 0.01 / −0.01 |
| 200 | 16 KB | 1 | 0.69 / 1.47 | 0.71 / 1.26 | −0.02 / 0.20 |
| 200 | 1 KB | 4 | 0.67 / 1.03 | 0.67 / 0.96 | 0.00 / 0.07 |

*(ms; representative cells.)* ktables' overhead over a bare consumer is within
noise at p50, ≤0.2 ms at p99 (only on 16 KB payloads).

### M1 — propagation under sustained open-loop load (same vs cross-process)

| topology | rate | saturated | p50 | p99 | max |
|---|---:|:---:|---:|---:|---:|
| cross-process | 1000/s | no | 0.50 | **2.25** | 5.50 |
| same-process | 1000/s | no | 0.55 | **8.48** | 26.40 |
| cross-process | 5000/s | no | 0.45 | **1.76** | 3.51 |
| same-process | 5000/s | **yes** | 0.67 | 10.73 | 13.85 |

*(ms.)* Same-process p99 is ~3.8× cross-process at 1000/s and **saturates** at
5000/s; cross-process stays tight.

### M2 — barrier() latency

| scenario | poll (ms) | backlog | partitions | p50 | p99 | max |
|---|---:|---:|---:|---:|---:|---:|
| idle | 20 | 0 | 1 | **504.6** | 524.3 | 544.3 |
| idle | 50 | 0 | 1 | **511.2** | 513.8 | 571.9 |
| idle | 100 | 0 | 1 | **505.6** | 604.7 | 609.3 |
| idle | 200 | 0 | 1 | **602.1** | 604.7 | 606.7 |
| idle | 500 | 0 | 1 | **501.3** | 502.0 | 1003.0 |
| after-burst | 200 | 10 / 100 / 1000 | 1 | ~202 | ~203 | ~205 |
| concurrent (×8) | — | — | 1 | 201.7 | 203.9 | 604.2 |
| under churn | — | — | 1 | **0.88** | 1.68 | 6.61 |

*(ms.)* The idle floor is **~500 ms flat across `poll_timeout_ms`** at the default
`fetch_max_wait_ms=500` (see finding #1). Under churn it collapses to ~1 ms.

### M2b — barrier() vs `fetch_max_wait_ms` (controlled experiment)

Idle barrier (and `end_offsets()` in isolation) with `fetch_max_wait_ms` injected
into the consumer; 30 samples/cell.

| `fetch_max_wait_ms` | `poll_timeout_ms` | `end_offsets()` p50 | **barrier p50** | barrier p99 |
|---:|---:|---:|---:|---:|
| **500** (default) | **200** (default) | 311 ms | **695 ms** | 705 ms |
| 100 | 200 | 45 ms | 160 ms | 300 ms |
| 50 | 200 | 23 ms | 179 ms | 201 ms |
| 10 | 200 | 5 ms | 195 ms | 200 ms |
| **10** | **20** | 8 ms | **30 ms** | 36 ms |

`end_offsets()` ≈ **½ × `fetch_max_wait_ms`** (the ListOffsets lands behind the
in-flight Fetch at a random phase of its long-poll). Lowering *both* knobs takes the
idle barrier from ~700 ms to ~30 ms — a ~24× reduction.

### M3 — write latency (publish to ack)

| payload | partitions | idempotence | p50 | p99 | sequential rps |
|---:|---:|:---:|---:|---:|---:|
| 100 B | 1 | on | 0.26 | 1.22 | 2,433 |
| 1 KB | 1 | on | 0.26 | 1.45 | 3,202 |
| 16 KB | 1 | on | 0.91 | 2.15 | 1,001 |
| 1 KB | 4 | on | 0.26 | 0.68 | 3,558 |
| 1 KB | 1 | **off** | 0.26 | 1.03 | 2,872 |
| 1 KB (raw baseline) | 1 | on | 0.26 | 1.40 | — |

*(ms.)* KafkaTableWriter adds ~0 over a bare producer.

### M4 — write throughput (pipelined in-flight depth)

| in-flight depth | throughput (rps) | latency p50 | latency p99 |
|---:|---:|---:|---:|
| 1 | 2,708 | 0.29 | 1.04 |
| 8 | 4,632 | 1.36 | 2.78 |
| **64** | **18,179** | 1.43 | 8.73 |
| 256 | 14,573 | 9.84 | 31.02 |

*(ms.)* Peaks at depth ≈ 64; depth 256 regresses.

### M5 — cold-start catch-up (start() to caught_up)

| kind | N | decoder | total (ms) | replay (ms) | connect (ms) | replay rps |
|---|---:|---|---:|---:|---:|---:|
| flat | 1,000 | raw | 7.1 | 1.3 | 5.8 | 785,546 |
| flat | 10,000 | raw | 20.6 | 14.5 | 6.1 | 689,275 |
| flat | 100,000 | raw | 193.2 | 185.0 | 8.2 | 540,482 |
| flat | 10,000 | json | 34.3 | 24.8 | 9.6 | 403,910 |
| flat | 10,000 | pydantic | 30.2 | 20.8 | 9.3 | 479,869 |
| grouped | 10,000 | raw | 26.4 | 18.4 | 8.0 | 543,301 |
| grouped | 100,000 | raw | 230.5 | 221.1 | 9.4 | 452,227 |

Replay is linear in record count; connect/metadata is a ~6–9 ms fixed cost. Live
catch-up (20k preload + concurrent writes) terminates `caught_up` in 36 ms.

### M8 — memory footprint (bytes per key)

| kind | N | payload | dict (B/key) | tracemalloc (B/key) | RSS Δ (B/key) |
|---|---:|---:|---:|---:|---:|
| flat | 10,000 | 256 B | 355.7 | 418.4 | 252.3 |
| flat | 100,000 | 256 B | 374.3 | 383.0 | 182.8 |
| flat | 10,000 | 1 KB | 1,123.7 | 1,154.3 | 1,700.7 |
| flat | 10,000 | 16 KB | 16,483.7 | 16,489.0 | 15,129.0 |
| grouped | 10,000 | 256 B | 714.3 † | 490.5 | 0.0 |
| grouped | 100,000 | 256 B | 751.7 † | 463.7 | 368.3 |

Flat ≈ payload + ~100 B/key. **† The grouped `dict_bytes` over-counts** (values are
shared by reference between the inner dict and the index; the `getsizeof` walk
counts each twice) — the **tracemalloc** figure is accurate: grouped is **~17–21%
above flat**, not 2×.

### M9 — reader idle CPU vs poll_timeout_ms

| poll (ms) | ~wakeups/s | idle CPU (of one core) |
|---:|---:|---:|
| 20 | 50 | 1.714% |
| 50 | 20 | 1.059% |
| 100 | 10 | 0.651% |
| 200 | 5 | 0.499% |
| 500 | 2 | 0.462% |

### M6 — in-memory read cost (validates O(·) claims)

| operation | sizes and min latency | complexity |
|---|---|---|
| `get_member` / `has_member` | ~120 ns, flat across 1k↔100k | O(1) ✓ |
| `members(group)` | 10: 157 ns · 100: 389 ns · 1000: 2000 ns | O(\|group\|) ✓ |
| `groups()` | 100: 732 ns · 1k: 6.8 µs · 10k: 116 µs | O(#groups) ✓ |
| `snapshot()` flat / grouped | 1k: 2.0/3.3 µs · 100k: 324/530 µs | O(N) ✓ |
| codec `encode` / `decode` | 108 ns / 249 ns | O(key length) |

## Significant findings & what they mean

### 1. Idle `barrier()` ≈ `max(fetch_max_wait_ms, poll_timeout_ms)` — and both are now tunable
The barrier has two serial waits: the end-offset **snapshot** (`end_offsets()`,
which lands behind the consumer's in-flight fetch long-poll, ≈ `fetch_max_wait_ms`)
and the **resolution** (the reader proves it on its next `getmany`, ≈
`poll_timeout_ms`). The controlled experiment (M2b) confirms it: `end_offsets()`
latency tracks ½ × `fetch_max_wait_ms` exactly (311/45/23/5 ms at 500/100/50/10),
and you are gated by whichever knob is larger:

| | barrier p50 |
|---|---|
| default (fmw 500, poll 200) | ~600–700 ms |
| lower only poll (fmw 500, poll 20) | ~500 ms (fmw floor) |
| lower only fmw (fmw 10, poll 200) | ~195 ms (poll floor) |
| **lower both (fmw 10, poll 20)** | **~30 ms** |

**Meaning:** to minimize barrier latency on a *quiet* table, lower **both**
`fetch_max_wait_ms` and `poll_timeout_ms` — ~24× faster (~700 to ~30 ms). The cost is
more frequent fetches and reader wake-ups (broker traffic + CPU; M9). Barriers are
already ~1 ms when the table is actively consuming. **`fetch_max_wait_ms` is now a
`KafkaTable`/`GroupedKafkaTable` constructor parameter** (it was previously pinned to
aiokafka's 500 ms default) — see the README's `barrier()` section. *(An earlier
draft of this report read "idle ≈ fetch_max_wait_ms, poll-independent"; the
experiment refined it to the `max(·)` model above.)*

### 2. ktables' latency overhead over raw Kafka is ~zero
Propagation and write p50 deltas vs a bare aiokafka consumer/producer are within
noise; only ≤0.2 ms at p99 on 16 KB payloads (decode + dict insert). The LWW dict,
the `on_set`/`on_delete` hooks, and the barrier machinery add no meaningful latency.

### 3. Same-process co-location inflates the propagation tail 4–5× under load
At 1000/s, same-process p99 = 8.48 ms vs 2.25 ms cross-process; at 5000/s
same-process **saturates** while cross-process holds p99 = 1.76 ms. A process that
writes heavily *and* hosts a reader contends for one event loop. Real deployments
(writer/readers in different services) behave like the cross-process numbers.

### 4. Write throughput peaks near in-flight depth 64
~18k rps at depth 64 (~6.7× sequential); depth 256 *lowers* throughput and blows
p99 to 31 ms. Batch ~64 in-flight; wider just queues latency.

### 5. Decode cost is the dominant per-record CPU — and is yours to choose
Replay: raw 689k rec/s, pydantic-v2 480k, `json.loads` 404k. Pydantic v2's Rust
validator beats stdlib `json.loads`. Catch-up is otherwise linear in record count
with a ~6–9 ms fixed connect cost.

### 6. Grouped tables cost ~20% more memory, not 2×
The clean `dict_bytes` walk double-counts the value objects (shared by reference
between `_data` and `_index`); the accurate tracemalloc figure is ~17–21% over flat.
The grouped index duplicates **keys and structure**, not values. *(The grouped
`dict_bytes` metric is buggy for this reason; the tracemalloc figure is
trustworthy.)*

### 7. The poll latency/CPU trade-off, quantified
Idle reader CPU runs 0.46% to 1.71% of a core as `poll_timeout_ms` drops 500 to 20 ms.
Lowering `poll_timeout_ms` helps the *resolution* half of the barrier (and
propagation-under-load); lowering `fetch_max_wait_ms` helps the *snapshot* half.

### 8. Documented read complexities all hold (M6).

## Recommendations / tuning guidance

- **Fast `barrier()` on quiet tables**: set both `fetch_max_wait_ms` and
  `poll_timeout_ms` low (e.g. 10 / 20 ms gives a ~30 ms idle barrier), weighing the
  fetch/CPU cost. Both are constructor parameters.
- **Write throughput**: pipeline ~64 writes in flight; don't exceed it.
- **Propagation under your own load**: don't co-locate a heavy writer with a reader
  in the same process if tail latency matters.
- **Catch-up / memory at scale**: prefer a cheap decoder; budget ~(payload + 100 B)/key
  flat and ~20% more for grouped.

## Run-to-run stability (measurement noise)

The result tables above are a single `full` run. To bound run-to-run noise, the
table below is the dispersion of repeated **`quick`-profile** runs (11 runs). Quick
uses smaller per-cell samples, so these CVs **upper-bound** the full-profile latency
noise; the single-shot metrics (M5/M8/M9) carry the run-to-run noise shown directly.

| metric (cell) | runs | median | range | CV% |
|---|---:|---:|---:|---:|
| barrier idle (poll 200) p50 | 3 | 402.9 ms | 402.7–403.2 | **0%** |
| barrier after-burst (B=100) p50 | 3 | 201.9 ms | 201.7–202.1 | **0%** |
| barrier concurrent p50 | 3 | 201.5 ms | 201.3–201.5 | **0%** |
| barrier churn p50 | 3 | 1.08 ms | 1.02–1.26 | 9% |
| propagation raw-baseline p50 | 4 | 0.67 ms | 0.65–0.76 | 6% |
| propagation same-process p50 | 5 | 0.69 ms | 0.68–0.88 | 11% |
| propagation open-loop p50 | 5 | 0.55 ms | 0.47–0.72 | 9–16% |
| write latency p50 | 4 | 0.38 ms | 0.27–0.63 | **36%** |
| write throughput latency p50 | 4 | 2.49 ms | 1.47–2.70 | 22% |
| catch-up flat 1k (total) | 4 | 10.6 ms | 5.6–14.1 | 30% |
| catch-up grouped 1k (total) | 4 | 9.9 ms | 8.9–13.1 | 15% |
| memory dict B/key (flat & grouped) | 2 | — | exact | **0%** |
| reader idle CPU | 3 | — | tiny abs. | 35–38% |

**Takeaways:** timer-gated metrics (barrier idle/after-burst/concurrent) and the
exact `dict_bytes` memory walk are **deterministic** (CV ~0%) — trust them tightly.
The sub-millisecond latencies (propagation, write) and tiny CPU fractions carry high
*relative* jitter only because the absolute values sit near the measurement floor
(±0.1 ms on a 0.3 ms write is ±33%, though the absolute noise is small) — read their
p50s as ±10–35% and lean on p99 trends and the cross-/same-process and
ktables-vs-raw **deltas**. Single-shot catch-up at small N is ±30% run-to-run
(connect/metadata variance dominates). *(Visualizations are not included — this
report is tables-only.)*

## Caveats & threats to validity

- **Single-node Redpanda, RF=1, loopback.** Write/propagation numbers **understate**
  a replicated cluster; treat them as a floor. The **relationships and deltas** are
  what transfer.
- **Grouped `dict_bytes` over-counts shared values** (finding #6) — use tracemalloc.
- **`poll=500` idle barrier hit a 1003 ms max** — occasionally a barrier spans two
  fetch cycles.

## How to reproduce

```sh
# full profile (~50 min):
KTABLES_BENCH_PROFILE=full uv run --group bench pytest benchmarks/ -m benchmark \
    --benchmark-disable-gc --benchmark-json=results/micro.json

# the barrier-vs-fetch_max_wait experiment (M2b):
uv run --group bench pytest benchmarks/test_experiment_barrier_fetch_wait.py -s -q

# diff two runs:
uv run --group bench python -m benchmarks.compare results/old.json results/new.json
```
