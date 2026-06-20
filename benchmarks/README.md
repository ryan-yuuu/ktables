# ktables benchmark suite

Performance benchmarks for ktables, run against a **real Redpanda broker**
(testcontainers; Docker required). The plan and methodology live in
[`notes/ktables-benchmark-test-plan.md`](../notes/ktables-benchmark-test-plan.md).

These numbers are **comparative** — for regression tracking and tuning guidance
(e.g. "lowering `poll_timeout_ms` cut p99 propagation X→Y"), **not** production
SLAs. See [Caveats](#caveats).

## What it measures

| Metric | Question | Module |
|---|---|---|
| **M1** propagation | publish → visible in a reader's dict | `test_propagation.py` |
| **M2** barrier | how long `barrier()` takes | `test_barrier.py` |
| **M3/M4** write | publish→ack latency; sustained throughput | `test_write.py` |
| **M5** catch-up | `start()` → caught up (time-to-usable view) | `test_catchup.py` |
| **M6** reads | in-memory read cost (validates the O(·) claims) | `test_reads_micro.py` |
| **M7** baseline | the same, through bare aiokafka (ktables as a delta) | in M1/M3 modules |
| **M8** memory | bytes/key held in RAM (flat vs grouped) | `test_memory.py` |
| **M9** reader CPU | idle CPU vs `poll_timeout_ms` | `test_reader_cpu.py` |

## Running

Requires Docker and the `bench` dependency group:

```sh
# Whole macro suite (quick profile — a fast smoke):
uv run --group bench pytest benchmarks/ -m benchmark

# A single metric, with per-cell console output:
uv run --group bench pytest benchmarks/test_propagation.py -s

# The in-memory micro suite (pytest-benchmark; no broker):
uv run --group bench pytest benchmarks/test_reads_micro.py \
    --benchmark-disable-gc --benchmark-json results/micro.json
```

### Profiles

Select with `KTABLES_BENCH_PROFILE` (default `quick`):

- **quick** — reduced cells, ~hundreds–1k samples. Minutes. The PR smoke.
- **full** — the full sweep (poll/payload/partitions/backlog/rates), p99-grade
  sample budgets, multiple repeats. Tens of minutes+.
- **soak** — long-running stability (reserved for `test_memory`/manual runs).

```sh
KTABLES_BENCH_PROFILE=full uv run --group bench pytest benchmarks/ -m benchmark
```

The broker-free harness unit tests run without Docker:

```sh
uv run --group bench pytest benchmarks/test_harness.py
```

## Reading the results

Each macro run writes one artifact to `results/bench-<profile>-<epoch>.json` (an
`env` block + per-cell rows with the full percentile distribution). The micro
suite uses pytest-benchmark's own `--benchmark-json`.

- **Read percentiles, not means.** Latency is right-skewed; p50/p99/p99.9 are the
  point. For the **micro** reads: **min** for the O(1) lookups (least-noisy
  estimate of true cost), **median + IQR** for the O(N) operations (snapshot/codec
  allocate, so GC is part of the real cost).
- **Diff two runs** for regressions:

  ```sh
  uv run --group bench python -m benchmarks.compare results/old.json results/new.json
  ```

  Exits non-zero on any `p50`/`p99` regression beyond `--threshold` (default 20%).

## Findings so far (quick profile, single-node loopback)

- **`barrier()` latency on a quiet table is ≈ `max(fetch_max_wait_ms,
  poll_timeout_ms)`** (empirically confirmed; see `REPORT.md` finding #1): the
  end-offset snapshot waits behind the consumer's fetch long-poll
  (`fetch_max_wait_ms`, default 500 ms) and the reader resolves it on its next poll
  (`poll_timeout_ms`). Idle ≈ 500 ms by default, after-burst/concurrent ≈ 1× poll,
  and **~1 ms under churn**. To minimize it, lower **both** knobs
  (`fetch_max_wait_ms=10, poll_timeout_ms=20` → ~30 ms, a ~20× cut) at the cost of
  more fetches/wakeups (see M9). *(An earlier note here said "idle ≈ 2× poll"; the
  full-profile sweep showed it is `fetch_max_wait_ms`-gated, not poll-scaled.)*
- **Same-process co-location biases propagation in the tail**: at rate 1000/s,
  same-process p99 ≈ 2.6–3.8× the cross-process p99 (p50 nearly equal), and
  same-process saturates at 5000/s while cross-process sustains it. M1 reports both
  topologies so the bias is measured, not assumed.
- **Grouped tables cost ~17–21% more memory** than flat (the nested index duplicates
  keys/structure; values are shared by reference) and replay ~20–25% slower on catch-up.
- **ktables adds ~0.1 ms p50** over a bare aiokafka consumer (M1 vs M7).

## Caveats

- **Single-node Redpanda, RF=1, loopback.** `acks=all` is a local write, so write
  latency **understates** a replicated (RF≥3) cluster, and there is no cross-host
  network.
- **Docker/testcontainers host noise** → high tail variance; pin/quiet the host
  for stable p99.9. The HDR ceiling + dropped-sample counter prevent a stalled run
  from silently improving the reported tail.
- **Topology**: M1/M7 measure both same-process (a service that also reads) and
  cross-process (distinct services); M2/M3/M5/M6/M8/M9 are intrinsically
  in-process.
- **Delete propagation** (M1 delete variant, when added) is same-process only — a
  tombstone carries no value payload to ferry a cross-process send timestamp.

## How it works (methodology)

- **Instrumented via the `on_set` hook**, never read-polling (a busy `get()` loop
  would starve the reader on the single event loop). The send timestamp rides in
  the payload (`CLOCK_MONOTONIC`, comparable across processes on one host).
- **Coordinated-omission correction** (HdrHistogram `record_corrected_value`) on
  open-loop cells, with an **absolute-deadline** generator and a **saturation
  gate** that flags cells where the target rate could not be sustained.
- **Sample reconciliation**: sent records must equal stamped + skipped, or the
  cell fails — no silently-lost samples.
- **HdrHistogram** for all macro percentiles (mergeable; one central range so
  merges never raise; out-of-range samples counted, never dropped silently).
- **Raw-Kafka baseline** built from the *same* consumer/producer kwargs as
  KafkaTable, so the delta is pure ktables overhead.

## Design decisions

- **HdrHistogram** for all macro percentiles — high-dynamic-range, mergeable
  across cells/repeats, with built-in coordinated-omission correction. A single
  central range (1 µs … ~600 s) means merges never raise; out-of-range samples are
  counted, never silently folded into the distribution.
- **pytest-benchmark** for the in-memory micro reads (M6) — its calibration and
  native save/compare fit pure-CPU functions; it is the wrong tool for the async,
  broker-backed latency metrics, which use the HdrHistogram harness instead.
- **A bare-aiokafka baseline (M7), not the JVM OpenMessaging Benchmark /
  `kafka-perf-test`** — keeps the suite in-process and apples-to-apples (same
  client, loop, container) and avoids a cross-runtime dependency in a Python/uv
  project. OMB targets cluster-scale *broker* characterization, out of scope here.
- **No `asv`** — pytest-benchmark's `--benchmark-save`/`--benchmark-compare` covers
  micro regression tracking, and asv's per-commit isolated-env model fights
  testcontainers. Macro tracking is a versioned JSON artifact + `compare.py`.
- **Cross-process writer for the headline propagation-under-load cells** — the
  writer runs in a separate OS process so its producer/Sender does not steal event
  loop time from the reader (a same-process bias that shows in the tail). Each M1
  cell reports both topologies so the bias is measured, not assumed.
