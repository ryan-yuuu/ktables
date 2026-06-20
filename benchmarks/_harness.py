"""Benchmark harness — pure-logic building blocks for the ktables benchmark suite.

Broker-backed drivers live in the ``test_*`` modules; this module holds the
reusable, mostly broker-free machinery they share (latency recording, env
capture, artifact I/O, load scheduling). See ``notes/ktables-benchmark-test-plan.md``.
"""

from __future__ import annotations

import asyncio
import json
import platform
import struct
import time
from collections.abc import Awaitable, Callable, Mapping, Sequence
from contextlib import suppress
from dataclasses import asdict, dataclass
from importlib.metadata import version as _pkg_version
from pathlib import Path
from types import MappingProxyType
from typing import Any

import aiokafka
import cpuinfo
from aiokafka import AIOKafkaConsumer
from hdrh.histogram import HdrHistogram

from ktables import KafkaTable

# One central HDR range for every recorder so merges (`add`) never raise on a
# range mismatch. 1 us .. 600 s nominal at 3 significant figures; HdrHistogram
# rounds the ceiling up to a bucket boundary (~1000 s effective), and anything
# beyond that is reported as a dropped sample (see LatencyRecorder.record).
_LOWEST_US = 1
_HIGHEST_US = 600_000_000
_SIG_FIGS = 3
_US_PER_SEC = 1_000_000


@dataclass(frozen=True, slots=True)
class LatencySummary:
    """An immutable percentile summary of a latency distribution, in microseconds."""

    count: int
    dropped: int
    min_us: int
    p50_us: int
    p90_us: int
    p95_us: int
    p99_us: int
    p999_us: int
    max_us: int
    mean_us: float
    stddev_us: float


class LatencyRecorder:
    """Records non-negative latencies (seconds in, microseconds stored) into an
    HdrHistogram and produces a :class:`LatencySummary`.

    ``record`` is for closed-loop service-time samples; ``record_open_loop``
    applies HdrHistogram's coordinated-omission correction for open-loop, fixed-rate
    measurement. Both return ``False`` (and increment ``dropped``) when a value
    exceeds the histogram's trackable range, so a silently-lost tail sample
    surfaces as a counter instead of quietly improving the percentiles.
    """

    def __init__(self) -> None:
        self._h = HdrHistogram(_LOWEST_US, _HIGHEST_US, _SIG_FIGS)
        self._dropped = 0

    @staticmethod
    def _to_us(seconds: float) -> int:
        if seconds < 0:
            raise ValueError(f"latency must be non-negative, got {seconds}")
        # Floor sub-microsecond samples at 1 us (HDR's lowest trackable value).
        return max(1, round(seconds * _US_PER_SEC))

    def record(self, seconds: float) -> bool:
        """Record one closed-loop latency sample. Returns False if out of range."""
        recorded = self._h.record_value(self._to_us(seconds))
        if not recorded:
            self._dropped += 1
        return recorded

    def record_open_loop(self, seconds: float, expected_interval: float) -> bool:
        """Record one open-loop sample with coordinated-omission correction.

        ``expected_interval`` is the intended (open-loop) send period in seconds;
        HdrHistogram back-fills synthetic samples when ``seconds`` exceeds it.
        """
        if expected_interval <= 0:
            raise ValueError(f"expected_interval must be > 0, got {expected_interval}")
        recorded = self._h.record_corrected_value(self._to_us(seconds), self._to_us(expected_interval))
        if not recorded:
            self._dropped += 1
        return recorded

    def merge(self, other: LatencyRecorder) -> None:
        """Fold another recorder's samples (and dropped count) into this one."""
        self._h.add(other._h)
        self._dropped += other._dropped

    def summary(self) -> LatencySummary:
        count = self._h.get_total_count()
        if count == 0:
            return LatencySummary(
                count=0, dropped=self._dropped, min_us=0, p50_us=0, p90_us=0, p95_us=0,
                p99_us=0, p999_us=0, max_us=0, mean_us=0.0, stddev_us=0.0,
            )
        pct = self._h.get_value_at_percentile
        return LatencySummary(
            count=count,
            dropped=self._dropped,
            min_us=self._h.get_min_value(),
            p50_us=pct(50.0),
            p90_us=pct(90.0),
            p95_us=pct(95.0),
            p99_us=pct(99.0),
            p999_us=pct(99.9),
            max_us=self._h.get_max_value(),
            mean_us=self._h.get_mean_value(),
            stddev_us=self._h.get_stddev(),
        )


# -- sample accounting -------------------------------------------------------


class SampleAccountingError(AssertionError):
    """Raised when sent records do not equal stamped (applied) + skipped records —
    i.e. a sample went silently missing, which would bias the distribution."""


def assert_samples_accounted(*, sent: int, stamped: int, skipped: int = 0) -> None:
    """Fail the cell unless every sent record was either stamped (fired
    ``on_set``/``on_delete``) or skipped (keyless / decode error)."""
    unaccounted = sent - stamped - skipped
    if unaccounted != 0:
        raise SampleAccountingError(
            f"sample accounting mismatch: sent={sent} stamped={stamped} skipped={skipped} unaccounted={unaccounted}"
        )


# -- open-loop saturation gate -----------------------------------------------


@dataclass(frozen=True, slots=True)
class SaturationVerdict:
    """Whether an open-loop cell's measurement is trustworthy. ``saturated`` cells
    are discarded: the system could not sustain the target rate, so the latencies
    (even coordinated-omission-corrected) are not meaningful."""

    sends: int
    lagged_sends: int
    max_lag_s: float
    lagged_fraction: float
    backlog_grew: bool
    saturated: bool

    def as_summary(self) -> dict[str, Any]:
        """The subset recorded into a result row and shipped across the
        cross-process queue (kept here so both sides stay in sync)."""
        return {"saturated": self.saturated, "max_lag_s": self.max_lag_s, "lagged_fraction": self.lagged_fraction, "sends": self.sends}


def assess_saturation(
    scheduled: Sequence[float],
    actual: Sequence[float],
    period: float,
    *,
    max_lagged_fraction: float = 0.01,
    backlog_grew: bool = False,
) -> SaturationVerdict:
    """Compare an open-loop generator's intended absolute send schedule against
    when sends actually happened. A send is "lagged" if it slipped more than one
    full period; too many lagged sends — or a growing reader backlog — means the
    cell is saturated and must be discarded."""
    if period <= 0:
        raise ValueError(f"period must be > 0, got {period}")
    if len(scheduled) != len(actual):
        raise ValueError(f"scheduled and actual must be the same length, got {len(scheduled)} and {len(actual)}")
    lags = [a - s for s, a in zip(scheduled, actual, strict=True)]
    sends = len(lags)
    lagged_sends = sum(1 for lag in lags if lag > period)
    max_lag_s = max(lags) if lags else 0.0
    lagged_fraction = lagged_sends / sends if sends else 0.0
    saturated = lagged_fraction > max_lagged_fraction or backlog_grew
    return SaturationVerdict(sends, lagged_sends, max_lag_s, lagged_fraction, backlog_grew, saturated)


# -- raw-Kafka (M7) baseline kwargs ------------------------------------------
#
# The baseline consumer/producer must reproduce KafkaTable's settings so the
# delta isolates ktables overhead, not config differences. Mirrors
# KafkaTable.start() / KafkaTableWriter.start() in kafka_table.py; aiokafka's own
# defaults differ (enable_auto_commit=True, auto_offset_reset='latest',
# enable_idempotence=False), so these overrides are load-bearing.

BASELINE_CONSUMER_KWARGS: Mapping[str, object] = MappingProxyType(
    {"group_id": None, "enable_auto_commit": False, "auto_offset_reset": "earliest"}
)
BASELINE_PRODUCER_KWARGS: Mapping[str, object] = MappingProxyType({"enable_idempotence": True})


# -- environment capture & artifact I/O --------------------------------------


def capture_environment(*, extra: dict[str, Any] | None = None) -> dict[str, Any]:
    """A reproducibility block for a benchmark artifact: interpreter, platform,
    library versions, and a cpuinfo CPU block, merged with caller-supplied
    ``extra`` (Redpanda image tag, sweep parameters, aiokafka knobs, etc.)."""
    env: dict[str, Any] = {
        "python_version": platform.python_version(),
        "platform": platform.platform(),
        "aiokafka_version": aiokafka.__version__,
        "ktables_version": _pkg_version("ktables"),
        "hdrhistogram_version": _pkg_version("hdrhistogram"),
        "cpu": cpuinfo.get_cpu_info(),
    }
    if extra:
        env.update(extra)
    return env


def summary_to_dict(summary: LatencySummary) -> dict[str, Any]:
    """A JSON-serializable dict for one :class:`LatencySummary`."""
    return asdict(summary)


def write_artifact(path: Path, payload: dict[str, Any]) -> None:
    """Write a benchmark result artifact as pretty JSON, creating parent dirs."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True))


# -- open-loop load generator ------------------------------------------------


@dataclass(frozen=True, slots=True)
class OpenLoopRun:
    """The schedule a :func:`run_open_loop` call actually achieved: the intended
    absolute deadlines and the wall-clock instants each send fired, for
    :func:`assess_saturation`."""

    scheduled: list[float]
    actual: list[float]
    period: float

    def assess(self, *, max_lagged_fraction: float = 0.01, backlog_grew: bool = False) -> SaturationVerdict:
        """Saturation verdict for this run (the generator carries exactly the
        three fields :func:`assess_saturation` needs)."""
        return assess_saturation(self.scheduled, self.actual, self.period, max_lagged_fraction=max_lagged_fraction, backlog_grew=backlog_grew)


async def run_open_loop(*, rate_hz: float, count: int, send: Callable[[int], Awaitable[None]]) -> OpenLoopRun:
    """Fire ``send(i)`` at absolute deadlines ``t0 + i/rate_hz`` (i in 0..count-1).

    Deadlines are absolute, so a slow send does not push later deadlines back
    (no self-coordinated omission). Each send is launched as a task and not
    awaited inline — that is what makes the load *open*-loop — and all are awaited
    before returning. The returned schedule feeds :func:`assess_saturation`.
    """
    if rate_hz <= 0:
        raise ValueError(f"rate_hz must be > 0, got {rate_hz}")
    if count < 0:
        raise ValueError(f"count must be >= 0, got {count}")
    period = 1.0 / rate_hz
    loop = asyncio.get_running_loop()
    t0 = loop.time()
    scheduled: list[float] = []
    actual: list[float] = []
    tasks: list[asyncio.Task[None]] = []
    try:
        for i in range(count):
            target = t0 + i * period
            delay = target - loop.time()
            if delay > 0:
                await asyncio.sleep(delay)
            scheduled.append(target)
            actual.append(loop.time())
            tasks.append(asyncio.ensure_future(send(i)))
        if tasks:
            await asyncio.gather(*tasks)
    except BaseException:
        for task in tasks:
            task.cancel()
        raise
    return OpenLoopRun(scheduled, actual, period)


# -- propagation payload codec -----------------------------------------------
#
# A propagation record carries a sequence number and the send timestamp
# (system-wide CLOCK_MONOTONIC nanoseconds) so the reader's on_set can compute
# t_apply - t_send — within one process or across the cross-process writer.
# Both are unsigned 64-bit big-endian; the rest is zero padding to a target size.

_PAYLOAD_HEADER = struct.Struct(">QQ")  # seq, t_send_ns
PAYLOAD_HEADER_SIZE = _PAYLOAD_HEADER.size  # 16 bytes


def encode_payload(*, seq: int, t_send_ns: int, size: int) -> bytes:
    """Encode a propagation record of exactly ``size`` bytes (>= header size)."""
    if size < PAYLOAD_HEADER_SIZE:
        raise ValueError(f"size must be >= {PAYLOAD_HEADER_SIZE} (header), got {size}")
    header = _PAYLOAD_HEADER.pack(seq, t_send_ns)
    return header + b"\x00" * (size - PAYLOAD_HEADER_SIZE)


def decode_payload(data: bytes) -> tuple[int, int]:
    """Decode a propagation record to ``(seq, t_send_ns)``, ignoring padding."""
    return _PAYLOAD_HEADER.unpack_from(data)


def identity_codec(data: bytes) -> bytes:
    """Pass-through value codec for byte-payload cells (the encoder == the decoder).
    Shared by the macro modules whose cells carry opaque bytes."""
    return data


def stamping_encoder(payload_size: int) -> Callable[[int], bytes]:
    """A KafkaTableWriter value-encoder that stamps the send timestamp
    (CLOCK_MONOTONIC ns) at encode time — i.e. inside ``set()``, immediately
    before ``send_and_wait`` — alongside the sequence number, padded to
    ``payload_size``. Shared by the same-process and cross-process writers."""

    def encode(seq: int) -> bytes:
        return encode_payload(seq=seq, t_send_ns=time.clock_gettime_ns(time.CLOCK_MONOTONIC), size=payload_size)

    return encode


# -- instrumented reader (propagation probe) ---------------------------------


class PropagationProbe:
    """An instrumented :class:`~ktables.KafkaTable` for measuring publish→visible
    latency. Its ``on_set`` decodes the embedded send timestamp and records
    ``t_apply - t_send`` — but only while :attr:`recording` is on, so warm-up
    records are excluded from the measured distribution.

    Pass ``expected_interval`` (the open-loop send period, seconds) for
    sustained-load cells to apply coordinated-omission correction; leave it
    ``None`` for closed-loop (idle / single-write) cells.
    """

    def __init__(
        self,
        *,
        bootstrap_servers: str,
        topic: str,
        poll_timeout_ms: int = 200,
        ensure_topic: bool = True,
        expected_interval: float | None = None,
    ) -> None:
        self.recorder = LatencyRecorder()
        self.recording = False
        self.stamped = 0
        self._expected_interval = expected_interval
        self.table: KafkaTable[tuple[int, int]] = KafkaTable(
            bootstrap_servers=bootstrap_servers,
            topic=topic,
            value_decoder=decode_payload,
            on_set=self._on_set,
            poll_timeout_ms=poll_timeout_ms,
            ensure_topic=ensure_topic,
        )

    def _on_set(self, key: str, value: tuple[int, int]) -> None:
        if not self.recording:
            return
        _seq, t_send_ns = value
        # Clamp to >= 0: on_set must never raise (a raising hook kills the
        # reader), and a "future" t_send from clock skew would otherwise be
        # rejected by the recorder.
        latency_s = max(0.0, (time.clock_gettime_ns(time.CLOCK_MONOTONIC) - t_send_ns) / 1e9)
        recorded = (
            self.recorder.record(latency_s)
            if self._expected_interval is None
            else self.recorder.record_open_loop(latency_s, self._expected_interval)
        )
        # Count only records that entered the histogram, so sample reconciliation
        # alone catches a dropped (out-of-range) sample.
        if recorded:
            self.stamped += 1


# -- raw-aiokafka baseline (M7) ----------------------------------------------


class RawPropagationBaseline:
    """Publish→consume latency through a **bare** aiokafka consumer (no ktables),
    kwargs-matched to KafkaTable (:data:`BASELINE_CONSUMER_KWARGS` + the same
    ``poll_timeout_ms`` and ``seek_to_beginning``), so ktables' propagation can be
    reported as a delta over raw Kafka. Mirrors :class:`PropagationProbe`'s
    measurement point (decode the embedded send timestamp on consume) without the
    dict, hooks, or barrier machinery."""

    def __init__(self, *, bootstrap_servers: str, topic: str, poll_timeout_ms: int = 200) -> None:
        self.recorder = LatencyRecorder()
        self.recording = False
        self.stamped = 0
        self.consumed = 0  # total records seen (used to detect warm-up drain)
        self._poll_ms = poll_timeout_ms
        self._consumer = AIOKafkaConsumer(topic, bootstrap_servers=bootstrap_servers, **BASELINE_CONSUMER_KWARGS)
        self._task: asyncio.Task[None] | None = None
        self._stop: asyncio.Event | None = None

    async def start(self) -> None:
        await self._consumer.start()
        tps = sorted(self._consumer.assignment(), key=lambda tp: tp.partition)
        await self._consumer.seek_to_beginning(*tps)
        self._stop = asyncio.Event()
        self._task = asyncio.create_task(self._run())

    async def _run(self) -> None:
        assert self._stop is not None
        while not self._stop.is_set():
            batches = await self._consumer.getmany(timeout_ms=self._poll_ms)
            for records in batches.values():
                for record in records:
                    self._handle(record)

    def _handle(self, record: Any) -> None:
        """Process one consumed record (extracted from the loop so the
        decode/record/count logic is unit-testable without a broker)."""
        if record.value is None:  # tombstone — not a propagation sample
            return
        self.consumed += 1
        if not self.recording:
            return
        _seq, t_send_ns = decode_payload(record.value)
        if self.recorder.record(max(0.0, (time.clock_gettime_ns(time.CLOCK_MONOTONIC) - t_send_ns) / 1e9)):
            self.stamped += 1

    async def stop(self) -> None:
        if self._stop is not None:
            self._stop.set()
        if self._task is not None:
            self._task.cancel()
            with suppress(asyncio.CancelledError):
                await self._task
        await self._consumer.stop()

    async def __aenter__(self) -> RawPropagationBaseline:
        await self.start()
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.stop()


# -- async polling helper ----------------------------------------------------


async def wait_until(predicate: Callable[[], bool], *, timeout: float, interval: float = 0.005) -> bool:
    """Poll ``predicate`` until it is true or ``timeout`` elapses. The raw-Kafka
    baseline has no ``barrier()``, so this is how a measured phase waits for all
    sent records to be consumed."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        if predicate():
            return True
        await asyncio.sleep(interval)
    return predicate()


async def preload(writer: Any, items: Any, *, depth: int = 500) -> None:
    """Pipeline keyed writes (gather in chunks of ``depth``) so preloading a large
    topic stays fast. Each item is the positional args for ``writer.set`` —
    ``(key, value)`` for a flat writer, ``(group, member, value)`` for grouped."""
    items = list(items)
    for base in range(0, len(items), depth):
        await asyncio.gather(*(writer.set(*item) for item in items[base : base + depth]))
