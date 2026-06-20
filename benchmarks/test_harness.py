"""Broker-free unit tests for the benchmark harness pure logic.

These run without Docker (no broker fixtures) and are the TDD target for
``benchmarks/_harness.py``. Run with ``uv run --group bench pytest
benchmarks/test_harness.py``.
"""

from __future__ import annotations

import asyncio
import json
import time
import types

import pytest

from benchmarks._harness import (
    BASELINE_CONSUMER_KWARGS,
    BASELINE_PRODUCER_KWARGS,
    LatencyRecorder,
    PropagationProbe,
    RawPropagationBaseline,
    SampleAccountingError,
    assert_samples_accounted,
    assess_saturation,
    capture_environment,
    decode_payload,
    encode_payload,
    identity_codec,
    preload,
    run_open_loop,
    stamping_encoder,
    summary_to_dict,
    wait_until,
    write_artifact,
)


def _ns_record(*, key: bytes | None, value: bytes | None, partition: int = 0, offset: int = 0):
    """A minimal ConsumerRecord stand-in — KafkaTable._apply reads only these four."""
    return types.SimpleNamespace(key=key, value=value, partition=partition, offset=offset)


class TestLatencyRecorder:
    """HDR-backed latency recorder: microsecond recording, percentile summary,
    coordinated-omission correction, a dropped-above-ceiling guard, and merge."""

    def test_records_identical_values_and_reports_count_and_percentiles(self) -> None:
        rec = LatencyRecorder()
        for _ in range(1000):
            rec.record(0.001)  # 1 ms == 1000 us
        s = rec.summary()
        assert s.count == 1000
        assert s.dropped == 0
        # 3-significant-figure HDR resolves 1000 us to within ~0.1%; allow 1%.
        for value in (s.min_us, s.p50_us, s.p99_us, s.p999_us, s.max_us):
            assert abs(value - 1000) <= 10
        assert abs(s.mean_us - 1000) <= 10

    def test_rejects_negative_latency(self) -> None:
        # The HDR-backed recorder is for non-negative latencies only; the
        # possibly-negative M1a post-ack lag uses a separate collector.
        rec = LatencyRecorder()
        with pytest.raises(ValueError):
            rec.record(-0.001)

    def test_drops_values_above_ceiling_and_counts_them(self) -> None:
        rec = LatencyRecorder()
        rec.record(0.001)
        assert rec.record(5000.0) is False  # 5000 s is beyond the rounded ceiling
        s = rec.summary()
        assert s.count == 1  # the dropped sample is NOT in the histogram
        assert s.dropped == 1

    def test_open_loop_correction_synthesizes_tail_samples(self) -> None:
        # One 5 ms sample at a 1 ms expected interval back-fills 5 synthetic
        # samples (5,4,3,2,1 ms) — coordinated-omission correction.
        rec = LatencyRecorder()
        assert rec.record_open_loop(0.005, expected_interval=0.001) is True
        s = rec.summary()
        assert s.count == 5
        assert abs(s.max_us - 5000) <= 50

    def test_record_open_loop_rejects_non_positive_interval(self) -> None:
        rec = LatencyRecorder()
        with pytest.raises(ValueError):
            rec.record_open_loop(0.001, expected_interval=0)

    def test_record_open_loop_drops_above_ceiling(self) -> None:
        rec = LatencyRecorder()
        assert rec.record_open_loop(5000.0, expected_interval=0.001) is False
        s = rec.summary()
        assert s.count == 0
        assert s.dropped == 1

    def test_summary_of_empty_recorder_is_zeroed(self) -> None:
        s = LatencyRecorder().summary()
        assert s.count == 0 and s.dropped == 0
        assert s.p99_us == 0 and s.max_us == 0 and s.mean_us == 0.0

    def test_merge_combines_counts_and_dropped(self) -> None:
        a, b = LatencyRecorder(), LatencyRecorder()
        for _ in range(500):
            a.record(0.001)
        for _ in range(500):
            b.record(0.002)
        a.record(5000.0)  # dropped on a
        a.merge(b)
        s = a.summary()
        assert s.count == 1000
        assert s.dropped == 1

    def test_merge_sums_dropped_from_both_record_paths(self) -> None:
        # Both sides have a drop, via different record methods — both must survive.
        a, b = LatencyRecorder(), LatencyRecorder()
        a.record(0.001)
        a.record(5000.0)  # closed-loop drop
        b.record_open_loop(5000.0, expected_interval=0.001)  # open-loop drop
        a.merge(b)
        assert a.summary().dropped == 2

    def test_merge_of_near_ceiling_in_range_value_does_not_raise(self) -> None:
        # The central-range invariant: every recorder shares one HDR range, so a
        # near-ceiling in-range value merges cleanly (HdrHistogram.add raises on a
        # range mismatch). Guards a future per-recorder-range regression.
        a, b = LatencyRecorder(), LatencyRecorder()
        b.record(599.0)  # in range, near the nominal ceiling
        a.merge(b)  # must not raise
        assert a.summary().count == 1
        assert abs(a.summary().max_us - 599_000_000) <= 599_000_000 * 0.01


class TestSampleAccounting:
    """Reconcile records sent against records stamped (on_set/on_delete) + skipped
    (keyless/decode-error counters), so a silently-lost sample fails the cell."""

    def test_passes_when_stamped_plus_skipped_equals_sent(self) -> None:
        assert_samples_accounted(sent=100, stamped=100)
        assert_samples_accounted(sent=100, stamped=90, skipped=10)

    def test_raises_on_unaccounted_samples(self) -> None:
        with pytest.raises(SampleAccountingError, match="unaccounted=5"):
            assert_samples_accounted(sent=100, stamped=95)

    def test_raises_on_over_count(self) -> None:
        # Over-count (double-fired hook / warm-up leak) is the symmetric corruption.
        with pytest.raises(SampleAccountingError, match="unaccounted=-5"):
            assert_samples_accounted(sent=100, stamped=105)


class TestSaturationAssessment:
    """Open-loop validity gate: a cell is saturated (and discarded) if the
    generator fell behind its absolute schedule, or the reader backlog grew."""

    def test_clean_run_is_not_saturated(self) -> None:
        period = 0.01
        scheduled = [i * period for i in range(100)]
        actual = [t + 0.0001 for t in scheduled]  # tiny lag, far under one period
        v = assess_saturation(scheduled, actual, period)
        assert v.lagged_sends == 0
        assert v.saturated is False

    def test_flagged_when_sends_lag_beyond_a_period(self) -> None:
        period = 0.01
        scheduled = [i * period for i in range(100)]
        actual = [t + 0.05 for t in scheduled]  # 5x-period lag on every send
        v = assess_saturation(scheduled, actual, period)
        assert v.lagged_sends == 100
        assert v.saturated is True

    def test_flagged_when_reader_backlog_grew(self) -> None:
        period = 0.01
        scheduled = [i * period for i in range(10)]
        actual = list(scheduled)  # no scheduler lag at all
        v = assess_saturation(scheduled, actual, period, backlog_grew=True)
        assert v.saturated is True

    def test_rejects_mismatched_lengths(self) -> None:
        with pytest.raises(ValueError):
            assess_saturation([0.0, 0.01], [0.0], 0.01)

    def test_rejects_non_positive_period(self) -> None:
        with pytest.raises(ValueError):
            assess_saturation([0.0], [0.0], 0.0)

    def test_threshold_is_strict_greater_than(self) -> None:
        # lagged_fraction exactly at the threshold is NOT saturated; just above is.
        period = 0.01
        scheduled = [i * period for i in range(100)]
        # exactly 1 of 100 lagged == 0.01 fraction == default threshold -> not saturated
        at = [t + (0.05 if i == 0 else 0.0) for i, t in enumerate(scheduled)]
        assert assess_saturation(scheduled, at, period).saturated is False
        # 2 of 100 lagged == 0.02 > 0.01 -> saturated
        over = [t + (0.05 if i < 2 else 0.0) for i, t in enumerate(scheduled)]
        assert assess_saturation(scheduled, over, period).saturated is True

    def test_backlog_grew_is_echoed_and_ors_with_lag(self) -> None:
        verdict = assess_saturation([0.0], [0.0], 0.01, backlog_grew=True)
        assert verdict.backlog_grew is True
        assert verdict.saturated is True

    def test_as_summary_exposes_the_wire_fields(self) -> None:
        summary = assess_saturation([0.0], [0.0], 0.01).as_summary()
        assert set(summary) == {"saturated", "max_lag_s", "lagged_fraction", "sends"}


class TestBaselineKwargs:
    """The raw-Kafka (M7) baseline must reproduce KafkaTable's consumer/producer
    settings exactly, or the delta measures config differences, not ktables.
    Mirrors KafkaTable.start() / KafkaTableWriter.start() in kafka_table.py."""

    def test_consumer_kwargs_match_kafkatable(self) -> None:
        assert BASELINE_CONSUMER_KWARGS["group_id"] is None
        assert BASELINE_CONSUMER_KWARGS["enable_auto_commit"] is False
        assert BASELINE_CONSUMER_KWARGS["auto_offset_reset"] == "earliest"

    def test_producer_kwargs_match_writer(self) -> None:
        assert BASELINE_PRODUCER_KWARGS["enable_idempotence"] is True


class TestEnvironmentCapture:
    def test_includes_versions_and_merges_extra(self) -> None:
        env = capture_environment(extra={"redpanda_image": "img:1", "poll_timeout_ms": 200})
        assert env["redpanda_image"] == "img:1"
        assert env["poll_timeout_ms"] == 200
        assert "python_version" in env
        assert "aiokafka_version" in env
        assert "ktables_version" in env
        assert "cpu" in env  # the cpuinfo block

    def test_works_without_extra(self) -> None:
        env = capture_environment()
        assert "python_version" in env
        assert "ktables_version" in env


class TestArtifactIO:
    def test_summary_to_dict_is_json_serializable(self) -> None:
        rec = LatencyRecorder()
        rec.record(0.001)
        d = summary_to_dict(rec.summary())
        assert d["count"] == 1
        assert json.loads(json.dumps(d)) == d  # round-trips through JSON

    def test_write_artifact_round_trips(self, tmp_path) -> None:
        payload = {"env": {"x": 1}, "metrics": {"m1": [1, 2, 3]}}
        path = tmp_path / "result.json"
        write_artifact(path, payload)
        assert json.loads(path.read_text()) == payload

    def test_write_artifact_creates_parent_dirs(self, tmp_path) -> None:
        path = tmp_path / "nested" / "dir" / "r.json"
        write_artifact(path, {"a": 1})
        assert json.loads(path.read_text()) == {"a": 1}


class TestOpenLoopScheduler:
    """The open-loop generator fires sends at absolute deadlines (t0 + i*period),
    not a fixed delta per iteration, so it does not self-coordinate."""

    async def test_fires_every_send_on_a_driftless_absolute_schedule(self) -> None:
        fired: list[int] = []

        async def send(i: int) -> None:
            fired.append(i)

        run = await run_open_loop(rate_hz=1000.0, count=20, send=send)
        assert sorted(fired) == list(range(20))
        assert len(run.scheduled) == len(run.actual) == 20
        # Absolute schedule: each deadline is exactly i periods after the first
        # (no accumulated drift), and no send wakes before its deadline.
        for i in range(20):
            assert abs((run.scheduled[i] - run.scheduled[0]) - i * run.period) < 1e-9
            assert run.actual[i] + 1e-6 >= run.scheduled[i]

    async def test_count_zero_is_a_noop(self) -> None:
        async def send(i: int) -> None:
            raise AssertionError("send must not be called when count == 0")

        run = await run_open_loop(rate_hz=1000.0, count=0, send=send)
        assert run.scheduled == [] and run.actual == []

    async def test_rejects_non_positive_rate(self) -> None:
        async def send(i: int) -> None:
            pass

        with pytest.raises(ValueError):
            await run_open_loop(rate_hz=0.0, count=1, send=send)

    async def test_rejects_negative_count(self) -> None:
        async def send(i: int) -> None:
            pass

        with pytest.raises(ValueError):
            await run_open_loop(rate_hz=10.0, count=-1, send=send)

    async def test_propagates_send_error_and_cancels_pending(self) -> None:
        async def boom(i: int) -> None:
            raise RuntimeError("send failed")

        with pytest.raises(RuntimeError, match="send failed"):
            await run_open_loop(rate_hz=1000.0, count=5, send=boom)

    async def test_error_actually_cancels_the_in_flight_sends(self) -> None:
        # Stronger than the above: a send that blocks forever must be CANCELLED
        # (not leaked) when a later send raises.
        started: list[asyncio.Task[None]] = []
        block = asyncio.Event()

        async def send(i: int) -> None:
            if i == 3:
                raise RuntimeError("boom")
            started.append(asyncio.current_task())  # type: ignore[arg-type]
            await block.wait()  # never set

        with pytest.raises(RuntimeError, match="boom"):
            await run_open_loop(rate_hz=100_000.0, count=5, send=send)
        await asyncio.gather(*started, return_exceptions=True)  # let cancellations settle
        assert started and all(task.cancelled() for task in started)

    async def test_assess_method_delegates_to_assess_saturation(self) -> None:
        async def send(i: int) -> None:
            pass

        run = await run_open_loop(rate_hz=1000.0, count=10, send=send)
        assert run.assess().saturated is False


class TestIdentityCodec:
    def test_passes_bytes_through(self) -> None:
        assert identity_codec(b"abc") == b"abc"


class TestPreload:
    async def test_pipelines_every_item_to_writer_set(self) -> None:
        class _FakeWriter:
            def __init__(self) -> None:
                self.calls: list[tuple] = []

            async def set(self, *args: object) -> None:
                self.calls.append(args)

        writer = _FakeWriter()
        await preload(writer, [("k0", 0), ("k1", 1), ("k2", 2)], depth=2)
        assert writer.calls == [("k0", 0), ("k1", 1), ("k2", 2)]


class TestPropagationPayload:
    """The propagation payload carries a sequence number and the send timestamp
    so on_set can compute t_apply - t_send (incl. across processes), padded to a
    target byte size."""

    def test_round_trips_seq_and_timestamp_at_requested_size(self) -> None:
        b = encode_payload(seq=42, t_send_ns=123_456_789, size=100)
        assert len(b) == 100
        seq, t_send_ns = decode_payload(b)
        assert seq == 42
        assert t_send_ns == 123_456_789

    def test_decode_ignores_padding(self) -> None:
        small = encode_payload(seq=7, t_send_ns=9, size=16)  # header only, no pad
        big = encode_payload(seq=7, t_send_ns=9, size=4096)
        assert decode_payload(small) == decode_payload(big) == (7, 9)

    def test_rejects_size_smaller_than_header(self) -> None:
        with pytest.raises(ValueError):
            encode_payload(seq=1, t_send_ns=1, size=4)


class TestStampingEncoder:
    """The writer-side encoder stamps the send timestamp at encode time (i.e.
    just before send_and_wait), embedding it with the sequence number."""

    def test_embeds_seq_and_a_recent_timestamp_at_requested_size(self) -> None:
        enc = stamping_encoder(64)
        before = time.clock_gettime_ns(time.CLOCK_MONOTONIC)
        data = enc(7)
        after = time.clock_gettime_ns(time.CLOCK_MONOTONIC)
        assert len(data) == 64
        seq, t_send_ns = decode_payload(data)
        assert seq == 7
        assert before <= t_send_ns <= after


class TestPropagationProbe:
    """The instrumented reader: its on_set decodes the payload and records
    t_apply - t_send into a LatencyRecorder, but only while ``recording`` is on
    (so warm-up records are excluded). Broker-free: drives KafkaTable._apply
    directly, exactly as ktables' own unit tests do."""

    @staticmethod
    def _probe() -> PropagationProbe:
        # ensure_topic=False + never start(): no broker, no connection.
        return PropagationProbe(bootstrap_servers="localhost:9092", topic="bench.unit", poll_timeout_ms=200, ensure_topic=False)

    def test_records_latency_for_applied_record_when_recording(self) -> None:
        probe = self._probe()
        probe.recording = True
        t_send = time.clock_gettime_ns(time.CLOCK_MONOTONIC) - 5_000_000  # 5 ms ago
        probe.table._apply(_ns_record(key=b"m1", value=encode_payload(seq=1, t_send_ns=t_send, size=64)))
        s = probe.recorder.summary()
        assert s.count == 1
        assert probe.stamped == 1
        assert abs(s.p50_us - 5000) <= 500  # ~5 ms, plus the tiny now() gap

    def test_does_not_record_while_not_recording(self) -> None:
        probe = self._probe()  # recording defaults False (warm-up phase)
        payload = encode_payload(seq=1, t_send_ns=time.clock_gettime_ns(time.CLOCK_MONOTONIC), size=64)
        probe.table._apply(_ns_record(key=b"w1", value=payload))
        assert probe.recorder.summary().count == 0
        assert probe.stamped == 0

    def test_clamps_a_negative_latency_to_zero_rather_than_raising(self) -> None:
        # on_set must never raise (a raising hook kills the ktables reader). A
        # timestamp in the "future" (clock skew) clamps to ~0, not a ValueError.
        probe = self._probe()
        probe.recording = True
        future = time.clock_gettime_ns(time.CLOCK_MONOTONIC) + 1_000_000_000  # 1 s ahead
        probe.table._apply(_ns_record(key=b"m1", value=encode_payload(seq=1, t_send_ns=future, size=64)))
        assert probe.recorder.summary().count == 1  # recorded (clamped), reader survived

    def test_counts_only_successfully_recorded_samples(self) -> None:
        # stamped must track records that entered the histogram, not records seen —
        # so sample reconciliation alone catches a dropped (out-of-range) sample.
        probe = self._probe()
        probe.recording = True

        class _RejectingRecorder:
            def record(self, seconds: float) -> bool:
                return False

            def record_open_loop(self, seconds: float, expected_interval: float) -> bool:
                return False

        probe.recorder = _RejectingRecorder()  # type: ignore[assignment]
        now = time.clock_gettime_ns(time.CLOCK_MONOTONIC)
        probe.table._apply(_ns_record(key=b"m1", value=encode_payload(seq=1, t_send_ns=now, size=64)))
        assert probe.stamped == 0  # record() returned False — not counted

    def test_open_loop_path_applies_co_correction(self) -> None:
        # The expected_interval branch (used by sustained-load cells) must record
        # via record_open_loop, synthesizing CO back-fill samples.
        probe = PropagationProbe(
            bootstrap_servers="localhost:9092", topic="bench.unit", poll_timeout_ms=200, ensure_topic=False, expected_interval=0.001
        )
        probe.recording = True
        t_send = time.clock_gettime_ns(time.CLOCK_MONOTONIC) - 5_000_000  # 5 ms ago, vs a 1 ms interval
        probe.table._apply(_ns_record(key=b"m1", value=encode_payload(seq=1, t_send_ns=t_send, size=64)))
        assert probe.recorder.summary().count > 1  # CO back-fill synthesized extra samples
        assert probe.stamped == 1  # but one real record


class TestRawPropagationBaseline:
    """The M7 baseline's per-record handling (the consumer loop is broker-only and
    validated by the M7 integration cell; this covers the decode/record/count logic)."""

    @staticmethod
    def _baseline() -> RawPropagationBaseline:
        # Constructing AIOKafkaConsumer does not connect; _handle is broker-free.
        return RawPropagationBaseline(bootstrap_servers="localhost:9092", topic="bench.unit", poll_timeout_ms=200)

    async def test_tombstone_is_neither_consumed_nor_recorded(self) -> None:
        baseline = self._baseline()  # AIOKafkaConsumer needs a running loop at construction
        baseline.recording = True
        baseline._handle(_ns_record(key=b"k", value=None))
        assert baseline.consumed == 0 and baseline.stamped == 0

    async def test_consumes_but_does_not_record_during_warmup(self) -> None:
        baseline = self._baseline()  # recording defaults False
        payload = encode_payload(seq=1, t_send_ns=time.clock_gettime_ns(time.CLOCK_MONOTONIC), size=64)
        baseline._handle(_ns_record(key=b"k", value=payload))
        assert baseline.consumed == 1 and baseline.stamped == 0

    async def test_records_latency_when_recording(self) -> None:
        baseline = self._baseline()
        baseline.recording = True
        t_send = time.clock_gettime_ns(time.CLOCK_MONOTONIC) - 3_000_000  # 3 ms ago
        baseline._handle(_ns_record(key=b"k", value=encode_payload(seq=1, t_send_ns=t_send, size=64)))
        assert baseline.consumed == 1 and baseline.stamped == 1
        assert abs(baseline.recorder.summary().p50_us - 3000) <= 500


class TestWaitUntil:
    async def test_returns_true_once_predicate_holds(self) -> None:
        state = {"n": 0}

        def predicate() -> bool:
            state["n"] += 1
            return state["n"] >= 3

        assert await wait_until(predicate, timeout=5) is True

    async def test_returns_false_on_timeout(self) -> None:
        assert await wait_until(lambda: False, timeout=0.05, interval=0.005) is False


class TestCompare:
    """compare.py diffs two artifacts and flags latency regressions per matched cell."""

    def test_flags_latency_regression_beyond_threshold(self) -> None:
        from benchmarks.compare import compare

        old = {"metrics": {"propagation": [{"poll_timeout_ms": 200, "p50_us": 100, "p99_us": 200}]}}
        new = {"metrics": {"propagation": [{"poll_timeout_ms": 200, "p50_us": 150, "p99_us": 260}]}}
        regressions = compare(old, new, threshold=0.2)  # p50 +50%, p99 +30% — both flagged
        assert len(regressions) == 2

    def test_no_regression_within_threshold(self) -> None:
        from benchmarks.compare import compare

        old = {"metrics": {"m": [{"k": 1, "p50_us": 100, "p99_us": 200}]}}
        new = {"metrics": {"m": [{"k": 1, "p50_us": 105, "p99_us": 210}]}}
        assert compare(old, new, threshold=0.2) == []

    def test_ignores_cells_without_a_match(self) -> None:
        from benchmarks.compare import compare

        old = {"metrics": {"m": [{"k": 1, "p50_us": 100}]}}
        new = {"metrics": {"m": [{"k": 2, "p50_us": 100}]}}  # different cell — not comparable
        assert compare(old, new, threshold=0.2) == []

    def test_handles_empty_or_missing_metrics(self) -> None:
        from benchmarks.compare import compare

        assert compare({}, {}, threshold=0.2) == []
        assert compare({"metrics": {}}, {"metrics": {"m": [{"k": 1, "p50_us": 100}]}}, threshold=0.2) == []

    def test_skips_zero_baseline_without_dividing(self) -> None:
        from benchmarks.compare import compare

        old = {"metrics": {"m": [{"k": 1, "p50_us": 0, "p99_us": 0}]}}  # empty-recorder summary
        new = {"metrics": {"m": [{"k": 1, "p50_us": 50, "p99_us": 90}]}}
        assert compare(old, new, threshold=0.2) == []  # no ZeroDivisionError, no false regression

    def test_run_outcome_fields_do_not_desync_cell_matching(self) -> None:
        from benchmarks.compare import compare

        # A cell that flips saturated False->True (or status) must still match and
        # report the regression, not be silently skipped.
        old = {"metrics": {"prop": [{"rate_hz": 1000, "saturated": False, "p50_us": 100, "p99_us": 200}]}}
        new = {"metrics": {"prop": [{"rate_hz": 1000, "saturated": True, "p50_us": 500, "p99_us": 900}]}}
        assert len(compare(old, new, threshold=0.2)) == 2
