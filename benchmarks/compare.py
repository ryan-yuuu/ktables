"""Diff two benchmark result artifacts and flag latency regressions per cell.

The macro suite writes one JSON artifact per run (``results/bench-*.json``). This
matches cells between two runs by their identifying parameters and flags any
``p50_us``/``p99_us`` that grew beyond a threshold. Usage::

    uv run --group bench python -m benchmarks.compare results/old.json results/new.json
    uv run --group bench python -m benchmarks.compare old.json new.json --threshold 0.3

Exits non-zero if any regression is found (so it can gate CI).
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

# Fields that are measurements (compared / ignored for cell identity), not cell
# parameters. Anything else in a row identifies the cell.
_MEASUREMENT_FIELDS = frozenset(
    {
        "count", "dropped", "min_us", "p50_us", "p90_us", "p95_us", "p99_us", "p999_us", "max_us", "mean_us", "stddev_us",
        "throughput_rps", "sequential_rps", "total_start_ms", "catch_up_ms", "connect_metadata_ms", "replayed_at_catch_up",
        "traced_bytes", "traced_bytes_per_key", "rss_delta_bytes", "rss_delta_bytes_per_key", "dict_bytes", "dict_bytes_per_key",
        "cpu_fraction", "approx_wakeups_per_s", "max_lag_ms",
        # Per-run OUTCOMES, not cell identity: a cell that flips saturated/degraded
        # between runs must still match its baseline (or the regression is missed).
        "saturated", "status",
    }
)
# Latency fields a regression is reported on.
_COMPARE_FIELDS = ("p50_us", "p99_us")

Regression = tuple[str, dict[str, Any], str, float, float, float]


def row_key(row: dict[str, Any]) -> tuple[tuple[str, Any], ...]:
    """A hashable identity for a cell: its non-measurement, scalar parameters."""
    return tuple(sorted((k, v) for k, v in row.items() if k not in _MEASUREMENT_FIELDS and isinstance(v, (str, int, float, bool))))


def compare(old: dict[str, Any], new: dict[str, Any], threshold: float) -> list[Regression]:
    """Return regressions where a new latency exceeds the old by > ``threshold``
    (fractional), for every cell present in both artifacts."""
    regressions: list[Regression] = []
    for metric, new_rows in new.get("metrics", {}).items():
        old_by_key = {row_key(r): r for r in old.get("metrics", {}).get(metric, [])}
        for new_row in new_rows:
            key = row_key(new_row)
            old_row = old_by_key.get(key)
            if old_row is None:
                continue
            for field in _COMPARE_FIELDS:
                old_value, new_value = old_row.get(field), new_row.get(field)
                if not isinstance(old_value, (int, float)) or not isinstance(new_value, (int, float)) or old_value <= 0:
                    continue
                delta = (new_value - old_value) / old_value
                if delta > threshold:
                    regressions.append((metric, dict(key), field, float(old_value), float(new_value), delta))
    return regressions


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Flag latency regressions between two benchmark artifacts.")
    parser.add_argument("old", type=Path, help="baseline artifact JSON")
    parser.add_argument("new", type=Path, help="new artifact JSON")
    parser.add_argument("--threshold", type=float, default=0.2, help="fractional regression threshold (default 0.2 = 20%%)")
    args = parser.parse_args(argv)

    old = json.loads(args.old.read_text())
    new = json.loads(args.new.read_text())
    regressions = compare(old, new, args.threshold)

    if not regressions:
        print(f"No latency regressions > {args.threshold:.0%}.")
        return 0
    print(f"{len(regressions)} latency regression(s) > {args.threshold:.0%}:")
    for metric, params, field, old_value, new_value, delta in regressions:
        print(f"  {metric} {params} {field}: {old_value:.0f} -> {new_value:.0f} (+{delta:.0%})")
    return 1


if __name__ == "__main__":
    sys.exit(main())
