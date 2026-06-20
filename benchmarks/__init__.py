"""ktables performance benchmark suite.

A broker-backed (Redpanda via testcontainers) benchmark suite measuring write
propagation latency, ``barrier()`` latency, write/throughput, cold-start, memory,
and in-memory read costs. See ``notes/ktables-benchmark-test-plan.md`` for the
plan and ``benchmarks/README.md`` for how to run it.

This package is intentionally outside ``testpaths=["tests"]`` so the normal test
suite never collects it; run with ``uv run --group bench pytest benchmarks/``.
"""
