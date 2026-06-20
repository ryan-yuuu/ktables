"""Benchmark-suite fixtures: profile selection, a topic factory, and a session
results collector that writes one JSON artifact per run.

Run the suite with::

    uv run --group bench pytest benchmarks/

Select a profile with ``KTABLES_BENCH_PROFILE=quick|full|soak`` (default
``quick``). The session-scoped Redpanda broker (``redpanda``/``bootstrap``
fixtures) is inherited from the repo-root ``conftest.py``.
"""

from __future__ import annotations

import os
import time
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator
from pathlib import Path
from typing import Any

import pytest
from aiokafka.admin import AIOKafkaAdminClient

from benchmarks._harness import capture_environment, write_artifact
from ktables import ensure_topic

# Read at import (collection) time so benchmark modules can size their parametrized
# cell lists per profile.
PROFILE = os.environ.get("KTABLES_BENCH_PROFILE", "quick")
RESULTS_DIR = Path(__file__).resolve().parent.parent / "results"


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line("markers", "benchmark: a performance benchmark (broker-backed; opt-in)")


@pytest.fixture
async def bench_topic(bootstrap: str) -> AsyncIterator[Callable[..., Awaitable[str]]]:
    """An async factory creating fresh, uniquely-named compacted topics with a
    chosen partition count (KafkaTable always ensures 1 partition itself, so
    multi-partition cells must pre-create the topic and run with
    ``ensure_topic=False``). All created topics are deleted on teardown."""
    created: list[str] = []

    async def _make(*, partitions: int = 1) -> str:
        name = f"ktables.bench.{uuid.uuid4().hex[:8]}"
        await ensure_topic(bootstrap, name, num_partitions=partitions)
        created.append(name)
        return name

    yield _make

    if created:
        admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap)
        await admin.start()
        try:
            await admin.delete_topics(created)
        finally:
            await admin.close()


@pytest.fixture(scope="session")
def bench_results(redpanda: Any, bootstrap: str) -> Iterator[dict[str, Any]]:
    """Session-wide results accumulator. Benchmark tests append rows under
    ``results["metrics"][<metric>]``; the whole document (with an environment
    block) is written to ``results/bench-<profile>-<epoch>.json`` at session end."""
    results: dict[str, Any] = {
        "env": capture_environment(
            extra={
                "profile": PROFILE,
                "redpanda_image": str(getattr(redpanda, "image", "unknown")),
                "bootstrap": bootstrap,
            }
        ),
        "metrics": {},
    }
    yield results
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    path = RESULTS_DIR / f"bench-{PROFILE}-{int(time.time())}.json"
    write_artifact(path, results)
    print(f"\n[benchmarks] results written to {path}")
