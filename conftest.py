"""Repo-root pytest config — the shared session-scoped Redpanda broker.

Both the test suite (``tests/``) and the benchmark suite (``benchmarks/``)
materialize against one real Redpanda broker that testcontainers starts once per
session and tears down at the end. Redpanda is Kafka-API compatible, so aiokafka
talks to it unchanged — there is no Kafka fallback.

These fixtures live at the repo root (rather than in ``tests/conftest.py``) so
both trees inherit a single definition: a benchmark run and a test run started
in the same session would share the same broker. The broker starts lazily — only
when a test/benchmark actually requests it — so the broker-free unit suite
(``-m 'not integration'``) never touches Docker.
"""

from __future__ import annotations

from collections.abc import Iterator

import pytest
from testcontainers.kafka import RedpandaContainer

# Pinned for reproducibility. testcontainers' own default tag is years stale, so
# we override it with a current stable release.
REDPANDA_IMAGE = "docker.redpanda.com/redpandadata/redpanda:v25.3.15"


@pytest.fixture(scope="session")
def redpanda() -> Iterator[RedpandaContainer]:
    """One Redpanda broker for the whole session, started lazily — only when a
    broker-backed test/benchmark actually requests it."""
    with RedpandaContainer(REDPANDA_IMAGE) as container:
        yield container


@pytest.fixture(scope="session")
def bootstrap(redpanda: RedpandaContainer) -> str:
    """``host:port`` of the session Redpanda broker (a random mapped port)."""
    return redpanda.get_bootstrap_server()
