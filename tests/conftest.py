"""Shared test fixtures — a session-scoped Redpanda broker for integration tests.

Integration tests (anything that, directly or transitively, requests the
``bootstrap`` or ``topic`` fixture) run against a real Redpanda broker that
testcontainers spins up once per session and tears down at the end. Redpanda is
Kafka-API compatible, so aiokafka talks to it unchanged — there is no Kafka
fallback.

Such tests are auto-marked ``integration`` (see ``pytest_collection_modifyitems``)
so the broker-free unit suite can be selected on its own:

    uv run pytest -m "not integration"   # no Docker required
    uv run pytest                         # full suite; needs Docker
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
    """One Redpanda broker for the whole session, started lazily — only when an
    integration test actually requests it (so ``-m 'not integration'`` never
    touches Docker)."""
    with RedpandaContainer(REDPANDA_IMAGE) as container:
        yield container


@pytest.fixture(scope="session")
def bootstrap(redpanda: RedpandaContainer) -> str:
    """``host:port`` of the session Redpanda broker (a random mapped port)."""
    return redpanda.get_bootstrap_server()


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    """Auto-mark any broker-backed test as ``integration``.

    Keying off fixture usage keeps the marker from drifting from reality: a test
    is an integration test iff it (transitively) requests ``bootstrap``/``topic``.
    This runs at collection time, before fixtures resolve, so ``-m 'not
    integration'`` deselects them without ever starting the container.
    """
    for item in items:
        fixturenames = set(getattr(item, "fixturenames", ()))
        if {"bootstrap", "topic"} & fixturenames:
            item.add_marker("integration")
