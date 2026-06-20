"""Test-suite fixtures — auto-marking of broker-backed tests.

The session-scoped Redpanda broker (the ``redpanda``/``bootstrap`` fixtures) lives
in the repo-root ``conftest.py`` so the benchmark suite can share it; this file
keeps only the test-suite-specific collection hook.

Broker-backed tests are auto-marked ``integration`` (see
``pytest_collection_modifyitems``) so the broker-free unit suite can be selected
on its own:

    uv run pytest -m "not integration"   # no Docker required
    uv run pytest                         # full suite; needs Docker
"""

from __future__ import annotations

import pytest


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
