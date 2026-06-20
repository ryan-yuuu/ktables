"""Cross-process open-loop writer entrypoint for M1/M7.

Runs in a **separate OS process** (spawned by the propagation benchmark) so the
reader/measurement process owns its event loop alone — the headline-propagation
topology for sustained-load cells (plan §4.2). The send timestamp embedded by
``stamping_encoder`` is system-wide ``CLOCK_MONOTONIC``, comparable to the reader's
clock on the same host, so the reader's ``on_set`` computes ``t_apply - t_send``
across the process boundary.

``writer_main`` is a top-level function so it survives the ``spawn`` start method
(macOS / modern Linux default). It publishes ``count`` stamped records at
``rate_hz`` and returns a small saturation verdict to the parent via the queue.
"""

from __future__ import annotations

import asyncio
from typing import Any

from benchmarks._harness import run_open_loop, stamping_encoder
from ktables import KafkaTableWriter


async def _publish(bootstrap: str, topic: str, rate_hz: float, count: int, payload_size: int, key_prefix: str):
    encode = stamping_encoder(payload_size)
    writer: KafkaTableWriter[int] = KafkaTableWriter(bootstrap_servers=bootstrap, topic=topic, value_encoder=encode, ensure_topic=False)
    async with writer:
        return await run_open_loop(rate_hz=rate_hz, count=count, send=lambda i: writer.set(f"{key_prefix}{i}", i))


def writer_main(bootstrap: str, topic: str, rate_hz: float, count: int, payload_size: int, key_prefix: str, result_queue: Any) -> None:
    """Process entrypoint: publish the open-loop load, then report the generator's
    saturation verdict (did it keep up with the target send rate?) to the parent."""
    run = asyncio.run(_publish(bootstrap, topic, rate_hz, count, payload_size, key_prefix))
    result_queue.put(run.assess().as_summary())
