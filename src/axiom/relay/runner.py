"""The Relay's continuous run loop."""

import asyncio
import logging
from uuid import UUID

import asyncpg
from redis.asyncio import Redis

from axiom.relay.relay import run_relay_cycle

logger = logging.getLogger("axiom.relay")


async def run_forever(
    pool: asyncpg.Pool,
    redis: Redis,
    *,
    instance_id: UUID,
    batch_size: int,
    claim_lease_seconds: int,
    max_retries: int,
    poll_interval_seconds: float,
    shutdown_event: asyncio.Event,
) -> None:
    """Claim, publish, and settle repeatedly until shutdown_event is set.

    An idle cycle (nothing to claim) waits up to poll_interval_seconds
    before retrying, so an empty outbox doesn't become a tight loop
    hammering Postgres — but a shutdown signal during that wait
    interrupts it immediately rather than waiting out the full interval.

    A cycle that found work loops immediately to drain any backlog.
    """
    logger.info("relay loop starting, instance_id=%s", instance_id)

    while not shutdown_event.is_set():
        processed = await run_relay_cycle(
            pool,
            redis,
            instance_id=instance_id,
            batch_size=batch_size,
            claim_lease_seconds=claim_lease_seconds,
            max_retries=max_retries,
        )

        if processed == 0:
            try:
                await asyncio.wait_for(shutdown_event.wait(), timeout=poll_interval_seconds)
            except TimeoutError:
                pass  # normal: poll interval elapsed with no shutdown signal

    logger.info("relay loop stopped, instance_id=%s", instance_id)