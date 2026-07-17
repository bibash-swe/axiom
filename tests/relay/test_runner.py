"""Tests for the Relay's continuous run loop."""

import asyncio
from collections.abc import Awaitable, Callable
from uuid import UUID, uuid4

import asyncpg
from redis.asyncio import Redis

from axiom.relay.runner import run_forever

MakeOutboxRow = Callable[..., Awaitable[tuple[UUID, UUID]]]


async def test_run_forever_processes_work_and_stops_on_shutdown(
    pool: asyncpg.Pool, redis_client: Redis, make_outbox_row: MakeOutboxRow
) -> None:
    """The loop dispatches pending work.

    Shutdown interrupts an idle wait rather than waiting out the full
    poll interval.
    """
    version = f"test_{uuid4().hex[:8]}"
    _, outbox_id = await make_outbox_row(
        idempotency_key=f"k_{uuid4()}", workflow_version=version
    )

    shutdown_event = asyncio.Event()
    task = asyncio.create_task(
        run_forever(
            pool,
            redis_client,
            instance_id=uuid4(),
            batch_size=100,
            claim_lease_seconds=30,
            max_retries=5,
            poll_interval_seconds=5.0,
            shutdown_event=shutdown_event,
        )
    )

    await asyncio.sleep(0.2)
    shutdown_event.set()

    await asyncio.wait_for(task, timeout=1.0)

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT dispatched FROM workflow_outbox WHERE id = $1", outbox_id
        )
    assert row is not None
    assert row["dispatched"] is True