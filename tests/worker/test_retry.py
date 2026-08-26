"""Retry of transient handler failures.

Before this existed, any exception from a handler was terminal: one 429 or
dropped connection from a provider killed a workflow permanently. These
tests drive the real loop — Relay and Worker together against real Postgres
and Redis — because the retry path is a round trip through the outbox, and
testing only the worker half would prove the message was written, not that
anything ever picks it up again.
"""

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any
from uuid import UUID, uuid4

import asyncpg
import pytest
from redis.asyncio import Redis

from axiom.ingress.repository import submit_workflow
from axiom.relay.runner import run_forever as relay_run_forever
from axiom.worker.execution import NonRetryableError, retry_delay_seconds
from axiom.worker.runner import HandlerRegistry
from axiom.worker.runner import run_forever as worker_run_forever
from axiom.worker.worker import claim_workflow, schedule_retry

# Small enough that retries are effectively immediate; the backoff maths is
# covered separately by the pure-function test at the bottom.
FAST_BASE = 0.01
FAST_CAP = 0.01


async def _terminal_status(pool: asyncpg.Pool, workflow_id: UUID) -> str | None:
    async with pool.acquire() as conn:
        status: str | None = await conn.fetchval(
            "SELECT status FROM workflow_states WHERE id = $1 AND status IN "
            "('COMPLETED', 'FAILED', 'CANCELED', 'DEAD_LETTERED', 'DISPATCH_FAILED')",
            workflow_id,
        )
    return status


async def _run_until_terminal(
    pool: asyncpg.Pool,
    redis_client: Redis,
    *,
    workflow_version: str,
    workflow_id: UUID,
    handlers: HandlerRegistry,
    max_retries: int,
    timeout: float = 15.0,
) -> str | None:
    """Run a Relay and a Worker together until the workflow reaches a terminal state."""
    stream = f"workflow_stream_{workflow_version}"
    relay_stop = asyncio.Event()
    worker_stop = asyncio.Event()

    relay_task = asyncio.create_task(
        relay_run_forever(
            pool,
            redis_client,
            instance_id=uuid4(),
            batch_size=10,
            claim_lease_seconds=30,
            max_retries=5,
            poll_interval_seconds=0.05,
            shutdown_event=relay_stop,
        )
    )
    worker_task = asyncio.create_task(
        worker_run_forever(
            pool,
            redis_client,
            stream_name=stream,
            consumer_name=f"w-{uuid4()}",
            worker_id=uuid4(),
            handlers=handlers,
            lease_seconds=30,
            heartbeat_interval_seconds=10,
            xautoclaim_min_idle_seconds=35,
            max_retries=max_retries,
            retry_base_seconds=FAST_BASE,
            retry_cap_seconds=FAST_CAP,
            batch_size=10,
            shutdown_event=worker_stop,
        )
    )

    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    status: str | None = None
    while loop.time() < deadline:
        status = await _terminal_status(pool, workflow_id)
        if status is not None:
            break
        await asyncio.sleep(0.05)

    relay_stop.set()
    worker_stop.set()
    await asyncio.wait_for(asyncio.gather(relay_task, worker_task), timeout=5.0)
    return status


async def _submit(pool: asyncpg.Pool, *, workflow_type: str, workflow_version: str) -> UUID:
    result = await submit_workflow(
        pool,
        workflow_type=workflow_type,
        workflow_version=workflow_version,
        idempotency_key=f"retry_{uuid4()}",
        input_data={},
    )
    return result.id


async def test_transient_failure_is_retried_until_it_succeeds(
    pool: asyncpg.Pool, redis_client: Redis
) -> None:
    """A handler that fails twice then succeeds completes, having been attempted 3 times."""
    version = f"t{uuid4().hex[:8]}"
    attempts = 0

    async def flaky(
        p: asyncpg.Pool, wid: UUID, generation: int, input_data: dict[str, Any]
    ) -> dict[str, Any]:
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise ConnectionError("provider unavailable")
        return {"attempts": attempts}

    workflow_id = await _submit(pool, workflow_type="flaky", workflow_version=version)

    status = await _run_until_terminal(
        pool,
        redis_client,
        workflow_version=version,
        workflow_id=workflow_id,
        handlers={"flaky": flaky},
        max_retries=5,
    )

    assert status == "COMPLETED", f"expected COMPLETED after retries, got {status}"
    assert attempts == 3, f"handler should have run 3 times, ran {attempts}"

    async with pool.acquire() as conn:
        generation = await conn.fetchval(
            "SELECT lease_generation FROM workflow_states WHERE id = $1", workflow_id
        )
    # One claim per attempt: lease_generation is the attempt counter the
    # poison-pill ceiling is measured against.
    assert generation == 3


async def test_non_retryable_error_is_terminal_immediately(
    pool: asyncpg.Pool, redis_client: Redis
) -> None:
    """NonRetryableError fails the workflow on the first attempt, with no redispatch."""
    version = f"t{uuid4().hex[:8]}"
    attempts = 0

    async def permanent(
        p: asyncpg.Pool, wid: UUID, generation: int, input_data: dict[str, Any]
    ) -> dict[str, Any]:
        nonlocal attempts
        attempts += 1
        raise NonRetryableError("prompt rejected")

    workflow_id = await _submit(pool, workflow_type="permanent", workflow_version=version)

    status = await _run_until_terminal(
        pool,
        redis_client,
        workflow_version=version,
        workflow_id=workflow_id,
        handlers={"permanent": permanent},
        max_retries=5,
    )

    assert status == "FAILED"
    assert attempts == 1, f"a permanent failure must not be retried, ran {attempts} times"

    async with pool.acquire() as conn:
        outbox_rows = await conn.fetchval(
            "SELECT count(*) FROM workflow_outbox WHERE workflow_id = $1", workflow_id
        )
    # Only the original dispatch from Ingress; no retry was scheduled.
    assert outbox_rows == 1


async def test_retries_are_bounded_and_end_in_dead_letter(
    pool: asyncpg.Pool, redis_client: Redis
) -> None:
    """An always-failing handler stops after max_retries attempts and dead-letters."""
    version = f"t{uuid4().hex[:8]}"
    max_retries = 2
    attempts = 0

    async def always_fails(
        p: asyncpg.Pool, wid: UUID, generation: int, input_data: dict[str, Any]
    ) -> dict[str, Any]:
        nonlocal attempts
        attempts += 1
        raise TimeoutError("provider timed out")

    workflow_id = await _submit(pool, workflow_type="always_fails", workflow_version=version)

    status = await _run_until_terminal(
        pool,
        redis_client,
        workflow_version=version,
        workflow_id=workflow_id,
        handlers={"always_fails": always_fails},
        max_retries=max_retries,
    )

    assert status == "DEAD_LETTERED"
    # Attempts are capped: the claim after the ceiling dead-letters before
    # the handler is invoked again, so this cannot run away.
    assert attempts == max_retries, f"expected {max_retries} attempts, got {attempts}"


async def test_schedule_retry_is_fenced_against_a_superseded_worker(
    pool: asyncpg.Pool, make_workflow_row: Callable[..., Awaitable[UUID]]
) -> None:
    """A worker that lost its lease cannot release and redispatch the workflow."""
    workflow_id = await make_workflow_row(
        idempotency_key=f"fenced_retry_{uuid4()}", workflow_type="fenced"
    )
    claimed = await claim_workflow(
        pool, workflow_id=workflow_id, worker_id=uuid4(), lease_seconds=30
    )
    assert claimed is not None

    stale_generation = claimed.lease_generation
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE workflow_states SET lease_generation = lease_generation + 1 WHERE id = $1",
            workflow_id,
        )

    scheduled = await schedule_retry(
        pool,
        workflow_id=workflow_id,
        lease_generation=stale_generation,
        delay_seconds=0.0,
        error_log={"error": "should not land"},
    )

    assert scheduled is False
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT status, (SELECT count(*) FROM workflow_outbox WHERE workflow_id = $1) "
            "AS outbox_count FROM workflow_states WHERE id = $1",
            workflow_id,
        )
    assert row is not None
    # Still RUNNING under the new generation, and no redispatch was written.
    assert row["status"] == "RUNNING"
    assert row["outbox_count"] == 0


@pytest.mark.parametrize("attempt", [1, 2, 3, 8, 20])
def test_retry_delay_is_bounded_and_jittered(attempt: int) -> None:
    """Delays stay within the exponential ceiling and are not a fixed value."""
    base, cap = 1.0, 60.0
    ceiling = min(cap, base * 2 ** (attempt - 1))

    samples = [retry_delay_seconds(attempt, base_seconds=base, cap_seconds=cap) for _ in range(200)]

    assert all(0.0 <= s <= ceiling for s in samples)
    # Full jitter, not exponential-with-a-wobble: the point is decorrelating
    # workflows that all failed on the same provider outage, which a narrow
    # spread around the ceiling would not achieve.
    assert min(samples) < ceiling * 0.25
    assert max(samples) > ceiling * 0.75
