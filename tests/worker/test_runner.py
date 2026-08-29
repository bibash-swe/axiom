"""Tests for the Worker's message processing and consumption loop."""

import asyncio
import json
from collections.abc import Awaitable, Callable
from typing import Any
from uuid import UUID, uuid4

import asyncpg
from redis.asyncio import Redis

from axiom.worker.execution import NonRetryableError
from axiom.worker.runner import HandlerRegistry, run_forever

DispatchWorkflow = Callable[..., Awaitable[UUID]]


async def _echo_handler(
    pool: asyncpg.Pool, workflow_id: UUID, generation: int, input_data: dict[str, Any]
) -> dict[str, Any]:
    return {"echoed": input_data}


async def _failing_handler(
    pool: asyncpg.Pool, workflow_id: UUID, generation: int, input_data: dict[str, Any]
) -> dict[str, Any]:
    raise ValueError("deliberate handler failure")


async def _permanent_handler(
    pool: asyncpg.Pool, workflow_id: UUID, generation: int, input_data: dict[str, Any]
) -> dict[str, Any]:
    raise NonRetryableError("deliberate permanent failure")


async def _run_briefly(
    pool: asyncpg.Pool,
    redis_client: Redis,
    *,
    stream_name: str,
    handlers: HandlerRegistry,
    duration: float = 0.5,
) -> None:
    """Run the loop for a short, bounded window, then shut it down cleanly."""
    shutdown = asyncio.Event()
    task = asyncio.create_task(
        run_forever(
            pool,
            redis_client,
            stream_name=stream_name,
            consumer_name=f"w-{uuid4()}",
            worker_id=uuid4(),
            handlers=handlers,
            lease_seconds=30,
            heartbeat_interval_seconds=10,
            xautoclaim_min_idle_seconds=35,
            max_retries=5,
            retry_base_seconds=0.01,
            retry_cap_seconds=0.01,
            max_chain_depth=50,
            batch_size=10,
            shutdown_event=shutdown,
        )
    )
    await asyncio.sleep(duration)
    shutdown.set()
    await asyncio.wait_for(task, timeout=2.0)


async def test_happy_path_completes_and_acks(
    pool: asyncpg.Pool, redis_client: Redis, dispatch_workflow: DispatchWorkflow
) -> None:
    """A dispatched workflow with a registered handler completes and is acked."""
    stream = f"workflow_stream_test_{uuid4().hex[:8]}"
    wid = await dispatch_workflow(stream_name=stream, workflow_type="echo", input_data={"x": 1})

    await _run_briefly(pool, redis_client, stream_name=stream, handlers={"echo": _echo_handler})

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT status, output_data FROM workflow_states WHERE id = $1", wid
        )
    assert row is not None
    assert row["status"] == "COMPLETED"
    assert json.loads(row["output_data"]) == {"echoed": {"x": 1}}

    pending = await redis_client.xpending(stream, "workers")
    assert pending["pending"] == 0


async def test_unregistered_workflow_type_fails_and_acks(
    pool: asyncpg.Pool, redis_client: Redis, dispatch_workflow: DispatchWorkflow
) -> None:
    """A workflow_type with no registered handler fails cleanly, not a crash."""
    stream = f"workflow_stream_test_{uuid4().hex[:8]}"
    wid = await dispatch_workflow(stream_name=stream, workflow_type="nonexistent")

    await _run_briefly(pool, redis_client, stream_name=stream, handlers={})

    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT status FROM workflow_states WHERE id = $1", wid)
    assert row is not None
    assert row["status"] == "FAILED"

    pending = await redis_client.xpending(stream, "workers")
    assert pending["pending"] == 0


async def test_handler_exception_releases_for_retry_and_acks(
    pool: asyncpg.Pool, redis_client: Redis, dispatch_workflow: DispatchWorkflow
) -> None:
    """A handler that raises is caught, released back to PENDING, redispatched, and acked.

    An ordinary exception is assumed transient — the failures that dominate
    this workload are. The current message is acked because the retry
    arrives as a new one; see test_retry.py for the full round trip.
    """
    stream = f"workflow_stream_test_{uuid4().hex[:8]}"
    wid = await dispatch_workflow(stream_name=stream, workflow_type="always_fails")

    await _run_briefly(
        pool, redis_client, stream_name=stream, handlers={"always_fails": _failing_handler}
    )

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT status, error_log, worker_id, lease_expires_at "
            "FROM workflow_states WHERE id = $1",
            wid,
        )
        redispatches = await conn.fetchval(
            "SELECT count(*) FROM workflow_outbox WHERE workflow_id = $1", wid
        )
    assert row is not None
    assert row["status"] == "PENDING"
    assert "deliberate handler failure" in row["error_log"]
    # Ownership is released along with the status, or nothing else could claim it.
    assert row["worker_id"] is None
    assert row["lease_expires_at"] is None
    assert redispatches == 1

    pending = await redis_client.xpending(stream, "workers")
    assert pending["pending"] == 0


async def test_non_retryable_handler_exception_is_terminal_and_acks(
    pool: asyncpg.Pool, redis_client: Redis, dispatch_workflow: DispatchWorkflow
) -> None:
    """NonRetryableError settles FAILED with no redispatch — the opt-out path."""
    stream = f"workflow_stream_test_{uuid4().hex[:8]}"
    wid = await dispatch_workflow(stream_name=stream, workflow_type="permanent")

    await _run_briefly(
        pool, redis_client, stream_name=stream, handlers={"permanent": _permanent_handler}
    )

    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT status FROM workflow_states WHERE id = $1", wid)
        redispatches = await conn.fetchval(
            "SELECT count(*) FROM workflow_outbox WHERE workflow_id = $1", wid
        )
    assert row is not None
    assert row["status"] == "FAILED"
    assert redispatches == 0

    pending = await redis_client.xpending(stream, "workers")
    assert pending["pending"] == 0


async def test_reclaims_a_row_left_by_a_crashed_worker(
    pool: asyncpg.Pool, redis_client: Redis
) -> None:
    """A row left mid-execution by a crashed worker is reclaimed and completed.

    Simulated via a pre-claimed, stale-leased row; XAUTOCLAIM picks up
    the corresponding stream message, and generation correctly increments.
    """
    stream = f"workflow_stream_test_{uuid4().hex[:8]}"

    async with pool.acquire() as conn:
        wid = await conn.fetchval(
            "INSERT INTO workflow_states "
            "(workflow_type, workflow_version, idempotency_key, input_data, "
            " status, worker_id, lease_generation, lease_expires_at) "
            "VALUES ('echo', 'v1', $1, '{}'::jsonb, 'RUNNING', $2, 1, NOW() - INTERVAL '1 hour') "
            "RETURNING id",
            f"reclaim_{uuid4()}",
            uuid4(),
        )
    payload = json.dumps({"event_type": "WORKFLOW_STARTED", "workflow_id": str(wid)})
    await redis_client.xadd(stream, {"payload": payload})

    await _run_briefly(pool, redis_client, stream_name=stream, handlers={"echo": _echo_handler})

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT status, lease_generation FROM workflow_states WHERE id = $1", wid
        )
    assert row is not None
    assert row["status"] == "COMPLETED"
    assert row["lease_generation"] == 2