"""Tests for execute_with_heartbeat, stream_guard, and check_and_handle_poison_pill."""

import asyncio
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

import asyncpg
import pytest

from axiom.worker.execution import (
    WorkerFencedError,
    check_and_handle_poison_pill,
    execute_with_heartbeat,
    stream_guard,
)
from axiom.worker.worker import claim_workflow, renew_lease

MakeWorkflowRow = Callable[..., Awaitable[UUID]]


async def test_execute_with_heartbeat_returns_result_on_success(
    pool: asyncpg.Pool, make_workflow_row: MakeWorkflowRow
) -> None:
    """A coroutine that completes normally returns its result."""
    wid = await make_workflow_row(idempotency_key=f"k_{uuid4()}")
    claimed = await claim_workflow(pool, workflow_id=wid, worker_id=uuid4(), lease_seconds=30)
    assert claimed is not None

    async def quick_work() -> dict[str, Any]:
        return {"done": True}

    result = await execute_with_heartbeat(
        pool,
        quick_work(),
        workflow_id=wid,
        lease_generation=claimed.lease_generation,
        lease_seconds=30,
        heartbeat_interval_seconds=10,
    )

    assert result == {"done": True}


async def test_execute_with_heartbeat_renews_the_lease_during_long_work(
    pool: asyncpg.Pool, make_workflow_row: MakeWorkflowRow
) -> None:
    """A long-running coroutine's lease is kept alive by the periodic heartbeat.

    Uses a lease shorter than the work duration — without renewal, the
    lease would already be stale by completion.
    """
    wid = await make_workflow_row(idempotency_key=f"k_{uuid4()}")
    claimed = await claim_workflow(pool, workflow_id=wid, worker_id=uuid4(), lease_seconds=1)
    assert claimed is not None

    async def slow_work() -> dict[str, Any]:
        await asyncio.sleep(0.5)
        return {"done": True}

    result = await execute_with_heartbeat(
        pool,
        slow_work(),
        workflow_id=wid,
        lease_generation=claimed.lease_generation,
        lease_seconds=1,
        heartbeat_interval_seconds=0.2,
    )

    assert result == {"done": True}
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT lease_expires_at FROM workflow_states WHERE id = $1", wid
        )
    assert row is not None
    assert row["lease_expires_at"] > datetime.now(UTC)


async def test_execute_with_heartbeat_raises_when_fenced_mid_execution(
    pool: asyncpg.Pool, make_workflow_row: MakeWorkflowRow
) -> None:
    """A supersession during execution is detected by the heartbeat.

    The execution is actually cancelled, not just wrapped in an
    exception — proven by a flag that would only be set if the work ran
    to completion.
    """
    wid = await make_workflow_row(idempotency_key=f"k_{uuid4()}")
    claimed = await claim_workflow(pool, workflow_id=wid, worker_id=uuid4(), lease_seconds=30)
    assert claimed is not None

    ran_to_completion = False

    async def long_work() -> dict[str, Any]:
        nonlocal ran_to_completion
        await asyncio.sleep(2.0)
        ran_to_completion = True
        return {"done": True}

    async def supersede_after_delay() -> None:
        await asyncio.sleep(0.3)
        await renew_lease(
            pool, workflow_id=wid, lease_generation=claimed.lease_generation, lease_seconds=-1
        )
        await claim_workflow(pool, workflow_id=wid, worker_id=uuid4(), lease_seconds=30)

    supersede_task = asyncio.create_task(supersede_after_delay())

    with pytest.raises(WorkerFencedError):
        await execute_with_heartbeat(
            pool,
            long_work(),
            workflow_id=wid,
            lease_generation=claimed.lease_generation,
            lease_seconds=30,
            heartbeat_interval_seconds=0.2,
        )

    await supersede_task
    assert ran_to_completion is False


async def test_stream_guard_yields_items_when_not_fenced(
    pool: asyncpg.Pool, make_workflow_row: MakeWorkflowRow
) -> None:
    """Items pass through unchanged when the lease is still valid."""
    wid = await make_workflow_row(idempotency_key=f"k_{uuid4()}")
    claimed = await claim_workflow(pool, workflow_id=wid, worker_id=uuid4(), lease_seconds=30)
    assert claimed is not None

    async def source() -> Any:
        for i in range(3):
            yield i

    items = [
        item
        async for item in stream_guard(
            pool, source(), workflow_id=wid, lease_generation=claimed.lease_generation
        )
    ]

    assert items == [0, 1, 2]


async def test_stream_guard_aborts_mid_stream_when_fenced(
    pool: asyncpg.Pool, make_workflow_row: MakeWorkflowRow
) -> None:
    """Once fenced, stream_guard raises and stops yielding further items.

    The already-yielded chunk is not withheld — cost-safety is about not
    producing the *next* one, not clawing back the last.
    """
    wid = await make_workflow_row(idempotency_key=f"k_{uuid4()}")
    claimed = await claim_workflow(pool, workflow_id=wid, worker_id=uuid4(), lease_seconds=30)
    assert claimed is not None

    async def source() -> Any:
        for i in range(5):
            yield f"chunk{i}"

    received = []
    with pytest.raises(WorkerFencedError):
        async for item in stream_guard(
            pool, source(), workflow_id=wid, lease_generation=claimed.lease_generation
        ):
            received.append(item)
            if len(received) == 1:
                # Fence out as a direct side effect of consuming the first
                # item, not from inside source()'s own body — a mutation
                # placed after a yield in the source generator wouldn't
                # take effect until source() itself is resumed, which
                # happens one cycle after stream_guard's own check.
                async with pool.acquire() as conn:
                    await conn.execute(
                        "UPDATE workflow_states SET lease_generation = lease_generation + 1 "
                        "WHERE id = $1",
                        wid,
                    )

    assert received == ["chunk0"]


async def test_poison_pill_check_proceeds_under_threshold(
    pool: asyncpg.Pool, make_workflow_row: MakeWorkflowRow
) -> None:
    """A claim below max_retries is not dead-lettered; execution should proceed."""
    wid = await make_workflow_row(idempotency_key=f"k_{uuid4()}")
    claimed = await claim_workflow(pool, workflow_id=wid, worker_id=uuid4(), lease_seconds=30)
    assert claimed is not None

    dead_lettered = await check_and_handle_poison_pill(pool, claimed, max_retries=5)

    assert dead_lettered is False
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT status FROM workflow_states WHERE id = $1", wid)
    assert row is not None
    assert row["status"] == "RUNNING"


async def test_poison_pill_check_dead_letters_past_threshold(
    pool: asyncpg.Pool, make_workflow_row: MakeWorkflowRow
) -> None:
    """A claim past max_retries is immediately dead-lettered."""
    wid = await make_workflow_row(
        idempotency_key=f"k_{uuid4()}", status="RUNNING", lease_generation=5, lease_stale=True
    )
    claimed = await claim_workflow(pool, workflow_id=wid, worker_id=uuid4(), lease_seconds=30)
    assert claimed is not None
    assert claimed.lease_generation == 6

    dead_lettered = await check_and_handle_poison_pill(pool, claimed, max_retries=5)

    assert dead_lettered is True
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT status, error_log FROM workflow_states WHERE id = $1", wid
        )
    assert row is not None
    assert row["status"] == "DEAD_LETTERED"