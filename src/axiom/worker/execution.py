"""Fencing-aware execution: heartbeat, cost-safety abort, and the poison-pill check.

Two complementary, deliberately separate mechanisms sit on top of
worker.py's primitives:

- The periodic heartbeat owns keeping the lease alive. It runs regardless
  of what kind of work is happening — the only thing that protects a
  slow, non-streaming call with no chunk boundary to check against.
- stream_guard() is a read-only, fast-path detector layered on top, only
  relevant to streaming work. It never renews the lease itself — that
  would duplicate the heartbeat's job — it only checks, so a losing
  worker can abort mid-stream instead of waiting for the next heartbeat
  tick to notice.

Both raise WorkerFencedError on supersession. That's the single signal
the orchestration layer needs: abandon this execution, do not settle, do
not ack — the Last-In-Chain ordering means an un-acked message simply
gets redelivered to whoever holds the current lease.
"""

import asyncio
import logging
from collections.abc import AsyncIterator, Coroutine
from typing import Any
from uuid import UUID

import asyncpg

from axiom.contracts.enums import WorkflowStatus
from axiom.worker.worker import ClaimedWorkflow, renew_lease, settle_terminal

logger = logging.getLogger("axiom.worker")


class WorkerFencedError(Exception):
    """This worker has been superseded — a later claim now owns the row.

    Raised by both the heartbeat path and stream_guard(). Either way the
    required response is identical: abandon execution, do not settle, do
    not ack.
    """


async def _heartbeat_loop(
    pool: asyncpg.Pool,
    *,
    workflow_id: UUID,
    lease_generation: int,
    lease_seconds: int,
    interval_seconds: float,
) -> None:
    """Renew the lease every interval_seconds until cancelled or until renewal fails.

    A renewal that raises an exception is treated the same as an explicit
    False: "cannot confirm ownership" must mean "assume the worst and
    stop," per the cost-safety philosophy — fail-safe, not fail-open.
    """
    while True:
        await asyncio.sleep(interval_seconds)
        try:
            ok = await renew_lease(
                pool,
                workflow_id=workflow_id,
                lease_generation=lease_generation,
                lease_seconds=lease_seconds,
            )
        except Exception:
            logger.exception(
                "heartbeat renewal errored for workflow_id=%s; treating as fenced", workflow_id
            )
            return
        if not ok:
            logger.warning("heartbeat detected fencing for workflow_id=%s", workflow_id)
            return


async def execute_with_heartbeat[T](
    pool: asyncpg.Pool,
    coro: Coroutine[Any, Any, T],
    *,
    workflow_id: UUID,
    lease_generation: int,
    lease_seconds: int,
    heartbeat_interval_seconds: float,
) -> T:
    """Run coro to completion while a background task renews the lease periodically.

    Races the execution against the heartbeat: if the heartbeat loop ever
    stops (fenced out, or a renewal error), the execution is canceled
    immediately and WorkerFencedError is raised instead of returning a
    result. A genuine exception from coro itself propagates normally.
    """
    heartbeat_task = asyncio.create_task(
        _heartbeat_loop(
            pool,
            workflow_id=workflow_id,
            lease_generation=lease_generation,
            lease_seconds=lease_seconds,
            interval_seconds=heartbeat_interval_seconds,
        )
    )
    execution_task = asyncio.create_task(coro)

    done, _pending = await asyncio.wait(
        {heartbeat_task, execution_task}, return_when=asyncio.FIRST_COMPLETED
    )

    if execution_task in done:
        heartbeat_task.cancel()
        try:
            await heartbeat_task
        except asyncio.CancelledError:
            pass
        return execution_task.result()

    execution_task.cancel()
    try:
        await execution_task
    except asyncio.CancelledError:
        pass
    raise WorkerFencedError(f"worker fenced out during execution, workflow_id={workflow_id}")


async def _lease_generation_matches(
    pool: asyncpg.Pool, *, workflow_id: UUID, lease_generation: int
) -> bool:
    """Read-only fencing check — never renews, only detects."""
    async with pool.acquire() as conn:
        current = await conn.fetchval(
            "SELECT lease_generation FROM workflow_states WHERE id = $1", workflow_id
        )
    return bool(current == lease_generation)


async def stream_guard[ItemT](
    pool: asyncpg.Pool,
    source: AsyncIterator[ItemT],
    *,
    workflow_id: UUID,
    lease_generation: int,
) -> AsyncIterator[ItemT]:
    """Wrap a streaming execution with a per-chunk fencing check for cost-safety.

    After every yielded item, checks whether this worker still holds the
    current lease_generation. On the first failed check, attempts to
    close the underlying source (stopping further generation — and
    billing) before raising WorkerFencedError. The already-yielded item
    is not withheld: it was already generated, and cost-safety is about
    not producing the *next* one, not clawing back the last.
    """
    async for item in source:
        yield item
        if not await _lease_generation_matches(
            pool, workflow_id=workflow_id, lease_generation=lease_generation
        ):
            aclose = getattr(source, "aclose", None)
            if aclose is not None:
                await aclose()
            raise WorkerFencedError(
                f"stream_guard: fenced out mid-stream, workflow_id={workflow_id}"
            )


async def check_and_handle_poison_pill(
    pool: asyncpg.Pool, claimed: ClaimedWorkflow, *, max_retries: int
) -> bool:
    """Dead-letter immediately if this claim's generation exceeds max_retries.

    lease_generation only increments on a genuine claim/reclaim — never
    on a fenced-out write — which is what makes it safe to use natively
    as the attempt counter here, with no separate column needed.

    Returns True if dead-lettered (caller should ack and skip execution
    entirely), False if execution should proceed normally. Raises
    WorkerFencedError if the dead-letter write itself was fenced out — a
    newer claim already superseded this one, so the same "do not ack"
    rule applies as everywhere else.
    """
    if claimed.lease_generation <= max_retries:
        return False

    ok = await settle_terminal(
        pool,
        workflow_id=claimed.id,
        lease_generation=claimed.lease_generation,
        status=WorkflowStatus.DEAD_LETTERED,
        error_log={
            "error": "max_retries exceeded",
            "max_retries": max_retries,
            "lease_generation": claimed.lease_generation,
        },
    )
    if not ok:
        raise WorkerFencedError(
            f"poison-pill dead-letter write fenced out, workflow_id={claimed.id}"
        )
    return True