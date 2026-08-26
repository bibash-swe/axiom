"""Does one worker actually work on more than one message at a time?

XREADGROUP is called with count=batch_size, so up to batch_size messages
enter this consumer's PEL at once. If they are then processed serially, a
worker holds messages it is not working on — invisible to every other
worker until min_idle_time elapses — and its effective concurrency is 1
regardless of how many it fetched.

These tests measure that directly rather than inferring it.
"""

import asyncio
import json
from collections.abc import Awaitable, Callable
from typing import Any
from uuid import UUID, uuid4

import asyncpg
from redis.asyncio import Redis

from axiom.worker.runner import HandlerRegistry, run_forever

DispatchWorkflow = Callable[..., Awaitable[UUID]]

HANDLER_SECONDS = 0.4
WORKFLOW_COUNT = 5


class ConcurrencyTracker:
    """Records how many handler invocations overlap, and how many ran in total."""

    def __init__(self) -> None:
        """Start with nothing recorded."""
        self.active = 0
        self.max_active = 0
        self.invocations: list[UUID] = []

    def enter(self, workflow_id: UUID) -> None:
        """Record a handler starting, updating the high-water mark."""
        self.invocations.append(workflow_id)
        self.active += 1
        self.max_active = max(self.max_active, self.active)

    def exit(self) -> None:
        """Record a handler finishing."""
        self.active -= 1


def _make_slow_handler(tracker: ConcurrencyTracker) -> Any:
    async def _handler(
        pool: asyncpg.Pool, workflow_id: UUID, generation: int, input_data: dict[str, Any]
    ) -> dict[str, Any]:
        tracker.enter(workflow_id)
        try:
            await asyncio.sleep(HANDLER_SECONDS)
            return {"ok": True}
        finally:
            tracker.exit()

    return _handler


async def _all_terminal(pool: asyncpg.Pool, workflow_ids: list[UUID]) -> bool:
    async with pool.acquire() as conn:
        remaining = await conn.fetchval(
            "SELECT count(*) FROM workflow_states "
            "WHERE id = ANY($1::uuid[]) AND status NOT IN "
            "('COMPLETED', 'FAILED', 'CANCELED', 'DEAD_LETTERED', 'DISPATCH_FAILED')",
            workflow_ids,
        )
    return bool(remaining == 0)


async def _run_until_done(
    pool: asyncpg.Pool,
    redis_client: Redis,
    *,
    stream_name: str,
    handlers: HandlerRegistry,
    workflow_ids: list[UUID],
    timeout: float,
    consumer_suffix: str = "",
) -> float:
    """Run one worker until every workflow is terminal. Returns elapsed seconds."""
    shutdown = asyncio.Event()
    task = asyncio.create_task(
        run_forever(
            pool,
            redis_client,
            stream_name=stream_name,
            consumer_name=f"w{consumer_suffix}-{uuid4()}",
            worker_id=uuid4(),
            handlers=handlers,
            lease_seconds=30,
            heartbeat_interval_seconds=10,
            xautoclaim_min_idle_seconds=35,
            max_retries=5,
            batch_size=10,
            shutdown_event=shutdown,
        )
    )
    loop = asyncio.get_running_loop()
    start = loop.time()
    while loop.time() - start < timeout:
        if await _all_terminal(pool, workflow_ids):
            break
        await asyncio.sleep(0.05)
    elapsed = loop.time() - start
    shutdown.set()
    await asyncio.wait_for(task, timeout=5.0)
    return elapsed


async def test_worker_works_on_a_batch_concurrently(
    pool: asyncpg.Pool, redis_client: Redis, dispatch_workflow: DispatchWorkflow
) -> None:
    """One worker given N slow messages should overlap them, not run them one at a time.

    XREADGROUP already pulled all N into this consumer's PEL, so processing
    them serially means N-1 of them are held, unworked and unavailable to
    any other worker, for the whole run.
    """
    stream = f"workflow_stream_test_{uuid4().hex[:8]}"
    tracker = ConcurrencyTracker()

    workflow_ids = [
        await dispatch_workflow(stream_name=stream, workflow_type="slow")
        for _ in range(WORKFLOW_COUNT)
    ]

    elapsed = await _run_until_done(
        pool,
        redis_client,
        stream_name=stream,
        handlers={"slow": _make_slow_handler(tracker)},
        workflow_ids=workflow_ids,
        timeout=WORKFLOW_COUNT * HANDLER_SECONDS * 3,
    )

    assert await _all_terminal(pool, workflow_ids), "not every workflow reached a terminal state"
    assert len(tracker.invocations) == WORKFLOW_COUNT, (
        f"expected {WORKFLOW_COUNT} handler invocations, got {len(tracker.invocations)}"
    )

    # The load-bearing assertion: messages already in this consumer's PEL
    # must be worked on in parallel, not queued behind each other.
    assert tracker.max_active > 1, (
        f"worker never had more than {tracker.max_active} handler running at once. "
        f"It fetched {WORKFLOW_COUNT} messages into its PEL and processed them "
        f"serially, so effective concurrency is 1 and the other "
        f"{WORKFLOW_COUNT - 1} were held unworked."
    )

    serial_floor = WORKFLOW_COUNT * HANDLER_SECONDS
    assert elapsed < serial_floor * 0.7, (
        f"took {elapsed:.2f}s; serial execution would take ~{serial_floor:.2f}s, "
        "so this is not running concurrently"
    )


async def test_no_workflow_is_executed_twice_under_two_workers(
    pool: asyncpg.Pool, redis_client: Redis, dispatch_workflow: DispatchWorkflow
) -> None:
    """Two workers on one stream must still execute each workflow exactly once.

    Guards the concurrency change against introducing duplicate execution:
    the claim query, not the fetch pattern, is what makes this safe, and it
    must stay that way.
    """
    stream = f"workflow_stream_test_{uuid4().hex[:8]}"
    tracker = ConcurrencyTracker()
    handlers: HandlerRegistry = {"slow": _make_slow_handler(tracker)}

    workflow_ids = [
        await dispatch_workflow(stream_name=stream, workflow_type="slow")
        for _ in range(WORKFLOW_COUNT)
    ]

    await asyncio.gather(
        _run_until_done(
            pool,
            redis_client,
            stream_name=stream,
            handlers=handlers,
            workflow_ids=workflow_ids,
            timeout=WORKFLOW_COUNT * HANDLER_SECONDS * 3,
            consumer_suffix="a",
        ),
        _run_until_done(
            pool,
            redis_client,
            stream_name=stream,
            handlers=handlers,
            workflow_ids=workflow_ids,
            timeout=WORKFLOW_COUNT * HANDLER_SECONDS * 3,
            consumer_suffix="b",
        ),
    )

    assert await _all_terminal(pool, workflow_ids)
    executed = [w for w in tracker.invocations]
    assert len(executed) == len(set(executed)), (
        f"a workflow was executed more than once: {executed}"
    )

    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT status, output_data FROM workflow_states WHERE id = ANY($1::uuid[])",
            workflow_ids,
        )
    assert all(r["status"] == "COMPLETED" for r in rows)
    assert all(json.loads(r["output_data"]) == {"ok": True} for r in rows)
