"""The Worker's stream consumption loop: parse, claim, execute, settle, ack.

XREADGROUP is the primary consumption path — the normal way a worker gets
new work. XAUTOCLAIM is strictly the reclaim path, for messages left idle
by a crashed worker; it is never the primary dequeue mechanism, a
distinction worth keeping precise (see docs/decisions.md).

Handlers are dependency-injected, not hardcoded: this module knows how to
run a workflow reliably, not what any particular workflow_type actually
does. A handler receives (pool, workflow_id, lease_generation, input_data)
— the fencing context is passed through explicitly so a handler that
needs to stream can wrap its own iterator in stream_guard() itself,
without this module needing to know or care whether it does.
"""

import asyncio
import logging
from collections.abc import Callable, Coroutine
from typing import Any, cast
from uuid import UUID

import asyncpg
from redis.asyncio import Redis
from redis.exceptions import ResponseError

from axiom.contracts.enums import WorkflowStatus
from axiom.contracts.events import WorkflowStartedEvent
from axiom.worker.execution import (
    NonRetryableError,
    WorkerFencedError,
    check_and_handle_poison_pill,
    execute_with_heartbeat,
    retry_delay_seconds,
)
from axiom.worker.worker import claim_workflow, schedule_retry, settle_terminal

logger = logging.getLogger("axiom.worker")

WorkflowHandler = Callable[
    [asyncpg.Pool, UUID, int, dict[str, Any]], Coroutine[Any, Any, dict[str, Any]]
]
HandlerRegistry = dict[str, WorkflowHandler]

_GROUP_NAME = "workers"

# redis-py's stubs type streaming-command returns loosely (covering both
# decode_responses cases). These match the actual shape, verified
# directly against a real Redis instance before writing this module —
# see the empirical check in the accompanying design notes.
_StreamMessage = tuple[str, dict[str, str]]
_XAutoclaimResult = tuple[str, list[_StreamMessage], list[str]]
_XReadGroupResult = list[tuple[str, list[_StreamMessage]]]


async def ensure_consumer_group(redis: Redis, *, stream_name: str) -> None:
    """Create the consumer group if it doesn't exist yet. Idempotent across restarts."""
    try:
        await redis.xgroup_create(stream_name, _GROUP_NAME, id="0", mkstream=True)
    except ResponseError as exc:
        if "BUSYGROUP" not in str(exc):
            raise


async def process_message(
    pool: asyncpg.Pool,
    redis: Redis,
    *,
    stream_name: str,
    message_id: str,
    payload: str,
    worker_id: UUID,
    handlers: HandlerRegistry,
    lease_seconds: int,
    heartbeat_interval_seconds: int,
    max_retries: int,
    retry_base_seconds: float,
    retry_cap_seconds: float,
) -> None:
    """Process one stream message end to end. Never raises.

    Every failure path is handled internally — this is called from a
    loop that must keep running regardless of one message's outcome. Ack
    only ever follows a successful terminal write or a confirmed
    already-handled no-op, per the Last-In-Chain ordering.
    """
    try:
        event = WorkflowStartedEvent.model_validate_json(payload)
    except Exception:
        logger.exception(
            "malformed outbox payload, message_id=%s — acking to avoid a poison loop",
            message_id,
        )
        await redis.xack(stream_name, _GROUP_NAME, message_id)
        return

    workflow_id = event.workflow_id

    claimed = await claim_workflow(
        pool, workflow_id=workflow_id, worker_id=worker_id, lease_seconds=lease_seconds
    )
    if claimed is None:
        # Already handled by someone else, or a genuine duplicate delivery
        # of an already-claimed, still-fresh-leased row. Safe no-op.
        await redis.xack(stream_name, _GROUP_NAME, message_id)
        return

    try:
        dead_lettered = await check_and_handle_poison_pill(
            pool, claimed, max_retries=max_retries
        )
    except WorkerFencedError:
        logger.warning("fenced during poison-pill check, workflow_id=%s", workflow_id)
        return

    if dead_lettered:
        await redis.xack(stream_name, _GROUP_NAME, message_id)
        return

    handler = handlers.get(claimed.workflow_type)
    if handler is None:
        settled = await settle_terminal(
            pool,
            workflow_id=workflow_id,
            lease_generation=claimed.lease_generation,
            status=WorkflowStatus.FAILED,
            error_log={
                "error": f"no handler registered for workflow_type={claimed.workflow_type!r}"
            },
        )
        if settled:
            await redis.xack(stream_name, _GROUP_NAME, message_id)
        return

    try:
        output = await execute_with_heartbeat(
            pool,
            handler(pool, workflow_id, claimed.lease_generation, claimed.input_data),
            workflow_id=workflow_id,
            lease_generation=claimed.lease_generation,
            lease_seconds=lease_seconds,
            heartbeat_interval_seconds=heartbeat_interval_seconds,
        )
    except WorkerFencedError:
        logger.warning("fenced during execution, workflow_id=%s", workflow_id)
        return
    except NonRetryableError as exc:
        settled = await settle_terminal(
            pool,
            workflow_id=workflow_id,
            lease_generation=claimed.lease_generation,
            status=WorkflowStatus.FAILED,
            error_log={"error": str(exc), "error_type": type(exc).__name__, "retryable": False},
        )
        if settled:
            await redis.xack(stream_name, _GROUP_NAME, message_id)
        return
    except Exception as exc:
        # Everything a handler raises that isn't explicitly permanent gets
        # another attempt. The ceiling is not enforced here — the next
        # claim's poison-pill check owns giving up, so there is exactly one
        # place that decides a workflow is dead.
        delay = retry_delay_seconds(
            claimed.lease_generation,
            base_seconds=retry_base_seconds,
            cap_seconds=retry_cap_seconds,
        )
        scheduled = await schedule_retry(
            pool,
            workflow_id=workflow_id,
            lease_generation=claimed.lease_generation,
            delay_seconds=delay,
            error_log={
                "error": str(exc),
                "error_type": type(exc).__name__,
                "attempt": claimed.lease_generation,
                "retry_in_seconds": round(delay, 3),
            },
        )
        if scheduled:
            logger.warning(
                "handler failed on attempt %d, retrying workflow_id=%s in %.2fs: %s",
                claimed.lease_generation,
                workflow_id,
                delay,
                exc,
            )
            await redis.xack(stream_name, _GROUP_NAME, message_id)
        return

    settled = await settle_terminal(
        pool,
        workflow_id=workflow_id,
        lease_generation=claimed.lease_generation,
        status=WorkflowStatus.COMPLETED,
        output_data=output,
    )
    if settled:
        await redis.xack(stream_name, _GROUP_NAME, message_id)


def _log_if_failed(task: asyncio.Task[None]) -> None:
    """Surface an unexpected escape from process_message, which promises not to raise.

    Without this the exception sits unretrieved on the task and asyncio
    reports it only at garbage-collection time, detached from any context.
    """
    if task.cancelled():
        return
    exc = task.exception()
    if exc is not None:
        logger.error("process_message raised unexpectedly", exc_info=exc)


async def run_forever(
    pool: asyncpg.Pool,
    redis: Redis,
    *,
    stream_name: str,
    consumer_name: str,
    worker_id: UUID,
    handlers: HandlerRegistry,
    lease_seconds: int,
    heartbeat_interval_seconds: int,
    xautoclaim_min_idle_seconds: int,
    max_retries: int,
    retry_base_seconds: float,
    retry_cap_seconds: float,
    batch_size: int,
    shutdown_event: asyncio.Event,
) -> None:
    """Consume stream_name until shutdown_event is set, working on messages concurrently.

    Each cycle checks the reclaim path (XAUTOCLAIM — messages left idle
    by a crashed worker) before the fresh path (XREADGROUP). XREADGROUP's
    own BLOCK option provides the idle wait natively; no separate sleep
    is needed the way the Relay needed one for Postgres polling.

    batch_size is the ceiling on messages in flight at once, and both
    fetches ask only for as many as there are free slots. Fetching more
    than can be worked on would move them into this consumer's PEL, where
    they are held — unworked and invisible to every other worker — until
    min_idle_time elapses. Concurrency is safe here because the claim
    query, not the fetch pattern, is what prevents two workers executing
    one row: a second claimant finds the row already RUNNING on a live
    lease and correctly gets nothing.

    On shutdown, in-flight work is drained rather than abandoned; each
    task holds a claimed row and an un-acked message.
    """
    await ensure_consumer_group(redis, stream_name=stream_name)
    logger.info("worker starting, stream=%s, consumer=%s", stream_name, consumer_name)

    in_flight: set[asyncio.Task[None]] = set()

    def _spawn(message_id: str, fields: dict[str, str]) -> None:
        """Start one message processing concurrently with the others in flight."""
        task = asyncio.create_task(
            process_message(
                pool,
                redis,
                stream_name=stream_name,
                message_id=message_id,
                payload=fields.get("payload", ""),
                worker_id=worker_id,
                handlers=handlers,
                lease_seconds=lease_seconds,
                heartbeat_interval_seconds=heartbeat_interval_seconds,
                max_retries=max_retries,
                retry_base_seconds=retry_base_seconds,
                retry_cap_seconds=retry_cap_seconds,
            )
        )
        in_flight.add(task)
        # Holding a strong reference until completion: the loop keeps only a
        # weak reference to a task, so an unreferenced one can be collected
        # mid-flight.
        task.add_done_callback(in_flight.discard)
        task.add_done_callback(_log_if_failed)

    while not shutdown_event.is_set():
        free = batch_size - len(in_flight)
        if free <= 0:
            # Saturated. Wait for a slot rather than fetching messages that
            # would sit unworked in our PEL, invisible to idle workers until
            # min_idle_time elapses.
            await asyncio.wait(in_flight, return_when=asyncio.FIRST_COMPLETED)
            continue

        _next_cursor, reclaimed, _deleted = cast(
            _XAutoclaimResult,
            await redis.xautoclaim(
                stream_name,
                _GROUP_NAME,
                consumer_name,
                min_idle_time=xautoclaim_min_idle_seconds * 1000,
                start_id="0-0",
                count=free,
            ),
        )
        for message_id, fields in reclaimed:
            _spawn(message_id, fields)

        free = batch_size - len(in_flight)
        if free <= 0:
            continue

        response = cast(
            _XReadGroupResult,
            await redis.xreadgroup(
                _GROUP_NAME,
                consumer_name,
                streams={stream_name: ">"},
                count=free,
                block=500,
            ),
        )
        for _stream, messages in response or []:
            for message_id, fields in messages:
                _spawn(message_id, fields)

    # Graceful drain: in-flight work has already claimed its rows and holds
    # un-acked messages, so abandoning it would strand both until the lease
    # and min_idle_time expire.
    if in_flight:
        logger.info("draining %d in-flight message(s) before stopping", len(in_flight))
        await asyncio.gather(*in_flight, return_exceptions=True)

    logger.info("worker stopped, stream=%s, consumer=%s", stream_name, consumer_name)
