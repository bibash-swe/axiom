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
    WorkerFencedError,
    check_and_handle_poison_pill,
    execute_with_heartbeat,
)
from axiom.worker.worker import claim_workflow, settle_terminal

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
    except Exception as exc:
        settled = await settle_terminal(
            pool,
            workflow_id=workflow_id,
            lease_generation=claimed.lease_generation,
            status=WorkflowStatus.FAILED,
            error_log={"error": str(exc), "error_type": type(exc).__name__},
        )
        if settled:
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
    batch_size: int,
    shutdown_event: asyncio.Event,
) -> None:
    """Consume stream_name until shutdown_event is set.

    Each cycle checks the reclaim path (XAUTOCLAIM — messages left idle
    by a crashed worker) before the fresh path (XREADGROUP). XREADGROUP's
    own BLOCK option provides the idle wait natively; no separate sleep
    is needed the way the Relay needed one for Postgres polling.
    """
    await ensure_consumer_group(redis, stream_name=stream_name)
    logger.info("worker starting, stream=%s, consumer=%s", stream_name, consumer_name)

    while not shutdown_event.is_set():
        _next_cursor, reclaimed, _deleted = cast(
            _XAutoclaimResult,
            await redis.xautoclaim(
                stream_name,
                _GROUP_NAME,
                consumer_name,
                min_idle_time=xautoclaim_min_idle_seconds * 1000,
                start_id="0-0",
                count=batch_size,
            ),
        )
        for message_id, fields in reclaimed:
            await process_message(
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
            )

        response = cast(
            _XReadGroupResult,
            await redis.xreadgroup(
                _GROUP_NAME,
                consumer_name,
                streams={stream_name: ">"},
                count=batch_size,
                block=500,
            ),
        )
        for _stream, messages in response or []:
            for message_id, fields in messages:
                await process_message(
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
                )

    logger.info("worker stopped, stream=%s, consumer=%s", stream_name, consumer_name)
