"""A long-running workflow must not lose its stream message.

Found by the exhaustive model in test_protocol_model.py, reproduced here
against real Postgres and Redis.

XAUTOCLAIM's min_idle_time measures how long a message has sat unacked in the
PEL. Heartbeats do not reset it — they extend the Postgres lease, which is a
different clock. So any workflow running longer than xautoclaim_min_idle_seconds
has its message handed to a second worker, whose claim then fails against the
live lease. Treating that as a duplicate and acking deletes the only thing that
could ever redeliver the work.
"""

import json
from typing import Any
from uuid import UUID, uuid4

import asyncpg
import pytest
from redis.asyncio import Redis

from axiom.worker.runner import ensure_consumer_group, process_message
from axiom.worker.worker import claim_workflow

GROUP = "workers"


async def _dispatch(pool: asyncpg.Pool, redis: Redis, stream: str) -> tuple[UUID, str]:
    async with pool.acquire() as conn:
        workflow_id: UUID = await conn.fetchval(
            "INSERT INTO workflow_states (workflow_type, workflow_version, idempotency_key) "
            "VALUES ('slow', 'slow', $1) RETURNING id",
            f"strand_{uuid4()}",
        )
    await ensure_consumer_group(redis, stream_name=stream)
    payload = json.dumps({"event_type": "WORKFLOW_STARTED", "workflow_id": str(workflow_id)})
    message_id: str = await redis.xadd(stream, {"payload": payload})
    return workflow_id, message_id


async def _never_runs(
    pool: asyncpg.Pool, workflow_id: UUID, generation: int, input_data: dict[str, Any]
) -> dict[str, Any]:
    raise AssertionError("the second worker must not execute a live-leased workflow")


@pytest.mark.parametrize("heartbeated", [False, True])
async def test_a_still_running_workflow_keeps_its_message(
    pool: asyncpg.Pool, redis_client: Redis, heartbeated: bool
) -> None:
    """Worker A is mid-flight; B reclaims the message and must not ack it away.

    heartbeated=True is the common case: a handler that outlives the idle
    threshold renews its lease, so B's claim is guaranteed to fail rather than
    merely likely to.
    """
    stream = f"workflow_stream_strand_{uuid4().hex[:8]}"
    workflow_id, message_id = await _dispatch(pool, redis_client, stream)

    # Worker A takes the message and claims the row. Still running.
    await redis_client.xreadgroup(GROUP, "worker-a", streams={stream: ">"}, count=1)
    claimed = await claim_workflow(
        pool, workflow_id=workflow_id, worker_id=uuid4(), lease_seconds=30
    )
    assert claimed is not None

    if heartbeated:
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE workflow_states SET lease_expires_at = NOW() + INTERVAL '30 seconds' "
                "WHERE id = $1",
                workflow_id,
            )

    # XAUTOCLAIM hands the message to B, because it has been idle too long —
    # a fact about the PEL, not about whether A is healthy.
    reclaimed = await redis_client.xautoclaim(stream, GROUP, "worker-b", min_idle_time=0, count=10)
    assert reclaimed[1], "expected the message to be reclaimable"

    await process_message(
        pool,
        redis_client,
        stream_name=stream,
        message_id=message_id,
        payload=json.dumps({"event_type": "WORKFLOW_STARTED", "workflow_id": str(workflow_id)}),
        worker_id=uuid4(),
        handlers={"slow": _never_runs},
        lease_seconds=30,
        heartbeat_interval_seconds=10,
        max_retries=5,
        retry_base_seconds=0.01,
        retry_cap_seconds=0.01,
        max_chain_depth=50,
    )

    async with pool.acquire() as conn:
        status = await conn.fetchval(
            "SELECT status FROM workflow_states WHERE id = $1", workflow_id
        )
    pending = await redis_client.xpending(stream, GROUP)

    await redis_client.delete(stream)

    assert status == "RUNNING", "worker A still owns this workflow"
    assert pending["pending"] == 1, (
        "the message was acked while the workflow was still RUNNING — "
        "nothing can redeliver it now, so a crash in worker A strands it forever"
    )
