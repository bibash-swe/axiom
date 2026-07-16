"""The Outbox Relay: claim, publish, settle.

A dumb pipe, not a processor — no heartbeat, just a short claim-lease that
expires. See docs/decisions.md for the Option A (aggressive timeouts over
heartbeat) reasoning, and the phase-split transaction pattern that keeps
Redis I/O from ever holding a Postgres connection open.
"""

import json
from dataclasses import dataclass
from uuid import UUID

import asyncpg
from redis.asyncio import Redis

# Claim: short transaction, released immediately. retry_count < $2 is the
# poison-pill enforcement point; claimed_at staleness is the lease.
_CLAIM_BATCH = """
    UPDATE workflow_outbox
    SET claimed_at = NOW(), claimed_by = $1
    WHERE id IN (
        SELECT id FROM workflow_outbox
        WHERE dispatched = FALSE
          AND retry_count < $2
          AND (claimed_at IS NULL OR claimed_at < NOW() - make_interval(secs => $3))
        ORDER BY created_at ASC
        LIMIT $4
        FOR UPDATE SKIP LOCKED
    )
    RETURNING id, workflow_id, payload, workflow_version
"""

_SETTLE_SUCCESS = """
    UPDATE workflow_outbox
    SET dispatched = TRUE, claimed_at = NULL
    WHERE id = ANY($1::uuid[]) AND claimed_by = $2
"""

# Atomic: only rows that actually cross the retry threshold flip
# workflow_states to terminal. A transient failure just releases its lease
# for the next claim — it must never touch workflow_states.
_SETTLE_FAILURES = """
    WITH failed_terminal AS (
        UPDATE workflow_outbox
        SET retry_count = retry_count + 1, claimed_at = NULL
        WHERE id = ANY($1::uuid[]) AND claimed_by = $2
        RETURNING workflow_id, retry_count
    )
    UPDATE workflow_states
    SET status = 'DISPATCH_FAILED', error_log = $3::jsonb
    WHERE id IN (SELECT workflow_id FROM failed_terminal WHERE retry_count >= $4)
"""


@dataclass(frozen=True)
class ClaimedOutboxRow:
    """A typed view over one claimed outbox row — payload stays an opaque string."""

    id: UUID
    workflow_id: UUID
    payload: str
    workflow_version: str


async def claim_batch(
    pool: asyncpg.Pool,
    *,
    instance_id: UUID,
    batch_size: int,
    claim_lease_seconds: int,
    max_retries: int,
) -> list[ClaimedOutboxRow]:
    """Claim up to batch_size undispatched, unclaimed-or-stale-claimed rows."""
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            _CLAIM_BATCH, instance_id, max_retries, claim_lease_seconds, batch_size
        )

    return [
        ClaimedOutboxRow(
            id=row["id"],
            workflow_id=row["workflow_id"],
            payload=row["payload"],
            workflow_version=row["workflow_version"],
        )
        for row in rows
    ]


async def publish_batch(
    redis: Redis, rows: list[ClaimedOutboxRow]
) -> tuple[list[UUID], list[UUID]]:
    """Publish each row's payload opaquely to its version-routed stream.

    No open transaction here — this can run as long as Redis takes. A
    broad except is deliberate: any failure (timeout, connection error, or
    anything else) means this row didn't publish, full stop — it goes to
    the retry path, never a crash of the whole batch.
    """
    success_ids: list[UUID] = []
    failed_ids: list[UUID] = []

    for row in rows:
        stream_name = f"workflow_stream_{row.workflow_version}"
        try:
            await redis.xadd(stream_name, {"payload": row.payload})
            success_ids.append(row.id)
        except Exception:  # noqa: BLE001 — deliberate: any failure means "retry this row"
            failed_ids.append(row.id)

    return success_ids, failed_ids


async def settle_success(pool: asyncpg.Pool, *, instance_id: UUID, success_ids: list[UUID]) -> None:
    """Mark successfully-published rows dispatched; releases their claim."""
    if not success_ids:
        return
    async with pool.acquire() as conn:
        await conn.execute(_SETTLE_SUCCESS, success_ids, instance_id)


async def settle_failures(
    pool: asyncpg.Pool,
    *,
    instance_id: UUID,
    failed_ids: list[UUID],
    max_retries: int,
) -> None:
    """Increment retry_count for failed rows; dead-letter only those past max_retries."""
    if not failed_ids:
        return
    error_log = json.dumps({"error": "Poison pill: max relay retries exceeded"})
    async with pool.acquire() as conn:
        await conn.execute(_SETTLE_FAILURES, failed_ids, instance_id, error_log, max_retries)


async def run_relay_cycle(
    pool: asyncpg.Pool,
    redis: Redis,
    *,
    instance_id: UUID,
    batch_size: int,
    claim_lease_seconds: int,
    max_retries: int,
) -> int:
    """One full claim → publish → settle cycle. Returns rows processed (0 means idle)."""
    claimed = await claim_batch(
        pool,
        instance_id=instance_id,
        batch_size=batch_size,
        claim_lease_seconds=claim_lease_seconds,
        max_retries=max_retries,
    )
    if not claimed:
        return 0

    success_ids, failed_ids = await publish_batch(redis, claimed)

    await settle_success(pool, instance_id=instance_id, success_ids=success_ids)
    await settle_failures(
        pool, instance_id=instance_id, failed_ids=failed_ids, max_retries=max_retries
    )

    return len(claimed)
