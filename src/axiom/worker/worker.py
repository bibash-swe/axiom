"""The Worker's claim, heartbeat, and terminal-write primitives.

Fencing is via lease_generation alone — it increments on every genuine
claim/reclaim, never on a fenced-out write, which is what makes it safe
to use as both the correctness guard for every write below AND, natively,
the DLQ attempt counter checked by the orchestration layer built on top
of these primitives (see worker/execution.py, not here).
"""

import json
from dataclasses import dataclass
from typing import Any
from uuid import UUID

import asyncpg

from axiom.contracts.enums import WorkflowStatus

# No SKIP LOCKED here, unlike the Relay's batch claim — this targets one
# specific row by primary key, not a scan across many candidates. A
# concurrent conflicting claim simply blocks, re-checks the WHERE clause
# once the other transaction commits, and correctly affects zero rows.
_CLAIM = """
    UPDATE workflow_states
    SET status = 'RUNNING',
        worker_id = $2,
        lease_generation = lease_generation + 1,
        lease_expires_at = NOW() + make_interval(secs => $3)
    WHERE id = $1
      AND (status = 'PENDING' OR (status = 'RUNNING' AND lease_expires_at < NOW()))
    RETURNING id, lease_generation, workflow_type, workflow_version, input_data
"""

_RENEW_LEASE = """
    UPDATE workflow_states
    SET lease_expires_at = NOW() + make_interval(secs => $3)
    WHERE id = $1 AND lease_generation = $2
"""

_SETTLE_TERMINAL = """
    UPDATE workflow_states
    SET status = $3, output_data = $4::jsonb, error_log = $5::jsonb, updated_at = NOW()
    WHERE id = $1 AND lease_generation = $2
"""


@dataclass(frozen=True)
class ClaimedWorkflow:
    """A typed view over a successfully-claimed workflow_states row."""

    id: UUID
    lease_generation: int
    workflow_type: str
    workflow_version: str
    input_data: dict[str, Any]


async def claim_workflow(
    pool: asyncpg.Pool, *, workflow_id: UUID, worker_id: UUID, lease_seconds: int
) -> ClaimedWorkflow | None:
    """Attempt to claim a specific workflow by id.

    Returns None if the row wasn't claimable — already handled by another
    worker, or in a state this claim can't touch. That's a safe no-op:
    the caller should ack the stream message and move on, not retry.
    """
    async with pool.acquire() as conn:
        row = await conn.fetchrow(_CLAIM, workflow_id, worker_id, lease_seconds)

    if row is None:
        return None

    raw_input = row["input_data"]
    return ClaimedWorkflow(
        id=row["id"],
        lease_generation=row["lease_generation"],
        workflow_type=row["workflow_type"],
        workflow_version=row["workflow_version"],
        input_data=json.loads(raw_input) if raw_input else {},
    )


async def renew_lease(
    pool: asyncpg.Pool, *, workflow_id: UUID, lease_generation: int, lease_seconds: int
) -> bool:
    """Renew the lease for a still-valid claim.

    Returns False if this worker has been fenced out — a later claim now
    owns this row. The caller must treat False as "stop immediately," not
    "retry": this is the same check that powers stream_guard()'s
    cost-safety abort, not just liveness.
    """
    async with pool.acquire() as conn:
        result = await conn.execute(_RENEW_LEASE, workflow_id, lease_generation, lease_seconds)
    return result == "UPDATE 1"


async def settle_terminal(
    pool: asyncpg.Pool,
    *,
    workflow_id: UUID,
    lease_generation: int,
    status: WorkflowStatus,
    output_data: dict[str, Any] | None = None,
    error_log: dict[str, Any] | None = None,
) -> bool:
    """Fenced terminal write. Returns False if this worker was fenced out.

    A False return means the caller must NOT ack the stream message — see
    docs/decisions.md for the Last-In-Chain ordering this depends on.
    """
    async with pool.acquire() as conn:
        result = await conn.execute(
            _SETTLE_TERMINAL,
            workflow_id,
            lease_generation,
            status.value,
            json.dumps(output_data) if output_data is not None else None,
            json.dumps(error_log) if error_log is not None else None,
        )
    return result == "UPDATE 1"