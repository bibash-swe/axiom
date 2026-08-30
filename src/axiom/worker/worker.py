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
from axiom.contracts.events import WorkflowStartedEvent

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
    RETURNING id, lease_generation, workflow_type, workflow_version, input_data, chain_depth
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

# Release back to PENDING and redispatch, in one statement so a workflow can
# never end up released-but-not-redispatched (stranded until nothing) or
# redispatched-but-still-RUNNING (claimable by nobody until the lease lapses).
# The CTE carries the fence: if lease_generation no longer matches, it
# returns no rows and the INSERT inserts nothing, so a superseded worker
# cannot resurrect a workflow that another worker now owns.
_SCHEDULE_RETRY = """
    WITH fenced AS (
        UPDATE workflow_states
        SET status = 'PENDING',
            error_log = $3::jsonb,
            worker_id = NULL,
            lease_expires_at = NULL,
            updated_at = NOW()
        WHERE id = $1 AND lease_generation = $2
        RETURNING id, workflow_version
    )
    INSERT INTO workflow_outbox
        (workflow_id, event_type, payload, workflow_version, available_at)
    SELECT id, 'WORKFLOW_STARTED', $4::jsonb, workflow_version,
           NOW() + make_interval(secs => $5)
    FROM fenced
"""

# Chaining is three statements rather than one CTE, unlike _SCHEDULE_RETRY
# above, for one concrete reason: the outbox payload has to carry the
# successor's id, and that id doesn't exist until the successor row is
# inserted. Building the payload in SQL with jsonb_build_object would work but
# would put a second, hand-typed copy of the WorkflowStartedEvent shape outside
# contracts/ — exactly the drift that package exists to prevent. Atomicity
# comes from the enclosing transaction, which is just as strong.
_COMPLETE_PARENT = """
    UPDATE workflow_states
    SET status = 'COMPLETED', output_data = $3::jsonb, updated_at = NOW()
    WHERE id = $1 AND lease_generation = $2
    RETURNING workflow_version, chain_depth
"""

# Same ON CONFLICT ... RETURNING (xmax = 0) shape as the ingress insert, and
# for the same reason (docs/decisions.md #8): a replay must be absorbed rather
# than forked, and the caller must be able to tell which happened so it does
# not write a second outbox event for a successor that already has one.
_INSERT_SUCCESSOR = """
    INSERT INTO workflow_states
        (workflow_type, workflow_version, status, idempotency_key,
         input_data, parent_workflow_id, chain_depth)
    VALUES ($1, $2, 'PENDING', $3, $4::jsonb, $5, $6)
    ON CONFLICT (idempotency_key)
    DO UPDATE SET idempotency_key = EXCLUDED.idempotency_key
    RETURNING id, (xmax = 0) AS is_new_row
"""

_INSERT_SUCCESSOR_EVENT = """
    INSERT INTO workflow_outbox (workflow_id, event_type, payload, workflow_version)
    VALUES ($1, 'WORKFLOW_STARTED', $2::jsonb, $3)
"""


def chain_idempotency_key(parent_workflow_id: UUID, next_workflow_type: str) -> str:
    """The successor's idempotency key, derived so it is the same on every replay.

    A random key would let one parent produce two successors if its chain
    write ever ran twice. Deriving it from the parent makes the second write
    collide with the first and become a no-op instead.

    The 'chain:' prefix is reserved at the database — see migration 003 — so a
    client-submitted key can never land on a chain step. Worst case is 143
    characters against a VARCHAR(255) column: 6 for the prefix, 36 for the
    UUID, 1 separator, and workflow_type's own VARCHAR(100) ceiling.
    """
    return f"chain:{parent_workflow_id}:{next_workflow_type}"


@dataclass(frozen=True)
class ClaimedWorkflow:
    """A typed view over a successfully-claimed workflow_states row."""

    id: UUID
    lease_generation: int
    workflow_type: str
    workflow_version: str
    input_data: dict[str, Any]
    chain_depth: int


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
        chain_depth=row["chain_depth"],
    )


_IS_TERMINAL = """
    SELECT s.is_terminal
    FROM workflow_states w
    JOIN workflow_statuses s ON s.status = w.status
    WHERE w.id = $1
"""


async def is_settled(pool: asyncpg.Pool, workflow_id: UUID) -> bool:
    """Whether this workflow has reached a terminal status, or no longer exists.

    Reads is_terminal from workflow_statuses (migration 005) rather than
    carrying a second copy of the terminal set here. A missing row counts as
    settled: there is nothing left to run, and holding its message forever
    would leak it.
    """
    async with pool.acquire() as conn:
        terminal = await conn.fetchval(_IS_TERMINAL, workflow_id)
    return terminal is not False


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


async def schedule_retry(
    pool: asyncpg.Pool,
    *,
    workflow_id: UUID,
    lease_generation: int,
    delay_seconds: float,
    error_log: dict[str, Any],
) -> bool:
    """Release a failed workflow back to PENDING and redispatch it after delay_seconds.

    Fenced like every other write here: returns False if this worker was
    superseded, in which case the caller must NOT ack — the current lease
    holder owns what happens next.

    No attempt counter is incremented here. lease_generation already counts
    attempts because every redispatch ends in a fresh claim, and the ceiling
    is enforced once, in check_and_handle_poison_pill().
    """
    event = WorkflowStartedEvent(workflow_id=workflow_id)
    async with pool.acquire() as conn:
        result = await conn.execute(
            _SCHEDULE_RETRY,
            workflow_id,
            lease_generation,
            json.dumps(error_log),
            event.model_dump_json(),
            delay_seconds,
        )
    return result == "INSERT 0 1"


async def settle_and_chain(
    pool: asyncpg.Pool,
    *,
    workflow_id: UUID,
    lease_generation: int,
    output_data: dict[str, Any],
    next_workflow_type: str,
    next_input_data: dict[str, Any],
) -> UUID | None:
    """Complete a workflow and create its successor in one transaction.

    Returns the successor's id, or None if this worker was fenced out — in
    which case nothing at all was written and the caller must NOT ack, same
    rule as every other write in this module.

    The single transaction is the whole point. Completing the parent and
    dispatching the successor as two separate writes would leave a window
    where a crash ends the chain silently: the parent reads COMPLETED, the
    caller sees a successful workflow, and the remaining steps simply never
    happen. There is no reconciliation pass that could detect that, because a
    COMPLETED row with no successor is exactly what the last step of a chain
    looks like.

    The successor inherits workflow_version rather than taking the current
    default, so a chain runs to completion on the version it started on and
    cordon-and-drain stays meaningful mid-chain.
    """
    async with pool.acquire() as conn, conn.transaction():
        parent = await conn.fetchrow(
            _COMPLETE_PARENT, workflow_id, lease_generation, json.dumps(output_data)
        )
        if parent is None:
            return None

        successor = await conn.fetchrow(
            _INSERT_SUCCESSOR,
            next_workflow_type,
            parent["workflow_version"],
            chain_idempotency_key(workflow_id, next_workflow_type),
            json.dumps(next_input_data),
            workflow_id,
            parent["chain_depth"] + 1,
        )
        if successor is None:
            raise RuntimeError("settle_and_chain: successor insert returned no row")

        successor_id: UUID = successor["id"]

        # Only on a genuine insert. A conflict means this exact successor was
        # already created — necessarily inside a transaction that also wrote
        # its outbox event — so writing a second event here would dispatch the
        # same workflow twice.
        if successor["is_new_row"]:
            event = WorkflowStartedEvent(workflow_id=successor_id)
            await conn.execute(
                _INSERT_SUCCESSOR_EVENT,
                successor_id,
                event.model_dump_json(),
                parent["workflow_version"],
            )

        return successor_id
