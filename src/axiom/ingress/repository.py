"""The atomic idempotent write — the transaction that makes duplicate workflow execution impossible.

See docs/decisions.md #8 for why this uses ON CONFLICT DO UPDATE rather
than DO NOTHING, and why the xmax = 0 trick is trusted here.
"""

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID

import asyncpg

from axiom.contracts.enums import WorkflowStatus
from axiom.contracts.events import WorkflowStartedEvent

# xmax = 0 distinguishes a fresh insert from a replayed conflict — see
# docs/decisions.md #8 for why this is trusted and how it was verified.
_INSERT_WORKFLOW = """
    INSERT INTO workflow_states
        (workflow_type, workflow_version, status, idempotency_key, input_data)
    VALUES ($1, $2, 'PENDING', $3, $4::jsonb)
    ON CONFLICT (idempotency_key)
    DO UPDATE SET idempotency_key = EXCLUDED.idempotency_key
    RETURNING id, status, workflow_type, workflow_version, created_at, (xmax = 0) AS is_new_row
"""

_INSERT_OUTBOX = """
    INSERT INTO workflow_outbox (workflow_id, event_type, payload, workflow_version)
    VALUES ($1, 'WORKFLOW_STARTED', $2::jsonb, $3)
"""


@dataclass(frozen=True)
class SubmittedWorkflow:
    """Typed view over the row returned by submit_workflow().

    A dataclass, not a Pydantic model: this wraps a trusted database
    result rather than untrusted input, so there's nothing left to
    validate — only something to type.
    """

    id: UUID
    status: WorkflowStatus
    workflow_type: str
    workflow_version: str
    created_at: datetime
    is_new_row: bool


async def submit_workflow(
    pool: asyncpg.Pool,
    *,
    workflow_type: str,
    workflow_version: str,
    idempotency_key: str,
    input_data: dict[str, Any],
) -> SubmittedWorkflow:
    """Atomically create a workflow and its dispatch event, or replay an existing one.

    On a fresh submission: one workflow_states row, one outbox event, same
    transaction. On replay (same idempotency_key): the existing row is
    returned and no second outbox event is ever written — see
    docs/decisions.md #8 and #9.
    """
    async with pool.acquire() as conn, conn.transaction():
        row = await conn.fetchrow(
            _INSERT_WORKFLOW,
            workflow_type,
            workflow_version,
            idempotency_key,
            json.dumps(input_data),
        )

        # Structurally unreachable: idempotency_key is UNIQUE and DO UPDATE
        # has no WHERE clause, so this INSERT ... ON CONFLICT always
        # returns exactly one row. An explicit check, not assert — asserts
        # are stripped under python -O, and this is load-bearing.
        if row is None:
            raise RuntimeError("submit_workflow: INSERT ... RETURNING produced no row")

        is_new_row: bool = row["is_new_row"]

        if is_new_row:
            event = WorkflowStartedEvent(workflow_id=row["id"])
            await conn.execute(
                _INSERT_OUTBOX,
                row["id"],
                event.model_dump_json(),
                workflow_version,
            )

        return SubmittedWorkflow(
            id=row["id"],
            status=WorkflowStatus(row["status"]),
            workflow_type=row["workflow_type"],
            workflow_version=row["workflow_version"],
            created_at=row["created_at"],
            is_new_row=is_new_row,
        )
