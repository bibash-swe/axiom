import asyncio
import json
from uuid import uuid4

import asyncpg

from axiom.contracts.enums import WorkflowStatus
from axiom.contracts.events import WorkflowStartedEvent
from axiom.ingress.repository import submit_workflow


async def test_fresh_submission_creates_state_and_outbox(pool: asyncpg.Pool):
    idem_key = f"idem_{uuid4()}"
    payload = {"source": "test_fresh"}

    result = await submit_workflow(
        pool=pool,
        workflow_type="ETL_PIPELINE",
        workflow_version="v1",
        idempotency_key=idem_key,
        input_data=payload,
    )

    assert result.is_new_row is True
    assert result.status == WorkflowStatus.PENDING
    assert result.workflow_type == "ETL_PIPELINE"

    async with pool.acquire() as conn:
        outbox = await conn.fetch("SELECT * FROM workflow_outbox WHERE workflow_id = $1", result.id)
        assert len(outbox) == 1

        # Parse the JSONB payload and strictly validate it against the wire contract
        event_payload = json.loads(outbox[0]["payload"])
        event = WorkflowStartedEvent.model_validate(event_payload)
        assert event.workflow_id == result.id


async def test_idempotent_replay_returns_existing_row_without_new_outbox_or_overwrite(pool: asyncpg.Pool):
    idem_key = f"idem_{uuid4()}"
    original_payload = {"source": "test_replay"}

    first = await submit_workflow(
        pool=pool,
        workflow_type="ETL_PIPELINE",
        workflow_version="v1",
        idempotency_key=idem_key,
        input_data=original_payload,
    )

    second = await submit_workflow(
        pool=pool,
        workflow_type="ETL_PIPELINE",
        workflow_version="v1",
        idempotency_key=idem_key,
        input_data={"malicious": "override"},
    )

    assert second.is_new_row is False
    assert second.id == first.id

    async with pool.acquire() as conn:
        # Guarantee no duplicated dispatch events
        outbox = await conn.fetch("SELECT * FROM workflow_outbox WHERE workflow_id = $1", first.id)
        assert len(outbox) == 1

        # Guarantee the original payload survived the DO UPDATE clause
        state = await conn.fetchrow("SELECT input_data FROM workflow_states WHERE id = $1", first.id)
        assert json.loads(state["input_data"]) == original_payload


async def test_concurrent_submissions_resolve_cleanly(pool: asyncpg.Pool):
    """
    Simulates a network retry storm where multiple identical requests hit the
    repository at the exact same millisecond.
    """
    idem_key = f"idem_{uuid4()}"
    payload = {"source": "test_race"}

    coros = [
        submit_workflow(
            pool=pool,
            workflow_type="RACE_CONDITION_FLOW",
            workflow_version="v1",
            idempotency_key=idem_key,
            input_data=payload,
        )
        for _ in range(10)
    ]

    results = await asyncio.gather(*coros)

    workflow_ids = {r.id for r in results}
    assert len(workflow_ids) == 1

    new_row_flags = [r.is_new_row for r in results]
    assert new_row_flags.count(True) == 1
    assert new_row_flags.count(False) == 9

    async with pool.acquire() as conn:
        outbox = await conn.fetch(
            "SELECT * FROM workflow_outbox WHERE workflow_id = $1",
            list(workflow_ids)[0]
        )
        assert len(outbox) == 1