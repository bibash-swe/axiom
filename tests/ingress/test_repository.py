"""Tests for the atomic idempotent ingress transaction.

asyncio_mode = auto (pytest.ini) already marks every async def test_*
automatically — no pytestmark needed.
"""

import asyncio
import json
from uuid import uuid4

import asyncpg

from axiom.contracts.enums import WorkflowStatus
from axiom.contracts.events import WorkflowStartedEvent
from axiom.ingress.repository import submit_workflow


async def test_fresh_submission_creates_state_and_outbox(pool: asyncpg.Pool) -> None:
    """A fresh submission writes exactly one state row and one outbox event.

    The event payload must be a genuine, parseable WorkflowStartedEvent — not
    just any JSON blob with the right column values.
    """
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
        row = await conn.fetchrow("SELECT * FROM workflow_states WHERE id = $1", result.id)
        assert row is not None
        assert row["idempotency_key"] == idem_key

        outbox = await conn.fetch(
            "SELECT * FROM workflow_outbox WHERE workflow_id = $1", result.id
        )
        assert len(outbox) == 1
        assert outbox[0]["event_type"] == "WORKFLOW_STARTED"
        assert outbox[0]["workflow_version"] == "v1"

        # The actual regression this guards against: someone reverting
        # repository.py's outbox write back to an ad-hoc dict instead of
        # WorkflowStartedEvent.model_dump_json(). The column checks above
        # wouldn't catch that; only opening the payload itself does.
        event = WorkflowStartedEvent.model_validate_json(outbox[0]["payload"])
        assert event.workflow_id == result.id
        assert event.event_type == "WORKFLOW_STARTED"


async def test_idempotent_replay_returns_existing_row_without_new_outbox(
    pool: asyncpg.Pool,
) -> None:
    """A replay with the same idempotency_key returns the original row.

    Even with a different payload, it creates no second outbox event, and never
    overwrites the originally-committed input_data.
    """
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
        outbox = await conn.fetch(
            "SELECT * FROM workflow_outbox WHERE workflow_id = $1", first.id
        )
        assert len(outbox) == 1

        # The property that actually matters here: DO UPDATE SET only
        # touches idempotency_key, so the replay's "malicious" payload
        # must never reach input_data. If a future edit widens the DO
        # UPDATE clause, this is what catches it — the outbox-count check
        # above would still pass even if this broke.
        state_row = await conn.fetchrow(
            "SELECT input_data FROM workflow_states WHERE id = $1", first.id
        )
        assert state_row is not None
        stored_input = json.loads(state_row["input_data"])
        assert stored_input == original_payload


async def test_concurrent_submissions_resolve_cleanly(pool: asyncpg.Pool) -> None:
    """Simulates a network retry storm against the repository.

    10 identical requests hit the repository at the same moment. All must resolve
    to one row, exactly one claiming to be new, and exactly one outbox event —
    the actual race, not just sequential replay.
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
            next(iter(workflow_ids)),
        )
        assert len(outbox) == 1