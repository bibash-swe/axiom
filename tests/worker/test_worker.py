"""Tests for the Worker's claim, heartbeat, and terminal-write primitives."""

from collections.abc import Awaitable, Callable
from uuid import UUID, uuid4

import asyncpg

from axiom.contracts.enums import WorkflowStatus
from axiom.worker.worker import claim_workflow, renew_lease, settle_terminal

MakeWorkflowRow = Callable[..., Awaitable[UUID]]


async def test_claim_workflow_claims_a_pending_row(
    pool: asyncpg.Pool, make_workflow_row: MakeWorkflowRow
) -> None:
    """A fresh PENDING row is claimable, and generation starts at 1."""
    wid = await make_workflow_row(idempotency_key=f"k_{uuid4()}", input_data={"x": 1})

    claimed = await claim_workflow(pool, workflow_id=wid, worker_id=uuid4(), lease_seconds=30)

    assert claimed is not None
    assert claimed.id == wid
    assert claimed.lease_generation == 1
    assert claimed.input_data == {"x": 1}


async def test_claim_workflow_rejects_a_fresh_lease(
    pool: asyncpg.Pool, make_workflow_row: MakeWorkflowRow
) -> None:
    """A row already claimed with a fresh, non-expired lease can't be claimed again."""
    wid = await make_workflow_row(
        idempotency_key=f"k_{uuid4()}", status="RUNNING", lease_generation=1, lease_stale=False
    )

    claimed = await claim_workflow(pool, workflow_id=wid, worker_id=uuid4(), lease_seconds=30)

    assert claimed is None


async def test_claim_workflow_reclaims_a_stale_lease(
    pool: asyncpg.Pool, make_workflow_row: MakeWorkflowRow
) -> None:
    """A row whose lease has expired is reclaimable, and generation increments again."""
    wid = await make_workflow_row(
        idempotency_key=f"k_{uuid4()}", status="RUNNING", lease_generation=1, lease_stale=True
    )

    claimed = await claim_workflow(pool, workflow_id=wid, worker_id=uuid4(), lease_seconds=30)

    assert claimed is not None
    assert claimed.lease_generation == 2


async def test_renew_lease_succeeds_for_current_generation(
    pool: asyncpg.Pool, make_workflow_row: MakeWorkflowRow
) -> None:
    """Renewing with the correct generation succeeds."""
    wid = await make_workflow_row(idempotency_key=f"k_{uuid4()}")
    claimed = await claim_workflow(pool, workflow_id=wid, worker_id=uuid4(), lease_seconds=30)
    assert claimed is not None

    ok = await renew_lease(
        pool, workflow_id=wid, lease_generation=claimed.lease_generation, lease_seconds=30
    )

    assert ok is True


async def test_renew_lease_fails_for_stale_generation(
    pool: asyncpg.Pool, make_workflow_row: MakeWorkflowRow
) -> None:
    """Renewing with a superseded generation fails — the fenced-out signal."""
    wid = await make_workflow_row(idempotency_key=f"k_{uuid4()}")
    claimed = await claim_workflow(pool, workflow_id=wid, worker_id=uuid4(), lease_seconds=30)
    assert claimed is not None

    ok = await renew_lease(
        pool, workflow_id=wid, lease_generation=claimed.lease_generation + 1, lease_seconds=30
    )

    assert ok is False


async def test_settle_terminal_succeeds_for_current_generation(
    pool: asyncpg.Pool, make_workflow_row: MakeWorkflowRow
) -> None:
    """A settlement write for the correct generation succeeds and persists the outcome."""
    wid = await make_workflow_row(idempotency_key=f"k_{uuid4()}")
    claimed = await claim_workflow(pool, workflow_id=wid, worker_id=uuid4(), lease_seconds=30)
    assert claimed is not None

    ok = await settle_terminal(
        pool,
        workflow_id=wid,
        lease_generation=claimed.lease_generation,
        status=WorkflowStatus.COMPLETED,
        output_data={"result": "done"},
    )

    assert ok is True
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT status, output_data FROM workflow_states WHERE id = $1", wid
        )
    assert row is not None
    assert row["status"] == "COMPLETED"


async def test_settle_terminal_fails_for_fenced_out_generation(
    pool: asyncpg.Pool, make_workflow_row: MakeWorkflowRow
) -> None:
    """The core guarantee: a fenced-out worker's write is a no-op.

    The row must stay untouched by the zombie, and the legitimate
    reclaiming worker must still be able to complete it cleanly afterward.
    """
    wid = await make_workflow_row(idempotency_key=f"k_{uuid4()}")
    zombie_claim = await claim_workflow(
        pool, workflow_id=wid, worker_id=uuid4(), lease_seconds=-1
    )
    assert zombie_claim is not None

    legit_claim = await claim_workflow(pool, workflow_id=wid, worker_id=uuid4(), lease_seconds=30)
    assert legit_claim is not None
    assert legit_claim.lease_generation == zombie_claim.lease_generation + 1

    zombie_result = await settle_terminal(
        pool,
        workflow_id=wid,
        lease_generation=zombie_claim.lease_generation,
        status=WorkflowStatus.COMPLETED,
        output_data={"from": "zombie"},
    )
    assert zombie_result is False

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT status, output_data FROM workflow_states WHERE id = $1", wid
        )
    assert row is not None
    assert row["status"] == "RUNNING"
    assert row["output_data"] is None

    legit_result = await settle_terminal(
        pool,
        workflow_id=wid,
        lease_generation=legit_claim.lease_generation,
        status=WorkflowStatus.COMPLETED,
        output_data={"from": "legit"},
    )
    assert legit_result is True