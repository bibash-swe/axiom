"""Exhaustive check of the worker protocol, and the two things that make it mean anything.

The checker has to be shown capable of finding a violation, and the model has to
be shown to match the real SQL.
"""

from dataclasses import replace
from uuid import UUID, uuid4

import asyncpg
import pytest

from axiom.contracts.enums import WorkflowStatus
from axiom.worker.protocol_model import (
    Config,
    Phase,
    State,
    Successors,
    Worker,
    explore,
    settled_by,
    successors,
    with_worker,
)
from axiom.worker.worker import claim_workflow, renew_lease, settle_terminal


def test_no_safety_violation_in_any_reachable_state() -> None:
    """Every interleaving of two workers, not a sample of them."""
    states, violations = explore(Config())

    assert violations == [], "\n\n".join(v.render() for v in violations)
    assert states > 1000, f"only {states} states — the model is barely exercising anything"


def test_still_holds_with_three_workers() -> None:
    """Two workers can hide a bug that needs three to expose."""
    _states, violations = explore(Config(workers=3))
    assert violations == [], "\n\n".join(v.render() for v in violations)


def test_still_holds_when_the_lease_outlives_the_xautoclaim_threshold() -> None:
    """Is LEASE < XAUTOCLAIM a safety invariant or a liveness one?

    .env.example says breaking it "guarantees split-brain execution".
    Inverting it here settles which.
    """
    _states, violations = explore(Config(lease_duration=4, xautoclaim_idle=1))
    assert violations == [], "\n\n".join(v.render() for v in violations)


def _broken(kind: str) -> Successors:
    """The real protocol with one rule broken.

    claim_without_incrementing has to *replace* the claim edge, not sit beside
    it: leaving the real one in place means the second worker still gets a
    fresh generation and the bug cannot manifest.
    """

    def step(state: State, config: Config) -> list[tuple[str, State]]:
        out = successors(state, config)
        if kind == "claim_without_incrementing":
            out = [(label, s) for label, s in out if " claim (gen" not in label]

        for i, worker in enumerate(state.workers):
            delivered_idle = state.delivered_to == i and worker.phase is Phase.IDLE
            claimable = state.status == "PENDING" or (
                state.status == "RUNNING" and state.lease_expires_at < state.now
            )

            if kind == "claim_without_incrementing" and delivered_idle and claimable:
                reused = with_worker(
                    replace(
                        state,
                        status="RUNNING",
                        lease_expires_at=state.now + config.lease_duration,
                    ),
                    i,
                    Worker(Phase.HOLDING, state.generation),
                )
                out.append((f"w{i} claim reusing gen {state.generation}", reused))

            if kind == "unfenced_settle" and (
                worker.phase is Phase.HOLDING and worker.generation != state.generation
            ):
                out.append((f"w{i} settle while fenced", settled_by(state, i, "COMPLETED")))

            if kind == "ack_before_settle" and (
                worker.phase is Phase.HOLDING and state.delivered_to == i
            ):
                early = with_worker(
                    replace(state, acked=True, delivered_to=None), i, Worker(Phase.DONE, 0)
                )
                out.append((f"w{i} ack early", early))

            if (
                kind == "claim_terminal"
                and delivered_idle
                and state.is_terminal()
                and state.generation < config.max_generation
            ):
                generation = state.generation + 1
                revived = with_worker(
                    replace(state, status="RUNNING", generation=generation),
                    i,
                    Worker(Phase.HOLDING, generation),
                )
                out.append((f"w{i} claim a finished workflow", revived))
        return out

    return step


@pytest.mark.parametrize(
    ("kind", "expected"),
    [
        ("claim_without_incrementing", "at_most_one_live_owner"),
        ("unfenced_settle", "settled_at_most_once"),
        ("ack_before_settle", "ack_implies_terminal"),
        ("claim_terminal", "terminal_is_absorbing"),
    ],
)
def test_the_checker_catches_a_broken_protocol(kind: str, expected: str) -> None:
    """A clean run above is only evidence if these fail."""
    _states, violations = explore(Config(), successor_fn=_broken(kind))

    assert violations, f"{kind} went undetected — the invariants are too weak"
    assert expected in {v.invariant for v in violations}, (
        f"{kind} caught, but not by {expected}: {sorted({v.invariant for v in violations})}"
    )


async def _row(pool: asyncpg.Pool, *, status: str, generation: int, lease: str) -> UUID:
    """A workflow row in an exact state, bypassing the normal path."""
    async with pool.acquire() as conn:
        workflow_id: UUID = await conn.fetchval(
            "INSERT INTO workflow_states "
            "(workflow_type, workflow_version, idempotency_key, status, lease_generation, "
            " lease_expires_at) "
            f"VALUES ('model', 'model', $1, $2, $3, NOW() + INTERVAL '{lease}') RETURNING id",
            f"model_{uuid4()}",
            status,
            generation,
        )
    return workflow_id


@pytest.mark.parametrize(
    ("status", "lease", "claimable"),
    [
        ("PENDING", "-1 second", True),
        ("RUNNING", "-1 second", True),
        ("RUNNING", "1 hour", False),
        ("COMPLETED", "-1 second", False),
        ("FAILED", "-1 second", False),
    ],
)
async def test_the_models_claim_predicate_matches_real_postgres(
    pool: asyncpg.Pool, status: str, lease: str, claimable: bool
) -> None:
    """The join between model and code.

    If this drifts, every exhaustive result above describes a system we do not run.
    """
    workflow_id = await _row(pool, status=status, generation=1, lease=lease)
    claimed = await claim_workflow(
        pool, workflow_id=workflow_id, worker_id=uuid4(), lease_seconds=30
    )

    expired = lease.startswith("-")
    modelled = status == "PENDING" or (status == "RUNNING" and expired)

    assert (claimed is not None) is claimable, f"{status}/{lease}: database disagreed"
    assert modelled is claimable, f"{status}/{lease}: model disagreed with the database"


@pytest.mark.parametrize("stale", [True, False])
async def test_fenced_writes_are_no_ops_in_the_database_too(
    pool: asyncpg.Pool, stale: bool
) -> None:
    """A stale generation must change nothing, for both renew and settle."""
    workflow_id = await _row(pool, status="RUNNING", generation=5, lease="1 hour")
    generation = 4 if stale else 5

    renewed = await renew_lease(
        pool, workflow_id=workflow_id, lease_generation=generation, lease_seconds=30
    )
    settled = await settle_terminal(
        pool,
        workflow_id=workflow_id,
        lease_generation=generation,
        status=WorkflowStatus.COMPLETED,
    )

    assert renewed is not stale
    assert settled is not stale
