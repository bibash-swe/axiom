"""The workflow state machine, checked exhaustively rather than by sampling.

Until migration 005 there was no state machine — there was a set of nine
permitted status *values* and a convention that every query would carry the
right predicate. `UPDATE workflow_states SET status = 'RUNNING'` on a COMPLETED
row succeeded, and nothing anywhere refused it.

The transition space is small and finite: 9 states squared is 81 ordered pairs.
That means it can be covered completely, so this file covers it completely.
Every cell is attempted against a real row in a real database and the outcome
compared with an expectation written here, independently of the migration —
which matters, because a test that asked the transition table what to expect
could only ever prove the trigger agrees with the table, never that the table
is right.
"""

from itertools import product
from uuid import UUID, uuid4

import asyncpg
import pytest

from axiom.contracts.enums import WorkflowStatus
from axiom.relay.relay import settle_failures

ALL_STATES: list[str] = [s.value for s in WorkflowStatus]

# Derived by reading every write to workflow_states.status in src/ — six of
# them, across worker.py and relay.py — not by copying migration 005. If this
# set and the table ever disagree, one of them is wrong and
# test_the_migration_agrees_with_the_independently_derived_set says so.
LEGAL_TRANSITIONS: frozenset[tuple[str, str]] = frozenset(
    {
        ("PENDING", "RUNNING"),  # worker: claim_workflow
        ("PENDING", "DISPATCH_FAILED"),  # relay: settle_failures at max_retries
        ("RUNNING", "PENDING"),  # worker: schedule_retry
        ("RUNNING", "COMPLETED"),  # worker: settle_terminal / settle_and_chain
        ("RUNNING", "FAILED"),  # worker: settle_terminal
        ("RUNNING", "DEAD_LETTERED"),  # worker: check_and_handle_poison_pill
    }
)

# Reserved for the Phase 5 API — cancellation and human-in-the-loop resume.
# Unreachable today, and deliberately so: giving them transitions now would be
# encoding a guess about a design that has not been built.
UNIMPLEMENTED_STATES: frozenset[str] = frozenset(
    {"WAITING_FOR_INPUT", "CANCELING", "CANCELED"}
)

TERMINAL_STATES: frozenset[str] = frozenset(
    {"COMPLETED", "FAILED", "CANCELED", "DEAD_LETTERED", "DISPATCH_FAILED"}
)


async def _attempt_transition(pool: asyncpg.Pool, *, source: str, target: str) -> bool:
    """Create a row in `source`, try to move it to `target`. True if the database allowed it."""
    async with pool.acquire() as conn:
        workflow_id: UUID = await conn.fetchval(
            "INSERT INTO workflow_states "
            "(workflow_type, workflow_version, idempotency_key, status) "
            "VALUES ('transition_probe', 'tprobe', $1, $2) RETURNING id",
            f"transition_{uuid4()}",
            source,
        )
        try:
            await conn.execute(
                "UPDATE workflow_states SET status = $2 WHERE id = $1", workflow_id, target
            )
            permitted = True
        except asyncpg.exceptions.CheckViolationError:
            permitted = False
        finally:
            await conn.execute("DELETE FROM workflow_states WHERE id = $1", workflow_id)
    return permitted


@pytest.mark.parametrize(("source", "target"), list(product(ALL_STATES, ALL_STATES)))
async def test_every_state_pair_is_permitted_exactly_when_it_should_be(
    pool: asyncpg.Pool, source: str, target: str
) -> None:
    """All 81 ordered pairs. Complete coverage of the transition space, not a sample.

    The diagonal is a special case with a real reason. The trigger fires only
    WHEN (OLD.status IS DISTINCT FROM NEW.status), so a write that leaves the
    status alone is never a transition and is never checked — which is exactly
    what the ingress ON CONFLICT DO UPDATE relies on, since a resubmitted
    idempotency key lands on a row in whatever state it has already reached,
    terminal included.
    """
    permitted = await _attempt_transition(pool, source=source, target=target)

    if source == target:
        expected = True
        why = "a write that does not change status is not a transition"
    else:
        expected = (source, target) in LEGAL_TRANSITIONS
        why = "listed in LEGAL_TRANSITIONS" if expected else "no code path performs it"

    assert permitted is expected, (
        f"{source} -> {target}: database {'allowed' if permitted else 'refused'} it, "
        f"expected {'allowed' if expected else 'refused'} — {why}"
    )


async def test_no_transition_out_of_a_terminal_state_is_possible(pool: asyncpg.Pool) -> None:
    """The property the whole migration exists for, stated directly.

    Covered cell-by-cell above, but asserted here as one claim because it is
    the one a reader actually wants: once a workflow is done, it is done.
    """
    escapes = [
        (source, target)
        for source, target in product(sorted(TERMINAL_STATES), ALL_STATES)
        if source != target and await _attempt_transition(pool, source=source, target=target)
    ]
    assert escapes == [], f"terminal states are not terminal: {escapes}"


async def test_the_migration_agrees_with_the_independently_derived_set(
    pool: asyncpg.Pool,
) -> None:
    """The table is the specification; this file derived the same thing from the code."""
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT from_status, to_status FROM workflow_state_transitions")
    in_database = {(r["from_status"], r["to_status"]) for r in rows}

    assert in_database == LEGAL_TRANSITIONS, (
        f"only in the database: {sorted(in_database - LEGAL_TRANSITIONS)}; "
        f"only in the code-derived set: {sorted(LEGAL_TRANSITIONS - in_database)}"
    )


async def test_the_vocabulary_is_identical_in_all_three_places(pool: asyncpg.Pool) -> None:
    """WorkflowStatus, chk_status and workflow_statuses must not drift apart.

    Three copies of the same list is two too many, but each exists for a
    reason — Python needs the enum, the column needs a CHECK, and the
    transition table needs something to reference. This is what stops them
    diverging silently.
    """
    async with pool.acquire() as conn:
        status_rows = await conn.fetch("SELECT status FROM workflow_statuses")
        table_states = {r["status"] for r in status_rows}
        check_clause: str = await conn.fetchval(
            "SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conname = 'chk_status'"
        )

    assert table_states == set(ALL_STATES), "workflow_statuses disagrees with WorkflowStatus"
    for state in ALL_STATES:
        assert f"'{state}'" in check_clause, f"{state} missing from chk_status"


async def test_the_status_metadata_is_derivable_rather_than_asserted(pool: asyncpg.Pool) -> None:
    """is_terminal and is_implemented must follow from the transition table itself.

    Both columns are conveniences — they could be computed on every read — so
    the risk is that someone edits the transitions and forgets the flags.

    The derivation only holds for *implemented* states, and getting that wrong
    is what this test caught on its first run. "Terminal means nothing leads
    out of it" is true of COMPLETED and false of WAITING_FOR_INPUT, which is
    non-terminal and has no outbound transitions purely because Phase 5 has not
    been built. For those three states is_terminal is a forward declaration
    about a design that does not exist yet, so it is asserted against the
    documented intent rather than derived — and the honest distinction is the
    point, since deriving it would be inventing the HITL design in a CHECK
    constraint.
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT status, is_terminal, is_implemented FROM workflow_statuses")

    for row in rows:
        status = row["status"]
        participates = any(status in pair for pair in LEGAL_TRANSITIONS)
        assert row["is_implemented"] is participates, f"{status}: is_implemented is wrong"

        if row["is_implemented"]:
            has_outbound = any(source == status for source, _ in LEGAL_TRANSITIONS)
            assert row["is_terminal"] is not has_outbound, (
                f"{status}: is_terminal disagrees with the transition table"
            )
        else:
            assert status in UNIMPLEMENTED_STATES, f"{status}: unexpected unimplemented state"

        assert (status in TERMINAL_STATES) is row["is_terminal"]


async def test_a_relay_dispatch_failure_cannot_overwrite_a_workflow_that_already_ran(
    pool: asyncpg.Pool,
) -> None:
    """Regression: the Relay could terminalize a workflow that had already completed.

    settle_failures had no status predicate, and decision #7's argument that it
    needed none — "a row that never dispatched can never be claimed by a
    worker" — does not survive the ambiguous failure this engine is built for.
    A publish that times out client-side but lands server-side is counted as a
    failure while a worker consumes the message perfectly happily; enough of
    those and the Relay stamps DISPATCH_FAILED over a COMPLETED workflow.
    """
    async with pool.acquire() as conn:
        workflow_id: UUID = await conn.fetchval(
            "INSERT INTO workflow_states "
            "(workflow_type, workflow_version, idempotency_key, status, output_data) "
            "VALUES ('relay_probe', 'rprobe', $1, 'COMPLETED', '{\"done\": true}'::jsonb) "
            "RETURNING id",
            f"relay_race_{uuid4()}",
        )
        instance_id = uuid4()
        outbox_id: UUID = await conn.fetchval(
            "INSERT INTO workflow_outbox "
            "(workflow_id, event_type, payload, workflow_version, retry_count, claimed_by) "
            "VALUES ($1, 'WORKFLOW_STARTED', '{}'::jsonb, 'rprobe', 5, $2) RETURNING id",
            workflow_id,
            instance_id,
        )

    # The publish "failed" for the sixth time, past max_retries.
    await settle_failures(pool, instance_id=instance_id, failed_ids=[outbox_id], max_retries=5)

    async with pool.acquire() as conn:
        status = await conn.fetchval(
            "SELECT status FROM workflow_states WHERE id = $1", workflow_id
        )
        # The outbox FK has no ON DELETE CASCADE, unlike workflow_call_memos.
        await conn.execute("DELETE FROM workflow_outbox WHERE id = $1", outbox_id)
        await conn.execute("DELETE FROM workflow_states WHERE id = $1", workflow_id)

    assert status == "COMPLETED", (
        f"a completed workflow was overwritten to {status} by a dispatch failure"
    )


async def test_both_production_insert_paths_create_a_workflow_as_pending(
    pool: asyncpg.Pool,
) -> None:
    """Migration 005 enforces transitions, not birth states — this covers the gap.

    An INSERT is not a transition, and the test fixtures deliberately construct
    rows directly in RUNNING and terminal states to reach scenarios that would
    otherwise need a full round trip. So the guarantee that a workflow always
    begins PENDING is asserted here rather than by constraint.
    """
    from axiom.ingress.repository import submit_workflow
    from axiom.worker.worker import claim_workflow, settle_and_chain

    submitted = await submit_workflow(
        pool,
        workflow_type="birth_probe",
        workflow_version="bprobe",
        idempotency_key=f"birth_{uuid4()}",
        input_data={},
    )
    async with pool.acquire() as conn:
        assert await conn.fetchval(
            "SELECT status FROM workflow_states WHERE id = $1", submitted.id
        ) == "PENDING"

    claimed = await claim_workflow(
        pool, workflow_id=submitted.id, worker_id=uuid4(), lease_seconds=30
    )
    assert claimed is not None
    successor_id = await settle_and_chain(
        pool,
        workflow_id=submitted.id,
        lease_generation=claimed.lease_generation,
        output_data={},
        next_workflow_type="birth_probe_next",
        next_input_data={},
    )
    async with pool.acquire() as conn:
        assert await conn.fetchval(
            "SELECT status FROM workflow_states WHERE id = $1", successor_id
        ) == "PENDING"
