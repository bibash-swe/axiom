-- The state machine becomes an object the database enforces, not a convention
-- every query is trusted to honour.
--
-- Before this, `UPDATE workflow_states SET status = 'RUNNING'` on a COMPLETED
-- row succeeded. Nothing refused it. Safety rested entirely on every query
-- carrying the right predicate — the worker's claim does, correctly — so one
-- carelessly written future query was all it took, and no constraint would
-- have caught it. Decision #7 chose the nine-state vocabulary but never wrote
-- down which transitions between those states are legal, so there was nothing
-- to enforce even in principle.
--
-- Two reference tables and one trigger. The tables are the specification, as
-- data: "what can happen next" becomes a query rather than a reading of
-- worker.py and relay.py.

-- The vocabulary, in one place. chk_status on workflow_states and
-- WorkflowStatus in contracts/enums.py are the other two copies; a test
-- asserts all three agree, which is what stops them drifting.
CREATE TABLE workflow_statuses (
    status      VARCHAR(30) PRIMARY KEY,

    -- Terminal means no outbound transition exists. The tests currently
    -- hardcode this set in four separate files; now it is derivable.
    is_terminal BOOLEAN NOT NULL,

    -- Whether any code path that exists today can produce this state. Recorded
    -- honestly rather than aspirationally: three states are reserved for the
    -- Phase 5 API (cancellation, human-in-the-loop resume) and are currently
    -- unreachable. Their transitions are deliberately absent from the table
    -- below, so the trigger refuses them until the phase that implements them
    -- adds the rows. Encoding a guess about Phase 5's design now would be
    -- exactly the assumption this migration exists to remove.
    is_implemented BOOLEAN NOT NULL
);

INSERT INTO workflow_statuses (status, is_terminal, is_implemented) VALUES
    ('PENDING',            FALSE, TRUE),
    ('RUNNING',            FALSE, TRUE),
    ('WAITING_FOR_INPUT',  FALSE, FALSE),
    ('CANCELING',          FALSE, FALSE),
    ('COMPLETED',          TRUE,  TRUE),
    ('FAILED',             TRUE,  TRUE),
    ('CANCELED',           TRUE,  FALSE),
    ('DEAD_LETTERED',      TRUE,  TRUE),
    ('DISPATCH_FAILED',    TRUE,  TRUE);

-- Every legal status change, derived from the six writes that exist in the
-- codebase rather than from what a workflow engine is generally assumed to do.
-- Anything absent here is refused by the trigger below, which includes all 40
-- transitions out of a terminal state.
CREATE TABLE workflow_state_transitions (
    from_status  VARCHAR(30) NOT NULL REFERENCES workflow_statuses(status),
    to_status    VARCHAR(30) NOT NULL REFERENCES workflow_statuses(status),

    -- Which component performs it. Not decoration: the write surface of this
    -- table is the thing decision #7 was protecting when it rejected QUEUED
    -- and ZOMBIE_RECLAIMED, and naming the writer per transition keeps that
    -- argument checkable instead of remembered.
    performed_by TEXT NOT NULL,

    PRIMARY KEY (from_status, to_status),
    CONSTRAINT chk_no_self_transition CHECK (from_status <> to_status)
);

INSERT INTO workflow_state_transitions (from_status, to_status, performed_by) VALUES
    ('PENDING', 'RUNNING',         'worker: claim_workflow'),
    ('PENDING', 'DISPATCH_FAILED', 'relay: settle_failures at max_retries'),
    ('RUNNING', 'PENDING',         'worker: schedule_retry'),
    ('RUNNING', 'COMPLETED',       'worker: settle_terminal / settle_and_chain'),
    ('RUNNING', 'FAILED',          'worker: settle_terminal'),
    ('RUNNING', 'DEAD_LETTERED',   'worker: check_and_handle_poison_pill');

-- A reclaim is RUNNING -> RUNNING with lease_generation incremented. It is not
-- a status change, so it is excluded by chk_no_self_transition above and by the
-- trigger's WHEN clause below, and it is fenced by lease_generation instead —
-- which is the correct guard for it, since what makes a reclaim legal is
-- whether the lease lapsed, not what the status says.

CREATE FUNCTION enforce_workflow_state_transition() RETURNS TRIGGER AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM workflow_state_transitions
        WHERE from_status = OLD.status AND to_status = NEW.status
    ) THEN
        RAISE EXCEPTION
            'illegal workflow state transition: % -> % (workflow_id=%)',
            OLD.status, NEW.status, OLD.id
            USING ERRCODE = '23514';  -- check_violation, so asyncpg raises CheckViolationError
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- WHEN (OLD.status IS DISTINCT FROM NEW.status) is load-bearing, not an
-- optimisation. It means the function never runs for the writes that dominate
-- this table — heartbeat lease renewals, output_data writes, and the ingress
-- ON CONFLICT DO UPDATE. That last one matters most and is the reason this
-- clause exists: the PostgreSQL 18 CREATE TRIGGER documentation is explicit
-- that "an INSERT with an ON CONFLICT DO UPDATE clause may cause both insert
-- and update operations, so it will fire both kinds of triggers as needed", so
-- a resubmitted idempotency key does reach this trigger, on a row in whatever
-- state it has already reached — terminal included. It survives because the
-- conflict update rewrites idempotency_key and never touches status.
--
-- Those writes still evaluate the WHEN expression; the docs note it is "not
-- materially different from testing the same condition at the beginning of the
-- trigger function". What they skip is the PL/pgSQL invocation and the lookup
-- against workflow_state_transitions, which is the part with a cost worth
-- avoiding.
CREATE TRIGGER trg_enforce_workflow_state_transition
    BEFORE UPDATE ON workflow_states
    FOR EACH ROW
    WHEN (OLD.status IS DISTINCT FROM NEW.status)
    EXECUTE FUNCTION enforce_workflow_state_transition();

-- Deliberately not enforced here: the status a row is *born* with. An INSERT is
-- not a transition, and the test fixtures construct rows directly in RUNNING
-- and terminal states to set up scenarios that would otherwise take a full
-- round trip to reach. Both production insert paths (ingress submit, chained
-- successor) write PENDING, which is asserted by test rather than by
-- constraint.
