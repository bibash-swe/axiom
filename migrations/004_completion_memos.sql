-- Completion memos: the record of a paid provider call, so a re-run reuses it
-- instead of paying again.
--
-- decisions.md #18 measured that provider-side idempotency keys do not exist,
-- so the only place this gap can close is here. A reclaimed workflow re-runs
-- its handler from the start (#13), and today that re-issues every provider
-- call it already made. This table is what a re-run consults first.
--
-- The write against this table is deliberately UNFENCED — there is no
-- lease_generation predicate anywhere below, unlike every write in
-- workflow_states. A superseded worker must still be able to record what it
-- spent: the memo is not workflow state, it is a receipt for money that has
-- already left, and that stays true no matter who owns the workflow next.
-- Indeed the reclaim case depends on it, because the memo the winner reads is
-- usually the one the loser wrote.

CREATE TABLE workflow_call_memos (
    workflow_id UUID    NOT NULL REFERENCES workflow_states(id) ON DELETE CASCADE,
    call_index  INTEGER NOT NULL,

    -- SHA-256 of the canonicalised request. A guard, never part of the key:
    -- a handler that issues a *different* call at the same index has broken
    -- the determinism this design assumes, and must fail loudly rather than
    -- be handed the answer to a question it did not ask.
    fingerprint CHAR(64) NOT NULL,

    response    JSONB   NOT NULL,

    -- Which attempt actually paid. Recorded, never enforced — that asymmetry
    -- is the point of the unfenced write, and keeping the column makes it
    -- auditable instead of merely asserted: after an incident, "a fenced
    -- worker paid for this" is a query rather than a guess.
    written_by_lease_generation INTEGER NOT NULL,

    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- (workflow_id, call_index) and not the fingerprint, because a handler may
    -- legitimately issue the identical request twice — sampling two
    -- completions from one prompt is the obvious case — and keying on content
    -- would silently collapse them into one paid call and one copy.
    PRIMARY KEY (workflow_id, call_index),
    CONSTRAINT chk_call_index_non_negative CHECK (call_index >= 0)
);

-- No secondary index. Every read is by the full primary key; the composite PK
-- already serves it as an index scan, and ON DELETE CASCADE handles the only
-- other access pattern.
