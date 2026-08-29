-- Composition: a workflow can hand off to a successor.
--
-- decisions.md #13 chose chains of separate rows over intra-step replay, but
-- nothing implemented the link. These two columns are that link: a successor
-- records which workflow produced it, and how far down the chain it sits.
--
-- Both are set once, when the successor row is created, and never updated.
-- That immutability is what lets the worker read chain_depth at claim time
-- and reason about it without re-checking.

ALTER TABLE workflow_states
    ADD COLUMN parent_workflow_id UUID REFERENCES workflow_states(id),
    ADD COLUMN chain_depth        INTEGER NOT NULL DEFAULT 0;

-- Walking a chain — "what did this step produce, and what ran before it" — is
-- the first question anyone debugging a chained workflow asks. Partial, since
-- roots (the overwhelming majority of rows) have no parent to look up.
CREATE INDEX idx_workflow_states_parent
    ON workflow_states (parent_workflow_id)
    WHERE parent_workflow_id IS NOT NULL;

-- Successor rows get a derived idempotency key, 'chain:<parent_id>:<type>',
-- which is what makes creating one twice a no-op instead of a fork. That
-- reserves the 'chain:' prefix: a client free to submit the same key could
-- silently take over a chain step that another workflow is about to create,
-- or have its own workflow taken over. Enforced here rather than in Ingress
-- because Ingress is not the only writer of this table.
ALTER TABLE workflow_states
    ADD CONSTRAINT chk_idempotency_key_namespace
    CHECK (idempotency_key NOT LIKE 'chain:%' OR parent_workflow_id IS NOT NULL);
