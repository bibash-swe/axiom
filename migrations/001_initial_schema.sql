-- Axiom initial schema.
-- The status CHECK constraint below must list exactly the same nine values
-- as WorkflowStatus in src/axiom/contracts/enums.py. Nothing enforces that
-- agreement automatically yet — see docs/decisions.md once written, and
-- the planned consistency test in tests/contracts/.

CREATE TABLE workflow_states (
    id                  UUID PRIMARY KEY DEFAULT uuidv7(),
    workflow_type       VARCHAR(100) NOT NULL,
    workflow_version    VARCHAR(20)  NOT NULL DEFAULT 'v1',
    status              VARCHAR(30)  NOT NULL DEFAULT 'PENDING',
    idempotency_key     VARCHAR(255) NOT NULL,

    -- Fencing: worker_id + lease_generation together let a superseded
    -- worker's final write become a guaranteed no-op (WHERE lease_generation
    -- = $stale_gen affects 0 rows). lease_generation also doubles as the
    -- DLQ attempt counter — it only increments on a genuine claim/reclaim,
    -- never on a fenced-out write, so it can't misfire on a healthy race.
    worker_id           UUID,
    lease_generation    INTEGER      NOT NULL DEFAULT 0,
    lease_expires_at    TIMESTAMPTZ,

    input_data          JSONB,
    output_data         JSONB,
    error_log           JSONB,

    created_at          TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ  NOT NULL DEFAULT now(),

    -- Enforced at the database, not just checked in application code — see
    -- the ON CONFLICT ... DO UPDATE ... RETURNING trick in the ingress
    -- repository, which depends on this constraint existing to work at all.
    CONSTRAINT unq_idempotency_key UNIQUE (idempotency_key),

    CONSTRAINT chk_status CHECK (status IN (
        'PENDING', 'RUNNING', 'WAITING_FOR_INPUT', 'CANCELING',
        'COMPLETED', 'FAILED', 'CANCELED', 'DEAD_LETTERED', 'DISPATCH_FAILED'
    ))
);

-- Serves the worker's reclaim query directly: WHERE status = 'RUNNING' AND
-- lease_expires_at < NOW(). Partial index — only RUNNING rows are ever
-- candidates for reclaim, so there's no reason to index anything else.
CREATE INDEX idx_workflow_states_reclaimable
    ON workflow_states (lease_expires_at)
    WHERE status = 'RUNNING';

-- Serves the worker's fresh-claim query: WHERE status = 'PENDING',
-- ordered oldest-first to prevent starvation under load.
CREATE INDEX idx_workflow_states_pending
    ON workflow_states (created_at)
    WHERE status = 'PENDING';


CREATE TABLE workflow_outbox (
    id                  UUID PRIMARY KEY DEFAULT uuidv7(),
    workflow_id         UUID NOT NULL REFERENCES workflow_states(id),
    event_type          VARCHAR(50)  NOT NULL,
    payload             JSONB        NOT NULL,
    workflow_version    VARCHAR(20)  NOT NULL,

    dispatched          BOOLEAN      NOT NULL DEFAULT FALSE,

    -- The Relay's own claim lease — deliberately separate from
    -- workflow_states' worker_id/lease_generation. The Relay is a dumb
    -- pipe, not a long-running processor: no heartbeat, just a short lease
    -- that expires. See docs/decisions.md for why Option A (aggressive
    -- timeouts) was chosen over a heartbeat here.
    claimed_at          TIMESTAMPTZ,
    claimed_by          UUID,

    -- Poison-pill threshold, independent of lease_generation. A row that
    -- fails this many dispatch attempts is dead-lettered by the Relay
    -- itself — it never reaches a worker, so workflow_states transitions
    -- straight to DISPATCH_FAILED with no fencing contest possible.
    retry_count         INTEGER      NOT NULL DEFAULT 0,

    created_at          TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- Serves the Relay's claim query directly. Partial index keeps this cheap
-- even with millions of historical dispatched=TRUE rows.
CREATE INDEX idx_outbox_undispatched
    ON workflow_outbox (created_at)
    WHERE dispatched = FALSE;


-- Cordon-and-drain bookkeeping. A row here represents a workflow schema
-- version that's been deliberately cordoned (stopped accepting new work)
-- while its in-flight jobs finish on the old worker pool.
CREATE TABLE workflow_versions (
    version             VARCHAR(20) PRIMARY KEY,
    cordoned_at         TIMESTAMPTZ,
    decommissioned_at   TIMESTAMPTZ,
    created_at          TIMESTAMPTZ  NOT NULL DEFAULT now()
);

INSERT INTO workflow_versions (version) VALUES ('v1');


-- Audit trail only — never the primary home for a dead-lettered workflow.
-- workflow_states.status = 'DEAD_LETTERED' (or 'DISPATCH_FAILED') stays
-- the source of truth, so the idempotency lookup and the claim query never
-- need to special-case a row that's missing from workflow_states entirely.
CREATE TABLE dlq_workflows (
    id                          UUID PRIMARY KEY DEFAULT uuidv7(),
    workflow_id                 UUID NOT NULL REFERENCES workflow_states(id),
    reason                      TEXT NOT NULL,
    lease_generation_at_dlq     INTEGER,
    outbox_retry_count_at_dlq   INTEGER,
    created_at                  TIMESTAMPTZ NOT NULL DEFAULT now()
);