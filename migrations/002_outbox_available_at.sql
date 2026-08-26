-- Retry scheduling for the outbox.
--
-- A failed handler needs to be tried again later, not immediately: an LLM
-- provider returning 429 or 503 wants backoff, and a tight redispatch loop
-- would just burn the remaining attempts in a few milliseconds. Giving the
-- outbox an availability time is the smallest mechanism that buys that —
-- the Relay already polls this table, so a row simply becomes invisible to
-- it until its time arrives.
--
-- Defaults to now(), so every existing row and every fresh dispatch from
-- Ingress is immediately claimable exactly as before.

ALTER TABLE workflow_outbox
    ADD COLUMN available_at TIMESTAMPTZ NOT NULL DEFAULT now();

-- Deliberately NOT adding available_at to idx_outbox_undispatched.
--
-- The Relay's claim filters on available_at but still orders by created_at
-- for starvation prevention, and a composite (available_at, created_at)
-- index cannot produce created_at order across an available_at range — it
-- would force a sort. The set of not-yet-available rows is small by nature
-- (only workflows currently in backoff), so filtering them out during the
-- existing created_at index scan costs almost nothing.
--
-- Revisit if retries ever become common enough that the undispatched
-- backlog is mostly rows waiting on a timer.
