# Architecture decisions

Every non-obvious choice in this repo, with the reasoning behind it — not
just the conclusion. Written so a reviewer (or future-me) can see these
were argued to, not defaulted into.

---

## 1. Organize by deployable component, not technical layer

**Decision:** `src/axiom/` is split into `ingress/`, `relay/`, `worker/`,
`cache/`, `janitor/`, `scheduler/`, `api/`, `observability/` — not
`models/`, `services/`, `routes/`.

**Why:** Ingress, the Relay, and the Worker Fleet are separate deployable
processes with separate failure domains — a worker crashing has nothing to
do with the ingress gateway crashing. A layer-based structure (`models/`,
`services/`) would scatter one component's full behavior across four
folders and actively hide the thing that matters most about this system:
which failures belong to which process. Organizing by component means the
folder structure *is* the architecture diagram.

**Also:** `src/` layout, not a flat `axiom/` at repo root. Without it,
`import axiom` can succeed by accident (Python finds the package via the
working directory) even when real packaging is broken — a bug that would
otherwise hide until deploy. `src/` forces a genuine install path locally,
the same one a real deployment uses.

---

## 2. `contracts/`: wire contracts only, never business logic

**Decision:** A dedicated `contracts/` package holds only what crosses a
process boundary between our own components — status vocabulary, event
payload shapes. It never holds a database query, a route handler, or a
Lua script.

**Why not shared-nothing:** Every component redefining its own guess at a
shared shape (the outbox payload, the status enum) is exactly how three of
our worst bugs happened during design — independent, drifting assumptions
about the same boundary. A shared contract doesn't couple components'
*behavior*; it's what lets that behavior stay decoupled, because both
sides are provably reading the same definition instead of two hand-typed
guesses.

**Why not `core/` or `domain/`:** Those names have no built-in constraint —
they become junk drawers over time ("where does this go? core, I guess").
`contracts/` has a hard boundary baked into the name: if it's not a schema
or enum crossing a boundary, it doesn't belong there.

---

## 3. `uv` + PEP 735 dependency groups

**Decision:** `uv` for dependency management; `[dependency-groups]` (a
real Python standard, not `uv`-specific) for dev-only tooling.

**Why:** `pyproject.toml` declares intent (loose version ranges);
`uv.lock` pins the exact resolved graph, so installs are reproducible
across machines — not "works on my machine." `uv.lock` is committed;
ignoring it (a common `.gitignore` mistake) would silently reopen that
exact problem.

---

## 4. No ORM — raw `asyncpg`

**Decision:** Direct SQL via `asyncpg`, no SQLAlchemy or any ORM.

**Why:** This system's entire reliability model depends on exact,
provable control over transaction boundaries and specific Postgres
mechanics — `SELECT ... FOR UPDATE SKIP LOCKED`, and the `xmax = 0`
insert-vs-update check verified directly against a real table before it
was trusted (see decision 8). An ORM's value proposition is abstracting
those mechanics away. Here, that abstraction is the opposite of what's
needed — raw SQL is the correct tool for a system whose core guarantee
lives in the exact shape of its queries, not a convenience we're doing
without.

---

## 5. Configuration is phase-gated, with one narrow exception

**Decision:** `.env.example` only declares a setting once the phase that
consumes it is being built — with one deliberate exception: the
`AXIOM_WORKER_LEASE_SECONDS` / `AXIOM_WORKER_XAUTOCLAIM_MIN_IDLE_SECONDS` /
`AXIOM_JANITOR_IDLE_THRESHOLD_SECONDS` triad is declared together, now,
before either the Worker or the Janitor exists.

**Why the general rule:** Declaring Phase 4 cache TTLs today, before the
cache is built, risks silent config drift — the value sits unused for
weeks, the implementation changes its mind, and nobody remembers to update
the file that was written a month earlier.

**Why the exception, precisely:** The exception isn't "these three values
are hard to understand in isolation" — it's that they're a single
correctness constraint enforced by *three different components built in
two different future phases*. Getting the ordering wrong (`LEASE` must be
`< XAUTOCLAIM`, which must be `< JANITOR`) reintroduces the split-brain
race we specifically designed the fencing mechanism to prevent — and if
each value is only declared when its own component is built, whoever
builds the Worker in Phase 3 has zero visibility into a Janitor constraint
that doesn't exist in the codebase yet. The margins between the three
don't need to be large (the safety comes from the Postgres status check
inside each mechanism, not the gap size) — they just need to exist, and be
visible together, before any of the three components can be built in
ignorant isolation.

This is the general test for any future candidate: does tuning this value
alone, during the phase where only its own component exists, risk a
*silent correctness failure* owned by a different, not-yet-built
component? If yes, declare it early. If the failure mode is just
"performance degrades a bit" (e.g. the cache's non-terminal TTL versus the
Worker's heartbeat interval), it doesn't qualify — wrong stakes.

---

## 6. Status enum: `StrEnum`, explicit string literals, never `auto()`

**Decision:** `WorkflowStatus` and `PublicStatus` are `StrEnum` (stdlib,
zero third-party dependencies), every member an explicit string literal.

**Why `StrEnum` over a bare `Enum`:** This value crosses three
serialization boundaries — Postgres `VARCHAR`, JSON payloads, Redis. A
bare `Enum` requires a `.value` call at every one of those sites; `StrEnum`
makes the value the string itself, closing off an entire class of "forgot
`.value`" bugs.

**Why never `auto()`:** Verified directly (not assumed) that `StrEnum`'s
`auto()` lowercases the member name — `PENDING = auto()` produces
`"pending"`, not `"PENDING"`, silently disagreeing with the uppercase
convention already baked into the Postgres `CHECK` constraint and every
other artifact in this system. More importantly: with `auto()`, the stored
wire value *is* the Python identifier — renaming `DEAD_LETTERED` to
`DEAD_LETTER` for style would silently change what's persisted to the
database. Explicit literals decouple those two concerns on purpose: the
Python name can be refactored freely; the wire value only changes when
someone deliberately edits the string.

---

## 7. The nine-state vocabulary, and two states that didn't make it

**Decision:** `PENDING`, `RUNNING`, `WAITING_FOR_INPUT`, `CANCELING`,
`COMPLETED`, `FAILED`, `CANCELED`, `DEAD_LETTERED`, `DISPATCH_FAILED`.
No `QUEUED`. No `ZOMBIE_RECLAIMED`.

**Why not `QUEUED`:** Adding a status the Relay would need to write
(distinguishing "not yet dispatched" from "dispatched, awaiting a worker")
would require the Relay to write to `workflow_states` on a non-terminal,
*contestable* transition — reopening the exact class of race we already
fixed once (a worker claiming `RUNNING` concurrently with the Relay's
write, unless perfectly guarded). The Relay's only existing write to this
table is the terminal, race-free `DISPATCH_FAILED` transition, which is
safe specifically because a row that never dispatched can never be
claimed by a worker — there's no contest possible. `QUEUED` doesn't have
that property. If dispatch visibility is ever needed, read it from
`workflow_outbox.dispatched` instead of widening the state machine's
write surface.

**Why not `ZOMBIE_RECLAIMED`:** This assumes the Janitor reclaims stalled
jobs. It doesn't — the Janitor never writes to `workflow_states` at all;
its only job is checking whether a PEL entry's row is *already* terminal
and force-`ACK`ing if so. The actual reclaiming of a stalled `RUNNING` job
is done entirely by the next worker via the ordinary `SKIP LOCKED` claim
query, independent of the Janitor. A `ZOMBIE_RECLAIMED` status would
require giving the Janitor write access to the core state machine —
directly undoing the scoping discipline that keeps its blast radius at
zero.

---

## 8. Idempotent ingress write: `ON CONFLICT DO UPDATE`, not `DO NOTHING`

**Decision:** The ingress insert uses
`ON CONFLICT (idempotency_key) DO UPDATE SET idempotency_key = EXCLUDED.idempotency_key RETURNING id, ..., (xmax = 0) AS is_new_row`.

**Why not `DO NOTHING`:** `DO NOTHING` returns zero rows on a conflict —
a replayed request would have no `id` to hand back to the client. `DO
UPDATE` (a harmless no-op self-update) always returns a row, first-insert
or replay alike, so a duplicate submission can be answered inline without
a second round trip.

**On trusting `xmax = 0`:** This is a real but internals-reliant Postgres
behavior, not a guaranteed public API — verified directly against a real
table (insert, re-insert same key, confirm `is_new_row` flips `true` →
`false` and the original row's data is untouched) before being trusted in
application code, rather than assumed from a remembered blog post.

---

## 9. Outbox event payload: a dispatch signal, not a data carrier

**Decision:** `WorkflowStartedEvent` carries exactly `event_type` and
`workflow_id`. Nothing else — no `workflow_type`, no `input_data`.

**Why:** Including workflow data in the event creates a second copy of
facts that already live in `workflow_states` — and a second copy is a
copy that can go stale. The event's only job is to be a wake-up signal:
"something happened, here's the id, go look." The Worker, in Phase 3,
re-reads everything else directly from Postgres at claim time. This is
the same principle behind every anti-entropy mechanism in this design:
nothing trusts its own memory of a fact when Postgres can just be asked.

**Also:** `event_type` is a Pydantic `Literal["WORKFLOW_STARTED"]`, not a
new `StrEnum` in `contracts/enums.py`. A single-member enum for one event
type is exactly the aspirational-vocabulary mistake ruled out in decision
5 — promote it to a real enum (and use Pydantic's discriminated unions)
the moment a second event type actually exists, not before.

---

## 10. PostgreSQL 18, and `uuidv7()` on every UUID primary key

**Decision:** `postgres:18`, and `uuidv7()` (not `gen_random_uuid()`) as
the default on `workflow_states.id`, `workflow_outbox.id`, and
`dlq_workflows.id`.

**Why:** Verified against the official PG18 release notes before
adopting, not taken on faith: `uuidv7()` is a real core function
(released September 2025), time-ordered rather than fully random, which
avoids the B-tree index fragmentation that UUIDv4 causes under the high
insert volume both of these tables see by design. `RETURNING OLD.*, NEW.*`
is also confirmed real and will likely be used for audit logging when the
Worker's claim query is built in Phase 3 — not used yet. The often-cited
"async I/O" win was corrected during review: PG18's AIO subsystem is
specifically a read-path optimization (sequential scans, bitmap heap
scans, vacuum), not a general read/write throughput multiplier — still
relevant here (our claim queries and cache-miss fallback are reads), just
not for the reason first assumed.

**Also:** the `CREATE EXTENSION pgcrypto` line was removed — it's been
unnecessary since `gen_random_uuid()` was folded into Postgres core in
version 13, doubly so now that UUID generation has moved to `uuidv7()`
entirely.

---

## 11. Local credentials: fail loud, not silent

**Decision:** `docker-compose.yml` reads Postgres credentials from `.env`
via variable substitution. `POSTGRES_USER` and `POSTGRES_DB` fall back to
a benign default (`${VAR:-axiom}`); `POSTGRES_PASSWORD` has no default and
uses `${VAR:?message}`, which makes `docker compose up` fail immediately
with a clear error if `.env` doesn't set it.

**Why the split:** Username and database name aren't secrets — a silent
default is harmless. The password is the one value where a silent,
insecure default (or an empty string) is the actual risk worth designing
against, so it's the only one required to fail loudly rather than
fall back quietly.

---

## 12. Relay poll interval: 100ms, not a full second

**Decision:** `AXIOM_RELAY_POLL_INTERVAL_SECONDS = 0.1`, checked on every
idle cycle of the Relay's run loop.

**Why:** An empty `SELECT ... FOR UPDATE SKIP LOCKED` against
`idx_outbox_undispatched` is trivially cheap even at high frequency — a
partial index scan that returns nothing costs sub-millisecond, so there's
no real Postgres-load argument for polling slowly. Against that near-zero
cost, tighter polling buys strictly better, more deterministic dispatch
latency for free. This number isn't new — it was committed to during the
original design discussion, before any code existed — but the first
concrete implementation of the run loop briefly drifted to 1 second with
no cost-based justification, simply because no one had gone back and
checked the number against the original reasoning. Written down explicitly
here specifically so that doesn't happen silently again.

## 13. Execution model: atomic steps, composed — not intra-step replay

**Decision:** A reclaimed workflow re-runs its handler from the start;
there is no checkpointing of partial progress within a single
`workflow_states` row. A multi-step agentic workflow is modeled as a
*chain* of separate Axiom workflow rows — the output of step N becomes
part of step N+1's input, dispatched as its own outbox event — not as
one workflow with internal event-sourced replay.

**Why:** This was already true of the code; it just hadn't been decided
on purpose. The alternative — Temporal-style replay, reconstructing
exact intermediate state from an event log — is a substantially larger
mechanism we haven't built and don't need for the guarantee this system
actually promises. DBOS is a real, production precedent for the same
choice: their workflows resume "from the last completed step," with
each `@DBOS.step()` checkpointed atomically and no finer-grained replay
within a step. Deciding this now, cheaply, rather than leaving it
implicit into Phase 5, where "human-in-the-loop resume" would otherwise
have to guess at a semantics nobody actually chose.

## 14. LISTEN/NOTIFY considered for Relay dispatch, deferred

**Decision:** The Relay keeps polling (100ms, per decision 12) as the
sole dispatch-wakeup mechanism. `LISTEN`/`NOTIFY` was not adopted.

**Why:** Postgres `NOTIFY` is not durable — a listener disconnected at
the exact moment a notification fires loses it permanently, with
nothing queued for later delivery. That means `NOTIFY` could only ever
be a fast-path wakeup layered on top of the existing poll as a
durability backstop, never a replacement for it. Worth revisiting if
dispatch latency ever becomes a measured problem, but the poll's latency
floor is already immaterial against workflow durations measured in
seconds to minutes, so the added complexity (trigger management,
listener reconnection handling) isn't justified by the win today.

## 15. Provider-side cancellation: measured for Mistral, unproven elsewhere

**Decision:** The cost-safety guarantee is stated *per provider*, and only
for a provider a probe has actually been run against. For Mistral
(`mistral-small-latest`, standard tier, August 2026), disconnecting does
prevent generation continuing to `max_tokens`. That is recorded here as a
dated measurement with a scope, not promoted into a property of the system.

**Why this needed measuring at all:** `stream_guard` closing the socket is
entirely within our control and is proven — see
`tests/worker/test_transport_cancellation.py`, which observes the close from
the server's side. Whether the *provider* stops generating, and stops
billing, once that socket closes is a property of someone else's
infrastructure. No test of our own code can establish it, and the README
previously asserted it anyway.

**The instrument, and two flaws that would have made the answer worthless.**
The only client-visible cost oracle Mistral exposes is the rate-limit token
budget. Two controls were run before trusting it, and both mattered:

1. *Does the limiter debit actual usage, or reserve `max_tokens` at
   admission?* Had it reserved, an aborted run and a full run would debit
   identically no matter what the backend did — the experiment would have
   reported "the provider kept generating" in every possible world,
   including the one where cancellation works perfectly. Verified it debits
   actual: `max_tokens=2000` requested, 23 tokens generated, 23 debited. The
   `x-ratelimit-tokens-query-cost` header matched `usage.total_tokens`
   exactly on every call.

2. *Is the counter stable?* It is not — it is a **sliding 60-second
   window**. Measured directly: 440 tokens spent, counter fell only 112,
   roughly 328 aged back in during the measurement. This invalidated the
   original probe's 2-second settle delay in the most dangerous possible
   direction: if the backend keeps generating, those tokens debit *as they
   generate*, over the following tens of seconds, so reading 2s after
   aborting sees nothing and prints "cancellation worked" regardless of the
   truth. The first method was biased toward the comfortable conclusion.

**Method after correction:** both arms read the counter at an identical
wall-clock offset from run start, so sliding refill affects them equally and
cancels out of the comparison; every trial is gated on an *observed* drained
window rather than an assumed-sufficient quiet period; ABORTED runs first in
each pair so residual FULL generation cannot bleed into it.

**Finding:**

```
FULL     644, 590, 585, 590     (predicted ~625)
ABORTED  108, -95, -10, -37
```

Every FULL run lands at the predicted full cost. No ABORTED run does — all
four sit in a noise band around zero. Had the backend continued to
`max_tokens`, ABORTED would read ~600, a signal many times the noise floor
and impossible to miss.

A second, independent signal agrees. The time the window takes to drain
before the *next* trial is a proxy for what the previous trial consumed:
after an ABORTED trial the next drains in 0 polls; after a FULL trial it
needs 5–9 polls (50–90s). Four for four, measured by an entirely different
mechanism than the delta arithmetic.

**What is still unknown:** whether the residual is zero or a tail of a few
dozen tokens generated after the socket closes. That is below this
instrument's noise floor and no number of re-runs fixes it — resolving it
needs a per-request usage API, not a rate-limit header. Budget for a tail
rather than assuming the cutoff is instant.

**What this does *not* cover:** fencing still does not prevent a
*reclaiming* worker from issuing a second billed call. Because a reclaimed
workflow re-runs its handler from the start (decision 13), a
stalled-then-reclaimed workflow can pay twice regardless of how cleanly the
first worker aborted. That is a separate gap, closed by provider-side
idempotency keys on egress, which are not yet implemented.

**Generalizing the lesson, since it recurred three times:** every
measurement built during this work initially returned a confident, plausible
number that did not survive scrutiny — the transport fixture measured its
own chunk cadence, the first probe measured its own settle delay, and the
second measured the previous trial's refund. None failed loudly; all three
would have been believed. Validate the instrument against a known quantity
before trusting what it says about an unknown one.

## 16. Composition: a handler returns its successor, written in one transaction

**Decision:** A handler ends its workflow by returning a dict, or continues
the chain by returning `NextStep(output=..., workflow_type=..., input_data=...)`.
The worker completes the current row and creates the successor row plus its
outbox event inside a single Postgres transaction. The successor is an
ordinary workflow — its own id, lease, retry budget and dead-letter ceiling —
linked to its parent by `parent_workflow_id` and `chain_depth` (migration 003).

**Why a return value rather than a `submit_next()` call a handler makes:**
An explicit call is a second write the handler could make and then crash
before its own completion committed, or commit after being fenced out. As a
return value, the handler cannot separate the two: there is exactly one
transaction, and it either completes the parent and creates the successor or
does neither. This is decision #13 finally implemented rather than described.

**Why one transaction is not negotiable:** completing the parent and
dispatching the successor as two writes leaves a window where a crash ends the
chain *silently*. The parent reads `COMPLETED`, the caller sees a successful
workflow, and the remaining steps never run. No reconciliation pass could ever
detect that, because a `COMPLETED` row with no successor is exactly what the
last step of a healthy chain looks like.

**Why three statements instead of one CTE,** unlike `_SCHEDULE_RETRY`: the
outbox payload must carry the successor's id, which does not exist until the
successor is inserted. Generating it with `jsonb_build_object` would put a
second, hand-typed copy of the `WorkflowStartedEvent` shape outside
`contracts/` — the drift that package exists to prevent (#2). The transaction
gives the same atomicity.

**Why the successor's idempotency key is derived, and why `chain:` is
reserved:** the key is `chain:<parent_id>:<workflow_type>`, so a chain write
that ever runs twice collides with itself and becomes a no-op instead of
forking the chain. That makes the prefix load-bearing, so a database `CHECK`
rejects it for any row without a parent — otherwise a client could submit a
key matching a chain step about to be created and silently take its place.
Ingress is not the only writer of this table, so the constraint belongs at the
database, next to `chk_status`.

**Why the successor inherits `workflow_version`:** the Relay routes to
`workflow_stream_{workflow_version}`, so a successor on a different version
publishes to a stream the current fleet is not consuming and the chain stalls
with no error anywhere. Inheriting also keeps cordon-and-drain meaningful: a
chain finishes on the version it started on.

**Why a depth ceiling, and why exceeding it fails loudly:** `max_retries`
does not bound a chain — every link is a new workflow with a fresh budget — so
a handler that always chains loops indefinitely, spending real money. The
ceiling is the only thing that stops it. A workflow that hits it is marked
`FAILED` with its handler's output still recorded, rather than completed with
the successor quietly dropped: silent truncation would be indistinguishable
from a chain that genuinely ended, and nobody would learn the rest of the work
never ran.

**What was measured before any of this was written.** Eight Postgres
mechanics were checked against the real PG 18.4 instance in the exact shape
the code uses, not inferred: `ADD COLUMN NOT NULL DEFAULT` did not rewrite a
5000-row table (same `pg_relation_filenode` before and after); the reserved
prefix `CHECK` accepts and rejects exactly the four intended cases; the
worst-case derived key is 143 characters against `VARCHAR(255)`; `xmax = 0`
flags the replay and leaves the original row untouched in the successor-insert
shape specifically; a fenced-out writer's transaction leaves **no** successor
and **no** orphan outbox row; a replayed chain write yields exactly one
successor and one event; two connections racing the same derived key produce
one row and one winner with no serialization error to handle.

**And the tests were then checked against themselves.** Eight mutations were
introduced one at a time — dropping version inheritance, skipping the outbox
insert, removing the fence from the parent update, randomizing the successor
key, an off-by-one in the depth ceiling, completing instead of failing at the
ceiling, leaving the parent claimable, and letting the claim ignore current
status — and each one was caught by a named test. A green suite that stays
green while the guarantee is broken is the failure mode this project has
already hit three times (#15); it is cheaper to check than to trust.

---

## Known open items
- **Poison-pilled outbox rows have no retention path:** Once `retry_count` 
  crosses `max_retries`, a `workflow_outbox` row sits permanently at 
  `dispatched = FALSE`, correctly excluded from all future claims, but 
  nothing ever archives or deletes it. The original outbox retention job 
  (see the early design notes) only ever covered `dispatched = TRUE`. The 
  fix is to widen that job's filter to 
  `WHERE dispatched = TRUE OR retry_count >= max_retries`, not to touch 
  `relay.py` — `dispatched` must keep meaning "this actually reached 
  Redis," never "we gave up." Low urgency (poison-pills should be rare 
  in a healthy system) but a real gap, not yet built.
- **`stream_guard()`'s per-chunk check has never been load-tested at
  realistic LLM token rates.** The abort mechanism is proven correct
  (a concurrent test confirms it fires precisely between chunks), but
  its overhead — a Postgres round-trip per yielded item — has only been
  tested against a synthetic 5-item generator, not against something
  simulating 50-100 tokens/sec. At that rate, multiplied across
  concurrently-streaming workers, this could become real read load with
  no measurement backing the assumption that it's fine.
- **Heartbeat timing has never been tested under realistic concurrent
  process pressure.** The 10s heartbeat / 30s lease ratio gives a 3x
  margin against a single missed tick, but that ratio was chosen for
  clean divisibility, not deliberately reasoned about against GC pauses
  or GIL contention under load — both plausible causes of a
  false-positive fencing event that would be self-inflicted rather than
  infrastructure-caused.
