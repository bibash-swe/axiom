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

## Known open items

- **SQL/Python drift risk:** nothing currently enforces that the
  `chk_status` constraint in `001_initial_schema.sql` and
  `WorkflowStatus` in `enums.py` stay in sync. Verified equal once, by
  hand, via a live query against the constraint — not yet a real test.
  Belongs in `tests/contracts/` once that exists.
- **Exhaustiveness of `INTERNAL_TO_PUBLIC_STATUS`:** same category of
  gap — verified once by hand, not yet a standing test.