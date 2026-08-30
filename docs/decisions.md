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
that property.

> **Correction, August 2026 (see #20).** The clause above — "a row that
> never dispatched can never be claimed by a worker" — is wrong, and it
> was load-bearing. A publish that times out client-side but lands
> server-side is recorded here as a failure while a worker consumes the
> message normally; enough of those and the Relay stamped
> `DISPATCH_FAILED` over a workflow that was already `RUNNING` or
> `COMPLETED`. That ambiguous failure is the exact case this engine
> exists to survive, so it could not be reasoned away. `settle_failures`
> now carries an explicit `status = 'PENDING'` guard and migration 005
> enforces the same thing structurally. The conclusion of this decision —
> reject `QUEUED`, keep the Relay's write surface minimal — still stands,
> and stands more firmly: the reasoning was too generous to a write the
> Relay makes to a table it does not own. If dispatch visibility is ever needed, read it from
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

## 17. `stream_guard` paces its fencing checks by time, not by chunk

**Decision:** `stream_guard()` checks the lease at most once per
`min_check_interval_seconds` (default 0.1) rather than after every yielded
item. Passing `0.0` restores per-chunk checking.

**Why this was an open item, and what closed it.** The previous entry in this
list flagged that the per-chunk Postgres round-trip had only ever been tested
against a synthetic 5-item generator, with no measurement behind the
assumption that it was affordable. It has now been measured, against a real
`mistral-small-latest` stream and a warm pool:

```
provider rate      500 completion tokens in 2.85s
                   = ~175 tokens/sec, delivered as ~46 chunks/sec
one fencing check  p50 0.767ms   p95 1.08ms   p99 1.56ms
1 stream           6.9% wall-clock inflation
20 streams         21.1% inflation, 753 fencing queries/sec from one worker
```

Those check latencies are against a local Docker Postgres over loopback, which
is the best case this query will ever see. A deployment with a network hop to
the database pays more per check, so the loopback numbers are a floor and the
argument below only gets stronger — but the figures should not be quoted as if
they were production numbers.

**Why that is a design flaw and not just a cost.** 3.5% of a second per stream
is affordable today. The shape is the problem: the load is proportional to the
provider's *token rate*, so a faster model silently buys more database load,
and nothing bounds it. A durable execution engine whose overhead scales with
someone else's product decisions has the dependency backwards.

**What the interval costs, stated rather than hidden.** A superseded worker
can keep consuming for up to one interval before the abort lands — at ~175
tokens/sec, roughly 18 further tokens. That is the honest trade: bounded,
tiny, and paid only on the rare fencing event, against unbounded steady-state
load paid on every chunk of every stream. The already-consumed chunks are
never persisted anyway, because the settle that follows is fenced.

**Why the default is 0.1 and not 0.25:** both bound the load acceptably (0.77%
versus 0.3% of a second per stream). 0.1 keeps abort latency crisp for a
fifth of a percent more, and cost-safety is the reason this mechanism exists.

**Why there is no config setting for it.** Nothing in `src/` consumes it yet —
no handler ships with the engine — and declaring a setting before its consumer
exists is the drift ruled out in #5. It is a parameter default until a caller
needs to tune it.

## 18. Provider-side idempotency keys do not exist on Mistral — the named fix is unavailable

**Finding:** Mistral does not honour an idempotency key on
`/v1/chat/completions`. Measured August 2026, `mistral-small-latest`, standard
tier, via `scripts/mistral_idempotency_probe.py`. Both `Idempotency-Key` and
`X-Idempotency-Key` behaved *identically to a deliberately meaningless header*:
the second of two identical requests came back with a new response `id`, new
content, and a full token charge.

```
Control A  no key, twice          distinct ids          (the oracle discriminates)
Control B  cost header vs usage   agreed on 8/8 calls   (the cost signal is real)
Control D  nonsense header, twice regenerated, billed   (unknown headers ignored)
Idempotency-Key,   same twice     regenerated, billed
X-Idempotency-Key, same twice     regenerated, billed
```

**Why the obvious oracle was rejected.** "The same text came back, so it was a
cached replay" is wrong twice over here: Mistral exposes a `random_seed`
parameter, and a low temperature can make two genuine generations identical
anyway. Either would report a cache hit in a world with no cache. The oracle
is instead the response `id`, plus `x-ratelimit-tokens-query-cost` — which #15
established equals `usage.total_tokens` exactly, and which was re-checked on
every call here rather than assumed still true three days later.

**Why Control D is the one that makes this conclusive.** A null result is only
worth anything if a positive result was reachable. Control A proves the
instrument can see a replay; Control D establishes what "this header did
nothing" looks like. The experiment matched D exactly, which is the difference
between "not supported" and "we sent it wrong."

**What this invalidates.** The README's cost-safety section, and #15, both
named provider-side idempotency keys as the fix for reclaim double-billing.
That remedy does not exist on this provider, and checking the other major ones
suggests it is not an industry standard either:

| Provider | Idempotency on chat completions | Evidence |
|---|---|---|
| Mistral | No | **Measured**, August 2026, by this probe |
| Anthropic | Not documented | Official API overview enumerates every request header — `x-api-key`, `Authorization`, `anthropic-workspace-id`, `anthropic-version`, `content-type` — and none is an idempotency key |
| OpenAI | Not documented for chat completions | Official Node SDK README documents automatic retries but no idempotency mechanism; `Idempotency-Key` appears only on the Agentic Commerce API |
| xAI (Grok) | Not documented | Chat completions reference documents only `Content-Type` and `Authorization` |

The official SDKs corroborate the documentation, and more strongly than it
does. Both `anthropic-sdk-python` and `openai-node` are Stainless-generated and
carry the generator's generic idempotency scaffolding — and both leave it
switched off. Anthropic's Python client sets `self._idempotency_header = None`
and attaches the header only `if idempotency_header and idempotency_key`, so it
never attaches one; `openai-node` declares `protected idempotencyHeader?:
string`, never assigns it, and its `if (this.idempotencyHeader ...)` branch is
dead. The capability is plumbed and deliberately not enabled, which is a
clearer signal than silence in a docs page.

The one place OpenAI *does* enable it is Agentic Commerce, and the primary
source is explicit that this covers checkout `create` and `complete` calls —
"safe duplicate requests return the same result. Parameter mismatches return
`idempotency_conflict` with HTTP 409". That is payment idempotency in the
Stripe tradition, protecting a discrete transaction. It does nothing for token
generation, which is the thing that costs us money here.

Only the Mistral row is a measurement. The rest is documentation and SDK
source, and this probe's whole point is that absence from documentation is not
absence of behaviour — Mistral's own docs were silent too, and it took an experiment to
turn that silence into a fact. The script now takes `PROBE_BASE_URL`,
`PROBE_MODEL` and `PROBE_API_KEY` so any OpenAI-compatible provider can be
measured the moment a key exists. Until then those three rows are marked "not
documented", never "does not support".

**The detail that makes this worse than a null result.** OpenAI's official SDK
retries connection errors, 408, 409, 429 and 5xx twice by default, with no
deduplication mechanism documented anywhere. So the industry-standard client
silently re-issues paid requests on exactly the failure class this engine
exists to survive. The double-billing exposure is not a corner case we
invented; it ships on by default in the most widely used client, and no
provider we checked offers the primitive that would make it safe.

**What replaces it, and what it can honestly promise.** The gap has to close
without provider cooperation: persist the completion in Postgres keyed by
`(workflow_id, call_index)` the moment it arrives, and have a re-running
handler reuse the record instead of re-calling. Three properties of that design
need deciding deliberately when it is built, and are recorded here so they are
not discovered late:

- The write must be **unfenced**, unlike every other write in this system. A
  superseded worker still has to record it, because the memo is not workflow
  state — it is a receipt for money already spent, and that stays true no
  matter who owns the row afterwards.
- It **narrows the window rather than eliminating it**. A worker that dies
  between receiving the response and committing the memo still pays twice. The
  exposure shrinks from the whole handler duration to a single Postgres write,
  which is a large improvement and not a guarantee. It should be described that
  way.
- It does **not** cover two workers genuinely in flight at once, only the
  sequential re-run that reclaim produces.

It also imports a determinism assumption — `call_index` is only stable if a
handler issues its calls in the same order every run — which is a real
constraint on handler authors and belongs in the public contract, not in a
comment.

## 19. Completion memos: the engine pays once per call, because nobody else will

**Decision:** A handler wraps each paid provider call in
`memoized_call()` (`src/axiom/worker/memo.py`), which performs the call once
per `(workflow_id, call_index)` and returns the committed response on every
later run. Migration 004 adds `workflow_call_memos`.

This is what #18 pointed at, built. It is not new scope: it closes the gap the
README's cost-safety section has been carrying as unresolved since Phase 3,
after #18 established that the remedy that section originally named does not
exist. It lives in `worker/` rather than a new package for the same reason
`stream_guard` does — it is an execution primitive handlers call with the
fencing context the runner already passes them, not a provider integration.

**What it is worth, measured.** The engine half is a call counter against a
fake provider (`tests/worker/test_memo.py`); the money half is a real workflow
that fails twice after paying and then succeeds, run end to end through a real
Relay and Worker against `mistral-small-latest`
(`scripts/memo_double_bill_probe.py`, August 2026):

```
run            status      handler runs  paid calls   tokens
without memo   COMPLETED              3           3      206
with memo      COMPLETED              3           1       61
```

Three attempts, one bill. The residual exposure was measured too, because #18
promised the memo would shrink the window "to a single Postgres write" without
ever saying how big that is (`scripts/memo_write_window_probe.py`, n=300,
loopback, `fsync`/`synchronous_commit`/`full_page_writes` all on, 931-byte
response):

```
write (residual window)  p50  1.235ms  p95  1.750ms  max 14.456ms
miss  (cost per call)    p50  0.423ms  p95  0.617ms  max  1.919ms
```

Against a ~2.8s completion, that turns "every reclaim re-bills" into "a reclaim
re-bills if the crash lands in roughly one part in 2,300" — and one part in
1,600 at p95, which is the number to quote when the question is risk rather
than average. The 14ms outlier is one sample in 300 and is left in rather than
trimmed, because the worst case is what a window claim is actually about.
Smaller, not zero, and it should never be described as zero. The miss figure is
the other side of the trade: every call that is *not* a replay pays ~0.4ms for
the lookup that finds nothing.

**The fingerprint is a guard, never part of the key.** A handler may
legitimately issue the identical request twice — two samples from one prompt is
the obvious case — so keying on content would silently collapse a deliberate
second call into a replay of the first. Keying on `call_index` keeps them
distinct; hashing the request and comparing on read is what catches the
opposite error, a handler that reaches the same index with a *different* call.
That raises `NonDeterministicHandlerError` (non-retryable) rather than
returning the memo, because the two alternatives are worse: serving it answers
a question the handler did not ask, and ignoring it reintroduces the double
bill.

**Failures are deliberately not memoized,** which leaves the one hole this
design cannot close. A call that raised may or may not have been billed — a
connection dropped after the provider committed the charge looks identical from
here to one that never landed — and memoizing failures would break ordinary
retry, which is the mechanism this engine most depends on. Retrying a possibly-
billed call is the cheaper wrong answer than never retrying a genuinely failed
one.

**On losing the insert race,** the caller returns the *stored* response, not
its own. Both were paid for and that money is gone either way, but only one can
be what the workflow observed on every future run; preferring our own would
make the handler's view depend on which attempt happened to be executing.

**Tests were checked by mutation,** as #16's were. Eight edits that each
silently reintroduce a double bill or a wrong answer — `DO NOTHING` relaxed to
`DO UPDATE`, the fingerprint guard removed, the memo read fenced like every
other worker read, failures memoized, `sort_keys` dropped — were all caught,
each by a test whose name describes the property it lost.

**What is still not known: how often this actually fires.** The natural
question — what fraction of workflows are re-executed — is a query away
(`lease_generation > 1`), and it returns nothing, because this project has no
production traffic and the local database is transient. Reclaim frequency is a
property of an operator's crash and stall rate, not of this code, so the honest
framing of the memo's value is the one used above: it is *per-event* savings of
a known size, times a rate nobody here has measured. That distinction is why
neither this entry nor the README attaches a dollar figure.

## 20. The state machine is enforced by the database, not by convention

**Decision:** Migration 005 makes the state machine an object Postgres
enforces. Two reference tables — `workflow_statuses` (the vocabulary, with
`is_terminal` and `is_implemented`) and `workflow_state_transitions` (the six
legal status changes, each naming the component that performs it) — plus a
`BEFORE UPDATE ... FOR EACH ROW` trigger that refuses anything absent from the
table.

**The problem.** #7 chose the nine-state vocabulary but never wrote down which
transitions between those states are legal, so there was nothing to enforce
even in principle. `chk_status` constrains the *values* a column may hold, not
the *edges* between them. Demonstrated before the migration:

```
INSERT ... status='COMPLETED';
UPDATE workflow_states SET status='RUNNING' WHERE id = ...;   -- succeeded
```

Every safety property in this engine rested on each query carrying the right
predicate. The worker's claim does, correctly. But that is a convention held by
five queries and by whoever writes the sixth, and it is the kind of thing that
survives review right up until it doesn't.

**What it found immediately.** The Relay's `settle_failures` had no status
predicate, and #7's justification for that — quoted and corrected above — does
not hold. A publish that times out client-side but lands server-side counts as
a failure here while a worker consumes the message; past `max_retries` the
Relay stamped `DISPATCH_FAILED` over a workflow that had already completed. It
now guards on `status = 'PENDING'`, so the wrong write affects zero rows, and
the trigger enforces the same thing if that line is ever removed. This bug was
not found by reading the code, which had been reviewed several times — it was
found by being forced to write down the legal edges.

**Verified exhaustively, not by sampling.** The transition space is 9 × 9 = 81
ordered pairs, which is finite and therefore coverable, so
`tests/contracts/test_state_machine.py` covers all of it: every cell attempted
against a real row and compared with an expectation derived independently from
the six status writes in `src/`, not read back from the migration. A test that
asked the table what to expect could only prove the trigger agrees with the
table, never that the table is right. Seven mutations of the enforcement itself
— trigger dropped, `WHEN` clause removed, function failing open, a terminal
state given an escape route, a legal transition deleted, `is_terminal` flipped,
the Relay guard removed — were all caught by named tests.

**Two mechanics verified against primary sources rather than memory,** both
load-bearing:

- The trigger's `WHEN (OLD.status IS DISTINCT FROM NEW.status)` is what keeps
  the ingress idempotent write working. PostgreSQL 18's `CREATE TRIGGER`
  documentation is explicit that "an `INSERT` with an `ON CONFLICT DO UPDATE`
  clause may cause both insert and update operations, so it will fire both
  kinds of triggers as needed" — so a resubmitted idempotency key *does* reach
  this trigger, on a row in whatever state it already holds, terminal included.
  It survives only because the conflict update rewrites `idempotency_key` and
  never touches `status`. Had this been assumed rather than checked, #8's
  idempotency guarantee would have broken on every resubmission of a finished
  workflow.
- `RAISE ... USING ERRCODE = '23514'` is `check_violation` per the error-codes
  appendix, which is what makes asyncpg raise `CheckViolationError` and lets
  the tests distinguish a refused transition from an unrelated failure.

**Cost, since this repo does not get to assert one.** The trigger fires on
every `PENDING -> RUNNING`, which is `claim_workflow` — the hottest path in the
engine. Measured over 300 claims against a warm pool: p50 1.663ms with the
trigger against 1.589ms without, so **+0.074ms, or +4.7%**, for a PL/pgSQL
invocation and a primary-key lookup. Acceptable, but it was shipped unmeasured
in the first version of this entry and that was the wrong way round.

**What is deliberately not enforced.** The status a row is *born* with. An
`INSERT` is not a transition, and the test fixtures construct rows directly in
`RUNNING` and terminal states to reach scenarios that would otherwise need a
full round trip. Both production insert paths write `PENDING`, asserted by test
rather than by constraint.

**Two limits that keep the word "structural" honest.**

The transition table is ordinary data owned by the role the application
connects as, so `INSERT INTO workflow_state_transitions VALUES
('COMPLETED','RUNNING',...)` widens what the trigger permits — verified, it
succeeds. So this is not "illegal transitions are impossible"; it is "illegal
transitions are no longer reachable by writing an ordinary query wrong", which
is the failure mode that actually existed and the one that produced the Relay
bug. Tampering is *detected* rather than prevented, by the test that asserts
the table's contents against an independently derived set. Prevention needs a
migration role distinct from the application role, which this project does not
have and which is a bigger change than this one.

The table is also not a complete description of the state machine, because
`chk_no_self_transition` forbids recording `RUNNING -> RUNNING`. That edge is
real — it is a reclaim, with `lease_generation` incremented — and it is
deliberately absent, since the trigger's `WHEN` clause means a write that does
not change status is never a transition and is guarded by the lease instead.
The table describes every status *change*; the reclaim is a change of owner.
Worth keeping straight, because "the table is the specification" is close
enough to true to mislead.

**The three unimplemented states stay unreachable.** `WAITING_FOR_INPUT`,
`CANCELING` and `CANCELED` are reserved for the Phase 5 API — cancellation and
human-in-the-loop resume — and have no transitions, so the trigger refuses
every path into them until the phase that implements them adds the rows. Giving
them edges now would be encoding a guess about a design that does not exist. It
also means `is_terminal` cannot be derived for those three, only asserted; the
test scopes its derivation to implemented states and says why, which is a
distinction the first version of that test got wrong and the run caught.

## 21. Exhaustive model checking, in Python, and the bug it found

**Decision:** `src/axiom/worker/protocol_model.py` states the
claim/fence/settle/ack protocol as a finite state machine.
`tests/worker/test_protocol_model.py` enumerates every reachable state by BFS
and checks four safety invariants at each one.

**Why not TLA+.** TLC is the standard tool and it needs a JVM, which this stack
does not have. It also verifies a model, not the code — the usual criticism, and
a bad fit for a repo that tests against real Postgres and Redis. Exhaustive BFS
over a bounded model is a few hundred lines of Python and gives the same
guarantee for safety. What it gives up is liveness under fairness, which TLC
does well and this does not do at all.

Two things stop it being theatre. The checker is fed deliberately broken
protocols and must find them — a checker that has never reported a violation is
not known to be able to. And the model's claim predicate is cross-checked
against real Postgres, so it cannot drift from the SQL it claims to mirror.

**The bug.** Modelling `process_message` faithfully means modelling what it does
when `claim_workflow` returns None: it acks. The model immediately produced a
7-step counterexample to `ack_implies_terminal`, reproduced against real
Postgres and Redis in `tests/worker/test_stranded_message.py`:

```
deliver->w0 | w0 claim (gen 1) | tick | tick
xautoclaim w0->w1 | w1 claim failed, acks as duplicate
  -> message acked, workflow still RUNNING, nothing can redeliver it
```

`XAUTOCLAIM`'s `min_idle_time` measures how long a message has sat unacked in
the PEL. Heartbeats do not reset it — they extend the Postgres lease, a
different clock. So with the shipped settings (lease 30s, heartbeat 10s,
XAUTOCLAIM 35s) **every workflow running longer than 35 seconds** has its
message handed to a second worker, whose claim then fails against the live
lease. The old code called that "a genuine duplicate delivery of an
already-claimed, still-fresh-leased row. Safe no-op" and acked. It is not safe:
the message is destroyed while the work is still in flight, and if the original
worker then dies the row strands in `RUNNING` with nothing to redeliver it.
Note the failure needs no crash to *set up* — only to be exposed.

**The fix.** On a failed claim, ack only if the workflow is actually settled
(`is_settled`, reading `is_terminal` from `workflow_statuses`). Otherwise leave
the message in the PEL, where a later reclaim finds it. The cost is a repeated
reclaim attempt while a long workflow runs; the alternative is losing the work.

**What this says about the phases.** 162 tests, mutation testing, an exhaustive
transition check and several code reviews did not find this. It needed a model
of the protocol as a whole, because no single component is wrong — the Relay,
the claim query and the ack rule are each individually correct.

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
- **Heartbeat timing has never been tested under realistic concurrent
  process pressure.** The 10s heartbeat / 30s lease ratio gives a 3x
  margin against a single missed tick, but that ratio was chosen for
  clean divisibility, not deliberately reasoned about against GC pauses
  or GIL contention under load — both plausible causes of a
  false-positive fencing event that would be self-inflicted rather than
  infrastructure-caused.
- **Completion memos have no retention path.** `workflow_call_memos` cascades
  when its workflow is deleted, so it inherits whatever retention
  `workflow_states` eventually gets — and `workflow_states` has none either. A
  memo stops being useful the moment its workflow reaches a terminal state, and
  it stores a whole provider response, so this table grows faster in bytes than
  anything else in the schema. Deleting on completion was rejected for now
  because it puts an extra write on the hot path to reclaim space no one is
  short of yet; the right fix is one retention job covering all three cases
  (dispatched outbox rows, poison-pilled outbox rows, terminal workflows and
  their memos) rather than three separate sweeps.
- **`ON DELETE CASCADE` on memos destroys the record of money spent.** Migration
  004 justified the cascade with "a memo has no meaning without its workflow",
  which is true of its *function* — replaying a call needs the workflow that
  makes the call — and false of its *value as an audit record*. Deleting a
  workflow row currently removes, silently and with no trace, the evidence of
  what a provider was paid. Nothing deletes workflow rows today, so this is
  latent rather than active, but the retention job above is exactly the thing
  that would trip it, and it should be settled before that job is written
  rather than discovered by it. The options are to keep the cascade and have
  retention archive memos first, or to break the FK and let memos outlive their
  workflow as standalone receipts.
- **Long workflows now inflate Redis' delivery counter.** #21's fix leaves a
  message unacked while its workflow runs, so `XAUTOCLAIM` re-claims it every
  `min_idle_time` and, per the Redis docs, increments the attempted-deliveries
  count each time. The same docs name a high delivery count as the way to spot
  a message consumers keep failing on — so a perfectly healthy 10-minute
  workflow will look like a poison pill to that signal. Nothing reads it yet;
  Phase 6 must not treat it as a fault indicator without also checking the row.
- **The model covers one message and no retry path.** #21's state machine has no
  `schedule_retry`, no Relay, and no chaining. The retry gap is the one that
  matters: releasing to PENDING writes a *new* outbox row, so a workflow can
  have two live stream messages, and that interleaving is unexplored.
- **Migrations are applied by hand.** `docker compose up` only auto-applies
  `migrations/` when it creates the volume, so 003 and 004 were run manually
  against an existing database. There is no runner, no record of which
  migrations a given database has, and nothing that would catch a partially
  migrated environment — a real operational gap now that migrations have
  outlived a single fresh boot.
