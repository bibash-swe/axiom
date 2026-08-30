"""Completion memos: pay a provider once per (workflow, call), across re-runs.

This closes — narrows, precisely — the gap the README's cost-safety section
names. Fencing already stops a *superseded* worker from generating more
tokens. It does nothing about the *reclaiming* worker, which re-runs the
handler from the start (decisions.md #13) and re-issues every provider call
the previous attempt already paid for.

The obvious fix, a provider-side idempotency key, was measured in August 2026
and does not exist (decisions.md #18). So the record has to live here. A
handler wraps each paid call in memoized_call(); the first attempt performs it
and commits the response, and every later attempt reads that response back
instead of calling out again.

Two properties are load-bearing and both are deliberate:

- The memo write is **unfenced**. Nothing here checks lease_generation, unlike
  every write in worker.py. A superseded worker still has to record what it
  spent — and in the reclaim case it is precisely that worker's memo the
  winner goes on to read.
- It is **not** a guarantee. See memoized_call's docstring for the measured
  size of what remains.
"""

import hashlib
import json
import logging
from collections.abc import Awaitable, Callable
from typing import Any
from uuid import UUID

import asyncpg

from axiom.worker.execution import NonRetryableError

logger = logging.getLogger("axiom.worker")

_SELECT_MEMO = """
    SELECT fingerprint, response
    FROM workflow_call_memos
    WHERE workflow_id = $1 AND call_index = $2
"""

# DO NOTHING, not DO UPDATE: the first memo written for a call wins forever.
# Overwriting would let a second paid response replace the one an earlier
# attempt already returned to a handler, so a workflow could observe two
# different answers to the same call and there would be no record that it had.
# The empty RETURNING on conflict is how the caller learns it lost that race.
_INSERT_MEMO = """
    INSERT INTO workflow_call_memos
        (workflow_id, call_index, fingerprint, response, written_by_lease_generation)
    VALUES ($1, $2, $3, $4::jsonb, $5)
    ON CONFLICT (workflow_id, call_index) DO NOTHING
    RETURNING call_index
"""


class NonDeterministicHandlerError(NonRetryableError):
    """A handler issued a different call at an index it has already memoized.

    Non-retryable on purpose: re-running produces the same divergence, and the
    only two alternatives are worse. Returning the memo anyway would hand the
    handler the answer to a question it did not ask; ignoring the memo and
    re-calling would silently reintroduce the double bill this module exists
    to prevent.

    The usual cause is a request that is not stable across runs — an embedded
    timestamp, a random seed, a dict serialized in nondeterministic order. Move
    that value into the workflow's input_data, where it is persisted once, and
    the request becomes reproducible.
    """


def _canonical_fingerprint(request: Any) -> str:
    """SHA-256 of the request, serialized so equal requests always agree.

    sort_keys is what makes two structurally identical dicts hash the same
    regardless of construction order. No default= coercion: a request carrying
    something unserializable should raise here, at the point of the mistake,
    rather than be silently stringified into a fingerprint that happens to
    include an object's memory address and therefore never matches again.
    """
    canonical = json.dumps(request, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


async def _load_memo(
    pool: asyncpg.Pool, *, workflow_id: UUID, call_index: int, fingerprint: str
) -> dict[str, Any] | None:
    """Read a memo, verifying it answers the request the caller is actually making."""
    async with pool.acquire() as conn:
        row = await conn.fetchrow(_SELECT_MEMO, workflow_id, call_index)

    if row is None:
        return None

    if row["fingerprint"] != fingerprint:
        raise NonDeterministicHandlerError(
            f"handler issued a different request at call_index={call_index} "
            f"for workflow_id={workflow_id}: memo fingerprint "
            f"{row['fingerprint'][:12]}… != current {fingerprint[:12]}…"
        )

    response: dict[str, Any] = json.loads(row["response"])
    return response


async def memoized_call(
    pool: asyncpg.Pool,
    *,
    workflow_id: UUID,
    lease_generation: int,
    call_index: int,
    request: Any,
    call: Callable[[], Awaitable[dict[str, Any]]],
) -> dict[str, Any]:
    """Perform a paid provider call once per (workflow_id, call_index), ever.

    On the first attempt this awaits call() and commits its response. On any
    re-run — a reclaim, a retry, a redelivery — it returns that committed
    response and call() is never awaited, so the provider is never asked twice.

    call_index must identify the same logical call on every run of this
    handler, which means the handler's sequence of calls has to be
    deterministic up to each one. That is a real constraint on handler
    authors, not an implementation detail: a handler that branches on
    wall-clock time or a fresh random value will mis-key. The fingerprint
    guard turns that from a silent wrong answer into
    NonDeterministicHandlerError.

    It also has to be unique *within* a run, and that half is not guarded.
    Two different calls at the same index are caught by the fingerprint, but
    two *identical* calls at the same index are indistinguishable from a
    replay, so the second returns the first's response and is never made.
    A handler sampling one prompt twice must therefore give the two calls
    different indices; the natural implementation, a counter incremented on
    every call the handler makes, does this for free.

    request is anything JSON-serializable that fully identifies the call —
    in practice the provider request body. It is never stored, only hashed.

    Failures are not memoized. If call() raises, nothing is written and the
    next attempt calls again, which is what makes an ordinary retry work at
    all. The cost of that choice is the one hole this design cannot close:
    a call that the provider billed but whose response never arrived is
    indistinguishable, from here, from one that never happened.

    **What this does not promise.** A worker that dies after the provider
    commits the charge and before the memo commits still pays twice. The
    exposure is not eliminated, it is shrunk: from the entire handler
    duration to one Postgres write, measured at p50 1.0ms / p95 1.6ms
    (loopback, fsync on, 859-byte response). Against a ~2.8s Mistral call
    that is a window of roughly one part in 2,800 rather than the current
    one part in one — every reclaim re-bills today. Nor does it help two
    workers genuinely running at once; it addresses the sequential re-run
    that reclaim produces.
    """
    fingerprint = _canonical_fingerprint(request)

    memo = await _load_memo(
        pool, workflow_id=workflow_id, call_index=call_index, fingerprint=fingerprint
    )
    if memo is not None:
        logger.info(
            "memo hit for workflow_id=%s call_index=%d — skipping paid call",
            workflow_id,
            call_index,
        )
        return memo

    response = await call()

    # Serialize before touching the database. The provider has already been
    # paid by this point, so a response that cannot be stored is not an
    # ordinary encoding error — it is money spent that this module has just
    # failed to protect, and it must say so rather than surface as a TypeError
    # from somewhere inside asyncpg. Non-retryable because another attempt
    # would pay again and fail again in exactly the same place.
    try:
        serialized = json.dumps(response)
    except (TypeError, ValueError) as exc:
        logger.error(
            "paid response at call_index=%d for workflow_id=%s is not JSON-serializable "
            "and cannot be memoized — this call will be paid for again if the workflow re-runs",
            call_index,
            workflow_id,
        )
        raise NonRetryableError(
            f"handler returned a non-serializable response at call_index={call_index} "
            f"for workflow_id={workflow_id}: {exc}"
        ) from exc

    async with pool.acquire() as conn:
        inserted = await conn.fetchrow(
            _INSERT_MEMO,
            workflow_id,
            call_index,
            fingerprint,
            serialized,
            lease_generation,
        )

    if inserted is not None:
        return response

    # Lost the insert race: another attempt memoized this call between our read
    # and our write. Both responses were paid for and that money is gone either
    # way, but only one of them can be what this workflow observed on every
    # future run — so return the durable one, not ours. Preferring ours would
    # make the handler's view of the call depend on which attempt happened to
    # be running, which is exactly the nondeterminism this module removes.
    logger.warning(
        "memo insert raced for workflow_id=%s call_index=%d — "
        "deferring to the stored response; this call was billed twice",
        workflow_id,
        call_index,
    )
    stored = await _load_memo(
        pool, workflow_id=workflow_id, call_index=call_index, fingerprint=fingerprint
    )
    if stored is None:
        # Only reachable if the winning row was deleted between the failed
        # insert and this read, which for a CASCADE means the workflow itself
        # is gone. Nothing sane left to return.
        raise RuntimeError(
            f"memo for workflow_id={workflow_id} call_index={call_index} "
            "vanished between insert and read"
        )
    return stored
