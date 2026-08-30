"""Completion memos: a paid provider call is performed once per workflow, ever.

The gap being closed is the one the README names and decisions.md #18
measured: fencing stops a superseded worker from generating more tokens, but
does nothing about the worker that reclaims the workflow and re-runs the
handler from the start, re-issuing every call the previous attempt paid for.

The counter these tests assert on stands in for money. A handler here records
how many times it actually reached its "provider", so `calls == 1` after a
retry or a reclaim is the whole claim, stated in the only unit that matters.
"""

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any
from uuid import UUID, uuid4

import asyncpg
import pytest
from redis.asyncio import Redis

from axiom.contracts.enums import WorkflowStatus
from axiom.ingress.repository import submit_workflow
from axiom.relay.runner import run_forever as relay_run_forever
from axiom.worker.execution import NonRetryableError
from axiom.worker.memo import NonDeterministicHandlerError, memoized_call
from axiom.worker.runner import HandlerRegistry
from axiom.worker.runner import run_forever as worker_run_forever
from axiom.worker.worker import claim_workflow, settle_terminal

FAST_BASE = 0.01
FAST_CAP = 0.01

_TERMINAL = ("COMPLETED", "FAILED", "CANCELED", "DEAD_LETTERED", "DISPATCH_FAILED")


class _Provider:
    """A stand-in for a paid API. Counts the calls that would have cost money."""

    def __init__(self, response: dict[str, Any] | None = None) -> None:
        self.calls = 0
        self._response = response or {"content": "generated", "usage": {"total_tokens": 412}}

    async def complete(self) -> dict[str, Any]:
        self.calls += 1
        return {**self._response, "call_number": self.calls}


@pytest.fixture
def memo_workflow(
    make_workflow_row: Callable[..., Awaitable[UUID]],
) -> Callable[..., Awaitable[UUID]]:
    """A RUNNING workflow at a given lease_generation, ready to have calls memoized against."""

    async def _make(*, lease_generation: int = 1) -> UUID:
        return await make_workflow_row(
            idempotency_key=f"memo_{uuid4()}",
            status="RUNNING",
            lease_generation=lease_generation,
        )

    return _make


async def _memo_rows(pool: asyncpg.Pool, workflow_id: UUID) -> list[dict[str, Any]]:
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT call_index, fingerprint, response, written_by_lease_generation "
            "FROM workflow_call_memos WHERE workflow_id = $1 ORDER BY call_index",
            workflow_id,
        )
    return [dict(r) for r in rows]


async def test_first_call_performs_the_work_and_records_it(
    pool: asyncpg.Pool, memo_workflow: Callable[..., Awaitable[UUID]]
) -> None:
    """Nothing is memoized yet, so the call happens and its response is committed."""
    workflow_id = await memo_workflow()
    provider = _Provider()

    result = await memoized_call(
        pool,
        workflow_id=workflow_id,
        lease_generation=1,
        call_index=0,
        request={"model": "m", "messages": [{"role": "user", "content": "hi"}]},
        call=provider.complete,
    )

    assert provider.calls == 1
    assert result["call_number"] == 1

    rows = await _memo_rows(pool, workflow_id)
    assert len(rows) == 1
    assert rows[0]["written_by_lease_generation"] == 1


async def test_second_run_reuses_the_memo_and_never_calls_out(
    pool: asyncpg.Pool, memo_workflow: Callable[..., Awaitable[UUID]]
) -> None:
    """The point of the whole module: a re-run pays nothing."""
    workflow_id = await memo_workflow()
    provider = _Provider()
    request = {"model": "m", "prompt": "summarize"}

    first = await memoized_call(
        pool, workflow_id=workflow_id, lease_generation=1, call_index=0,
        request=request, call=provider.complete,
    )
    second = await memoized_call(
        pool, workflow_id=workflow_id, lease_generation=2, call_index=0,
        request=request, call=provider.complete,
    )

    assert provider.calls == 1, "the second run must not reach the provider"
    assert second == first, "a re-run must observe exactly what the first run observed"

    # Still attributed to the attempt that actually paid, not the one that read it.
    rows = await _memo_rows(pool, workflow_id)
    assert rows[0]["written_by_lease_generation"] == 1


async def test_distinct_call_indices_are_memoized_independently(
    pool: asyncpg.Pool, memo_workflow: Callable[..., Awaitable[UUID]]
) -> None:
    """A handler making two calls memoizes two rows, and replays both."""
    workflow_id = await memo_workflow()
    provider = _Provider()

    async def run_handler() -> tuple[dict[str, Any], dict[str, Any]]:
        a = await memoized_call(
            pool, workflow_id=workflow_id, lease_generation=1, call_index=0,
            request={"step": "extract"}, call=provider.complete,
        )
        b = await memoized_call(
            pool, workflow_id=workflow_id, lease_generation=1, call_index=1,
            request={"step": "summarize"}, call=provider.complete,
        )
        return a, b

    first_a, first_b = await run_handler()
    assert provider.calls == 2

    replay_a, replay_b = await run_handler()
    assert provider.calls == 2, "a full re-run of a 2-call handler must cost nothing"
    assert (replay_a, replay_b) == (first_a, first_b)

    assert [r["call_index"] for r in await _memo_rows(pool, workflow_id)] == [0, 1]


async def test_identical_requests_at_different_indices_do_not_collapse(
    pool: asyncpg.Pool, memo_workflow: Callable[..., Awaitable[UUID]]
) -> None:
    """Sampling the same prompt twice is legitimate and must stay two paid calls.

    This is why the key is (workflow_id, call_index) and the fingerprint is
    only a guard. Keying on request content would silently turn a deliberate
    second sample into a replay of the first.
    """
    workflow_id = await memo_workflow()
    provider = _Provider()
    same = {"model": "m", "prompt": "write a haiku", "temperature": 1.0}

    a = await memoized_call(
        pool, workflow_id=workflow_id, lease_generation=1, call_index=0,
        request=same, call=provider.complete,
    )
    b = await memoized_call(
        pool, workflow_id=workflow_id, lease_generation=1, call_index=1,
        request=same, call=provider.complete,
    )

    assert provider.calls == 2
    assert a["call_number"] == 1
    assert b["call_number"] == 2


async def test_request_order_does_not_affect_the_fingerprint(
    pool: asyncpg.Pool, memo_workflow: Callable[..., Awaitable[UUID]]
) -> None:
    """Two structurally identical requests match however their dicts were built."""
    workflow_id = await memo_workflow()
    provider = _Provider()

    await memoized_call(
        pool, workflow_id=workflow_id, lease_generation=1, call_index=0,
        request={"model": "m", "temperature": 0.2}, call=provider.complete,
    )
    await memoized_call(
        pool, workflow_id=workflow_id, lease_generation=2, call_index=0,
        request={"temperature": 0.2, "model": "m"}, call=provider.complete,
    )

    assert provider.calls == 1, "key order must not make a replay look like a new call"


async def test_a_changed_request_at_a_memoized_index_fails_loudly(
    pool: asyncpg.Pool, memo_workflow: Callable[..., Awaitable[UUID]]
) -> None:
    """Handler nondeterminism is an error, not a silently wrong answer."""
    workflow_id = await memo_workflow()
    provider = _Provider()

    await memoized_call(
        pool, workflow_id=workflow_id, lease_generation=1, call_index=0,
        request={"prompt": "first question"}, call=provider.complete,
    )

    with pytest.raises(NonDeterministicHandlerError):
        await memoized_call(
            pool, workflow_id=workflow_id, lease_generation=2, call_index=0,
            request={"prompt": "a completely different question"}, call=provider.complete,
        )

    assert provider.calls == 1, "the divergent call must not be performed either"


async def test_a_failed_call_is_not_memoized(
    pool: asyncpg.Pool, memo_workflow: Callable[..., Awaitable[UUID]]
) -> None:
    """Retry has to keep working: a call that raised was not necessarily billed."""
    workflow_id = await memo_workflow()
    attempts = 0

    async def flaky() -> dict[str, Any]:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise ConnectionError("provider unavailable")
        return {"content": "second attempt"}

    request = {"prompt": "hello"}
    with pytest.raises(ConnectionError):
        await memoized_call(
            pool, workflow_id=workflow_id, lease_generation=1, call_index=0,
            request=request, call=flaky,
        )
    assert await _memo_rows(pool, workflow_id) == []

    result = await memoized_call(
        pool, workflow_id=workflow_id, lease_generation=2, call_index=0,
        request=request, call=flaky,
    )
    assert result["content"] == "second attempt"
    assert attempts == 2


async def test_an_unstorable_response_fails_loudly_rather_than_as_a_type_error(
    pool: asyncpg.Pool, memo_workflow: Callable[..., Awaitable[UUID]]
) -> None:
    """A paid response that cannot be stored is a priced failure, not an encoding bug.

    The call has already been billed when this is discovered, so it must not
    surface as a TypeError from inside asyncpg with no indication that money
    was involved — and it must not be retried, because the next attempt pays
    again and fails identically.
    """
    workflow_id = await memo_workflow()

    async def unstorable() -> dict[str, Any]:
        return {"content": object()}  # type: ignore[dict-item]

    with pytest.raises(NonRetryableError, match="non-serializable"):
        await memoized_call(
            pool, workflow_id=workflow_id, lease_generation=1, call_index=0,
            request={"prompt": "x"}, call=unstorable,
        )

    assert await _memo_rows(pool, workflow_id) == []


async def test_a_fenced_worker_still_records_what_it_spent(
    pool: asyncpg.Pool, memo_workflow: Callable[..., Awaitable[UUID]]
) -> None:
    """The reclaim case, and the reason the memo write is unfenced.

    A superseded worker's write to workflow_states is a guaranteed no-op — and
    its write to workflow_call_memos must not be, because the money is gone
    either way and the reclaiming worker is about to spend it again. Both
    halves are asserted here, because it is precisely the asymmetry that is
    easy to break by "tidying up" the memo insert to look like every other
    write in the worker.
    """
    workflow_id = await memo_workflow(lease_generation=1)
    provider = _Provider()

    # A second worker reclaims. The row is RUNNING with a live lease, so expire
    # it first — this is exactly what a stalled worker looks like from outside.
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE workflow_states SET lease_expires_at = NOW() - INTERVAL '1 second' "
            "WHERE id = $1",
            workflow_id,
        )
    reclaimed = await claim_workflow(
        pool, workflow_id=workflow_id, worker_id=uuid4(), lease_seconds=30
    )
    assert reclaimed is not None and reclaimed.lease_generation == 2

    # The original worker, now fenced but not yet aware of it, finishes its
    # paid call and records it.
    await memoized_call(
        pool, workflow_id=workflow_id, lease_generation=1, call_index=0,
        request={"prompt": "expensive"}, call=provider.complete,
    )

    fenced_out = await settle_terminal(
        pool, workflow_id=workflow_id, lease_generation=1,
        status=WorkflowStatus.COMPLETED, output_data={"ignored": True},
    )
    assert fenced_out is False, "the superseded worker must not be able to settle"

    rows = await _memo_rows(pool, workflow_id)
    assert len(rows) == 1, "…but it must still be able to record what it spent"
    assert rows[0]["written_by_lease_generation"] == 1

    # The reclaiming worker re-runs the handler and inherits the paid response.
    await memoized_call(
        pool, workflow_id=workflow_id, lease_generation=2, call_index=0,
        request={"prompt": "expensive"}, call=provider.complete,
    )
    assert provider.calls == 1, "the reclaiming worker must not pay again"


async def test_concurrent_first_calls_converge_on_one_response(
    pool: asyncpg.Pool, memo_workflow: Callable[..., Awaitable[UUID]]
) -> None:
    """Two workers genuinely in flight both pay — but both observe the same answer.

    decisions.md #18 is explicit that this design does not cover simultaneous
    execution, only the sequential re-run reclaim produces. What it must still
    guarantee is that the workflow has one durable view of the call, rather
    than each attempt proceeding on its own private response.
    """
    workflow_id = await memo_workflow()
    provider = _Provider()
    request = {"prompt": "raced"}

    results = await asyncio.gather(
        *(
            memoized_call(
                pool, workflow_id=workflow_id, lease_generation=gen, call_index=0,
                request=request, call=provider.complete,
            )
            for gen in (1, 2, 3)
        )
    )

    rows = await _memo_rows(pool, workflow_id)
    assert len(rows) == 1, "exactly one memo survives whoever raced"
    assert len({r["call_number"] for r in results}) == 1, (
        "every caller must observe the same stored response"
    )


async def test_memos_are_removed_with_their_workflow(
    pool: asyncpg.Pool, memo_workflow: Callable[..., Awaitable[UUID]]
) -> None:
    """A memo has no meaning without its workflow, so the FK cascades."""
    workflow_id = await memo_workflow()
    await memoized_call(
        pool, workflow_id=workflow_id, lease_generation=1, call_index=0,
        request={"prompt": "x"}, call=_Provider().complete,
    )
    assert len(await _memo_rows(pool, workflow_id)) == 1

    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM workflow_states WHERE id = $1", workflow_id)

    assert await _memo_rows(pool, workflow_id) == []


async def test_a_retried_workflow_does_not_repeat_its_paid_call(
    pool: asyncpg.Pool, redis_client: Redis
) -> None:
    """End to end through a real Relay and Worker: retry replays the step, not the bill.

    The unit tests above drive memoized_call directly. This one proves the
    mechanism survives the path an actual failure takes — outbox row, stream
    dispatch, fresh claim at a new lease_generation, handler re-invoked from
    the top — which is the only place the assumption "a re-run reaches the
    same call_index again" is really tested.
    """
    version = f"t{uuid4().hex[:8]}"
    provider = _Provider()
    invocations = 0

    async def handler(
        p: asyncpg.Pool, wid: UUID, generation: int, input_data: dict[str, Any]
    ) -> dict[str, Any]:
        nonlocal invocations
        invocations += 1
        completion = await memoized_call(
            p, workflow_id=wid, lease_generation=generation, call_index=0,
            request={"prompt": "expensive call"}, call=provider.complete,
        )
        # Fails *after* the paid call, which is the case that costs money:
        # the work was bought and then thrown away by the crash.
        if invocations < 3:
            raise ConnectionError("connection reset after the provider responded")
        return {"completion": completion}

    submitted = await submit_workflow(
        pool, workflow_type="paid", workflow_version=version,
        idempotency_key=f"memo_e2e_{uuid4()}", input_data={},
    )
    workflow_id = submitted.id

    status = await _run_until_terminal(
        pool, redis_client, workflow_version=version, workflow_id=workflow_id,
        handlers={"paid": handler},
    )

    assert status == "COMPLETED"
    assert invocations == 3, f"handler should have run 3 times, ran {invocations}"
    assert provider.calls == 1, (
        f"3 handler runs must cost 1 paid call, cost {provider.calls}"
    )

    async with pool.acquire() as conn:
        output = await conn.fetchval(
            "SELECT output_data FROM workflow_states WHERE id = $1", workflow_id
        )
    assert '"call_number": 1' in output, "the completed workflow used the first paid response"


async def _run_until_terminal(
    pool: asyncpg.Pool,
    redis_client: Redis,
    *,
    workflow_version: str,
    workflow_id: UUID,
    handlers: HandlerRegistry,
    timeout: float = 15.0,
) -> str | None:
    """Run a Relay and a Worker together until the workflow reaches a terminal state."""
    relay_stop = asyncio.Event()
    worker_stop = asyncio.Event()

    relay_task = asyncio.create_task(
        relay_run_forever(
            pool, redis_client, instance_id=uuid4(), batch_size=10,
            claim_lease_seconds=30, max_retries=5, poll_interval_seconds=0.05,
            shutdown_event=relay_stop,
        )
    )
    worker_task = asyncio.create_task(
        worker_run_forever(
            pool, redis_client,
            stream_name=f"workflow_stream_{workflow_version}",
            consumer_name=f"w-{uuid4()}", worker_id=uuid4(), handlers=handlers,
            lease_seconds=30, heartbeat_interval_seconds=10,
            xautoclaim_min_idle_seconds=35, max_retries=5,
            retry_base_seconds=FAST_BASE, retry_cap_seconds=FAST_CAP,
            max_chain_depth=50, batch_size=10, shutdown_event=worker_stop,
        )
    )

    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    status: str | None = None
    while loop.time() < deadline:
        async with pool.acquire() as conn:
            status = await conn.fetchval(
                "SELECT status FROM workflow_states WHERE id = $1 AND status = ANY($2::text[])",
                workflow_id,
                list(_TERMINAL),
            )
        if status is not None:
            break
        await asyncio.sleep(0.05)

    relay_stop.set()
    worker_stop.set()
    await asyncio.wait_for(asyncio.gather(relay_task, worker_task), timeout=5.0)
    return status
