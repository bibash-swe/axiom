"""Multi-step workflows: a handler hands off to the next step.

Until this existed, "multi-step" was a claim in the README with nothing
implementing it — a handler had no way to enqueue its successor, so every
workflow was exactly one step long.

These tests drive the real Relay and Worker against real Postgres and Redis.
That is not thoroughness for its own sake: a chain only advances if the
successor's outbox row is actually claimed, published to the stream its
version routes to, and consumed. Asserting that a row was written would prove
none of that.
"""

import asyncio
import json
from typing import Any
from uuid import UUID, uuid4

import asyncpg
import pytest
from redis.asyncio import Redis

from axiom.ingress.repository import submit_workflow
from axiom.relay.runner import run_forever as relay_run_forever
from axiom.worker.execution import NextStep
from axiom.worker.runner import HandlerRegistry, ensure_consumer_group, process_message
from axiom.worker.runner import run_forever as worker_run_forever
from axiom.worker.worker import chain_idempotency_key, claim_workflow, settle_and_chain

FAST_BASE = 0.01
FAST_CAP = 0.01

_TERMINAL = ("COMPLETED", "FAILED", "CANCELED", "DEAD_LETTERED", "DISPATCH_FAILED")

# Walks the chain forward from its root. This is the query idx_workflow_states_parent
# exists to serve, and the only way to see a chain as one object rather than
# a scattering of unrelated rows.
_WALK_CHAIN = """
    WITH RECURSIVE chain AS (
        SELECT id, workflow_type, workflow_version, status, input_data, output_data,
               error_log, parent_workflow_id, chain_depth, idempotency_key
        FROM workflow_states WHERE id = $1
        UNION ALL
        SELECT w.id, w.workflow_type, w.workflow_version, w.status, w.input_data,
               w.output_data, w.error_log, w.parent_workflow_id, w.chain_depth,
               w.idempotency_key
        FROM workflow_states w JOIN chain c ON w.parent_workflow_id = c.id
    )
    SELECT * FROM chain ORDER BY chain_depth
"""


async def _walk_chain(pool: asyncpg.Pool, root_id: UUID) -> list[dict[str, Any]]:
    async with pool.acquire() as conn:
        rows = await conn.fetch(_WALK_CHAIN, root_id)
    return [dict(r) for r in rows]


async def _chain_is_settled(pool: asyncpg.Pool, root_id: UUID, *, expected_length: int) -> bool:
    """The chain has reached its expected length and nothing in it is still running."""
    chain = await _walk_chain(pool, root_id)
    return len(chain) == expected_length and all(r["status"] in _TERMINAL for r in chain)


async def _run_chain(
    pool: asyncpg.Pool,
    redis_client: Redis,
    *,
    workflow_version: str,
    root_id: UUID,
    handlers: HandlerRegistry,
    expected_length: int,
    max_chain_depth: int = 50,
    timeout: float = 20.0,
) -> list[dict[str, Any]]:
    """Run a Relay and a Worker until the whole chain settles. Returns it root-first."""
    relay_stop = asyncio.Event()
    worker_stop = asyncio.Event()

    relay_task = asyncio.create_task(
        relay_run_forever(
            pool,
            redis_client,
            instance_id=uuid4(),
            batch_size=10,
            claim_lease_seconds=30,
            max_retries=5,
            poll_interval_seconds=0.05,
            shutdown_event=relay_stop,
        )
    )
    worker_task = asyncio.create_task(
        worker_run_forever(
            pool,
            redis_client,
            stream_name=f"workflow_stream_{workflow_version}",
            consumer_name=f"w-{uuid4()}",
            worker_id=uuid4(),
            handlers=handlers,
            lease_seconds=30,
            heartbeat_interval_seconds=10,
            xautoclaim_min_idle_seconds=35,
            max_retries=5,
            retry_base_seconds=FAST_BASE,
            retry_cap_seconds=FAST_CAP,
            max_chain_depth=max_chain_depth,
            batch_size=10,
            shutdown_event=worker_stop,
        )
    )

    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        if await _chain_is_settled(pool, root_id, expected_length=expected_length):
            break
        await asyncio.sleep(0.05)

    relay_stop.set()
    worker_stop.set()
    await asyncio.wait_for(asyncio.gather(relay_task, worker_task), timeout=5.0)
    return await _walk_chain(pool, root_id)


async def _submit_root(pool: asyncpg.Pool, *, workflow_type: str, version: str) -> UUID:
    result = await submit_workflow(
        pool,
        workflow_type=workflow_type,
        workflow_version=version,
        idempotency_key=f"chain_root_{uuid4()}",
        input_data={"seed": 1},
    )
    return result.id


async def test_a_three_step_chain_runs_to_completion(
    pool: asyncpg.Pool, redis_client: Redis
) -> None:
    """Three handlers, each handing off to the next, run in order with data flowing through."""
    version = f"t{uuid4().hex[:8]}"
    ran: list[str] = []

    async def step1(
        p: asyncpg.Pool, wid: UUID, generation: int, input_data: dict[str, Any]
    ) -> NextStep:
        ran.append("step1")
        value = input_data["seed"] + 1
        return NextStep(
            output={"value": value}, workflow_type="step2", input_data={"value": value}
        )

    async def step2(
        p: asyncpg.Pool, wid: UUID, generation: int, input_data: dict[str, Any]
    ) -> NextStep:
        ran.append("step2")
        value = input_data["value"] * 10
        return NextStep(
            output={"value": value}, workflow_type="step3", input_data={"value": value}
        )

    async def step3(
        p: asyncpg.Pool, wid: UUID, generation: int, input_data: dict[str, Any]
    ) -> dict[str, Any]:
        ran.append("step3")
        return {"final": input_data["value"]}

    root_id = await _submit_root(pool, workflow_type="step1", version=version)
    chain = await _run_chain(
        pool,
        redis_client,
        workflow_version=version,
        root_id=root_id,
        handlers={"step1": step1, "step2": step2, "step3": step3},
        expected_length=3,
    )

    assert ran == ["step1", "step2", "step3"], f"steps ran out of order or not at all: {ran}"
    assert [r["workflow_type"] for r in chain] == ["step1", "step2", "step3"]
    assert all(r["status"] == "COMPLETED" for r in chain), [
        (r["workflow_type"], r["status"], r["error_log"]) for r in chain
    ]

    # Structure: each step points back at the one that created it, and depth
    # is exactly its position.
    assert [r["chain_depth"] for r in chain] == [0, 1, 2]
    assert chain[0]["parent_workflow_id"] is None
    assert chain[1]["parent_workflow_id"] == chain[0]["id"]
    assert chain[2]["parent_workflow_id"] == chain[1]["id"]

    # Data actually flowed: seed 1 -> +1 -> x10.
    assert json.loads(chain[0]["output_data"]) == {"value": 2}
    assert json.loads(chain[1]["input_data"]) == {"value": 2}
    assert json.loads(chain[1]["output_data"]) == {"value": 20}
    assert json.loads(chain[2]["output_data"]) == {"final": 20}

    # Version inheritance is load-bearing, not cosmetic: the Relay routes to
    # workflow_stream_<version>, so a successor on the wrong version would be
    # published to a stream this worker is not consuming and the chain would
    # stall silently.
    assert [r["workflow_version"] for r in chain] == [version] * 3
    assert chain[1]["idempotency_key"] == chain_idempotency_key(chain[0]["id"], "step2")


async def test_a_failing_step_retries_without_re_running_earlier_steps(
    pool: asyncpg.Pool, redis_client: Redis
) -> None:
    """The point of composing atomic steps: a retry replays one step, not the chain.

    This is decision #13 made observable. If a chain were one workflow with
    internal progress, recovering step 2 would mean re-running step 1 — on an
    LLM workload, paying for it again.
    """
    version = f"t{uuid4().hex[:8]}"
    step1_runs = 0
    step2_runs = 0

    async def step1(
        p: asyncpg.Pool, wid: UUID, generation: int, input_data: dict[str, Any]
    ) -> NextStep:
        nonlocal step1_runs
        step1_runs += 1
        return NextStep(output={"from": "step1"}, workflow_type="step2", input_data={"n": 7})

    async def step2(
        p: asyncpg.Pool, wid: UUID, generation: int, input_data: dict[str, Any]
    ) -> dict[str, Any]:
        nonlocal step2_runs
        step2_runs += 1
        if step2_runs == 1:
            raise ConnectionError("provider unavailable")
        return {"n": input_data["n"]}

    root_id = await _submit_root(pool, workflow_type="step1", version=version)
    chain = await _run_chain(
        pool,
        redis_client,
        workflow_version=version,
        root_id=root_id,
        handlers={"step1": step1, "step2": step2},
        expected_length=2,
    )

    assert all(r["status"] == "COMPLETED" for r in chain), [
        (r["workflow_type"], r["status"], r["error_log"]) for r in chain
    ]
    assert step2_runs == 2, f"step2 should have been retried once, ran {step2_runs} times"
    assert step1_runs == 1, (
        f"step1 re-ran {step1_runs} times — a failure in step 2 must not replay step 1"
    )


async def test_the_chain_stops_at_the_depth_limit(
    pool: asyncpg.Pool, redis_client: Redis
) -> None:
    """A handler that always chains is stopped at the ceiling, loudly.

    Nothing else bounds this: max_retries is per-workflow and every link is a
    new workflow with a fresh budget, so without the ceiling this loops until
    someone notices the bill.
    """
    version = f"t{uuid4().hex[:8]}"
    max_chain_depth = 2
    runs = 0

    async def loop_forever(
        p: asyncpg.Pool, wid: UUID, generation: int, input_data: dict[str, Any]
    ) -> NextStep:
        nonlocal runs
        runs += 1
        return NextStep(
            output={"run": runs}, workflow_type="loop_forever", input_data={"run": runs}
        )

    root_id = await _submit_root(pool, workflow_type="loop_forever", version=version)
    chain = await _run_chain(
        pool,
        redis_client,
        workflow_version=version,
        root_id=root_id,
        handlers={"loop_forever": loop_forever},
        expected_length=max_chain_depth + 1,
        max_chain_depth=max_chain_depth,
    )

    # max_chain_depth is the highest permitted chain_depth and the root is 0,
    # so the chain holds exactly max_chain_depth + 1 workflows.
    assert [r["chain_depth"] for r in chain] == [0, 1, 2]
    assert [r["status"] for r in chain] == ["COMPLETED", "COMPLETED", "FAILED"]
    assert runs == 3, f"handler should have run once per link, ran {runs} times"

    last = chain[-1]
    error = json.loads(last["error_log"])
    assert error["error"] == "chain depth limit reached"
    assert error["max_chain_depth"] == max_chain_depth
    assert error["refused_workflow_type"] == "loop_forever"
    # The step's own work still succeeded — refusing the successor must not
    # throw away what the handler already computed (and already paid for).
    assert json.loads(last["output_data"]) == {"run": 3}

    # And nothing was dispatched for a fourth link.
    async with pool.acquire() as conn:
        orphans = await conn.fetchval(
            "SELECT count(*) FROM workflow_states WHERE parent_workflow_id = $1", last["id"]
        )
    assert orphans == 0


async def test_a_fenced_worker_creates_no_successor(pool: asyncpg.Pool) -> None:
    """A superseded worker must write nothing: not the completion, not the successor."""
    result = await submit_workflow(
        pool,
        workflow_type="fenced_chain",
        workflow_version="v1",
        idempotency_key=f"fenced_chain_{uuid4()}",
        input_data={},
    )
    workflow_id = result.id

    claimed = await claim_workflow(
        pool, workflow_id=workflow_id, worker_id=uuid4(), lease_seconds=30
    )
    assert claimed is not None
    stale_generation = claimed.lease_generation

    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE workflow_states SET lease_generation = lease_generation + 1 WHERE id = $1",
            workflow_id,
        )

    successor_id = await settle_and_chain(
        pool,
        workflow_id=workflow_id,
        lease_generation=stale_generation,
        output_data={"should": "not land"},
        next_workflow_type="never_created",
        next_input_data={},
    )

    assert successor_id is None
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT status, output_data FROM workflow_states WHERE id = $1", workflow_id
        )
        successors = await conn.fetchval(
            "SELECT count(*) FROM workflow_states WHERE parent_workflow_id = $1", workflow_id
        )
        events = await conn.fetchval(
            "SELECT count(*) FROM workflow_outbox WHERE workflow_id IN "
            "(SELECT id FROM workflow_states WHERE parent_workflow_id = $1)",
            workflow_id,
        )

    assert row is not None
    # Still owned by the newer generation, with no output written.
    assert row["status"] == "RUNNING"
    assert row["output_data"] is None
    assert successors == 0
    assert events == 0


async def test_replaying_the_chain_write_creates_exactly_one_successor(
    pool: asyncpg.Pool,
) -> None:
    """Two identical chain writes produce one successor and one dispatch event.

    The successor's key is derived from its parent precisely so this is a
    no-op rather than a fork. A second dispatch event would be worse than a
    second row: the same workflow would execute twice.
    """
    result = await submit_workflow(
        pool,
        workflow_type="replayed",
        workflow_version="v1",
        idempotency_key=f"replayed_{uuid4()}",
        input_data={},
    )
    workflow_id = result.id
    claimed = await claim_workflow(
        pool, workflow_id=workflow_id, worker_id=uuid4(), lease_seconds=30
    )
    assert claimed is not None

    first = await settle_and_chain(
        pool,
        workflow_id=workflow_id,
        lease_generation=claimed.lease_generation,
        output_data={"n": 1},
        next_workflow_type="successor",
        next_input_data={"n": 1},
    )
    second = await settle_and_chain(
        pool,
        workflow_id=workflow_id,
        lease_generation=claimed.lease_generation,
        output_data={"n": 1},
        next_workflow_type="successor",
        next_input_data={"n": 999},
    )

    assert first is not None
    assert second == first, "a replayed chain write must resolve to the same successor"

    async with pool.acquire() as conn:
        successors = await conn.fetch(
            "SELECT id, input_data FROM workflow_states WHERE parent_workflow_id = $1",
            workflow_id,
        )
        events = await conn.fetchval(
            "SELECT count(*) FROM workflow_outbox WHERE workflow_id = $1", first
        )

    assert len(successors) == 1
    assert events == 1
    # The replay must not overwrite the successor's input either.
    assert json.loads(successors[0]["input_data"]) == {"n": 1}


async def test_a_redelivered_message_does_not_chain_a_second_time(
    pool: asyncpg.Pool, redis_client: Redis
) -> None:
    """Redis is at-least-once, and the ack is the last thing a worker does.

    A worker that commits the chain write and then dies before acking will see
    the same message again. That redelivery must not run the step again or
    create a second successor — the window between "committed" and "acked" is
    the one place chaining could silently fork a workflow.
    """
    version = f"t{uuid4().hex[:8]}"
    runs = 0

    async def step1(
        p: asyncpg.Pool, wid: UUID, generation: int, input_data: dict[str, Any]
    ) -> NextStep:
        nonlocal runs
        runs += 1
        return NextStep(output={"run": runs}, workflow_type="step2", input_data={})

    async def step2(
        p: asyncpg.Pool, wid: UUID, generation: int, input_data: dict[str, Any]
    ) -> dict[str, Any]:
        return {"done": True}

    handlers: HandlerRegistry = {"step1": step1, "step2": step2}
    root_id = await _submit_root(pool, workflow_type="step1", version=version)
    chain = await _run_chain(
        pool,
        redis_client,
        workflow_version=version,
        root_id=root_id,
        handlers=handlers,
        expected_length=2,
    )
    assert len(chain) == 2 and runs == 1

    # Replay the root's dispatch exactly as the Relay first wrote it, and drive
    # it through the worker directly. Running the polling loop again would let
    # the "chain is settled" check pass before the message was ever read,
    # making this assert nothing.
    stream = f"workflow_stream_{version}"
    payload = json.dumps({"event_type": "WORKFLOW_STARTED", "workflow_id": str(root_id)})
    await redis_client.xadd(stream, {"payload": payload})
    await ensure_consumer_group(redis_client, stream_name=stream)
    consumer = f"redelivery-{uuid4()}"
    delivered = await redis_client.xreadgroup(
        "workers", consumer, streams={stream: ">"}, count=10
    )
    message_id = delivered[0][1][-1][0]

    await process_message(
        pool,
        redis_client,
        stream_name=stream,
        message_id=message_id,
        payload=payload,
        worker_id=uuid4(),
        handlers=handlers,
        lease_seconds=30,
        heartbeat_interval_seconds=10,
        max_retries=5,
        retry_base_seconds=FAST_BASE,
        retry_cap_seconds=FAST_CAP,
        max_chain_depth=50,
    )

    after = await _walk_chain(pool, root_id)
    assert runs == 1, f"the redelivered message re-ran the handler ({runs} runs)"
    assert len(after) == 2, f"redelivery forked the chain into {len(after)} workflows"
    assert [r["id"] for r in after] == [r["id"] for r in chain]
    # Acked, not left pending: an already-terminal workflow is a safe no-op,
    # so holding the message would strand it until min_idle_time for nothing.
    assert await redis_client.xpending(stream, "workers") == {
        "pending": 0,
        "min": None,
        "max": None,
        "consumers": [],
    }


async def test_a_client_cannot_submit_a_reserved_chain_key(pool: asyncpg.Pool) -> None:
    """The 'chain:' prefix belongs to the engine.

    Without this, a client could submit a key matching a chain step that is
    about to be created and silently take its place in someone else's chain.
    Enforced at the database because Ingress is not the only writer.
    """
    with pytest.raises(asyncpg.CheckViolationError):
        await submit_workflow(
            pool,
            workflow_type="impostor",
            workflow_version="v1",
            idempotency_key=chain_idempotency_key(uuid4(), "step2"),
            input_data={},
        )
