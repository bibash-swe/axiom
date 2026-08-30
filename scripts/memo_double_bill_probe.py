"""What does a re-run actually cost, with and without a completion memo?

decisions.md #18 established the two halves of the double-billing problem
separately: a re-running handler re-issues its provider calls (#13), and an
identical request is regenerated and re-billed in full rather than deduplicated
(#18, measured). The engine-side half of the fix is covered by
tests/worker/test_memo.py, which counts calls against a fake provider.

This closes the loop end to end, in the only unit that settles the argument:
tokens actually billed by a real provider, for a real workflow that fails
twice and then succeeds, driven through the real Relay and Worker.

Run A registers a handler that calls the provider directly.
Run B registers the same handler with the call wrapped in memoized_call().
Both are made to fail *after* the provider responds, which is exactly the
shape that costs money — the work was bought and then thrown away.

The token figure comes from `x-ratelimit-tokens-query-cost`, which #15
established equals `usage.total_tokens` exactly and which is re-checked here
on every call rather than assumed still true.

Costs roughly 300 tokens of a cheap model. Run deliberately:

    MISTRAL_API_KEY=... uv run python scripts/memo_double_bill_probe.py

Findings from the August 2026 run are recorded in docs/decisions.md #19.
"""

import asyncio
import os
import sys
from typing import Any
from uuid import UUID, uuid4

import asyncpg
import httpx

from axiom.config import settings
from axiom.ingress.repository import submit_workflow
from axiom.redis_client import close_redis, get_redis, init_redis
from axiom.relay.runner import run_forever as relay_run_forever
from axiom.worker.memo import memoized_call
from axiom.worker.runner import HandlerRegistry
from axiom.worker.runner import run_forever as worker_run_forever

API_KEY = os.environ.get("PROBE_API_KEY") or os.environ.get("MISTRAL_API_KEY")
BASE_URL = os.environ.get("PROBE_BASE_URL", "https://api.mistral.ai/v1")
URL = f"{BASE_URL.rstrip('/')}/chat/completions"
MODEL = os.environ.get("PROBE_MODEL", "mistral-small-latest")
COST_HDR = "x-ratelimit-tokens-query-cost"

PROMPT = "Name one deep-sea creature and one fact about it, in a single sentence."
MAX_TOKENS = 40

# Fails on invocations 1 and 2, succeeds on 3 — the poison-pill ceiling is well
# clear of that, so the workflow completes rather than dead-lettering.
FAILURES_BEFORE_SUCCESS = 2

TERMINAL = ("COMPLETED", "FAILED", "CANCELED", "DEAD_LETTERED", "DISPATCH_FAILED")


class Meter:
    """A real provider call, with every token it bills recorded."""

    def __init__(self, client: httpx.AsyncClient) -> None:
        """Meter a shared HTTP client; totals accumulate across every call made through it."""
        self._client = client
        self.calls = 0
        self.tokens = 0
        self.header_disagreements = 0

    async def complete(self) -> dict[str, Any]:
        """Issue one real completion and record what it cost."""
        response = await self._client.post(
            URL,
            json={
                "model": MODEL,
                "messages": [{"role": "user", "content": PROMPT}],
                "max_tokens": MAX_TOKENS,
                "temperature": 1.0,
            },
        )
        response.raise_for_status()
        payload = response.json()

        usage = payload.get("usage") or {}
        total = usage.get("total_tokens")
        raw_cost = response.headers.get(COST_HDR)
        header_cost = int(raw_cost) if raw_cost is not None else None

        # Control B, re-checked per call: the cost signal must still be real.
        if header_cost != total:
            self.header_disagreements += 1

        self.calls += 1
        self.tokens += total or 0
        return {
            "id": payload.get("id"),
            "content": (payload["choices"][0]["message"] or {}).get("content", ""),
            "total_tokens": total,
        }


async def run_scenario(
    pool: asyncpg.Pool, redis: Any, meter: Meter, *, use_memo: bool
) -> tuple[str | None, int]:
    """Drive one workflow that fails twice after paying, then succeeds."""
    version = f"probe{uuid4().hex[:8]}"
    invocations = 0

    async def handler(
        p: asyncpg.Pool, wid: UUID, generation: int, input_data: dict[str, Any]
    ) -> dict[str, Any]:
        nonlocal invocations
        invocations += 1

        if use_memo:
            completion = await memoized_call(
                p,
                workflow_id=wid,
                lease_generation=generation,
                call_index=0,
                request={"model": MODEL, "prompt": PROMPT, "max_tokens": MAX_TOKENS},
                call=meter.complete,
            )
        else:
            completion = await meter.complete()

        if invocations <= FAILURES_BEFORE_SUCCESS:
            raise ConnectionError("connection reset after the provider responded")
        return {"completion": completion}

    handlers: HandlerRegistry = {"probe": handler}
    submitted = await submit_workflow(
        pool,
        workflow_type="probe",
        workflow_version=version,
        idempotency_key=f"memo_probe_{uuid4()}",
        input_data={},
    )

    relay_stop = asyncio.Event()
    worker_stop = asyncio.Event()
    relay_task = asyncio.create_task(
        relay_run_forever(
            pool, redis, instance_id=uuid4(), batch_size=10, claim_lease_seconds=30,
            max_retries=5, poll_interval_seconds=0.05, shutdown_event=relay_stop,
        )
    )
    worker_task = asyncio.create_task(
        worker_run_forever(
            pool, redis, stream_name=f"workflow_stream_{version}",
            consumer_name=f"probe-{uuid4()}", worker_id=uuid4(), handlers=handlers,
            lease_seconds=30, heartbeat_interval_seconds=10,
            xautoclaim_min_idle_seconds=35, max_retries=5,
            retry_base_seconds=0.05, retry_cap_seconds=0.05,
            max_chain_depth=50, batch_size=10, shutdown_event=worker_stop,
        )
    )

    loop = asyncio.get_running_loop()
    deadline = loop.time() + 60.0
    status: str | None = None
    while loop.time() < deadline:
        async with pool.acquire() as conn:
            status = await conn.fetchval(
                "SELECT status FROM workflow_states WHERE id = $1 AND status = ANY($2::text[])",
                submitted.id,
                list(TERMINAL),
            )
        if status is not None:
            break
        await asyncio.sleep(0.1)

    relay_stop.set()
    worker_stop.set()
    await asyncio.wait_for(asyncio.gather(relay_task, worker_task), timeout=10.0)
    return status, invocations


async def main() -> None:
    """Run both scenarios against the same provider and report the difference."""
    if not API_KEY:
        print("set PROBE_API_KEY or MISTRAL_API_KEY", file=sys.stderr)
        raise SystemExit(2)

    pool = await asyncpg.create_pool(dsn=settings.database_url, min_size=2, max_size=10)
    assert pool is not None
    await init_redis()
    redis = get_redis()

    results: dict[str, tuple[str | None, int, int, int]] = {}
    async with httpx.AsyncClient(
        timeout=60.0, headers={"Authorization": f"Bearer {API_KEY}"}
    ) as client:
        for label, use_memo in (("without memo", False), ("with memo", True)):
            meter = Meter(client)
            status, invocations = await run_scenario(pool, redis, meter, use_memo=use_memo)
            results[label] = (status, invocations, meter.calls, meter.tokens)
            if meter.header_disagreements:
                print(
                    f"  control B FAILED on {meter.header_disagreements} call(s): "
                    "the cost header no longer matches usage.total_tokens"
                )

    print(
        f"\nmodel={MODEL}  handler fails {FAILURES_BEFORE_SUCCESS}x "
        "after paying, then succeeds\n"
    )
    print(f"  {'run':<14} {'status':<10} {'handler runs':>13} {'paid calls':>11} {'tokens':>8}")
    for label, (status, invocations, calls, tokens) in results.items():
        print(f"  {label:<14} {status or '-':<10} {invocations:>13} {calls:>11} {tokens:>8}")

    baseline = results["without memo"][3]
    memoized = results["with memo"][3]
    if baseline:
        print(
            f"\n  tokens avoided: {baseline - memoized} of {baseline} "
            f"({1 - memoized / baseline:.0%})"
        )

    await close_redis()
    await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
