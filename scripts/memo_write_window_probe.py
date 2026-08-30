"""How big is the window a completion memo leaves open?

decisions.md #18 promised the memo would shrink the double-bill exposure "from
the whole handler duration to a single Postgres write" without saying how big
that write is — and an unquantified window is exactly the kind of claim this
project is supposed to refuse. That number is the residual risk, so it gets
measured rather than described.

Two figures come out:

  write  — the memo INSERT. A worker that dies inside this window has paid the
           provider and recorded nothing, so the next attempt pays again. This
           is the exposure that remains after the memo exists.
  miss   — the lookup that finds nothing, i.e. the cost the memo adds to every
           call that is NOT a replay. That is the common case, so it is the
           tax the mechanism charges in exchange.

Both run against the real table through the real pool, so connection
acquisition and JSONB encoding are inside the measurement rather than assumed
free. Check `fsync`, `synchronous_commit` and `full_page_writes` are on before
believing a number from here: with them off this measures nothing useful.

    uv run python scripts/memo_write_window_probe.py
"""

import asyncio
import json
import statistics
import time
from uuid import UUID, uuid4

import asyncpg

from axiom.config import settings

N = 300

# Roughly a short chat completion with its usage block — the payload size is
# part of what is being measured, since it is what gets written and fsynced.
RESPONSE = json.dumps(
    {
        "id": "cmpl-" + "0" * 24,
        "content": "x" * 800,
        "usage": {"prompt_tokens": 21, "completion_tokens": 40, "total_tokens": 61},
    }
)
FINGERPRINT = "a" * 64


async def _make_workflow(pool: asyncpg.Pool) -> UUID:
    """A real parent row, because the memo table's FK requires one."""
    async with pool.acquire() as conn:
        workflow_id: UUID = await conn.fetchval(
            "INSERT INTO workflow_states (workflow_type, workflow_version, idempotency_key) "
            "VALUES ('memo_probe', 'probe', $1) RETURNING id",
            f"memo_window_{uuid4()}",
        )
    return workflow_id


def _report(label: str, samples: list[float]) -> None:
    ordered = sorted(samples)
    print(
        f"  {label:<24} p50 {statistics.median(ordered):6.3f}ms  "
        f"p95 {ordered[int(0.95 * len(ordered))]:6.3f}ms  "
        f"max {ordered[-1]:6.3f}ms"
    )


async def main() -> None:
    """Measure the memo write and the memo miss, then clean up after itself."""
    pool = await asyncpg.create_pool(dsn=settings.database_url, min_size=2, max_size=10)
    assert pool is not None

    async with pool.acquire() as conn:
        for setting in ("fsync", "synchronous_commit", "full_page_writes"):
            value = await conn.fetchval(f"SHOW {setting}")
            if value != "on":
                print(f"  WARNING: {setting}={value} — this measurement is not meaningful")
        # Warm the pool and the plan cache: first-call cost is not the steady
        # state a running worker experiences.
        for _ in range(20):
            await conn.fetchval("SELECT 1")

    workflow_id = await _make_workflow(pool)

    writes: list[float] = []
    for call_index in range(N):
        async with pool.acquire() as conn:
            started = time.perf_counter()
            await conn.fetchrow(
                """INSERT INTO workflow_call_memos
                       (workflow_id, call_index, fingerprint, response,
                        written_by_lease_generation)
                   VALUES ($1, $2, $3, $4::jsonb, 1)
                   ON CONFLICT (workflow_id, call_index) DO NOTHING
                   RETURNING call_index""",
                workflow_id,
                call_index,
                FINGERPRINT,
                RESPONSE,
            )
            writes.append((time.perf_counter() - started) * 1000)

    misses: list[float] = []
    for _ in range(N):
        absent = uuid4()  # guaranteed miss
        async with pool.acquire() as conn:
            started = time.perf_counter()
            await conn.fetchrow(
                "SELECT fingerprint, response FROM workflow_call_memos "
                "WHERE workflow_id = $1 AND call_index = $2",
                absent,
                0,
            )
            misses.append((time.perf_counter() - started) * 1000)

    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM workflow_states WHERE id = $1", workflow_id)

    print(f"\n  n={N} per figure, {len(RESPONSE)}-byte response\n")
    _report("write (residual window)", writes)
    _report("miss (cost per call)", misses)

    await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
