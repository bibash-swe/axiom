"""Shared fixtures for the test suite."""
import json
from collections.abc import AsyncIterator, Awaitable, Callable
from datetime import UTC, datetime, timedelta
from typing import Any, cast
from uuid import UUID, uuid4

import asyncpg
import pytest
import pytest_asyncio
from redis.asyncio import Redis

from axiom.config import settings


@pytest_asyncio.fixture
async def pool() -> AsyncIterator[asyncpg.Pool]:
    """A real asyncpg pool against the real local Postgres — never mocked.

    max_size=20: the concurrent-race tests fire 10+ simultaneous requests.
    A pool smaller than that would serialize some of them at the client
    level, which doesn't invalidate the DB-side atomicity guarantee being
    tested, but does weaken how much real concurrency the test exercises.
    """
    p = await asyncpg.create_pool(dsn=settings.database_url, min_size=2, max_size=20)
    yield p
    await p.close()


@pytest_asyncio.fixture
async def redis_client() -> AsyncIterator[Redis]:
    """A real Redis client against the real local Redis — never mocked."""
    r = Redis.from_url(settings.redis_url, decode_responses=True, socket_timeout=2)
    yield r
    await r.aclose()


@pytest.fixture
def make_outbox_row(
        pool: asyncpg.Pool,
) -> Callable[..., Awaitable[tuple[UUID, UUID]]]:
    """Factory fixture returning an async helper bound to the pool fixture.

    Pytest auto-injects this into any test that declares make_outbox_row
    as a parameter — no import needed, same mechanism that already makes
    pool and redis_client available across test files.
    """

    async def _make(*, idempotency_key: str, workflow_version: str) -> tuple[UUID, UUID]:
        async with pool.acquire() as conn:
            workflow_id = await conn.fetchval(
                "INSERT INTO workflow_states (workflow_type, workflow_version, idempotency_key) "
                "VALUES ('test_type', $1, $2) RETURNING id",
                workflow_version,
                idempotency_key,
            )
            outbox_id = await conn.fetchval(
                "INSERT INTO workflow_outbox (workflow_id, event_type, payload, workflow_version) "
                "VALUES ($1, 'WORKFLOW_STARTED', $2::jsonb, $3) RETURNING id",
                workflow_id,
                json.dumps({"event_type": "WORKFLOW_STARTED", "workflow_id": str(workflow_id)}),
                workflow_version,
            )
        return workflow_id, outbox_id

    return _make


@pytest.fixture
def make_workflow_row(pool: asyncpg.Pool) -> Callable[..., Awaitable[UUID]]:
    """Factory fixture: creates a workflow_states row in any state a test needs.

    PENDING by default, or pre-claimed (fresh- or stale-leased) to
    simulate an in-progress or crashed worker.
    """

    async def _make(
        *,
        idempotency_key: str,
        workflow_type: str = "test_type",
        workflow_version: str = "v1",
        input_data: dict[str, Any] | None = None,
        status: str = "PENDING",
        lease_generation: int = 0,
        lease_stale: bool = False,
    ) -> UUID:
        async with pool.acquire() as conn:
            if status == "PENDING":
                return cast(
                    UUID,
                    await conn.fetchval(
                        "INSERT INTO workflow_states "
                        "(workflow_type, workflow_version, idempotency_key, input_data) "
                        "VALUES ($1, $2, $3, $4::jsonb) RETURNING id",
                        workflow_type,
                        workflow_version,
                        idempotency_key,
                        json.dumps(input_data or {}),
                    ),
                )

            lease_expires_at = datetime.now(UTC) + (
                timedelta(hours=-1) if lease_stale else timedelta(hours=1)
            )
            return cast(
                UUID,
                await conn.fetchval(
                    "INSERT INTO workflow_states "
                    "(workflow_type, workflow_version, idempotency_key, input_data, "
                    " status, worker_id, lease_generation, lease_expires_at) "
                    "VALUES ($1, $2, $3, $4::jsonb, $5, $6, $7, $8) RETURNING id",
                    workflow_type,
                    workflow_version,
                    idempotency_key,
                    json.dumps(input_data or {}),
                    status,
                    uuid4(),
                    lease_generation,
                    lease_expires_at,
                ),
            )

    return _make


@pytest.fixture
def dispatch_workflow(
    pool: asyncpg.Pool, redis_client: Redis
) -> Callable[..., Awaitable[UUID]]:
    """Factory fixture: creates a PENDING row and pushes its WorkflowStartedEvent.

    It pushes onto the given stream — the same shape the Relay produces, letting runner
    tests exercise the real consumption path end to end.
    """

    async def _dispatch(
        *,
        stream_name: str,
        workflow_type: str,
        input_data: dict[str, Any] | None = None,
        idempotency_key: str | None = None,
    ) -> UUID:
        async with pool.acquire() as conn:
            workflow_id = cast(
                UUID,
                await conn.fetchval(
                    "INSERT INTO workflow_states "
                    "(workflow_type, workflow_version, idempotency_key, input_data) "
                    "VALUES ($1, 'v1', $2, $3::jsonb) RETURNING id",
                    workflow_type,
                    idempotency_key or f"k_{uuid4()}",
                    json.dumps(input_data or {}),
                ),
            )
        payload = json.dumps(
            {"event_type": "WORKFLOW_STARTED", "workflow_id": str(workflow_id)}
        )
        await redis_client.xadd(stream_name, {"payload": payload})
        return workflow_id

    return _dispatch