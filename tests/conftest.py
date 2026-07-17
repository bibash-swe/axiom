"""Shared fixtures for the test suite."""
import json
from collections.abc import AsyncIterator, Awaitable, Callable
from uuid import UUID

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