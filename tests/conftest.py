"""Shared fixtures for the test suite."""

import asyncpg
import pytest_asyncio

from axiom.config import settings


@pytest_asyncio.fixture
async def pool():
    """A real asyncpg pool against the real local Postgres — never mocked.

    max_size=20: the concurrent-race tests fire 10+ simultaneous requests:
    a pool smaller than that would serialize some of them at the client
    level, which doesn't invalidate the DB-side atomicity guarantee being
    tested, but does weaken how much real concurrency the test exercises.
    """
    p = await asyncpg.create_pool(dsn=settings.database_url, min_size=2, max_size=20)
    yield p
    await p.close()