"""Isolation for the Relay tests."""

from collections.abc import AsyncIterator

import asyncpg
import pytest_asyncio


@pytest_asyncio.fixture(autouse=True)
async def _empty_outbox(pool: asyncpg.Pool) -> AsyncIterator[None]:
    """Clear undispatched outbox rows before each Relay test.

    claim_batch is ORDER BY created_at ASC LIMIT batch_size, so a test's own row
    is only found while fewer than batch_size older undispatched rows exist.
    Leftovers from other tests silently push it past the limit and the failure
    looks like a Relay bug. Nothing in production removes these either — that
    part is the outbox retention gap in docs/decisions.md.
    """
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM workflow_outbox WHERE dispatched = FALSE")
    yield
