"""Shared Postgres connection pool, lifespan-managed by the FastAPI app."""

import asyncpg

from axiom.config import settings

_pool: asyncpg.Pool | None = None


async def init_pool() -> asyncpg.Pool:
    """Create the pool once; safe to call multiple times."""
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(
            dsn=settings.database_url,
            min_size=settings.db_pool_min_size,
            max_size=settings.db_pool_max_size,
        )
    return _pool


async def close_pool() -> None:
    """Close the pool on shutdown; safe to call even if never initialized."""
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None


def get_pool() -> asyncpg.Pool:
    """Return the initialized pool, or raise if called before startup."""
    if _pool is None:
        raise RuntimeError("DB pool not initialized — call init_pool() during app startup")
    return _pool