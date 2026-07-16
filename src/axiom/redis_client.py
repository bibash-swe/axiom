"""Shared Redis client, lifespan-managed by whichever process needs it.

A single redis.asyncio.Redis instance, not a separate pool wrapper like
db.py's asyncpg.Pool — redis-py's async client manages connection pooling
internally, so there's nothing extra to wrap here.
"""

from redis.asyncio import Redis

from axiom.config import settings

_client: Redis | None = None


async def init_redis() -> Redis:
    """Create the client once; safe to call multiple times."""
    global _client
    if _client is None:
        _client = Redis.from_url(settings.redis_url, decode_responses=True)
    return _client


async def close_redis() -> None:
    """Close the client on shutdown; safe to call even if never initialized."""
    global _client
    if _client is not None:
        await _client.aclose()
        _client = None


def get_redis() -> Redis:
    """Return the initialized client, or raise if called before startup."""
    if _client is None:
        raise RuntimeError("Redis client not initialized — call init_redis() during startup")
    return _client