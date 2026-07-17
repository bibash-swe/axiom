"""Tests for the Outbox Relay's claim/publish/settle logic."""

from collections.abc import Awaitable, Callable
from uuid import UUID, uuid4

import asyncpg
from redis.asyncio import Redis

from axiom.relay.relay import (
    claim_batch,
    publish_batch,
    run_relay_cycle,
    settle_failures,
    settle_success,
)

MakeOutboxRow = Callable[..., Awaitable[tuple[UUID, UUID]]]


async def test_claim_batch_claims_undispatched_rows(
    pool: asyncpg.Pool, make_outbox_row: MakeOutboxRow
) -> None:
    """A fresh, undispatched row is claimable, and reports the right fields."""
    version = f"test_{uuid4().hex[:8]}"
    workflow_id, outbox_id = await make_outbox_row(
        idempotency_key=f"k_{uuid4()}", workflow_version=version
    )

    claimed = await claim_batch(
        pool, instance_id=uuid4(), batch_size=100, claim_lease_seconds=30, max_retries=5
    )
    mine = [c for c in claimed if c.workflow_version == version]

    assert len(mine) == 1
    assert mine[0].id == outbox_id
    assert mine[0].workflow_id == workflow_id


async def test_claim_batch_respects_active_claim_lease(
    pool: asyncpg.Pool, make_outbox_row: MakeOutboxRow
) -> None:
    """A row claimed with a fresh lease is invisible to a second claimant."""
    version = f"test_{uuid4().hex[:8]}"
    _, outbox_id = await make_outbox_row(
        idempotency_key=f"k_{uuid4()}", workflow_version=version
    )

    first = await claim_batch(
        pool, instance_id=uuid4(), batch_size=100, claim_lease_seconds=30, max_retries=5
    )
    assert outbox_id in [c.id for c in first]

    second = await claim_batch(
        pool, instance_id=uuid4(), batch_size=100, claim_lease_seconds=30, max_retries=5
    )
    assert outbox_id not in [c.id for c in second]


async def test_publish_batch_routes_to_version_specific_stream(
    pool: asyncpg.Pool, redis_client: Redis, make_outbox_row: MakeOutboxRow
) -> None:
    """Each row's payload lands opaquely in workflow_stream_{its own version}."""
    version = f"test_{uuid4().hex[:8]}"
    _, outbox_id = await make_outbox_row(
        idempotency_key=f"k_{uuid4()}", workflow_version=version
    )

    claimed = await claim_batch(
        pool, instance_id=uuid4(), batch_size=100, claim_lease_seconds=30, max_retries=5
    )
    mine = [c for c in claimed if c.workflow_version == version]
    success_ids, failed_ids = await publish_batch(redis_client, mine)

    assert success_ids == [outbox_id]
    assert failed_ids == []
    assert await redis_client.xlen(f"workflow_stream_{version}") == 1


async def test_settle_success_marks_dispatched_and_releases_claim(
    pool: asyncpg.Pool, redis_client: Redis, make_outbox_row: MakeOutboxRow
) -> None:
    """A settled row is dispatched, claim-free, and excluded from future claims."""
    version = f"test_{uuid4().hex[:8]}"
    _, outbox_id = await make_outbox_row(
        idempotency_key=f"k_{uuid4()}", workflow_version=version
    )
    instance_a = uuid4()

    claimed = await claim_batch(
        pool, instance_id=instance_a, batch_size=100, claim_lease_seconds=30, max_retries=5
    )
    mine = [c for c in claimed if c.workflow_version == version]
    success_ids, _ = await publish_batch(redis_client, mine)
    await settle_success(pool, instance_id=instance_a, success_ids=success_ids)

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT dispatched, claimed_at FROM workflow_outbox WHERE id = $1", outbox_id
        )
    assert row is not None
    assert row["dispatched"] is True
    assert row["claimed_at"] is None

    reclaim = await claim_batch(
        pool, instance_id=instance_a, batch_size=100, claim_lease_seconds=30, max_retries=5
    )
    assert outbox_id not in [c.id for c in reclaim]


async def test_settle_success_respects_claimed_by_ownership(
    pool: asyncpg.Pool, make_outbox_row: MakeOutboxRow
) -> None:
    """A settlement attempt from the wrong instance must be a no-op."""
    version = f"test_{uuid4().hex[:8]}"
    _, outbox_id = await make_outbox_row(
        idempotency_key=f"k_{uuid4()}", workflow_version=version
    )

    claimed = await claim_batch(
        pool, instance_id=uuid4(), batch_size=100, claim_lease_seconds=30, max_retries=5
    )
    mine = [c for c in claimed if c.workflow_version == version]
    await settle_success(pool, instance_id=uuid4(), success_ids=[c.id for c in mine])

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT dispatched FROM workflow_outbox WHERE id = $1", outbox_id
        )
    assert row is not None
    assert row["dispatched"] is False


async def test_settle_failures_increments_retry_without_dead_lettering(
    pool: asyncpg.Pool, make_outbox_row: MakeOutboxRow
) -> None:
    """A single failure releases the claim and bumps retry_count, but stays PENDING."""
    version = f"test_{uuid4().hex[:8]}"
    workflow_id, outbox_id = await make_outbox_row(
        idempotency_key=f"k_{uuid4()}", workflow_version=version
    )
    instance_a = uuid4()

    claimed = await claim_batch(
        pool, instance_id=instance_a, batch_size=100, claim_lease_seconds=30, max_retries=5
    )
    mine = [c for c in claimed if c.workflow_version == version]
    await settle_failures(
        pool, instance_id=instance_a, failed_ids=[c.id for c in mine], max_retries=5
    )

    async with pool.acquire() as conn:
        outbox_row = await conn.fetchrow(
            "SELECT retry_count, claimed_at FROM workflow_outbox WHERE id = $1", outbox_id
        )
        state_row = await conn.fetchrow(
            "SELECT status FROM workflow_states WHERE id = $1", workflow_id
        )
    assert outbox_row is not None
    assert state_row is not None
    assert outbox_row["retry_count"] == 1
    assert outbox_row["claimed_at"] is None
    assert state_row["status"] == "PENDING"


async def test_settle_failures_dead_letters_at_max_retries(
    pool: asyncpg.Pool, make_outbox_row: MakeOutboxRow
) -> None:
    """Repeated failures dead-letter the workflow — exactly at the threshold, not before."""
    version = f"test_{uuid4().hex[:8]}"
    workflow_id, outbox_id = await make_outbox_row(
        idempotency_key=f"k_{uuid4()}", workflow_version=version
    )
    instance_a = uuid4()
    max_retries = 3

    for _ in range(max_retries):
        claimed = await claim_batch(
            pool,
            instance_id=instance_a,
            batch_size=100,
            claim_lease_seconds=0,
            max_retries=max_retries,
        )
        mine = [c for c in claimed if c.workflow_version == version]
        assert len(mine) == 1, "row must stay reclaimable below the retry threshold"
        await settle_failures(
            pool, instance_id=instance_a, failed_ids=[mine[0].id], max_retries=max_retries
        )

    async with pool.acquire() as conn:
        outbox_row = await conn.fetchrow(
            "SELECT retry_count FROM workflow_outbox WHERE id = $1", outbox_id
        )
        state_row = await conn.fetchrow(
            "SELECT status FROM workflow_states WHERE id = $1", workflow_id
        )
    assert outbox_row is not None
    assert state_row is not None
    assert outbox_row["retry_count"] == max_retries
    assert state_row["status"] == "DISPATCH_FAILED"

    final_claim = await claim_batch(
        pool,
        instance_id=instance_a,
        batch_size=100,
        claim_lease_seconds=0,
        max_retries=max_retries,
    )
    assert outbox_id not in [c.id for c in final_claim]


async def test_run_relay_cycle_processes_and_reports_count(
    pool: asyncpg.Pool, redis_client: Redis, make_outbox_row: MakeOutboxRow
) -> None:
    """A full cycle dispatches a pending row; an idle cycle correctly reports zero."""
    version = f"test_{uuid4().hex[:8]}"
    _, outbox_id = await make_outbox_row(
        idempotency_key=f"k_{uuid4()}", workflow_version=version
    )
    instance_a = uuid4()

    processed = await run_relay_cycle(
        pool,
        redis_client,
        instance_id=instance_a,
        batch_size=100,
        claim_lease_seconds=30,
        max_retries=5,
    )
    assert processed >= 1
    assert await redis_client.xlen(f"workflow_stream_{version}") == 1

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT dispatched FROM workflow_outbox WHERE id = $1", outbox_id
        )
    assert row is not None
    assert row["dispatched"] is True

    idle = await run_relay_cycle(
        pool,
        redis_client,
        instance_id=instance_a,
        batch_size=100,
        claim_lease_seconds=30,
        max_retries=5,
    )
    assert idle == 0