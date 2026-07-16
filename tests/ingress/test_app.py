"""Tests for the ingress HTTP gateway."""

from collections.abc import AsyncIterator
from uuid import uuid4

import asyncpg
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from axiom.ingress.app import _get_pool, app


@pytest_asyncio.fixture
async def client(pool: asyncpg.Pool) -> AsyncIterator[AsyncClient]:
    """An httpx client wired to the real pool fixture via dependency override.

    Bypasses the app's own lifespan-managed pool entirely, so tests don't
    depend on startup/shutdown timing.
    """
    app.dependency_overrides[_get_pool] = lambda: pool
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c
    app.dependency_overrides.clear()


async def test_create_workflow_returns_202_with_queued_status(client: AsyncClient) -> None:
    """A fresh submission returns 202 with the public (not internal) status vocabulary."""
    resp = await client.post(
        "/workflows",
        json={
            "workflow_type": "summarize_doc",
            "idempotency_key": f"key_{uuid4()}",
            "input_data": {"doc_id": "42"},
        },
    )

    assert resp.status_code == 202
    body = resp.json()
    assert body["status"] == "queued"  # public vocabulary, never "PENDING"
    assert body["replayed"] is False
    assert body["workflow_type"] == "summarize_doc"


async def test_create_workflow_replay_returns_same_id(client: AsyncClient) -> None:
    """A replay via HTTP resolves to the same id and is flagged as replayed."""
    idem_key = f"key_{uuid4()}"
    payload = {"workflow_type": "summarize_doc", "idempotency_key": idem_key, "input_data": {}}

    first = await client.post("/workflows", json=payload)
    second = await client.post("/workflows", json=payload)

    assert second.status_code == 202
    assert second.json()["id"] == first.json()["id"]
    assert second.json()["replayed"] is True


async def test_create_workflow_rejects_blank_idempotency_key(client: AsyncClient) -> None:
    """Whitespace-only idempotency_key fails the schema's own validator, not just min_length."""
    resp = await client.post(
        "/workflows",
        json={"workflow_type": "x", "idempotency_key": "   ", "input_data": {}},
    )
    assert resp.status_code == 422


async def test_create_workflow_rejects_missing_required_field(client: AsyncClient) -> None:
    """workflow_type is required — omitting it is a validation error, not a 500."""
    resp = await client.post(
        "/workflows",
        json={"idempotency_key": f"key_{uuid4()}", "input_data": {}},
    )
    assert resp.status_code == 422


async def test_create_workflow_converts_db_failure_to_500(client: AsyncClient) -> None:
    """A broken pool surfaces as a clean 500, not an unhandled exception leaking through."""
    broken_pool = await asyncpg.create_pool(
        dsn="postgresql://axiom:wrong_password@localhost:5432/axiom", min_size=0, max_size=1
    )
    app.dependency_overrides[_get_pool] = lambda: broken_pool

    resp = await client.post(
        "/workflows",
        json={"workflow_type": "x", "idempotency_key": f"key_{uuid4()}", "input_data": {}},
    )
    assert resp.status_code == 500
    await broken_pool.close()


async def test_healthz_returns_ok(client: AsyncClient) -> None:
    """Liveness probe responds without touching the database at all."""
    resp = await client.get("/healthz")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}