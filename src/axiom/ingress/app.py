"""The HTTP gateway — validates requests and writes atomically to Postgres."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import asyncpg
from fastapi import Depends, FastAPI, HTTPException, status

from axiom.contracts.enums import INTERNAL_TO_PUBLIC_STATUS
from axiom.db import close_pool, get_pool, init_pool
from axiom.ingress.repository import submit_workflow
from axiom.ingress.schemas import WorkflowSubmitRequest, WorkflowSubmitResponse


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Open the DB pool on startup, close it on shutdown."""
    await init_pool()
    yield
    await close_pool()


app = FastAPI(title="Axiom Ingress", lifespan=lifespan)


async def _get_pool() -> asyncpg.Pool:
    """FastAPI dependency wrapper — enables overriding the pool in tests."""
    return get_pool()


@app.post("/workflows", status_code=status.HTTP_202_ACCEPTED)
async def create_workflow(
    payload: WorkflowSubmitRequest,
    pool: asyncpg.Pool = Depends(_get_pool),
) -> WorkflowSubmitResponse:
    """Validate, write atomically, and return immediately — no work happens in this request."""
    try:
        result = await submit_workflow(
            pool,
            workflow_type=payload.workflow_type,
            workflow_version=payload.workflow_version,
            idempotency_key=payload.idempotency_key,
            input_data=payload.input_data,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail="workflow submission failed") from exc

    return WorkflowSubmitResponse(
        id=str(result.id),
        status=INTERNAL_TO_PUBLIC_STATUS[result.status],
        workflow_type=result.workflow_type,
        workflow_version=result.workflow_version,
        created_at=result.created_at.isoformat(),
        replayed=not result.is_new_row,
    )


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    """Liveness probe — confirms the process is up, no dependency checks."""
    return {"status": "ok"}