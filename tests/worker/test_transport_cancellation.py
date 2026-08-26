"""Proves that fencing actually closes the socket — and nothing beyond that.

Scope, stated precisely, because this test sits underneath a cost-safety
claim that is easy to overstate:

  PROVES     — when stream_guard() detects supersession, the underlying TCP
               connection is genuinely closed, observed from the *server's*
               side, not merely believed closed by our own code.
  PROVES     — that close happens at or before the moment our code reports
               being fenced, so the transport is never lagging the decision.
  DOES NOT   — say anything about whether a real LLM provider stops
  PROVE        generating, or stops billing, once we disconnect. That is a
               property of someone else's infrastructure, it varies by
               provider, and it is measurable only against a real provider.
               See scripts/mistral_cancellation_probe.py, and treat the
               cost-safety guarantee as unvalidated for any provider that
               probe has not been run against.

Runs against a real Postgres (per project convention — the fencing check is
never mocked) and a real local socket, so the only simulated thing is the
"provider," never the mechanism under test.

On the close signal: the server watches for the client's FIN by reading,
not by waiting for a write to fail. A failed write is structurally one
write late — a FIN does not fail a server-to-client write, since half-close
still permits it; that write provokes an RST and only the *next* write
fails. Measuring via failed writes therefore reports
`2 x chunk_interval` and is a measurement of the fixture rather than of
Axiom. This was established empirically across a 20x range of chunk
intervals (ratio held at 1.97-2.01 throughout) before this test was
rewritten to use EOF.
"""

import asyncio
import time
import uuid
from collections.abc import Awaitable, Callable
from uuid import UUID

import asyncpg
import httpx
import pytest

from axiom.worker.execution import WorkerFencedError, execute_with_heartbeat, stream_guard
from axiom.worker.worker import claim_workflow
from tests.fixtures.fake_stream_server import FakeStreamServer


async def _stream_from_fake_server(port: int):
    """Adapt the fake server's chunked response into the shape a streaming handler yields."""
    url = f"http://127.0.0.1:{port}/stream"
    async with httpx.AsyncClient(timeout=None) as client, client.stream("GET", url) as response:
        async for chunk in response.aiter_bytes():
            yield chunk


@pytest.mark.asyncio
async def test_fencing_closes_the_socket_before_reporting_fenced(
    pool: asyncpg.Pool,
    make_workflow_row: Callable[..., Awaitable[UUID]],
) -> None:
    """A superseded worker's stream socket is closed, and closed before the error surfaces.

    Worker A claims a row and consumes a slow stream through the real
    stream_guard/execute_with_heartbeat path. Mid-stream, worker B's reclaim
    is simulated by bumping lease_generation — the same effect a genuine
    crash-and-reclaim has. The assertions are made against the fake server's
    own observations, never against our client's belief about itself.
    """
    server = FakeStreamServer(chunk_interval_seconds=0.1, total_chunks=50)
    await server.start()

    workflow_id = await make_workflow_row(
        idempotency_key=f"transport_cancel_{uuid.uuid4()}",
        workflow_type="test_transport_cancellation",
    )

    claimed = await claim_workflow(
        pool, workflow_id=workflow_id, worker_id=uuid.uuid4(), lease_seconds=30
    )
    assert claimed is not None
    lease_generation = claimed.lease_generation

    async def handler_body() -> int:
        source = _stream_from_fake_server(server.port)
        chunks_seen = 0
        async for _item in stream_guard(
            pool, source, workflow_id=workflow_id, lease_generation=lease_generation
        ):
            chunks_seen += 1
        return chunks_seen

    fenced_at: float | None = None

    async def run_and_capture() -> None:
        nonlocal fenced_at
        try:
            await execute_with_heartbeat(
                pool,
                handler_body(),
                workflow_id=workflow_id,
                lease_generation=lease_generation,
                lease_seconds=30,
                heartbeat_interval_seconds=10,
            )
        except WorkerFencedError:
            fenced_at = time.monotonic()

    task = asyncio.create_task(run_and_capture())

    # Let real chunks flow first, so this proves more than "it works when
    # zero chunks were ever sent."
    await asyncio.sleep(1.0)

    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE workflow_states SET lease_generation = lease_generation + 1 WHERE id = $1",
            workflow_id,
        )

    await asyncio.wait_for(task, timeout=10)
    assert fenced_at is not None, "WorkerFencedError was never raised"
    assert server.log.chunks_written, "no chunks were ever streamed — test proved nothing"

    await server.stop()

    assert server.log.eof_observed_at is not None, (
        "The fake server never saw the client's FIN — the transport did not "
        "actually close after cancellation. stream_guard's aclose() is not "
        "reaching the socket for this HTTP client, and the cost-safety "
        "mechanism is not real in practice."
    )

    # stream_guard closes the source *before* raising, so the FIN must not
    # lag the error. The epsilon absorbs scheduling jitter only; this is a
    # measurement of Axiom, not of the fixture's write cadence, and it held
    # flat at ~-0.0001s across chunk intervals from 0.02s to 0.4s.
    close_lag = server.log.eof_observed_at - fenced_at
    assert close_lag < 0.05, (
        f"Socket closed {close_lag:.4f}s after the fencing error surfaced; "
        "aclose() is expected to complete before the raise, so a positive "
        "lag means the close path has regressed."
    )
