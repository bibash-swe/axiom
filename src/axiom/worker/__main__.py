"""Process entrypoint: python -m axiom.worker <stream_version>.

Owns the real pool/Redis connection lifecycle and OS signal handling —
same split as the Relay's __main__.py, keeping runner.run_forever() a
pure, fully-parameterized, testable loop.

No real workflow handlers are registered yet — this project's purpose is
the reliability engine, not any specific workflow's business logic. Real
handlers get added to the registry below as they're built.
"""

import asyncio
import logging
import signal
import sys
from uuid import uuid4

from axiom.config import settings
from axiom.db import close_pool, get_pool, init_pool
from axiom.redis_client import close_redis, get_redis, init_redis
from axiom.worker.runner import HandlerRegistry, run_forever

HANDLERS: HandlerRegistry = {}


async def _main(stream_version: str) -> None:
    await init_pool()
    await init_redis()

    shutdown_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, shutdown_event.set)

    worker_id = uuid4()

    try:
        await run_forever(
            get_pool(),
            get_redis(),
            stream_name=f"workflow_stream_{stream_version}",
            consumer_name=f"worker-{worker_id}",
            worker_id=worker_id,
            handlers=HANDLERS,
            lease_seconds=settings.worker_lease_seconds,
            heartbeat_interval_seconds=settings.worker_heartbeat_interval_seconds,
            xautoclaim_min_idle_seconds=settings.worker_xautoclaim_min_idle_seconds,
            max_retries=settings.worker_max_retries,
            batch_size=settings.worker_batch_size,
            shutdown_event=shutdown_event,
        )
    finally:
        await close_redis()
        await close_pool()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("usage: python -m axiom.worker <stream_version>  (e.g. v1)", file=sys.stderr)
        sys.exit(1)

    logging.basicConfig(level=settings.log_level)
    asyncio.run(_main(sys.argv[1]))