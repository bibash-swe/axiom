"""Process entrypoint: python -m axiom.relay.

Owns the real pool/Redis connection lifecycle and OS signal handling —
deliberately kept out of runner.run_forever() so that function stays a
pure, fully-parameterized, easily-testable loop, same discipline as
run_relay_cycle itself.
"""

import asyncio
import logging
import signal
from uuid import uuid4

from axiom.config import settings
from axiom.db import close_pool, get_pool, init_pool
from axiom.redis_client import close_redis, get_redis, init_redis
from axiom.relay.runner import run_forever


async def _main() -> None:
    await init_pool()
    await init_redis()

    shutdown_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, shutdown_event.set)

    try:
        await run_forever(
            get_pool(),
            get_redis(),
            instance_id=uuid4(),
            batch_size=settings.relay_batch_size,
            claim_lease_seconds=settings.relay_claim_lease_seconds,
            max_retries=settings.relay_max_retries,
            poll_interval_seconds=settings.relay_poll_interval_seconds,
            shutdown_event=shutdown_event,
        )
    finally:
        await close_redis()
        await close_pool()


if __name__ == "__main__":
    logging.basicConfig(level=settings.log_level)
    asyncio.run(_main())