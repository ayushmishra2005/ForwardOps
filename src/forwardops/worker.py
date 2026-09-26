import asyncio
import logging
import os
import signal
from uuid import uuid4

from forwardops.application.investigate import run_claimed
from forwardops.config import Settings, load_settings
from forwardops.logging import configure_logging
from forwardops.runtime import Runtime, build_runtime
from forwardops.storage.leases import claim_next, open_pool
from forwardops.storage.migrate import apply_migrations
from forwardops.storage.postgres import Database

logger = logging.getLogger(__name__)


async def process_one(
    pool,
    runtime: Runtime,
    *,
    suspend_after_new_tool_calls: int | None = None,
    owner: str | None = None,
) -> bool:
    worker_id = owner or f"{os.uname().nodename}:{os.getpid()}"
    claim = await claim_next(pool, worker_id, runtime.settings.lease_seconds)
    if claim is None:
        return False
    await run_claimed(
        Database(pool),
        runtime,
        claim,
        suspend_after_new_tool_calls=suspend_after_new_tool_calls,
        trace_id=str(uuid4()),
    )
    return True


async def run_worker(settings: Settings, stop: asyncio.Event | None = None) -> None:
    apply_migrations(
        settings.migration_database_url, settings.migrations_dir, settings.app_password
    )
    runtime = build_runtime(settings)
    pool = await open_pool(settings.database_url)
    try:
        while stop is None or not stop.is_set():
            worked = await process_one(pool, runtime)
            if not worked:
                try:
                    if stop is None:
                        await asyncio.sleep(settings.poll_seconds)
                    else:
                        await asyncio.wait_for(stop.wait(), timeout=settings.poll_seconds)
                except TimeoutError:
                    continue
    finally:
        customer_db = runtime.handlers.customer_db
        if customer_db is not None:
            await customer_db.close()
        await pool.close()


def main() -> None:
    configure_logging()
    settings = load_settings()
    stop = asyncio.Event()

    def _stop(*_args: object) -> None:
        stop.set()

    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)
    logger.info("worker starting")
    asyncio.run(run_worker(settings, stop))
