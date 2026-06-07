import asyncio
import logging
import signal

import structlog

from app.config import get_settings
from app.core.worker import WorkerPool, task
from app.db import get_redis_client, AsyncSessionLocal

structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.add_log_level,
        structlog.processors.JSONRenderer(),
    ]
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
settings = get_settings()


# ------------------------------------------------------------------
# Register your tasks here
# Any function decorated with @task will be available to workers
# ------------------------------------------------------------------

@task("send_email")
async def send_email(payload: dict) -> dict:
    """Simulate sending an email."""
    await asyncio.sleep(0.5)
    logger.info(f"Sent email to {payload.get('to')}")
    return {"sent": True, "to": payload.get("to")}


@task("process_image")
async def process_image(payload: dict) -> dict:
    """Simulate image processing."""
    await asyncio.sleep(1.0)
    logger.info(f"Processed image {payload.get('image_id')}")
    return {"processed": True, "image_id": payload.get("image_id")}


@task("generate_report")
async def generate_report(payload: dict) -> dict:
    """Simulate report generation."""
    await asyncio.sleep(2.0)
    logger.info(f"Generated report {payload.get('report_id')}")
    return {"generated": True, "report_id": payload.get("report_id")}


@task("send_webhook")
async def send_webhook(payload: dict) -> dict:
    """Simulate sending a webhook."""
    await asyncio.sleep(0.3)
    logger.info(f"Sent webhook to {payload.get('url')}")
    return {"delivered": True, "url": payload.get("url")}


# ------------------------------------------------------------------
# Worker pool startup
# ------------------------------------------------------------------

async def main() -> None:
    logger.info(f"Starting worker pool — concurrency={settings.worker_concurrency}")

    redis = await get_redis_client()

    async with AsyncSessionLocal() as db:
        pool = WorkerPool(
            redis=redis,
            db_session=db,
            concurrency=settings.worker_concurrency,
        )

        # Handle graceful shutdown on SIGTERM / SIGINT
        loop = asyncio.get_running_loop()

        def handle_shutdown():
            logger.info("Shutdown signal received")
            asyncio.create_task(pool.stop())

        loop.add_signal_handler(signal.SIGTERM, handle_shutdown)
        loop.add_signal_handler(signal.SIGINT, handle_shutdown)

        await pool.start()

        # Keep running until all worker tasks finish
        try:
            await asyncio.gather(*pool._tasks)
        except asyncio.CancelledError:
            pass
        finally:
            await pool.stop()
            logger.info("Worker pool shut down cleanly")


if __name__ == "__main__":
    asyncio.run(main())