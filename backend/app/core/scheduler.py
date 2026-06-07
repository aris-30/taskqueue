import asyncio
import json
import logging
import time

from app.core.queue import RedisQueue, DELAYED_KEY, QUEUE_KEY
from app.db import get_redis_client
from app.config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()


async def promote_delayed_jobs(queue: RedisQueue) -> int:
    """
    Check the delayed sorted set for jobs whose scheduled time has passed.
    Move them to their target queue so workers can pick them up.
    Returns the number of jobs promoted.
    """
    now = time.time()

    # Get all jobs whose score (scheduled timestamp) is <= now
    due_jobs = await queue.redis.zrangebyscore(
        DELAYED_KEY,
        "-inf",
        now,
        start=0,
        num=100,  # process max 100 at a time
    )

    if not due_jobs:
        return 0

    promoted = 0
    for raw in due_jobs:
        try:
            data = json.loads(raw)
            job_id = data["job_id"]
            target_queue = data.get("queue", "default")

            # Atomically remove from delayed set and push to queue
            removed = await queue.redis.zrem(DELAYED_KEY, raw)
            if removed:
                await queue.redis.lpush(
                    QUEUE_KEY.format(name=target_queue),
                    job_id
                )
                await queue.redis.hset(
                    f"tq:job:{job_id}",
                    mapping={"status": "queued"}
                )
                promoted += 1
                logger.info(f"Promoted delayed job {job_id} to queue {target_queue}")

        except Exception as e:
            logger.error(f"Failed to promote delayed job: {e}")

    return promoted


async def run_scheduler() -> None:
    """
    Main scheduler loop. Runs every second, checks for due delayed jobs,
    and promotes them to their target queues.
    """
    logger.info("Scheduler started")
    redis = await get_redis_client()
    queue = RedisQueue(redis)

    while True:
        try:
            promoted = await promote_delayed_jobs(queue)
            if promoted:
                logger.info(f"Scheduler promoted {promoted} delayed jobs")
        except Exception as e:
            logger.error(f"Scheduler error: {e}")

        await asyncio.sleep(1)


if __name__ == "__main__":
    import structlog
    structlog.configure()
    asyncio.run(run_scheduler())