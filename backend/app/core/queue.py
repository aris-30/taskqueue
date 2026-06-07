import json
import time
import logging
from datetime import datetime, timezone
from typing import Any

from redis.asyncio import Redis

from app.config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()

# Redis key patterns - all queue data lives under these namespaces
QUEUE_KEY = "tq:queue:{name}"           # List - pending jobs ready to run
DELAYED_KEY = "tq:delayed"              # Sorted set - jobs waiting for their scheduled time
DEAD_KEY = "tq:dead:{name}"            # List - jobs that exceeded max retries
PROCESSING_KEY = "tq:processing:{name}" # List - jobs currently being worked on
JOB_KEY = "tq:job:{job_id}"            # Hash - full job data snapshot in Redis
STATS_KEY = "tq:stats:{name}"          # Hash - running counters per queue


class RedisQueue:
    """
    Core queue engine. Wraps Redis with task-queue semantics:
    priority queues, delayed jobs, dead-letter routing, and atomic
    job handoff so jobs are never lost if a worker crashes.
    """

    def __init__(self, redis: Redis):
        self.redis = redis

    # ------------------------------------------------------------------
    # Enqueue
    # ------------------------------------------------------------------

    async def enqueue(
        self,
        job_id: str,
        task_name: str,
        payload: dict[str, Any],
        queue: str = "default",
        priority: str = "default",
        max_retries: int = 3,
        scheduled_at: datetime | None = None,
    ) -> str:
        job_data = {
            "id": job_id,
            "task_name": task_name,
            "payload": json.dumps(payload),
            "queue": queue,
            "priority": priority,
            "max_retries": max_retries,
            "retries": 0,
            "status": "queued",
            "enqueued_at": time.time(),
        }

        # Store job snapshot in Redis for fast lookups
        await self.redis.hset(JOB_KEY.format(job_id=job_id), mapping=job_data)
        await self.redis.expire(JOB_KEY.format(job_id=job_id), 86400 * 7)  # 7 days TTL

        if scheduled_at:
            # Delayed job: goes into a sorted set scored by unix timestamp
            # The scheduler process promotes these to the main queue when their time comes
            run_at = scheduled_at.timestamp()
            await self.redis.zadd(
                DELAYED_KEY,
                {json.dumps({"job_id": job_id, "queue": queue}): run_at}
            )
            logger.info(f"Job {job_id} scheduled for {scheduled_at}")
        else:
            # Immediate job: push to the front of the priority queue
            # We use LPUSH so highest priority queues get checked first
            await self.redis.lpush(QUEUE_KEY.format(name=queue), job_id)
            await self.redis.hincrby(STATS_KEY.format(name=queue), "enqueued", 1)
            logger.info(f"Job {job_id} enqueued to {queue}")

        return job_id

    # ------------------------------------------------------------------
    # Dequeue (called by workers)
    # ------------------------------------------------------------------

    async def dequeue(self, queues: list[str], timeout: int = 5) -> dict | None:
        """
        Blocking pop across multiple queues in priority order.
        BRPOPLPUSH atomically moves the job from the queue list to a
        processing list — if the worker crashes, we can recover it.
        """
        for queue_name in queues:
            job_id = await self.redis.rpoplpush(
                QUEUE_KEY.format(name=queue_name),
                PROCESSING_KEY.format(name=queue_name),
            )
            if job_id:
                job_data = await self.redis.hgetall(JOB_KEY.format(job_id=job_id))
                if job_data:
                    job_data["payload"] = json.loads(job_data.get("payload", "{}"))
                    job_data["_processing_queue"] = queue_name
                    return job_data

        # Nothing in any queue right now — do a blocking wait on the default queue
        result = await self.redis.brpoplpush(
            QUEUE_KEY.format(name=queues[-1]),
            PROCESSING_KEY.format(name=queues[-1]),
            timeout=timeout,
        )
        if result:
            job_data = await self.redis.hgetall(JOB_KEY.format(job_id=result))
            if job_data:
                job_data["payload"] = json.loads(job_data.get("payload", "{}"))
                job_data["_processing_queue"] = queues[-1]
                return job_data

        return None

    # ------------------------------------------------------------------
    # Acknowledge (job finished successfully)
    # ------------------------------------------------------------------

    async def acknowledge(self, job_id: str, queue: str, result: dict) -> None:
        """Remove job from processing list and record success."""
        await self.redis.lrem(PROCESSING_KEY.format(name=queue), 1, job_id)
        await self.redis.hset(
            JOB_KEY.format(job_id=job_id),
            mapping={
                "status": "success",
                "result": json.dumps(result),
                "finished_at": time.time(),
            }
        )
        await self.redis.hincrby(STATS_KEY.format(name=queue), "succeeded", 1)
        logger.info(f"Job {job_id} acknowledged as success")

    # ------------------------------------------------------------------
    # Fail (job errored — decide retry or dead-letter)
    # ------------------------------------------------------------------

    async def fail(self, job_id: str, queue: str, error: str, retries: int) -> str:
        """
        Called when a job throws an exception.
        If retries remain: requeue with exponential backoff delay.
        If no retries remain: route to dead-letter queue.
        Returns 'retrying' or 'dead'.
        """
        await self.redis.lrem(PROCESSING_KEY.format(name=queue), 1, job_id)
        max_retries = int(
            (await self.redis.hget(JOB_KEY.format(job_id=job_id), "max_retries")) or 3
        )

        if retries < max_retries:
            # Exponential backoff: 2^retries seconds (2s, 4s, 8s, ...)
            delay = settings.retry_backoff_base ** retries
            run_at = time.time() + delay
            await self.redis.zadd(
                DELAYED_KEY,
                {json.dumps({"job_id": job_id, "queue": queue}): run_at}
            )
            await self.redis.hset(
                JOB_KEY.format(job_id=job_id),
                mapping={
                    "status": "retrying",
                    "error": error,
                    "retries": retries + 1,
                }
            )
            await self.redis.hincrby(STATS_KEY.format(name=queue), "retried", 1)
            logger.warning(f"Job {job_id} failed, retrying in {delay}s (attempt {retries + 1}/{max_retries})")
            return "retrying"
        else:
            # No retries left — send to dead-letter queue
            await self.redis.lpush(DEAD_KEY.format(name=queue), job_id)
            await self.redis.hset(
                JOB_KEY.format(job_id=job_id),
                mapping={
                    "status": "dead",
                    "error": error,
                }
            )
            await self.redis.hincrby(STATS_KEY.format(name=queue), "dead", 1)
            logger.error(f"Job {job_id} exceeded max retries, moved to dead-letter queue")
            return "dead"

    # ------------------------------------------------------------------
    # Cancel
    # ------------------------------------------------------------------

    async def cancel(self, job_id: str, queue: str) -> bool:
        """Remove a pending job before it gets picked up by a worker."""
        removed = await self.redis.lrem(QUEUE_KEY.format(name=queue), 1, job_id)
        if removed:
            await self.redis.hset(
                JOB_KEY.format(job_id=job_id),
                mapping={"status": "cancelled"}
            )
            logger.info(f"Job {job_id} cancelled")
            return True
        return False

    # ------------------------------------------------------------------
    # Stats
    # ------------------------------------------------------------------

    async def get_queue_stats(self, queue_name: str) -> dict:
        """Return current depth and counters for a queue."""
        pending = await self.redis.llen(QUEUE_KEY.format(name=queue_name))
        delayed = await self.redis.zcount(DELAYED_KEY, "-inf", "+inf")
        dead = await self.redis.llen(DEAD_KEY.format(name=queue_name))
        stats = await self.redis.hgetall(STATS_KEY.format(name=queue_name))

        return {
            "name": queue_name,
            "pending": pending,
            "delayed": delayed,
            "dead": dead,
            "processed_total": int(stats.get("succeeded", 0)),
            "failed_total": int(stats.get("dead", 0)),
        }

    async def get_job(self, job_id: str) -> dict | None:
        """Fetch a job snapshot from Redis."""
        data = await self.redis.hgetall(JOB_KEY.format(job_id=job_id))
        if not data:
            return None
        if "payload" in data:
            data["payload"] = json.loads(data["payload"])
        return data