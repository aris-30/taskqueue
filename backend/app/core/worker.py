import asyncio
import logging
import time
import uuid
from typing import Callable

from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.queue import RedisQueue
from app.config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()

# Registry of task functions — workers look up task_name here to know what to run
TASK_REGISTRY: dict[str, Callable] = {}


def task(name: str):
    """
    Decorator to register a function as a task.
    Usage:
        @task("send_email")
        async def send_email(payload): ...
    """
    def decorator(func: Callable) -> Callable:
        TASK_REGISTRY[name] = func
        logger.info(f"Registered task: {name}")
        return func
    return decorator


class AsyncWorker:
    """
    A single async worker. Pulls jobs from Redis, executes them,
    and reports success or failure back to the queue engine.
    """

    def __init__(self, worker_id: str, queue: RedisQueue, db_session: AsyncSession):
        self.worker_id = worker_id
        self.queue = queue
        self.db = db_session
        self.running = False
        self.current_job_id: str | None = None

    async def run(self) -> None:
        self.running = True
        logger.info(f"Worker {self.worker_id} started")

        while self.running:
            try:
                await self._process_next()
            except asyncio.CancelledError:
                logger.info(f"Worker {self.worker_id} cancelled")
                break
            except Exception as e:
                logger.error(f"Worker {self.worker_id} unexpected error: {e}")
                await asyncio.sleep(1)

    async def _process_next(self) -> None:
        # Try to pull a job from queues in priority order
        job_data = await self.queue.dequeue(
            queues=settings.queue_list,
            timeout=5,
        )

        if not job_data:
            return  # Nothing in any queue, loop back and try again

        job_id = job_data["id"]
        task_name = job_data["task_name"]
        payload = job_data["payload"]
        queue_name = job_data["_processing_queue"]
        retries = int(job_data.get("retries", 0))

        self.current_job_id = job_id
        started_at = time.time()

        logger.info(f"Worker {self.worker_id} picked up job {job_id} ({task_name})")

        # Update job status in PostgreSQL
        await self._update_job_db(job_id, {
            "status": "running",
            "worker_id": self.worker_id,
            "started_at": started_at,
        })

        try:
            # Look up the registered task function
            task_fn = TASK_REGISTRY.get(task_name)
            if not task_fn:
                raise ValueError(f"No task registered with name '{task_name}'")

            # Execute with timeout
            result = await asyncio.wait_for(
                task_fn(payload),
                timeout=settings.job_timeout_seconds,
            )

            execution_time = time.time() - started_at

            # Mark as success
            await self.queue.acknowledge(job_id, queue_name, result or {})
            await self._update_job_db(job_id, {
                "status": "success",
                "result": result or {},
                "finished_at": time.time(),
                "execution_time": execution_time,
            })
            logger.info(f"Job {job_id} completed in {execution_time:.2f}s")

        except asyncio.TimeoutError:
            error = f"Job timed out after {settings.job_timeout_seconds}s"
            logger.error(f"Job {job_id} timed out")
            new_status = await self.queue.fail(job_id, queue_name, error, retries)
            await self._update_job_db(job_id, {
                "status": new_status,
                "error": error,
                "finished_at": time.time(),
            })

        except Exception as e:
            error = str(e)
            logger.error(f"Job {job_id} failed: {error}")
            new_status = await self.queue.fail(job_id, queue_name, error, retries)
            await self._update_job_db(job_id, {
                "status": new_status,
                "error": error,
                "retries": retries + 1,
                "finished_at": time.time(),
            })

        finally:
            self.current_job_id = None

    async def _update_job_db(self, job_id: str, fields: dict) -> None:
        """Persist job state changes to PostgreSQL."""
        from sqlalchemy import update
        from app.models.job import Job
        from datetime import datetime, timezone

        # Convert unix timestamps to datetime objects for PostgreSQL
        for key in ("started_at", "finished_at"):
            if key in fields and isinstance(fields[key], float):
                fields[key] = datetime.fromtimestamp(fields[key], tz=timezone.utc)

        try:
            await self.db.execute(
                update(Job).where(Job.id == job_id).values(**fields)
            )
            await self.db.commit()
        except Exception as e:
            logger.error(f"Failed to update job {job_id} in DB: {e}")
            await self.db.rollback()

    async def stop(self) -> None:
        self.running = False


class WorkerPool:
    """
    Manages N concurrent workers. Starts them all as asyncio tasks
    so they run truly concurrently without threads.
    """

    def __init__(self, redis: Redis, db_session: AsyncSession, concurrency: int):
        self.redis = redis
        self.db = db_session
        self.concurrency = concurrency
        self.workers: list[AsyncWorker] = []
        self._tasks: list[asyncio.Task] = []

    async def start(self) -> None:
        logger.info(f"Starting worker pool with {self.concurrency} workers")
        queue = RedisQueue(self.redis)

        for i in range(self.concurrency):
            worker_id = f"worker-{uuid.uuid4().hex[:8]}"
            worker = AsyncWorker(worker_id, queue, self.db)
            self.workers.append(worker)
            task = asyncio.create_task(worker.run(), name=worker_id)
            self._tasks.append(task)

    async def stop(self) -> None:
        logger.info("Shutting down worker pool")
        for worker in self.workers:
            await worker.stop()
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)

    def active_count(self) -> int:
        return sum(1 for w in self.workers if w.current_job_id is not None)