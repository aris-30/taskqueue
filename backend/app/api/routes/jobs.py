import logging
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func
from redis.asyncio import Redis
from python_ulid import ULID

from app.dependencies import get_db, get_redis, verify_api_key
from app.models.job import Job, JobStatus
from app.schemas.job import JobCreate, JobResponse, JobListResponse, JobFilter
from app.core.queue import RedisQueue
from app.core.events import event_bus

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/jobs", tags=["jobs"])


@router.post(
    "",
    response_model=JobResponse,
    status_code=status.HTTP_201_CREATED,
    dependencies=[Depends(verify_api_key)],
)
async def create_job(
    body: JobCreate,
    db: AsyncSession = Depends(get_db),
    redis: Redis = Depends(get_redis),
) -> JobResponse:
    """Submit a new job to the queue."""
    job_id = str(ULID())

    # Persist to PostgreSQL first so we have a record regardless of Redis state
    job = Job(
        id=job_id,
        task_name=body.task_name,
        queue=body.queue,
        priority=body.priority,
        payload=body.payload,
        max_retries=body.max_retries,
        scheduled_at=body.scheduled_at,
        status=JobStatus.PENDING,
    )
    db.add(job)
    await db.flush()

    # Then push to Redis queue
    queue = RedisQueue(redis)
    await queue.enqueue(
        job_id=job_id,
        task_name=body.task_name,
        payload=body.payload,
        queue=body.queue,
        priority=body.priority.value,
        max_retries=body.max_retries,
        scheduled_at=body.scheduled_at,
    )

    # Update status to queued
    job.status = JobStatus.QUEUED
    await db.commit()
    await db.refresh(job)

    # Broadcast to dashboard
    await event_bus.publish({
        "event": "job.created",
        "job_id": job_id,
        "task_name": body.task_name,
        "queue": body.queue,
        "priority": body.priority.value,
        "status": "queued",
    })

    logger.info(f"Created job {job_id} ({body.task_name})")
    return JobResponse.model_validate(job)


@router.get(
    "",
    response_model=JobListResponse,
    dependencies=[Depends(verify_api_key)],
)
async def list_jobs(
    db: AsyncSession = Depends(get_db),
    status: JobStatus | None = Query(default=None),
    queue: str | None = Query(default=None),
    task_name: str | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
) -> JobListResponse:
    """List jobs with optional filtering and pagination."""
    query = select(Job)

    if status:
        query = query.where(Job.status == status)
    if queue:
        query = query.where(Job.queue == queue)
    if task_name:
        query = query.where(Job.task_name == task_name)

    # Get total count
    count_query = select(func.count()).select_from(query.subquery())
    total = await db.scalar(count_query)

    # Apply pagination
    query = query.order_by(Job.created_at.desc())
    query = query.offset((page - 1) * page_size).limit(page_size)

    result = await db.execute(query)
    jobs = result.scalars().all()

    return JobListResponse(
        jobs=[JobResponse.model_validate(j) for j in jobs],
        total=total or 0,
        page=page,
        page_size=page_size,
    )


@router.get(
    "/{job_id}",
    response_model=JobResponse,
    dependencies=[Depends(verify_api_key)],
)
async def get_job(
    job_id: str,
    db: AsyncSession = Depends(get_db),
) -> JobResponse:
    """Get a single job by ID."""
    job = await db.get(Job, job_id)
    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job {job_id} not found",
        )
    return JobResponse.model_validate(job)


@router.delete(
    "/{job_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    dependencies=[Depends(verify_api_key)],
)
async def cancel_job(
    job_id: str,
    db: AsyncSession = Depends(get_db),
    redis: Redis = Depends(get_redis),
) -> None:
    """Cancel a pending job. Has no effect if the job is already running."""
    job = await db.get(Job, job_id)
    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job {job_id} not found",
        )

    if job.status not in (JobStatus.PENDING, JobStatus.QUEUED):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Cannot cancel job with status '{job.status}'",
        )

    queue = RedisQueue(redis)
    await queue.cancel(job_id, job.queue)

    job.status = JobStatus.CANCELLED
    await db.commit()

    await event_bus.publish({
        "event": "job.cancelled",
        "job_id": job_id,
    })