import logging

from fastapi import APIRouter, Depends
from redis.asyncio import Redis

from app.dependencies import get_redis, verify_api_key
from app.schemas.queue import QueueStats, QueueListResponse
from app.core.queue import RedisQueue
from app.config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()
router = APIRouter(prefix="/queues", tags=["queues"])


@router.get(
    "",
    response_model=QueueListResponse,
    dependencies=[Depends(verify_api_key)],
)
async def list_queues(
    redis: Redis = Depends(get_redis),
) -> QueueListResponse:
    """Get stats for all queues — depth, delayed, dead, throughput."""
    queue_engine = RedisQueue(redis)
    stats = []

    total_pending = 0
    total_delayed = 0
    total_dead = 0

    for queue_name in settings.queue_list:
        queue_stats = await queue_engine.get_queue_stats(queue_name)
        stats.append(QueueStats(**queue_stats))
        total_pending += queue_stats["pending"]
        total_delayed += queue_stats["delayed"]
        total_dead += queue_stats["dead"]

    # Count active workers by checking processing lists
    workers_active = 0
    for queue_name in settings.queue_list:
        processing_len = await redis.llen(f"tq:processing:{queue_name}")
        workers_active += processing_len

    return QueueListResponse(
        queues=stats,
        total_pending=total_pending,
        total_delayed=total_delayed,
        total_dead=total_dead,
        workers_active=workers_active,
    )


@router.get(
    "/{queue_name}",
    response_model=QueueStats,
    dependencies=[Depends(verify_api_key)],
)
async def get_queue(
    queue_name: str,
    redis: Redis = Depends(get_redis),
) -> QueueStats:
    """Get stats for a single queue by name."""
    if queue_name not in settings.queue_list:
        from fastapi import HTTPException, status
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Queue '{queue_name}' not found",
        )

    queue_engine = RedisQueue(redis)
    stats = await queue_engine.get_queue_stats(queue_name)
    return QueueStats(**stats)