from pydantic import BaseModel


class QueueStats(BaseModel):
    name: str
    pending: int
    delayed: int
    dead: int
    processed_total: int
    failed_total: int


class QueueListResponse(BaseModel):
    queues: list[QueueStats]
    total_pending: int
    total_delayed: int
    total_dead: int
    workers_active: int