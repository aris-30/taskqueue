from datetime import datetime
from typing import Any
from pydantic import BaseModel, Field
from app.models.job import JobStatus, JobPriority


class JobCreate(BaseModel):
    task_name: str = Field(..., description="Name of the task to execute")
    payload: dict[str, Any] = Field(default_factory=dict, description="Data passed to the worker")
    priority: JobPriority = Field(default=JobPriority.DEFAULT)
    queue: str = Field(default="default")
    max_retries: int = Field(default=3, ge=0, le=10)
    scheduled_at: datetime | None = Field(
        default=None,
        description="If set, job won't run until this time"
    )


class JobResponse(BaseModel):
    id: str
    task_name: str
    queue: str
    priority: JobPriority
    status: JobStatus
    payload: dict[str, Any]
    result: dict[str, Any] | None
    error: str | None
    retries: int
    max_retries: int
    created_at: datetime
    scheduled_at: datetime | None
    started_at: datetime | None
    finished_at: datetime | None
    worker_id: str | None
    execution_time: float | None

    model_config = {"from_attributes": True}


class JobListResponse(BaseModel):
    jobs: list[JobResponse]
    total: int
    page: int
    page_size: int


class JobFilter(BaseModel):
    status: JobStatus | None = None
    queue: str | None = None
    priority: JobPriority | None = None
    task_name: str | None = None
    page: int = Field(default=1, ge=1)
    page_size: int = Field(default=50, ge=1, le=200)