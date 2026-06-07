import enum
from datetime import datetime
from sqlalchemy import String, Integer, Float, DateTime, Enum, Text, JSON
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func

from app.db import Base


class JobStatus(str, enum.Enum):
    PENDING = "pending"
    QUEUED = "queued"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    RETRYING = "retrying"
    CANCELLED = "cancelled"
    DEAD = "dead"


class JobPriority(str, enum.Enum):
    CRITICAL = "critical"
    HIGH = "high"
    DEFAULT = "default"
    LOW = "low"


class Job(Base):
    __tablename__ = "jobs"

    id: Mapped[str] = mapped_column(String(26), primary_key=True)
    task_name: Mapped[str] = mapped_column(String(255), nullable=False)
    queue: Mapped[str] = mapped_column(String(100), default="default")
    priority: Mapped[str] = mapped_column(
        Enum(JobPriority), default=JobPriority.DEFAULT
    )
    status: Mapped[str] = mapped_column(
        Enum(JobStatus), default=JobStatus.PENDING, index=True
    )

    # The actual payload the worker will receive
    payload: Mapped[dict] = mapped_column(JSON, default=dict)

    # Result or error from execution
    result: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)

    # Retry tracking
    retries: Mapped[int] = mapped_column(Integer, default=0)
    max_retries: Mapped[int] = mapped_column(Integer, default=3)

    # Timing
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    scheduled_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    started_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    finished_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    # Worker that picked it up
    worker_id: Mapped[str | None] = mapped_column(String(100), nullable=True)

    # Execution time in seconds
    execution_time: Mapped[float | None] = mapped_column(Float, nullable=True)

    def __repr__(self) -> str:
        return f"<Job id={self.id} task={self.task_name} status={self.status}>"