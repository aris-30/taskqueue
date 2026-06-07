from functools import lru_cache
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # Database
    database_url: str = "postgresql+asyncpg://taskqueue:taskqueue@postgres:5432/taskqueue"

    # Redis
    redis_url: str = "redis://redis:6379/0"

    # App
    secret_key: str = "dev-secret-key-change-in-production"
    api_key: str = "dev-api-key-change-me"
    environment: str = "development"

    # Worker
    worker_concurrency: int = 10
    max_retries: int = 3
    retry_backoff_base: int = 2
    job_timeout_seconds: int = 300

    # Queues (ordered by priority: first = highest)
    queue_names: str = "critical,high,default,low"

    @property
    def queue_list(self) -> list[str]:
        return [q.strip() for q in self.queue_names.split(",")]


@lru_cache
def get_settings() -> Settings:
    return Settings()