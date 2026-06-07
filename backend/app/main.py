import logging

import structlog
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config import get_settings
from app.api.routes import jobs, queues
from app.api.websocket import router as ws_router
from app.db import engine
from app.models.job import Job  # noqa: F401 - ensures model is registered

settings = get_settings()

structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.add_log_level,
        structlog.processors.JSONRenderer(),
    ]
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_app() -> FastAPI:
    app = FastAPI(
        title="TaskQueue",
        description="A distributed task queue engine built with FastAPI and Redis",
        version="0.1.0",
        docs_url="/docs",
        redoc_url="/redoc",
    )

    # Allow the React dashboard to talk to the API
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["http://localhost:3000"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Routers
    app.include_router(jobs.router)
    app.include_router(queues.router)
    app.include_router(ws_router)

    @app.get("/health", tags=["health"])
    async def health() -> dict:
        return {"status": "ok", "environment": settings.environment}

    @app.get("/metrics", tags=["metrics"])
    async def metrics() -> str:
        """Prometheus metrics endpoint."""
        from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
        from fastapi.responses import Response
        return Response(
            content=generate_latest(),
            media_type=CONTENT_TYPE_LATEST,
        )

    @app.on_event("startup")
    async def startup() -> None:
        logger.info("TaskQueue API starting up")

    @app.on_event("shutdown")
    async def shutdown() -> None:
        logger.info("TaskQueue API shutting down")
        await engine.dispose()

    return app


app = create_app()