# TaskQueue — Distributed Task Queue Engine

A production-grade distributed task queue built from scratch in Python. Inspired by Celery and BullMQ, but built with modern async Python (FastAPI + asyncio) so every design decision is visible and explainable.

## What it does

- Clients submit jobs via a REST API
- Jobs are queued in Redis with priority levels (critical → high → default → low)
- A pool of async workers picks up jobs concurrently and executes them
- Failed jobs are automatically retried with exponential backoff
- Jobs that exceed max retries are routed to a dead-letter queue for inspection
- All job history and state is persisted to PostgreSQL
- A real-time WebSocket stream pushes live job updates to a React dashboard
- Prometheus metrics expose queue depth, throughput, and worker latency

## Tech stack

| Layer | Technology |
|---|---|
| API | FastAPI (Python 3.11) |
| Queue broker | Redis 7 |
| Database | PostgreSQL 16 + SQLAlchemy 2.0 (async) |
| Workers | Python asyncio worker pool |
| Migrations | Alembic |
| Metrics | Prometheus + custom instrumentation |
| Dashboard | React + TypeScript + Recharts |
| Infra | Docker Compose |

## Architecture

Clients hit the FastAPI REST layer, which pushes jobs into Redis priority queues. A pool of async Python workers pulls from those queues, executes jobs, and writes results to PostgreSQL. Failed jobs retry with exponential backoff and eventually land in a dead-letter queue. A WebSocket connection streams live job events to the React dashboard.

## Running locally

**Prerequisites:** Docker Desktop, that's it.

```bash
git clone https://github.com/aris-30/taskqueue.git
cd taskqueue
cp .env.example .env
docker compose up --build
```

Services will be available at:

| Service | URL |
|---|---|
| REST API | http://localhost:8000 |
| API Docs (Swagger) | http://localhost:8000/docs |
| Dashboard | http://localhost:3000 |
| Prometheus | http://localhost:9090 |

## API usage

**Submit a job:**
```bash
curl -X POST http://localhost:8000/jobs \
  -H "Content-Type: application/json" \
  -H "X-API-Key: dev-api-key-change-me" \
  -d '{
    "task_name": "send_email",
    "payload": {"to": "user@example.com", "subject": "Hello"},
    "priority": "high"
  }'
```

**Check job status:**
```bash
curl http://localhost:8000/jobs/{job_id} \
  -H "X-API-Key: dev-api-key-change-me"
```

**List all jobs:**
```bash
curl http://localhost:8000/jobs \
  -H "X-API-Key: dev-api-key-change-me"
```

## Key design decisions

**Why Redis for the queue?** Redis's atomic list operations (`BRPOPLPUSH`) make it ideal for reliable job handoff — a job is never lost between being dequeued and being acknowledged, even if a worker crashes mid-execution.

**Why asyncio workers instead of threads?** Most real-world jobs are I/O bound (HTTP calls, database queries). Async workers handle hundreds of concurrent jobs on a single process with far less memory overhead than threading.

**Why PostgreSQL for job history?** Redis is ephemeral and not designed for rich queries. Persisting job state to PostgreSQL lets us query history, debug failures, and build the dashboard without hammering Redis.

**How does retry backoff work?** Each retry delay is calculated as `backoff_base ^ retry_count` seconds (e.g. 2s, 4s, 8s). Failed jobs are re-inserted into Redis as delayed jobs using a sorted set scored by their next-run timestamp.

## Project structure

The backend lives in `/backend/app` and is split into `api/routes` for REST endpoints, `core` for the queue engine and worker pool, `models` for SQLAlchemy models, and `schemas` for Pydantic validation. The React dashboard lives in `/dashboard`.

## Running tests

```bash
docker compose run --rm api pytest tests/ -v --cov=app
```