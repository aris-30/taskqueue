import logging

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from app.core.events import event_bus

logger = logging.getLogger(__name__)
router = APIRouter(tags=["websocket"])


@router.websocket("/ws/jobs")
async def job_stream(websocket: WebSocket) -> None:
    """
    WebSocket endpoint. Streams live job events to the dashboard.
    Every time a job changes state anywhere in the system, the dashboard
    gets a push — no polling needed.
    """
    await websocket.accept()
    logger.info("WebSocket client connected")

    queue = event_bus.subscribe()

    try:
        async for message in event_bus.stream(queue):
            await websocket.send_text(message)
    except WebSocketDisconnect:
        logger.info("WebSocket client disconnected")
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
    finally:
        event_bus.unsubscribe(queue)