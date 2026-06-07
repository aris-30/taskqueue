import asyncio
import json
import logging
from typing import AsyncGenerator

logger = logging.getLogger(__name__)


class EventBus:
    """
    Simple in-process pub/sub event bus.
    Workers publish job state changes, WebSocket connections subscribe
    and stream them to the dashboard in real time.
    """

    def __init__(self):
        self._subscribers: list[asyncio.Queue] = []

    async def publish(self, event: dict) -> None:
        """Broadcast an event to all active WebSocket subscribers."""
        if not self._subscribers:
            return

        message = json.dumps(event)
        dead = []

        for queue in self._subscribers:
            try:
                queue.put_nowait(message)
            except asyncio.QueueFull:
                # Subscriber is too slow, drop them
                dead.append(queue)

        for queue in dead:
            self._subscribers.remove(queue)
            logger.warning("Dropped slow WebSocket subscriber")

    def subscribe(self) -> asyncio.Queue:
        """Register a new subscriber. Returns a queue to read events from."""
        queue: asyncio.Queue = asyncio.Queue(maxsize=100)
        self._subscribers.append(queue)
        logger.info(f"New WebSocket subscriber ({len(self._subscribers)} total)")
        return queue

    def unsubscribe(self, queue: asyncio.Queue) -> None:
        """Remove a subscriber when their WebSocket disconnects."""
        if queue in self._subscribers:
            self._subscribers.remove(queue)
            logger.info(f"WebSocket subscriber removed ({len(self._subscribers)} remaining)")

    async def stream(self, queue: asyncio.Queue) -> AsyncGenerator[str, None]:
        """Async generator that yields events as they arrive."""
        try:
            while True:
                message = await queue.get()
                yield message
        except asyncio.CancelledError:
            pass
        finally:
            self.unsubscribe(queue)


# Global singleton — imported by workers and WebSocket route
event_bus = EventBus()