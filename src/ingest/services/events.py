from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any


@dataclass
class StatusEvent:
    type: str
    payload: dict[str, Any]
    ts: datetime = field(default_factory=lambda: datetime.now(UTC).replace(tzinfo=None))

    def to_sse(self) -> str:
        data = {"type": self.type, "payload": self.payload, "ts": self.ts.isoformat() + "Z"}
        return f"event: {self.type}\ndata: {json.dumps(data, default=str)}\n\n"


class EventHub:
    """In-process pub/sub for portal SSE live updates."""

    def __init__(self) -> None:
        self._subscribers: dict[int, asyncio.Queue[StatusEvent | None]] = {}
        self._next_id = 0
        self._lock = asyncio.Lock()

    async def publish(self, event_type: str, payload: dict[str, Any]) -> None:
        event = StatusEvent(type=event_type, payload=payload)
        async with self._lock:
            subscribers = list(self._subscribers.values())
        for queue in subscribers:
            try:
                queue.put_nowait(event)
            except asyncio.QueueFull:
                # Drop oldest by getting one, then put
                try:
                    queue.get_nowait()
                except asyncio.QueueEmpty:
                    pass
                try:
                    queue.put_nowait(event)
                except asyncio.QueueFull:
                    pass

    async def subscribe(self) -> tuple[int, asyncio.Queue[StatusEvent | None]]:
        async with self._lock:
            sid = self._next_id
            self._next_id += 1
            queue: asyncio.Queue[StatusEvent | None] = asyncio.Queue(maxsize=100)
            self._subscribers[sid] = queue
            return sid, queue

    async def unsubscribe(self, sid: int) -> None:
        async with self._lock:
            queue = self._subscribers.pop(sid, None)
        if queue is not None:
            try:
                queue.put_nowait(None)
            except asyncio.QueueFull:
                pass


_hub: EventHub | None = None


def get_event_hub() -> EventHub:
    global _hub
    if _hub is None:
        _hub = EventHub()
    return _hub


def reset_event_hub() -> None:
    global _hub
    _hub = None
