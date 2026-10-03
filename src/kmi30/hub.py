"""In-memory live state for the dashboard plus WebSocket fan-out."""
from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any

from fastapi import WebSocket

from .models import Alert, Bar, Tick

log = logging.getLogger(__name__)


def _bar(b: Bar) -> dict:
    return {"ts": b.ts, "o": b.open, "h": b.high, "l": b.low, "c": b.close, "v": b.volume}


class Hub:
    def __init__(self, tz_offset_s: int, simulated: bool):
        self.clients: set[WebSocket] = set()
        self.tz_offset_s = tz_offset_s
        self.simulated = simulated
        self.day: str | None = None
        self.prev_close: float | None = None
        self.bars: list[dict] = []
        self.current_bar: dict | None = None
        self.last: dict | None = None
        self.high: float | None = None
        self.low: float | None = None
        self.alerts: list[dict] = []
        self.status: dict[str, Any] = {}
        self.heartbeat = time.monotonic()  # bumped by every agent cycle; drives liveness

    def reset_day(self, day: str, prev_close: float | None, alerts: list[Alert], bars: list[Bar]) -> None:
        self.day = day
        self.prev_close = prev_close
        self.bars = [_bar(b) for b in bars]
        self.current_bar = None
        self.last = None
        self.high = max((b.high for b in bars), default=None)
        self.low = min((b.low for b in bars), default=None)
        self.alerts = [a.to_dict() for a in alerts]

    def snapshot(self) -> dict:
        return {
            "type": "snapshot",
            "day": self.day,
            "simulated": self.simulated,
            "tz_offset_s": self.tz_offset_s,
            "prev_close": self.prev_close,
            "bars": self.bars,
            "current_bar": self.current_bar,
            "last": self.last,
            "high": self.high,
            "low": self.low,
            "alerts": self.alerts[-200:],
            "status": self.status,
        }

    def on_tick(self, t: Tick, current: Bar | None) -> None:
        self.last = {"ts": t.ts, "value": t.value, "source": t.source}
        self.high = t.value if self.high is None else max(self.high, t.value)
        self.low = t.value if self.low is None else min(self.low, t.value)
        self.current_bar = _bar(current) if current else None

    def on_bar(self, b: Bar) -> None:
        self.bars.append(_bar(b))

    def on_alert(self, a: Alert) -> None:
        self.alerts.append(a.to_dict())

    async def publish(self, msg_type: str, **payload: Any) -> None:
        await self.broadcast({"type": msg_type, **payload})

    async def push_update(self, new_bars: list[Bar], new_alerts: list[Alert]) -> None:
        await self.broadcast({
            "type": "update",
            "bars": [_bar(b) for b in new_bars],
            "current_bar": self.current_bar,
            "last": self.last,
            "high": self.high,
            "low": self.low,
            "prev_close": self.prev_close,
            "alerts": [a.to_dict() for a in new_alerts],
            "status": self.status,
        })

    async def broadcast(self, msg: dict) -> None:
        if not self.clients:
            return
        data = json.dumps(msg)
        dead = []
        for ws in list(self.clients):
            try:
                await asyncio.wait_for(ws.send_text(data), timeout=5)
            except Exception:  # noqa: BLE001 - any send failure means the client is gone
                dead.append(ws)
        for ws in dead:
            self.clients.discard(ws)
