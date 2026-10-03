from __future__ import annotations

import asyncio
import time
from datetime import datetime, timedelta, timezone


class Clock:
    """Wall clock. ``sleep`` takes real seconds."""

    simulated = False

    def now(self) -> datetime:
        return datetime.now(timezone.utc)

    async def sleep(self, seconds: float) -> None:
        await asyncio.sleep(seconds)

    def real_seconds(self, sim_seconds: float) -> float:
        return sim_seconds


class SimClock(Clock):
    """Accelerated clock for the simulator: sim time advances ``speed`` x real time."""

    simulated = True

    def __init__(self, start: datetime, speed: float):
        self.speed = speed
        self._base_sim = start
        self._base_real = time.monotonic()

    def now(self) -> datetime:
        return self._base_sim + timedelta(seconds=(time.monotonic() - self._base_real) * self.speed)

    def jump(self, to: datetime) -> None:
        self._base_sim = to
        self._base_real = time.monotonic()

    def real_seconds(self, sim_seconds: float) -> float:
        return sim_seconds / self.speed
