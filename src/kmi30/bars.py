"""Aggregate irregular ticks into one-minute bars."""
from __future__ import annotations

from .models import Bar, Tick


class BarBuilder:
    def __init__(self, interval_s: int = 60):
        self.interval = interval_s
        self.current: Bar | None = None

    def _bucket(self, ts: int) -> int:
        return ts - ts % self.interval

    def add(self, tick: Tick) -> list[Bar]:
        """Feed one tick; returns bars completed by it (zero or one)."""
        bucket = self._bucket(tick.ts)
        cur = self.current
        if cur is None:
            self.current = Bar(bucket, tick.value, tick.value, tick.value, tick.value, tick.volume)
            return []
        if bucket < cur.ts:
            return []  # late tick for an already-closed minute
        if bucket == cur.ts:
            cur.update(tick)
            return []
        self.current = Bar(bucket, tick.value, tick.value, tick.value, tick.value, tick.volume)
        return [cur]

    def flush(self) -> Bar | None:
        cur, self.current = self.current, None
        return cur
