from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import IntEnum


class Severity(IntEnum):
    INFO = 1
    WARNING = 2
    CRITICAL = 3

    @property
    def label(self) -> str:
        return self.name.lower()


@dataclass(frozen=True, slots=True)
class Tick:
    """One index observation. ``ts`` is true UTC epoch seconds."""

    ts: int
    value: float
    volume: float = 0.0
    source: str = "intraday"
    # Day-level figures published alongside the value on the indices page, kept for history.
    high: float | None = None
    low: float | None = None
    change: float | None = None
    change_pct: float | None = None

    @property
    def dt(self) -> datetime:
        return datetime.fromtimestamp(self.ts, tz=timezone.utc)


@dataclass(slots=True)
class DaySummary:
    """One trading day of KMI-30, maintained live during the session."""

    day: str  # YYYY-MM-DD, Pakistan time
    open: float
    high: float
    low: float
    close: float
    prev_close: float | None
    readings: int
    alerts: int
    first_ts: int
    last_ts: int

    @property
    def change(self) -> float | None:
        return self.close - self.prev_close if self.prev_close else None

    @property
    def change_pct(self) -> float | None:
        return (self.close / self.prev_close - 1) * 100 if self.prev_close else None

    def to_dict(self) -> dict:
        return {
            "day": self.day, "open": self.open, "high": self.high, "low": self.low, "close": self.close,
            "prev_close": self.prev_close, "change": self.change, "change_pct": self.change_pct,
            "readings": self.readings, "alerts": self.alerts, "first_ts": self.first_ts, "last_ts": self.last_ts,
        }


@dataclass(slots=True)
class Bar:
    """One-minute OHLC bar. ``ts`` is the UTC epoch of the minute start."""

    ts: int
    open: float
    high: float
    low: float
    close: float
    volume: float = 0.0
    ticks: int = 1

    def update(self, tick: Tick) -> None:
        self.high = max(self.high, tick.value)
        self.low = min(self.low, tick.value)
        self.close = tick.value
        self.volume += tick.volume
        self.ticks += 1


@dataclass(slots=True)
class Alert:
    detector: str
    key: str
    severity: Severity
    title: str
    detail: str
    ts: int
    value: float
    metrics: dict[str, float] = field(default_factory=dict)
    direction: int = 0  # +1 up, -1 down, 0 non-directional

    def to_dict(self) -> dict:
        return {
            "detector": self.detector,
            "key": self.key,
            "severity": self.severity.label,
            "title": self.title,
            "detail": self.detail,
            "ts": self.ts,
            "value": self.value,
            "metrics": self.metrics,
            "direction": self.direction,
        }
