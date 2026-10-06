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

    @property
    def dt(self) -> datetime:
        return datetime.fromtimestamp(self.ts, tz=timezone.utc)


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
