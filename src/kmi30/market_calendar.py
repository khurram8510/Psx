"""PSX trading-session calendar in Pakistan time."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo

from .config import MarketConfig

_WEEKDAYS = ("mon", "tue", "wed", "thu", "fri", "sat", "sun")


@dataclass(frozen=True)
class Session:
    start: datetime
    end: datetime


class MarketCalendar:
    def __init__(self, cfg: MarketConfig):
        self.tz = ZoneInfo(cfg.timezone)
        self.grace = timedelta(minutes=cfg.close_grace_min)
        self.holidays = {date.fromisoformat(d) for d in cfg.holidays}
        self._sessions = {
            day: [(time.fromisoformat(a), time.fromisoformat(b)) for a, b in windows]
            for day, windows in cfg.sessions.items()
        }

    def local(self, dt: datetime) -> datetime:
        return dt.astimezone(self.tz)

    def sessions_on(self, d: date) -> list[Session]:
        if d in self.holidays:
            return []
        windows = self._sessions.get(_WEEKDAYS[d.weekday()], [])
        return [
            Session(datetime.combine(d, a, self.tz), datetime.combine(d, b, self.tz))
            for a, b in windows
        ]

    def current_session(self, now: datetime, with_grace: bool = False) -> Session | None:
        local = self.local(now)
        pad = self.grace if with_grace else timedelta(0)
        for s in self.sessions_on(local.date()):
            if s.start <= local < s.end + pad:
                return s
        return None

    def is_open(self, now: datetime, with_grace: bool = False) -> bool:
        return self.current_session(now, with_grace) is not None

    def is_trading_day(self, d: date) -> bool:
        return bool(self.sessions_on(d))

    def first_open(self, d: date) -> datetime | None:
        s = self.sessions_on(d)
        return s[0].start if s else None

    def session_minutes(self, d: date | None = None) -> float:
        """Total scheduled minutes for a day, or the typical Mon-Thu day when ``d`` is None."""
        if d is None:
            d = date(2024, 1, 1)  # a Monday
        return sum((s.end - s.start).total_seconds() / 60 for s in self.sessions_on(d)) or 360.0

    def next_open(self, now: datetime) -> datetime | None:
        local = self.local(now)
        for offset in range(0, 15):
            d = local.date() + timedelta(days=offset)
            for s in self.sessions_on(d):
                if s.start > local:
                    return s.start
        return None

    def in_warmup(self, now: datetime, warmup_min: int) -> bool:
        s = self.current_session(now, with_grace=True)
        if s is None:
            return False
        return self.local(now) < s.start + timedelta(minutes=warmup_min)
