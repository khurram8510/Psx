"""Detection engine: shared volatility model, detector fan-out, warm-up and alert gating."""
from __future__ import annotations

import math
import statistics
from collections import defaultdict
from datetime import datetime, timezone

from .config import DetectionConfig
from .detectors import (
    SIGMA_FLOOR,
    Context,
    CusumChangePoint,
    Detector,
    LevelBreach,
    TrendReversal,
    VelocityShock,
    VolatilityRegime,
)
from .market_calendar import MarketCalendar
from .models import Alert, Bar, EodRow, Tick

GAP_S = 5 * 60  # bars further apart than this are treated as a session break or feed gap


class EwmaVolatility:
    """RiskMetrics-style EWMA variance of one-minute log returns."""

    def __init__(self, lam: float, seed_var: float):
        self.lam = lam
        self.var = max(seed_var, SIGMA_FLOOR**2)

    @property
    def sigma(self) -> float:
        return math.sqrt(self.var)

    def update(self, r: float) -> None:
        self.var = self.lam * self.var + (1 - self.lam) * r * r


class AlertGate:
    """Per-key cooldown. A higher severity than the last alert on the key bypasses cooldown."""

    def __init__(self, cooldown_s: int):
        self.cooldown = cooldown_s
        self.last: dict[str, tuple[int, int]] = {}

    def admit(self, a: Alert) -> bool:
        prev = self.last.get(a.key)
        if prev and a.ts - prev[0] < self.cooldown and int(a.severity) <= prev[1]:
            return False
        self.last[a.key] = (a.ts, int(a.severity))
        return True

    def restore(self, alerts: list[Alert]) -> None:
        for a in alerts:
            self.last[a.key] = (a.ts, int(a.severity))


def daily_variance_from_eod(rows: list[EodRow], lookback: int) -> float | None:
    closes = [r.close for r in rows[-(lookback + 1):]]
    if len(closes) < 6:
        return None
    rets = [math.log(b / a) for a, b in zip(closes, closes[1:]) if a > 0 and b > 0]
    return statistics.pvariance(rets) if len(rets) >= 5 else None


def variance_profile(bars_by_day: dict[str, list[Bar]], calendar: MarketCalendar, smooth: int = 2) -> dict[int, float]:
    """Mean squared one-minute return per PKT minute-of-day, smoothed across +/- ``smooth`` minutes."""
    acc: dict[int, list[float]] = defaultdict(list)
    for bars in bars_by_day.values():
        for prev, cur in zip(bars, bars[1:]):
            if cur.ts - prev.ts > GAP_S or prev.close <= 0:
                continue
            r = math.log(cur.close / prev.close)
            acc[_minute_of_day(cur.ts, calendar)].append(r * r)
    raw = {m: sum(v) / len(v) for m, v in acc.items()}
    out = {}
    for m in raw:
        neigh = [raw[k] for k in range(m - smooth, m + smooth + 1) if k in raw]
        out[m] = sum(neigh) / len(neigh)
    return out


def _minute_of_day(ts: int, calendar: MarketCalendar) -> int:
    local = calendar.local(datetime.fromtimestamp(ts, tz=timezone.utc))
    return local.hour * 60 + local.minute


class DetectionEngine:
    def __init__(self, cfg: DetectionConfig, calendar: MarketCalendar):
        self.cfg = cfg
        self.calendar = calendar
        self.gate = AlertGate(cfg.cooldown_min * 60)
        self.level = LevelBreach(cfg.level)
        self.bar_detectors: list[Detector] = []
        if cfg.velocity.enabled:
            self.bar_detectors.append(VelocityShock(cfg.velocity))
        if cfg.trend.enabled:
            self.bar_detectors.append(TrendReversal(cfg.trend))
        if cfg.cusum.enabled:
            self.bar_detectors.append(CusumChangePoint(cfg.cusum))
        if cfg.vol_regime.enabled:
            self.bar_detectors.append(VolatilityRegime(cfg.vol_regime))
        self.ctx = Context()
        self.vol = EwmaVolatility(cfg.ewma_lambda, SIGMA_FLOOR**2)
        self.last_bar: Bar | None = None

    def start_session(
        self,
        prev_close: float | None,
        daily_var: float | None,
        profile: dict[int, float] | None = None,
        session_minutes: float = 360.0,
    ) -> None:
        """Reset per-day state. Volatility is seeded from daily variance spread across the session."""
        flat = (daily_var / session_minutes) if daily_var else (0.01**2) / session_minutes
        self.ctx = Context(prev_close=prev_close, flat_variance=flat, variance_profile=profile or {})
        self.vol = EwmaVolatility(self.cfg.ewma_lambda, flat)
        self.last_bar = None
        self.level.reset()
        for d in self.bar_detectors:
            d.reset()

    def set_prev_close(self, prev_close: float) -> None:
        self.ctx.prev_close = prev_close

    @property
    def sigma_1m(self) -> float:
        return self.vol.sigma

    def _warm(self, ts: int) -> bool:
        return self.calendar.in_warmup(datetime.fromtimestamp(ts, tz=timezone.utc), self.cfg.warmup_min)

    def _gate(self, alerts: list[Alert], suppress: bool, silent: bool) -> list[Alert]:
        if silent or suppress:
            return []
        return [a for a in alerts if self.gate.admit(a)]

    def on_tick(self, tick: Tick, silent: bool = False) -> list[Alert]:
        if not self.cfg.level.enabled:
            return []
        return self._gate(self.level.on_tick(tick, self.ctx), False, silent)

    def on_bar(self, bar: Bar, silent: bool = False) -> list[Alert]:
        prev = self.last_bar
        gap = prev is None or bar.ts - prev.ts > GAP_S
        if gap and prev is not None:
            for d in self.bar_detectors:
                d.on_gap()
        r = None if gap or prev.close <= 0 else math.log(bar.close / prev.close)
        self.ctx.ret_1m = r
        self.ctx.sigma_1m = self.vol.sigma
        self.ctx.minute_of_day = _minute_of_day(bar.ts, self.calendar)

        warm = self._warm(bar.ts + 60)
        out: list[Alert] = []
        for d in self.bar_detectors:
            out += self._gate(d.on_bar(bar, self.ctx), warm and d.suppress_in_warmup, silent)
        if r is not None:
            self.vol.update(r)  # after detection so a shock is measured against pre-shock volatility
        self.last_bar = bar
        return out
