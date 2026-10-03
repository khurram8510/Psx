"""Trend-deviation detectors.

All detectors run on event time (tick/bar timestamps), never wall-clock, so live runs and
historical replays produce identical alerts for identical input.
"""
from __future__ import annotations

import math
from collections import deque
from dataclasses import dataclass, field

from .config import (
    CusumConfig,
    FeedConfig,
    LevelConfig,
    TrendConfig,
    VelocityConfig,
    VolRegimeConfig,
)
from .models import Alert, Bar, Severity, Tick

SIGMA_FLOOR = 1e-5  # ~0.001% per minute; prevents divide-by-zero on a frozen index


@dataclass
class Context:
    """Shared, per-bar inputs supplied by the engine."""

    prev_close: float | None = None
    sigma_1m: float = SIGMA_FLOOR
    ret_1m: float | None = None  # log return of this bar vs previous bar; None after a gap
    minute_of_day: int = 0  # PKT minute of the bar, used by the variance profile
    variance_profile: dict[int, float] = field(default_factory=dict)
    flat_variance: float = SIGMA_FLOOR**2


class Latch:
    """Multi-level threshold latch with hysteresis.

    ``levels`` is ascending ``(threshold, severity, rearm)``. A level fires once when crossed and
    re-arms only after the signal falls below that level's ``rearm`` value, which suppresses
    chatter around a threshold while still letting a warning escalate to critical.
    """

    def __init__(self, levels: list[tuple[float, Severity, float]]):
        self.levels = sorted(levels, key=lambda l: l[0])
        self.fired = 0

    def update(self, x: float) -> Severity | None:
        while self.fired > 0 and x < self.levels[self.fired - 1][2]:
            self.fired -= 1
        reached = sum(1 for thr, _, _ in self.levels if x >= thr)
        if reached > self.fired:
            self.fired = reached
            return self.levels[reached - 1][1]
        return None

    def reset(self) -> None:
        self.fired = 0


def _dir(sign: int) -> str:
    return "up" if sign > 0 else "down"


class Detector:
    name = "base"
    suppress_in_warmup = True

    def reset(self) -> None:  # new session day
        pass

    def on_gap(self) -> None:  # session break or feed gap
        pass

    def on_tick(self, tick: Tick, ctx: Context) -> list[Alert]:
        return []

    def on_bar(self, bar: Bar, ctx: Context) -> list[Alert]:
        return []


class LevelBreach(Detector):
    """Percent move vs previous close crossing configured bands."""

    name = "level"
    suppress_in_warmup = False

    def __init__(self, cfg: LevelConfig):
        self.cfg = cfg
        self.reset()

    def _latch(self) -> Latch:
        c = self.cfg
        return Latch([
            (c.warning_pct, Severity.WARNING, c.warning_pct - c.hysteresis_pct),
            (c.critical_pct, Severity.CRITICAL, c.critical_pct - c.hysteresis_pct),
        ])

    def reset(self) -> None:
        self.latches = {1: self._latch(), -1: self._latch()}

    def on_tick(self, tick: Tick, ctx: Context) -> list[Alert]:
        if not ctx.prev_close:
            return []
        pct = (tick.value / ctx.prev_close - 1.0) * 100.0
        out = []
        for sign, latch in self.latches.items():
            sev = latch.update(pct * sign)
            if sev:
                out.append(Alert(
                    self.name, f"level:{_dir(sign)}", sev,
                    f"KMI-30 {_dir(sign)} {abs(pct):.2f}% vs previous close",
                    f"Index at {tick.value:,.2f} against previous close {ctx.prev_close:,.2f}.",
                    tick.ts, tick.value, {"change_pct": round(pct, 3), "prev_close": ctx.prev_close}, sign,
                ))
        return out


class VelocityShock(Detector):
    """N-minute log return standardised by EWMA one-minute volatility."""

    name = "velocity"

    def __init__(self, cfg: VelocityConfig):
        self.cfg = cfg
        self.closes: deque[float] = deque(maxlen=cfg.window_min + 1)
        self.reset()

    def _latch(self) -> Latch:
        c = self.cfg
        return Latch([(c.warning_z, Severity.WARNING, c.rearm_z), (c.critical_z, Severity.CRITICAL, c.rearm_z)])

    def reset(self) -> None:
        self.closes.clear()
        self.latches = {1: self._latch(), -1: self._latch()}

    def on_gap(self) -> None:
        self.closes.clear()

    def on_bar(self, bar: Bar, ctx: Context) -> list[Alert]:
        self.closes.append(bar.close)
        if len(self.closes) < self.closes.maxlen:
            return []
        w = self.cfg.window_min
        r = math.log(self.closes[-1] / self.closes[0])
        z = r / (max(ctx.sigma_1m, SIGMA_FLOOR) * math.sqrt(w))
        out = []
        for sign, latch in self.latches.items():
            sev = latch.update(z * sign)
            if sev:
                out.append(Alert(
                    self.name, f"velocity:{_dir(sign)}", sev,
                    f"KMI-30 sharp {_dir(sign)} move: {r * 100:+.2f}% in {w} min",
                    f"{w}-minute return is {abs(z):.1f} standard deviations from current intraday volatility.",
                    bar.ts + 60, bar.close, {"z": round(z, 2), "return_pct": round(r * 100, 3), "window_min": w}, sign,
                ))
        return out


class TrendReversal(Detector):
    """Fast/slow EMA regime change confirmed by the sign of a rolling OLS slope.

    The EMA gap must exceed ``min_separation_sigma`` one-minute standard deviations, which keeps
    false reversals on a pure random walk near 0.6 per session at the defaults. Both directions
    share one cooldown key so a whipsaw cannot alternate past the cooldown.
    """

    name = "trend"

    def __init__(self, cfg: TrendConfig):
        if cfg.fast >= cfg.slow:
            raise ValueError("trend.fast must be smaller than trend.slow")
        self.cfg = cfg
        self.a_fast = 2.0 / (cfg.fast + 1)
        self.a_slow = 2.0 / (cfg.slow + 1)
        self.reset()

    def reset(self) -> None:
        self.ema_fast: float | None = None
        self.ema_slow: float | None = None
        self.n = 0
        self.regime = 0  # confirmed: +1 bullish, -1 bearish, 0 unknown
        self.pending = 0
        self.pending_count = 0
        self.window: deque[float] = deque(maxlen=self.cfg.slope_window)

    def _slope(self) -> float:
        ys = list(self.window)
        n = len(ys)
        mx = (n - 1) / 2
        my = sum(ys) / n
        num = sum((i - mx) * (y - my) for i, y in enumerate(ys))
        den = sum((i - mx) ** 2 for i in range(n))
        return num / den if den else 0.0

    def on_bar(self, bar: Bar, ctx: Context) -> list[Alert]:
        c = bar.close
        self.n += 1
        self.window.append(c)
        if self.ema_fast is None:
            self.ema_fast = self.ema_slow = c
            return []
        self.ema_fast += self.a_fast * (c - self.ema_fast)
        self.ema_slow += self.a_slow * (c - self.ema_slow)
        if self.n < self.cfg.slow:
            return []

        sep = self.ema_fast - self.ema_slow
        sep_pct = sep / self.ema_slow * 100
        sep_sigma = sep / (self.ema_slow * max(ctx.sigma_1m, SIGMA_FLOOR))
        weak = abs(sep_pct) < self.cfg.min_separation_pct or abs(sep_sigma) < self.cfg.min_separation_sigma
        side = 0 if weak else (1 if sep > 0 else -1)
        if side == 0 or side == self.regime:
            self.pending, self.pending_count = 0, 0
            return []
        if side != self.pending:
            self.pending, self.pending_count = side, 0
        self.pending_count += 1
        if self.pending_count < self.cfg.confirm_bars:
            return []

        slope = self._slope() if len(self.window) == self.window.maxlen else 0.0
        if (slope > 0) != (side > 0) or slope == 0:
            return []
        previous, self.regime = self.regime, side
        self.pending, self.pending_count = 0, 0
        if previous == 0:
            return []  # first confirmed regime of the session is a baseline, not a reversal
        label = "bullish" if side > 0 else "bearish"
        prev_label = "bullish" if previous > 0 else "bearish"
        slope_pct = slope / c * 100
        return [Alert(
            self.name, "trend", Severity.WARNING,
            f"KMI-30 trend reversal: {prev_label} to {label}",
            f"EMA{self.cfg.fast} crossed EMA{self.cfg.slow} and held for {self.cfg.confirm_bars} bars; "
            f"{self.cfg.slope_window}-minute slope {slope_pct:+.3f}% per minute.",
            bar.ts + 60, c,
            {"ema_fast": round(self.ema_fast, 2), "ema_slow": round(self.ema_slow, 2),
             "separation_pct": round(sep_pct, 3), "separation_sigma": round(sep_sigma, 2),
             "slope_pct_per_min": round(slope_pct, 4)},
            side,
        )]


class CusumChangePoint(Detector):
    """Two-sided CUSUM on standardised one-minute returns; catches sustained drift.

    Each standardised return is clipped to +/-4 so one isolated spike (the velocity
    detector's job) cannot trip CUSUM on its own when h=5 and k=0.5.
    """

    name = "cusum"
    CLIP = 4.0

    def __init__(self, cfg: CusumConfig):
        self.cfg = cfg
        self.reset()

    def reset(self) -> None:
        self.s_pos = 0.0
        self.s_neg = 0.0

    on_gap = reset

    def on_bar(self, bar: Bar, ctx: Context) -> list[Alert]:
        if ctx.ret_1m is None:
            return []
        z = max(-self.CLIP, min(self.CLIP, ctx.ret_1m / max(ctx.sigma_1m, SIGMA_FLOOR)))
        k, h = self.cfg.k, self.cfg.h
        self.s_pos = max(0.0, self.s_pos + z - k)
        self.s_neg = max(0.0, self.s_neg - z - k)
        sign = 1 if self.s_pos > h else -1 if self.s_neg > h else 0
        if not sign:
            return []
        stat = self.s_pos if sign > 0 else self.s_neg
        self.reset()
        return [Alert(
            self.name, f"cusum:{_dir(sign)}", Severity.WARNING,
            f"KMI-30 sustained {_dir(sign)}ward drift detected",
            f"CUSUM statistic {stat:.1f} exceeded threshold {h:.1f}: returns have shifted "
            f"{_dir(sign)} persistently rather than in one jump.",
            bar.ts + 60, bar.close, {"cusum": round(stat, 2), "h": h, "k": k}, sign,
        )]


class VolatilityRegime(Detector):
    """Rolling realised volatility vs expected volatility.

    Baseline is the same-time-of-day variance profile from stored history when at least
    ``min_history_days`` sessions exist. Before that, it is a slow EWMA of today's one-minute
    variance seeded from end-of-day volatility, so it adapts to the index's real intraday noise.
    """

    name = "vol_regime"

    def __init__(self, cfg: VolRegimeConfig):
        self.cfg = cfg
        self.rets: deque[tuple[int, float]] = deque(maxlen=cfg.window_min)
        self.reset()

    def reset(self) -> None:
        self.rets.clear()
        self.slow_var: float | None = None
        self.latch = Latch([
            (self.cfg.warning_ratio, Severity.WARNING, self.cfg.rearm_ratio),
            (self.cfg.critical_ratio, Severity.CRITICAL, self.cfg.rearm_ratio),
        ])

    def on_gap(self) -> None:
        self.rets.clear()

    def on_bar(self, bar: Bar, ctx: Context) -> list[Alert]:
        if ctx.ret_1m is None:
            return []
        if self.slow_var is None:
            self.slow_var = ctx.flat_variance
        baseline = self.slow_var
        lam = self.cfg.baseline_lambda
        self.slow_var = lam * self.slow_var + (1 - lam) * ctx.ret_1m ** 2
        self.rets.append((ctx.minute_of_day, ctx.ret_1m))
        if len(self.rets) < self.rets.maxlen:
            return []
        realised = sum(r * r for _, r in self.rets)
        if ctx.variance_profile:
            expected = sum(ctx.variance_profile.get(m, baseline) for m, _ in self.rets)
        else:
            expected = baseline * len(self.rets)
        ratio = math.sqrt(realised / expected) if expected > 0 else 0.0
        sev = self.latch.update(ratio)
        if not sev:
            return []
        w = self.cfg.window_min
        return [Alert(
            self.name, "vol_regime:up", sev,
            f"KMI-30 volatility spike: {ratio:.1f}x normal",
            f"{w}-minute realised volatility {math.sqrt(realised) * 100:.3f}% vs "
            f"{math.sqrt(expected) * 100:.3f}% expected "
            f"({'same time of day, history' if ctx.variance_profile else 'intraday baseline'}).",
            bar.ts + 60, bar.close, {"ratio": round(ratio, 2), "window_min": w},
        )]


class FeedHealth:
    """Time-based feed monitor. Driven by the agent loop with wall-clock time."""

    name = "feed"

    def __init__(self, cfg: FeedConfig):
        self.cfg = cfg
        self.reset()

    def reset(self) -> None:
        self.stale_alerted = False
        self.no_data_alerted = False
        self.failure_alerted = False

    def check(
        self,
        now_ts: int,
        session_open_ts: int | None,
        last_tick_ts: int | None,
        failures: int,
        last_value: float,
    ) -> list[Alert]:
        if not self.cfg.enabled:
            return []
        out: list[Alert] = []

        if failures >= self.cfg.failure_streak and not self.failure_alerted:
            self.failure_alerted = True
            out.append(Alert(self.name, "feed:errors", Severity.CRITICAL, "PSX data portal unreachable",
                             f"{failures} consecutive polls failed. Live view and alerts are degraded.",
                             now_ts, last_value, {"failures": failures}))
        elif failures == 0 and self.failure_alerted:
            self.failure_alerted = False
            out.append(Alert(self.name, "feed:recovered", Severity.INFO, "PSX data portal reachable again",
                             "Polling has recovered.", now_ts, last_value, {}))

        if session_open_ts is None:
            return out
        if last_tick_ts is None or last_tick_ts < session_open_ts:
            if (not self.no_data_alerted
                    and now_ts - session_open_ts >= self.cfg.no_data_after_open_min * 60):
                self.no_data_alerted = True
                out.append(Alert(self.name, "feed:no_data", Severity.INFO,
                                 "No KMI-30 data since market open",
                                 "Possible market holiday, delayed open, or feed outage. "
                                 "Add confirmed holidays to market.holidays.",
                                 now_ts, last_value, {}))
            return out

        age_min = (now_ts - last_tick_ts) / 60
        if age_min >= self.cfg.stale_min and not self.stale_alerted:
            self.stale_alerted = True
            out.append(Alert(self.name, "feed:stale", Severity.WARNING, "KMI-30 feed stale",
                             f"No new index value for {age_min:.1f} minutes during market hours.",
                             now_ts, last_value, {"age_min": round(age_min, 1)}))
        elif age_min < self.cfg.stale_min and self.stale_alerted:
            self.stale_alerted = False
            out.append(Alert(self.name, "feed:fresh", Severity.INFO, "KMI-30 feed updating again",
                             "New index values are arriving.", now_ts, last_value, {}))
        return out
