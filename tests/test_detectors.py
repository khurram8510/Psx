import math

from kmi30.config import CusumConfig, FeedConfig, LevelConfig, TrendConfig, VelocityConfig, VolRegimeConfig
from kmi30.detectors import (
    Context,
    CusumChangePoint,
    FeedHealth,
    Latch,
    LevelBreach,
    TrendReversal,
    VelocityShock,
    VolatilityRegime,
)
from kmi30.models import Severity, Tick

from .helpers import START, bars_from_closes, random_walk

SIGMA = 0.0006


def run_bars(det, closes, sigma=SIGMA):
    ctx = Context(sigma_1m=sigma, flat_variance=sigma**2)
    out, prev = [], None
    for b in bars_from_closes(closes):
        ctx.ret_1m = math.log(b.close / prev) if prev else None
        ctx.minute_of_day = 600
        out += det.on_bar(b, ctx)
        prev = b.close
    return out


def test_latch_hysteresis_and_escalation():
    l = Latch([(1.0, Severity.WARNING, 0.75), (2.0, Severity.CRITICAL, 1.75)])
    assert l.update(1.1) == Severity.WARNING
    assert l.update(1.2) is None
    assert l.update(0.9) is None  # still above re-arm
    assert l.update(2.1) == Severity.CRITICAL  # escalation fires despite warning latched
    assert l.update(1.8) is None
    assert l.update(2.2) is None  # critical not re-armed yet
    assert l.update(1.5) is None  # critical re-armed, warning still latched
    assert l.update(2.1) == Severity.CRITICAL
    assert l.update(0.5) is None
    assert l.update(1.0) == Severity.WARNING


def test_level_breach_bands_and_direction():
    det = LevelBreach(LevelConfig())
    ctx = Context(prev_close=100.0)
    fire = lambda v: det.on_tick(Tick(START, v), ctx)  # noqa: E731
    assert fire(100.5) == []
    a = fire(98.9)
    assert len(a) == 1 and a[0].severity == Severity.WARNING and a[0].direction == -1
    assert fire(98.95) == []
    assert fire(97.9)[0].severity == Severity.CRITICAL
    assert fire(101.2)[0].key == "level:up"
    assert det.on_tick(Tick(START, 50), Context(prev_close=None)) == []


def test_velocity_quiet_on_noise_and_fires_on_shock():
    det = VelocityShock(VelocityConfig())
    noise = random_walk(300, SIGMA, seed=3)
    assert len(run_bars(det, noise)) <= 2
    det.reset()
    closes = random_walk(30, SIGMA, seed=4)
    closes += [closes[-1] * math.exp(-0.004 * i) for i in range(1, 4)]  # -1.2% in 3 minutes
    alerts = run_bars(det, closes)
    assert alerts and alerts[0].direction == -1
    assert alerts[-1].severity == Severity.CRITICAL


def test_trend_reversal_fires_once_on_regime_change():
    det = TrendReversal(TrendConfig())
    down = [260_000 * math.exp(-0.0006 * i) for i in range(60)]
    up = [down[-1] * math.exp(0.0006 * i) for i in range(1, 60)]
    alerts = run_bars(det, down + up)
    assert len(alerts) == 1
    assert alerts[0].direction == 1 and "bearish to bullish" in alerts[0].title


def test_trend_reversal_rare_on_noise():
    total = sum(len(run_bars(TrendReversal(TrendConfig()), random_walk(360, SIGMA, seed=s))) for s in range(30))
    assert total / 30 < 1.5


def test_cusum_detects_drift_not_single_spike():
    det = CusumChangePoint(CusumConfig())
    flat = [260_000.0] * 20
    spike = flat + [flat[-1] * 1.01] + [flat[-1] * 1.01] * 5
    assert run_bars(det, spike) == []
    det.reset()
    drift = [260_000 * math.exp(1.2 * SIGMA * i) for i in range(30)]
    alerts = run_bars(det, drift)
    assert alerts and alerts[0].key == "cusum:up"


def test_vol_regime_fires_on_burst():
    det = VolatilityRegime(VolRegimeConfig())
    calm = random_walk(60, SIGMA, seed=5)
    burst = random_walk(20, SIGMA * 4, seed=6, start=calm[-1])
    assert run_bars(det, calm) == []
    alerts = run_bars(det, burst)
    assert alerts and alerts[0].severity >= Severity.WARNING


def test_feed_health_lifecycle():
    f = FeedHealth(FeedConfig())
    open_ts = START
    assert f.check(open_ts + 60, open_ts, None, 0, 0) == []
    nd = f.check(open_ts + 31 * 60, open_ts, None, 0, 0)
    assert nd and nd[0].key == "feed:no_data"
    last = open_ts + 32 * 60
    assert f.check(last + 60, open_ts, last, 0, 1) == []
    assert f.check(last + 200, open_ts, last, 0, 1)[0].key == "feed:stale"
    assert f.check(last + 260, open_ts, last, 0, 1) == []
    assert f.check(last + 300, open_ts, last + 290, 0, 1)[0].key == "feed:fresh"
    assert f.check(last + 400, None, last, 5, 1)[0].key == "feed:errors"
    assert f.check(last + 410, None, last, 0, 1)[0].key == "feed:recovered"
