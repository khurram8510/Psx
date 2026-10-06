import math

from kmi30.bars import BarBuilder
from kmi30.config import DetectionConfig
from kmi30.engine import AlertGate, DetectionEngine, daily_variance_from_closes, variance_profile
from kmi30.models import Alert, Bar, Severity, Tick

from .conftest import pkt
from .helpers import START, bars_from_closes, random_walk


def shock_closes():
    closes = random_walk(25, 0.0006, seed=11)
    return closes + [closes[-1] * math.exp(-0.004 * i) for i in range(1, 4)]


def engine(calendar, **kw):
    e = DetectionEngine(DetectionConfig(**kw), calendar)
    e.start_session(prev_close=260_000.0, daily_var=0.0006**2 * 360, session_minutes=360)
    return e


def test_warmup_suppresses_bar_detectors_not_level(calendar):
    e = engine(calendar)
    open_ts = int(pkt(2026, 9, 14, 9, 30).timestamp())
    closes = shock_closes()
    out = []
    for b in bars_from_closes(closes[-12:], start=open_ts):
        out += e.on_bar(b)
    assert out == []
    assert e.on_tick(Tick(open_ts + 300, 255_000.0))[0].detector == "level"


def test_shock_after_warmup_alerts(calendar):
    e = engine(calendar)
    out = []
    for b in bars_from_closes(shock_closes()):
        out += e.on_bar(b)
    assert any(a.detector == "velocity" for a in out)


def test_silent_mode_updates_state_without_alerts(calendar):
    e = engine(calendar)
    out = []
    for b in bars_from_closes(shock_closes()):
        out += e.on_bar(b, silent=True)
    assert out == []
    assert e.last_bar is not None


def test_gap_does_not_produce_return(calendar):
    e = engine(calendar)
    e.on_bar(Bar(START, 100, 100, 100, 100))
    e.on_bar(Bar(START + 3600, 90, 90, 90, 90))
    assert e.ctx.ret_1m is None


def test_gate_cooldown_and_escalation():
    g = AlertGate(900)
    a = lambda ts, sev: Alert("velocity", "velocity:down", sev, "t", "d", ts, 1.0)  # noqa: E731
    assert g.admit(a(0, Severity.WARNING))
    assert not g.admit(a(300, Severity.WARNING))
    assert g.admit(a(310, Severity.CRITICAL))
    assert not g.admit(a(320, Severity.WARNING))
    assert g.admit(a(310 + 900, Severity.WARNING))


def test_daily_variance_and_profile(calendar):
    closes = [100 * math.exp(0.01 * (-1) ** i) for i in range(30)]
    var = daily_variance_from_closes(closes, 20)
    assert var is not None and 0.0003 < var < 0.0005
    assert daily_variance_from_closes(closes[:3], 20) is None
    bars = bars_from_closes(random_walk(30, 0.001, seed=2))
    prof = variance_profile({"d1": bars, "d2": bars}, calendar)
    assert prof and all(v > 0 for v in prof.values())


def test_bar_builder():
    bb = BarBuilder()
    assert bb.add(Tick(START + 5, 10)) == []
    assert bb.add(Tick(START + 30, 12)) == []
    assert bb.add(Tick(START + 59, 9)) == []
    done = bb.add(Tick(START + 61, 11))
    assert len(done) == 1
    b = done[0]
    assert (b.ts, b.open, b.high, b.low, b.close, b.ticks) == (START, 10, 12, 9, 9, 3)
    assert bb.add(Tick(START + 10, 1)) == []  # late tick ignored
    assert bb.flush().close == 11
