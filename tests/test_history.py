"""Historical storage: migration, live daily summary, retention, export."""
import sqlite3
from datetime import timedelta

import pytest

from kmi30.__main__ import cmd_export
from kmi30.models import Tick
from kmi30.store import Store

from .conftest import pkt
from .test_agent import build, close, run

DAY = "2026-09-14"


def test_migration_adds_reading_columns_to_existing_database(tmp_path):
    path = tmp_path / "old.db"
    db = sqlite3.connect(path)
    db.execute("CREATE TABLE ticks (ts INTEGER PRIMARY KEY, value REAL NOT NULL,"
               " volume REAL NOT NULL DEFAULT 0, source TEXT NOT NULL)")
    db.execute("INSERT INTO ticks VALUES (1, 100.0, 0, 'indices')")
    db.commit()
    db.close()
    store = Store(str(path))
    store.add_ticks([Tick(2, 101.0, 0, "indices", 102.0, 99.0, 1.0, 1.0)])
    old, new = store.ticks_between(0, 10)
    assert old.value == 100.0 and old.high is None  # old rows keep working
    assert (new.high, new.low, new.change, new.change_pct) == (102.0, 99.0, 1.0, 1.0)
    Store(str(path)).close()  # migration is idempotent


async def test_agent_stores_full_readings_and_live_daily_summary(settings, calendar):
    clock_start = pkt(2026, 9, 14, 9, 30)
    from .test_agent import FakeClock

    clock = FakeClock(clock_start)
    store, posts = Store(":memory:"), []
    agent = build(settings, calendar, clock, store, posts)
    await run(agent, clock, clock_start, pkt(2026, 9, 14, 10, 30))

    ticks = store.ticks_between(0, 2**31)
    assert all(t.high is not None and t.change is not None for t in ticks)
    s = store.daily(DAY)
    assert s is not None
    assert s.open == ticks[0].value and s.close == ticks[-1].value
    assert s.high >= max(t.value for t in ticks) and s.low <= min(t.value for t in ticks)
    assert s.readings == len(ticks) and s.alerts == len(agent.hub.alerts) > 0
    assert s.prev_close == agent.prev_close
    assert s.change == pytest.approx(s.close - s.prev_close)
    await close(agent)

    # A restart mid-session rebuilds the same summary from stored readings, then keeps extending it.
    clock.t = pkt(2026, 9, 14, 10, 31)
    again = build(settings, calendar, clock, store, posts)
    await again.cycle()
    s2 = store.daily(DAY)
    assert s2.open == s.open and s2.readings == s.readings + 1 and s2.first_ts == s.first_ts
    await close(again)


async def test_history_kept_forever_by_default_and_pruned_when_configured(settings, calendar):
    from .test_agent import FakeClock

    assert settings.storage.retention_days == 0
    store, posts = Store(":memory:"), []
    old_ts = int((pkt(2026, 9, 14, 10, 0) - timedelta(days=400)).timestamp())
    store.add_ticks([Tick(old_ts, 1.0, 0, "indices")])
    agent = build(settings, calendar, FakeClock(pkt(2026, 9, 14, 9, 0)), store, posts)
    await agent.cycle()
    assert store.ticks_between(0, old_ts + 1)  # still there
    await close(agent)

    settings.storage.retention_days = 30
    agent = build(settings, calendar, FakeClock(pkt(2026, 9, 15, 9, 0)), store, posts)
    await agent.cycle()
    assert store.ticks_between(0, old_ts + 1) == []
    await close(agent)


async def test_export_cli(settings, calendar, tmp_path, capsys):
    from .test_agent import FakeClock

    settings.storage.path = str(tmp_path / "h.db")
    store, posts = Store(settings.storage.path), []
    clock = FakeClock(pkt(2026, 9, 14, 10, 0))
    agent = build(settings, calendar, clock, store, posts)
    await run(agent, clock, clock.t, pkt(2026, 9, 14, 10, 5))
    await close(agent)
    store.close()

    assert cmd_export(settings, DAY, False, None, None, None) == 0
    lines = capsys.readouterr().out.strip().splitlines()
    assert lines[0].startswith("time_pkt,epoch_utc,value") and len(lines) == 16
    out = tmp_path / "daily.csv"
    assert cmd_export(settings, None, True, None, None, str(out)) == 0
    rows = out.read_text().strip().splitlines()
    assert rows[1].startswith(f"{DAY},") and len(rows) == 2
    assert cmd_export(settings, "2026-09-20", False, None, None, None) == 1
    assert cmd_export(settings, "20-09-2026", False, None, None, None) == 2
