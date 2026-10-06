"""End-to-end: simulated PSX portal -> client -> agent -> store/Slack, on a controllable clock."""
import asyncio
import json
import math
from datetime import timedelta

import httpx
import pytest

from kmi30.agent import Agent
from kmi30.alerts import SlackNotifier
from kmi30.clock import Clock, SimClock
from kmi30.hub import Hub
from kmi30.psx_client import PSXClient
from kmi30.simulator import SimulatedPortal
from kmi30.store import Store

from .conftest import pkt

DAY = "2026-09-14"  # a Monday


class FakeClock(Clock):
    def __init__(self, t):
        self.t = t

    def now(self):
        return self.t


def build(settings, calendar, clock, store, posts, transport=None):
    settings.slack.webhook_url = "https://hooks.slack.test/x"
    portal = SimulatedPortal(calendar, clock)
    client = PSXClient(settings.source, calendar, transport=transport or portal.transport())

    def slack(req):
        posts.append(json.loads(req.content))
        return httpx.Response(200, text="ok")

    notifier = SlackNotifier(settings.slack, calendar.tz, transport=httpx.MockTransport(slack))
    agent = Agent(settings, calendar, store, client, notifier, Hub(18000, False), clock)
    agent.portal = portal
    return agent


async def drain(agent):
    deliver = asyncio.create_task(agent._deliver_loop())
    await agent._queue.join()
    deliver.cancel()


async def run(agent, clock, start, end, step_s=20):
    clock.t = start
    while clock.t < end:
        await agent.cycle()
        clock.t += timedelta(seconds=step_s)
    await drain(agent)


async def close(agent):
    await agent.client.aclose()
    await agent.notifier.aclose()


@pytest.mark.parametrize("feed", ["indices", "intraday"])
async def test_live_session_detects_simulated_drop_and_delivers(settings, calendar, feed):
    settings.source.feed = feed
    clock = FakeClock(pkt(2026, 9, 14, 9, 25))
    store, posts = Store(":memory:"), []
    agent = build(settings, calendar, clock, store, posts)

    await agent.cycle()  # pre-open: idle, no history yet
    assert agent.hub.status["market_open"] is False
    assert agent.prev_close is None

    await run(agent, clock, pkt(2026, 9, 14, 9, 30), pkt(2026, 9, 14, 10, 30))  # drop at 10:15-10:18

    expected_prev = agent.portal._close_for(agent.portal._trading_days_before(clock.t.date(), 1)[0])
    assert agent.prev_close == pytest.approx(expected_prev, abs=0.02)
    assert store.prev_close(DAY) == agent.prev_close
    detectors = {a["detector"] for a in agent.hub.alerts}
    assert {"velocity", "level"} <= detectors
    assert len(posts) == len(agent.hub.alerts) > 0
    assert len(store.alerts_since(0)) == len(agent.hub.alerts)
    assert store.bars_between(0, 2**31)
    if feed == "intraday":
        assert agent.client.timestamp_mode == "pkt_wallclock"
    else:
        assert all(t.source == "indices" for t in store.ticks_between(0, 2**31))
    await close(agent)


async def test_restart_rebuilds_today_from_store_without_realerting(settings, calendar):
    clock = FakeClock(pkt(2026, 9, 14, 9, 30))
    store, posts = Store(":memory:"), []
    first = build(settings, calendar, clock, store, posts)
    await run(first, clock, pkt(2026, 9, 14, 9, 30), pkt(2026, 9, 14, 10, 40))
    sent = len(posts)
    assert sent > 0
    await close(first)

    clock.t = pkt(2026, 9, 14, 10, 41)
    second = build(settings, calendar, clock, store, posts)
    await second.cycle()
    await drain(second)
    assert len(posts) == sent  # nothing re-sent
    assert len(second.hub.bars) > 60  # chart history rebuilt from SQLite, the page has none
    assert second.prev_close == first.prev_close
    assert {a["key"] for a in second.hub.alerts} == {a["key"] for a in first.hub.alerts}
    await close(second)


async def test_intraday_feed_restart_replays_series_silently(settings, calendar):
    settings.source.feed = "intraday"
    clock = FakeClock(pkt(2026, 9, 14, 10, 40))  # drop already happened before this process started
    store, posts = Store(":memory:"), []
    agent = build(settings, calendar, clock, store, posts)
    await agent.cycle()
    await drain(agent)
    assert agent.hub.alerts == [] and posts == []
    assert len(agent.hub.bars) > 60
    await close(agent)


async def test_portal_outage_alerts_and_backs_off(settings, calendar):
    clock = FakeClock(pkt(2026, 9, 14, 11, 0))
    store, posts = Store(":memory:"), []

    def dead(req):
        raise httpx.ConnectError("refused")

    agent = build(settings, calendar, clock, store, posts, transport=httpx.MockTransport(dead))
    for _ in range(settings.detection.feed.failure_streak):
        wait = await agent.cycle()
        clock.t += timedelta(seconds=10)
    assert wait > settings.source.poll_interval_s
    await drain(agent)
    assert any(a["key"] == "feed:errors" for a in agent.hub.alerts)
    await close(agent)


async def test_frozen_index_value_raises_stale_then_fresh(settings, calendar):
    page = {"value": 261_000.00}

    def indices(req):
        v = page["value"]
        html = ("<table><tr><th>Index</th><th>High</th><th>Low</th><th>Current</th><th>Change</th>"
                f"<th>% Change</th></tr><tr><td>KMI30</td><td>1</td><td>1</td><td>{v:,.2f}</td>"
                f"<td>{v - 260_500:,.2f}</td><td>0.19%</td></tr></table>")
        return httpx.Response(200, text=html)

    clock = FakeClock(pkt(2026, 9, 14, 10, 0))
    store, posts = Store(":memory:"), []
    agent = build(settings, calendar, clock, store, posts, transport=httpx.MockTransport(indices))
    await run(agent, clock, pkt(2026, 9, 14, 10, 0), pkt(2026, 9, 14, 10, 5), step_s=10)
    assert [a["key"] for a in agent.hub.alerts] == ["feed:stale"]
    page["value"] = 261_050.00
    await run(agent, clock, clock.t, clock.t + timedelta(seconds=20), step_s=10)
    assert [a["key"] for a in agent.hub.alerts] == ["feed:stale", "feed:fresh"]
    assert agent.prev_close == pytest.approx(260_500.0)
    await close(agent)


async def test_last_bar_closed_at_session_end_and_no_post_close_polling(settings, calendar):
    clock = FakeClock(pkt(2026, 9, 14, 15, 20))
    store, posts = Store(":memory:"), []
    agent = build(settings, calendar, clock, store, posts)
    await run(agent, clock, pkt(2026, 9, 14, 15, 20), pkt(2026, 9, 14, 15, 30))
    assert agent.bars.current is not None
    clock.t = pkt(2026, 9, 14, 15, 32)
    await agent.cycle()
    await drain(agent)
    assert agent.hub.status["market_open"] is False
    assert agent.bars.current is None
    last = store.bars_between(0, 2**31)[-1]
    assert last.ts == int(pkt(2026, 9, 14, 15, 29).timestamp())
    assert max(t.ts for t in store.ticks_between(0, 2**31)) < int(pkt(2026, 9, 14, 15, 30).timestamp())
    await close(agent)


async def test_volatility_seed_from_recorded_closes(settings, calendar):
    store, posts = Store(":memory:"), []
    for i in range(12):  # alternating +/-1.5% daily moves recorded on earlier sessions
        store.set_prev_close(f"2026-08-{10 + i:02d}", 260_000 * math.exp(0.015 * (-1) ** i))
    agent = build(settings, calendar, FakeClock(pkt(2026, 9, 14, 9, 0)), store, posts)
    await agent.cycle()
    per_minute = agent.engine.ctx.flat_variance * calendar.session_minutes(clock_day := agent.day)
    assert clock_day.isoformat() == DAY
    assert 0.03**2 * 0.8 < per_minute < 0.03**2 * 1.2  # log returns alternate by 3%
    fresh = build(settings, calendar, FakeClock(pkt(2026, 9, 14, 9, 0)), Store(":memory:"), posts)
    await fresh.cycle()
    default = (settings.detection.default_daily_vol_pct / 100) ** 2
    assert fresh.engine.ctx.flat_variance * calendar.session_minutes(fresh.day) == pytest.approx(default)
    await close(agent)
    await close(fresh)


async def test_sim_clock_reaches_open_at_low_speed(settings, calendar):
    """Regression: the idle path must not rewind the simulated clock just before the open."""
    clock = SimClock(pkt(2026, 9, 13, 20, 0), speed=30)  # Sunday evening
    store, posts = Store(":memory:"), []
    agent = build(settings, calendar, clock, store, posts)
    first = await agent.cycle()
    assert first == 1.0 and abs((clock.now() - pkt(2026, 9, 14, 9, 29)).total_seconds()) < 5
    wait = await agent.cycle()
    assert 0.2 <= wait <= 2.1  # waits out the final minute in scaled real time instead of jumping back
    clock.jump(clock.now() + timedelta(seconds=wait * 30 + 1))
    await agent.cycle()
    assert agent.hub.status["market_open"] is True
    await close(agent)
