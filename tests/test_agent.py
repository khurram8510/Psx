"""End-to-end: simulated PSX portal -> client -> agent -> store/Slack, on a controllable clock."""
import asyncio
import json
from datetime import timedelta

import httpx

from kmi30.agent import Agent
from kmi30.alerts import SlackNotifier
from kmi30.clock import Clock
from kmi30.hub import Hub
from kmi30.psx_client import PSXClient
from kmi30.simulator import SimulatedPortal
from kmi30.store import Store

from .conftest import pkt


class FakeClock(Clock):
    def __init__(self, t):
        self.t = t

    def now(self):
        return self.t


def build(settings, calendar, clock, store, posts):
    settings.slack.webhook_url = "https://hooks.slack.test/x"
    portal = SimulatedPortal(calendar, clock)
    client = PSXClient(settings.source, calendar, transport=portal.transport())

    def slack(req):
        posts.append(json.loads(req.content))
        return httpx.Response(200, text="ok")

    notifier = SlackNotifier(settings.slack, calendar.tz, transport=httpx.MockTransport(slack))
    return Agent(settings, calendar, store, client, notifier, Hub(18000, False), clock)


async def drain(agent):
    deliver = asyncio.create_task(agent._deliver_loop())
    await agent._queue.join()
    deliver.cancel()


async def test_live_session_detects_simulated_drop_and_delivers(settings, calendar):
    clock = FakeClock(pkt(2026, 9, 14, 9, 25))
    store, posts = Store(":memory:"), []
    agent = build(settings, calendar, clock, store, posts)

    await agent.cycle()  # pre-open: idle
    assert agent.hub.status["market_open"] is False
    assert agent.prev_close is not None

    clock.t = pkt(2026, 9, 14, 9, 30)
    while clock.t < pkt(2026, 9, 14, 10, 30):  # simulated drop happens 10:15-10:18
        await agent.cycle()
        clock.t += timedelta(seconds=30)
    await drain(agent)

    detectors = {a["detector"] for a in agent.hub.alerts}
    assert {"velocity", "level"} <= detectors
    assert len(posts) == len(agent.hub.alerts) > 0
    assert agent.client.timestamp_mode == "pkt_wallclock"
    assert len(store.alerts_since(0)) == len(agent.hub.alerts)
    assert store.bars_between(0, 2**31)
    await agent.client.aclose()
    await agent.notifier.aclose()


async def test_restart_mid_session_replays_silently(settings, calendar):
    clock = FakeClock(pkt(2026, 9, 14, 10, 40))  # drop already happened before this process started
    store, posts = Store(":memory:"), []
    agent = build(settings, calendar, clock, store, posts)
    await agent.cycle()
    await drain(agent)
    assert agent.hub.alerts == [] and posts == []
    assert len(agent.hub.bars) > 60  # history rebuilt for the chart
    await agent.client.aclose()
    await agent.notifier.aclose()


async def test_portal_outage_falls_back_then_alerts(settings, calendar):
    clock = FakeClock(pkt(2026, 9, 14, 11, 0))
    store, posts = Store(":memory:"), []
    agent = build(settings, calendar, clock, store, posts)

    def dead(req):
        raise httpx.ConnectError("refused")

    agent.client._http._transport = httpx.MockTransport(dead)
    for _ in range(settings.detection.feed.failure_streak):
        wait = await agent.cycle()
        clock.t += timedelta(seconds=10)
    assert wait > settings.source.poll_interval_s  # backing off
    await drain(agent)
    assert any(a["key"] == "feed:errors" for a in agent.hub.alerts)
    await agent.client.aclose()
    await agent.notifier.aclose()


async def test_sim_clock_reaches_open_at_low_speed(settings, calendar):
    """Regression: the idle path must not rewind the simulated clock just before the open."""
    from kmi30.clock import SimClock

    clock = SimClock(pkt(2026, 9, 13, 20, 0), speed=30)  # Sunday evening
    store, posts = Store(":memory:"), []
    agent = build(settings, calendar, clock, store, posts)
    first = await agent.cycle()
    assert first == 1.0 and abs((clock.now() - pkt(2026, 9, 14, 9, 29)).total_seconds()) < 5
    wait = await agent.cycle()
    assert 0.5 <= wait <= 2.1  # waits out the final minute in scaled real time instead of jumping back
    clock.jump(clock.now() + timedelta(seconds=wait * 30 + 1))
    await agent.cycle()
    assert agent.hub.status["market_open"] is True
    await agent.client.aclose()
    await agent.notifier.aclose()


async def test_last_bar_is_closed_at_session_end(settings, calendar):
    clock = FakeClock(pkt(2026, 9, 14, 15, 20))
    store, posts = Store(":memory:"), []
    agent = build(settings, calendar, clock, store, posts)
    await agent.cycle()
    clock.t = pkt(2026, 9, 14, 15, 33)  # inside the post-close grace window: final ticks fetched
    await agent.cycle()
    assert agent.bars.current is not None
    clock.t = pkt(2026, 9, 14, 15, 40)  # after close + grace
    await agent.cycle()
    await drain(agent)
    last = store.bars_between(0, 2**31)[-1]
    assert last.ts == int(pkt(2026, 9, 14, 15, 29).timestamp())
    assert agent.bars.current is None
    await agent.client.aclose()
    await agent.notifier.aclose()
