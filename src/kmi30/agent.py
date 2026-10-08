"""Orchestrator: poll PSX, build bars, run detectors, persist, alert, and feed the dashboard."""
from __future__ import annotations

import asyncio
import logging
import random
import time
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone

from . import metrics as m
from .alerts import SlackNotifier
from .bars import BarBuilder
from .clock import Clock, SimClock
from .config import Settings
from .detectors import FeedHealth
from .engine import DetectionEngine, daily_variance_from_closes, variance_profile
from .hub import Hub
from .market_calendar import MarketCalendar
from .models import Alert, Bar, Tick
from .psx_client import IndexSnapshot, PSXClient, PSXError
from .store import Store

log = logging.getLogger(__name__)

CATCHUP_WINDOW_S = 120  # intraday feed: ticks older than this on the first fetch are replayed silently
PREV_CLOSE_EPS = 1e-4  # relative change that counts as a corrected previous close
PREV_CLOSE_RETRY_S = 300  # intraday feed: how often to retry reading previous close from /indices


class Agent:
    def __init__(
        self,
        settings: Settings,
        calendar: MarketCalendar,
        store: Store,
        client: PSXClient,
        notifier: SlackNotifier,
        hub: Hub,
        clock: Clock | None = None,
    ):
        self.s = settings
        self.cal = calendar
        self.store = store
        self.client = client
        self.notifier = notifier
        self.hub = hub
        self.clock = clock or Clock()
        self.indices_feed = settings.source.feed == "indices"
        self.engine = DetectionEngine(settings.detection, calendar)
        self.feed = FeedHealth(settings.detection.feed)
        self.day: date | None = None
        self.bars = BarBuilder()
        self.last_tick_ts: int | None = None
        self.last_change_ts: int | None = None  # last time the index value actually moved
        self.last_value: float = 0.0
        self.failures = 0
        self.caught_up = False
        self.prev_close: float | None = None
        self._snapshot: IndexSnapshot | None = None
        self._prev_close_attempt = float("-inf")
        self._prev_close_recorded = False
        self._queue: asyncio.Queue[Alert] = asyncio.Queue(maxsize=1000)

    # ---------- day lifecycle ----------

    def _day_start_ts(self, d: date) -> int:
        return int(datetime.combine(d, datetime.min.time(), self.cal.tz).timestamp())

    async def _ensure_day(self, now: datetime) -> None:
        d = self.cal.local(now).date()
        if d == self.day:
            return
        log.info("starting session day %s", d)
        self.day = d
        self.bars = BarBuilder()
        self.last_tick_ts = self.last_change_ts = None
        self.last_value = 0.0
        self.caught_up = False
        self._snapshot = None
        self.feed.reset()

        day_start = self._day_start_ts(d)
        # Previous close: recorded from the indices page earlier today (restart), else the last
        # value this agent stored before today. The first indices poll confirms or corrects it.
        self.prev_close = self.store.prev_close(d.isoformat())
        self._prev_close_recorded = self.prev_close is not None
        if self.prev_close is None:
            last = self.store.last_tick_before(day_start)
            self.prev_close = last.value if last else None

        daily_var = self._daily_variance(d)
        profile = self._load_profile(d)
        self.engine.start_session(self.prev_close, daily_var, profile, self.cal.session_minutes(d))
        today_alerts = self.store.alerts_since(day_start)
        self.engine.gate.restore(today_alerts)
        self.hub.reset_day(d.isoformat(), self.prev_close, today_alerts, [])
        self._replay_stored_ticks(day_start)
        await self.hub.broadcast(self.hub.snapshot())  # connected browsers drop yesterday's chart
        self.hub.mark_synced()
        self.store.prune(int((now - timedelta(days=self.s.storage.retention_days)).timestamp()))
        log.info("day %s: prev_close=%s daily_sigma=%.4f%% profile_minutes=%d replayed_ticks=%s",
                 d, self.prev_close, daily_var ** 0.5 * 100, len(profile), self.last_tick_ts is not None)

    def _daily_variance(self, d: date) -> float:
        lookback = self.s.detection.daily_vol_lookback
        closes = self.store.prev_closes(d.isoformat(), lookback + 1)
        var = daily_variance_from_closes(closes, lookback)
        return var if var is not None else (self.s.detection.default_daily_vol_pct / 100) ** 2

    def _replay_stored_ticks(self, day_start: int) -> None:
        """Rebuild today's chart and detector state from the local store without alerting.

        The indices page is a point-in-time snapshot with no history, so after a restart the
        store is the only source for what happened earlier in the session.
        """
        for t in self.store.ticks_between(day_start, day_start + 86400):
            self.engine.on_tick(t, silent=True)
            for bar in self.bars.add(t):
                self.hub.on_bar(bar)
                self.engine.on_bar(bar, silent=True)
            self._track(t)
            self.hub.on_tick(t, self.bars.current)

    def _track(self, t: Tick) -> None:
        if t.value != self.last_value:
            self.last_change_ts = t.ts
        self.last_tick_ts, self.last_value = t.ts, t.value

    def _set_prev_close(self, value: float | None) -> None:
        """Apply the previous close published on the indices page; persist it once per day."""
        if not value or value <= 0:
            return
        changed = not self.prev_close or abs(value / self.prev_close - 1) > PREV_CLOSE_EPS
        if changed:
            log.info("previous close set from indices page: %s (was %s)", value, self.prev_close)
            self.prev_close = value
            self.engine.set_prev_close(value)
            self.hub.prev_close = value
        if self.day and (changed or not self._prev_close_recorded):
            self.store.set_prev_close(self.day.isoformat(), value)
            self._prev_close_recorded = True

    def _pkt_date(self, ts: int) -> date:
        return self.cal.local(datetime.fromtimestamp(ts, tz=timezone.utc)).date()

    def _load_profile(self, d: date) -> dict[int, float]:
        cfg = self.s.detection.vol_regime
        start = datetime.combine(d - timedelta(days=int(cfg.lookback_days * 1.6) + 3), datetime.min.time(), self.cal.tz)
        end = datetime.combine(d, datetime.min.time(), self.cal.tz)
        by_day: dict[str, list[Bar]] = defaultdict(list)
        for b in self.store.bars_between(int(start.timestamp()), int(end.timestamp())):
            by_day[self._pkt_date(b.ts).isoformat()].append(b)
        days = sorted(by_day)[-cfg.lookback_days:]
        if len(days) < cfg.min_history_days:
            return {}
        return variance_profile({k: by_day[k] for k in days}, self.cal)

    # ---------- polling ----------

    async def _poll_indices(self, now: datetime) -> list[Tick]:
        snap = await self.client.indices_snapshot()
        self._snapshot = snap
        self._set_prev_close(snap.prev_close)
        return [Tick(int(now.timestamp()), snap.current, 0.0, "indices")]

    async def _confirm_prev_close(self) -> None:
        """Intraday feed has no change figure, so read previous close from /indices once a day."""
        if self.day is None or self._prev_close_recorded:
            return
        if time.monotonic() - self._prev_close_attempt < self.clock.real_seconds(PREV_CLOSE_RETRY_S):
            return
        self._prev_close_attempt = time.monotonic()
        try:
            snap = await self.client.indices_snapshot()
        except PSXError as exc:
            log.warning("previous close lookup on /indices failed: %s", exc)
            return
        self._set_prev_close(snap.prev_close)

    async def _fetch(self, now: datetime) -> list[Tick]:
        started = time.perf_counter()
        try:
            ticks = await (self._poll_indices(now) if self.indices_feed else self.client.intraday())
            m.POLLS.labels("ok").inc()
            self.failures = 0
            return ticks
        except PSXError as exc:
            m.POLLS.labels("error").inc()
            self.failures += 1
            log.warning("%s fetch failed (%d in a row): %s", self.s.source.feed, self.failures, exc)
        finally:
            m.POLL_LATENCY.observe(time.perf_counter() - started)
        if self.indices_feed:
            return []
        try:  # intraday feed falls back to the indices page
            ticks = await self._poll_indices(now)
            m.POLLS.labels("fallback").inc()
            return ticks
        except PSXError as exc:
            log.warning("indices fallback failed: %s", exc)
            return []

    async def cycle(self) -> float:
        """Run one poll cycle. Returns real seconds to wait before the next one."""
        now = self.clock.now()
        await self._ensure_day(now)
        # The intraday series can publish the last ticks shortly after the close, so it polls
        # through a grace window. An indices snapshot after the close adds nothing.
        session = self.cal.current_session(now, with_grace=not self.indices_feed)
        m.MARKET_OPEN.set(1 if session else 0)
        if session is None:
            await self._close_bar()
            return await self._idle(now)

        if not self.indices_feed:
            await self._confirm_prev_close()
        ticks = await self._fetch(now)
        today = self.day
        fresh = [t for t in ticks
                 if self._pkt_date(t.ts) == today and (self.last_tick_ts is None or t.ts > self.last_tick_ts)]
        cutoff = int(now.timestamp()) - CATCHUP_WINDOW_S if not self.caught_up else None
        if self.failures == 0:  # only a successful primary fetch proves history has been replayed
            self.caught_up = True

        new_bars: list[Bar] = []
        alerts: list[Alert] = []
        for t in fresh:
            silent = cutoff is not None and t.ts < cutoff
            alerts += self.engine.on_tick(t, silent=silent)
            for bar in self.bars.add(t):
                self.store.add_bar(bar)
                self.hub.on_bar(bar)
                new_bars.append(bar)
                alerts += self.engine.on_bar(bar, silent=silent)
            self._track(t)
            self.hub.on_tick(t, self.bars.current)
        self.store.add_ticks(fresh)
        snap = self._snapshot
        if snap is not None:  # the page's own day range covers moves before this agent started
            if snap.high is not None:
                self.hub.high = snap.high
            if snap.low is not None:
                self.hub.low = snap.low

        now_ts = int(now.timestamp())
        # Staleness is judged on the value moving, since every indices poll yields a reading.
        live = self.cal.current_session(now)
        alerts += self.feed.check(now_ts, int(live.start.timestamp()) if live else None,
                                  self.last_change_ts, self.failures, self.last_value)
        for a in alerts:
            self._emit(a)

        self._update_status(now, session_open=True)
        await self.hub.push_update(new_bars, alerts)

        poll = self.s.source.poll_interval_s
        if self.failures:
            backoff = min(self.s.source.max_backoff_s, poll * 2 ** min(self.failures, 6))
            return self.clock.real_seconds(backoff * random.uniform(0.8, 1.2))
        return self.clock.real_seconds(poll)

    async def _close_bar(self) -> None:
        """Complete the in-progress bar at session end (close or Friday break)."""
        bar = self.bars.flush()
        if bar is None:
            return
        self.store.add_bar(bar)
        self.hub.on_bar(bar)
        self.hub.current_bar = None
        alerts = self.engine.on_bar(bar)
        for a in alerts:
            self._emit(a)
        await self.hub.push_update([bar], alerts)

    async def _idle(self, now: datetime) -> float:
        self._update_status(now, session_open=False)
        await self.hub.publish("status", status=self.hub.status)
        nxt = self.cal.next_open(now)
        if nxt is None:
            return 60.0
        remaining = (nxt - now).total_seconds()
        if isinstance(self.clock, SimClock) and remaining > 120:
            self.clock.jump(nxt - timedelta(minutes=1))  # skip the overnight gap in demos
            return 1.0
        return max(0.2, min(60.0, self.clock.real_seconds(remaining)))

    def _update_status(self, now: datetime, session_open: bool) -> None:
        self.hub.heartbeat = time.monotonic()
        nxt = self.cal.next_open(now)
        age = (int(now.timestamp()) - self.last_tick_ts) if self.last_tick_ts else None
        self.hub.status = {
            "now": int(now.timestamp()),
            "market_open": session_open,
            "next_open": int(nxt.timestamp()) if nxt else None,
            "last_tick_age_s": age,
            "failures": self.failures,
            "feed": self.s.source.feed,
            "timestamp_mode": self.client.timestamp_mode,
            "sigma_1m": self.engine.sigma_1m,
            "slack": self.notifier.enabled,
            "simulated": self.clock.simulated,
        }
        if age is not None:
            m.TICK_AGE.set(age)
        if self.last_value:
            m.VALUE.set(self.last_value)
            if self.prev_close:
                m.CHANGE_PCT.set((self.last_value / self.prev_close - 1) * 100)
        m.SIGMA.set(self.engine.sigma_1m)

    # ---------- alert delivery ----------

    def _emit(self, a: Alert) -> None:
        log.info("ALERT %s %s: %s", a.severity.label, a.key, a.title)
        m.ALERTS.labels(a.detector, a.severity.label).inc()
        self.hub.on_alert(a)
        try:
            self._queue.put_nowait(a)
        except asyncio.QueueFull:
            log.error("alert queue full; dropping %s", a.key)
            self.store.add_alert(a, delivered=False)

    async def _deliver_loop(self) -> None:
        while True:
            a = await self._queue.get()
            delivered = False
            try:
                if self.notifier.wants(a):
                    delivered = await self.notifier.send(a, self.prev_close)
                    m.SLACK.labels("ok" if delivered else "failed").inc()
            except Exception:  # noqa: BLE001 - delivery must never kill the loop
                log.exception("alert delivery crashed")
            finally:
                self.store.add_alert(a, delivered)
                self._queue.task_done()

    async def run(self, stop: asyncio.Event) -> None:
        deliver = asyncio.create_task(self._deliver_loop(), name="slack-delivery")
        try:
            while not stop.is_set():
                try:
                    wait = await self.cycle()
                except Exception:  # noqa: BLE001 - keep the agent alive; errors surface via metrics/logs
                    log.exception("poll cycle crashed")
                    wait = self.clock.real_seconds(self.s.source.poll_interval_s)
                try:
                    await asyncio.wait_for(stop.wait(), timeout=wait)
                except asyncio.TimeoutError:
                    pass
        finally:
            try:
                await asyncio.wait_for(self._queue.join(), timeout=15)
            except asyncio.TimeoutError:
                log.warning("shutdown with %d undelivered alerts", self._queue.qsize())
            deliver.cancel()
