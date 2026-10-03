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
from .engine import DetectionEngine, daily_variance_from_eod, variance_profile
from .hub import Hub
from .market_calendar import MarketCalendar
from .models import Alert, Bar, EodRow, Tick
from .psx_client import PSXClient, PSXError
from .store import Store

log = logging.getLogger(__name__)

EOD_RETRY_S = 300
CATCHUP_WINDOW_S = 120  # ticks older than this on the first fetch are replayed silently


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
        self.engine = DetectionEngine(settings.detection, calendar)
        self.feed = FeedHealth(settings.detection.feed)
        self.day: date | None = None
        self.bars = BarBuilder()
        self.last_tick_ts: int | None = None
        self.last_value: float = 0.0
        self.failures = 0
        self.caught_up = False
        self.prev_close: float | None = None
        self._eod_attempt = 0.0
        self._queue: asyncio.Queue[Alert] = asyncio.Queue(maxsize=1000)

    # ---------- day lifecycle ----------

    async def _ensure_day(self, now: datetime) -> None:
        d = self.cal.local(now).date()
        if d == self.day:
            if self.prev_close is None and time.monotonic() - self._eod_attempt > self.clock.real_seconds(EOD_RETRY_S):
                await self._load_prev_close(d)
            return
        log.info("starting session day %s", d)
        self.day = d
        self.bars = BarBuilder()
        self.last_tick_ts = None
        self.caught_up = False
        self.feed.reset()
        await self._load_prev_close(d)

        eod = self.store.eod()
        daily_var = daily_variance_from_eod([r for r in eod if self._pkt_date(r.ts) < d],
                                            self.s.detection.eod_vol_lookback)
        profile = self._load_profile(d)
        self.engine.start_session(self.prev_close, daily_var, profile, self.cal.session_minutes(d))
        day_start = int(datetime.combine(d, datetime.min.time(), self.cal.tz).timestamp())
        today_alerts = self.store.alerts_since(day_start)
        self.engine.gate.restore(today_alerts)
        self.hub.reset_day(d.isoformat(), self.prev_close, today_alerts, [])
        await self.hub.broadcast(self.hub.snapshot())  # connected browsers drop yesterday's chart
        self.store.prune(int((now - timedelta(days=self.s.storage.retention_days)).timestamp()))
        log.info("day %s: prev_close=%s daily_sigma=%s profile_minutes=%d", d, self.prev_close,
                 f"{daily_var ** 0.5:.4%}" if daily_var else "n/a", len(profile))

    def _pkt_date(self, ts: int) -> date:
        return self.cal.local(datetime.fromtimestamp(ts, tz=timezone.utc)).date()

    async def _load_prev_close(self, d: date) -> None:
        self._eod_attempt = time.monotonic()
        try:
            rows = await self.client.eod()
            self.store.upsert_eod(rows)
        except PSXError as exc:
            log.warning("EOD fetch failed: %s", exc)
            rows = self.store.eod()
        prior: list[EodRow] = [r for r in rows if self._pkt_date(r.ts) < d]
        prev = prior[-1].close if prior else None
        if prev is None:
            try:
                prev = (await self.client.indices_snapshot()).prev_close
            except PSXError as exc:
                log.warning("indices fallback for previous close failed: %s", exc)
        if prev:
            self.prev_close = prev
            self.engine.set_prev_close(prev)
            self.hub.prev_close = prev

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

    async def _fetch(self, now: datetime) -> list[Tick]:
        started = time.perf_counter()
        try:
            ticks = await self.client.intraday()
            m.POLLS.labels("ok").inc()
            self.failures = 0
            return ticks
        except PSXError as exc:
            m.POLLS.labels("error").inc()
            self.failures += 1
            log.warning("intraday fetch failed (%d in a row): %s", self.failures, exc)
        finally:
            m.POLL_LATENCY.observe(time.perf_counter() - started)
        try:
            snap = await self.client.indices_snapshot()
            m.POLLS.labels("fallback").inc()
            if snap.prev_close and not self.prev_close:
                self.prev_close = snap.prev_close
                self.engine.set_prev_close(snap.prev_close)
                self.hub.prev_close = snap.prev_close
            return [Tick(int(now.timestamp()), snap.current, 0.0, "indices")]
        except PSXError as exc:
            log.warning("indices fallback failed: %s", exc)
            return []

    async def cycle(self) -> float:
        """Run one poll cycle. Returns real seconds to wait before the next one."""
        now = self.clock.now()
        await self._ensure_day(now)
        session = self.cal.current_session(now, with_grace=True)
        m.MARKET_OPEN.set(1 if session else 0)
        if session is None:
            await self._close_bar()
            return await self._idle(now)

        ticks = await self._fetch(now)
        today = self.day
        fresh = [t for t in ticks
                 if self._pkt_date(t.ts) == today and (self.last_tick_ts is None or t.ts > self.last_tick_ts)]
        cutoff = int(now.timestamp()) - CATCHUP_WINDOW_S if not self.caught_up else None
        if self.failures == 0:  # only a real intraday response proves history has been replayed
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
            self.last_tick_ts, self.last_value = t.ts, t.value
            self.hub.on_tick(t, self.bars.current)
        self.store.add_ticks(fresh)

        now_ts = int(now.timestamp())
        # Staleness only applies inside the scheduled session, not the post-close grace window.
        live = self.cal.current_session(now)
        alerts += self.feed.check(now_ts, int(live.start.timestamp()) if live else None,
                                  self.last_tick_ts, self.failures, self.last_value)
        for a in alerts:
            self._emit(a)

        self._update_status(now, session_open=True)
        await self.hub.push_update(new_bars, alerts)

        if self.failures:
            backoff = min(self.s.source.max_backoff_s, self.s.source.poll_interval_s * 2 ** min(self.failures, 6))
            return backoff * random.uniform(0.8, 1.2)
        return self.s.source.poll_interval_s

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
        return max(0.5, min(60.0, self.clock.real_seconds(remaining)))

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
                    wait = self.s.source.poll_interval_s
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
