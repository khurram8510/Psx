"""Synthetic PSX Data Portal for demos and offline testing.

Serves the same JSON/HTML shapes as dps.psx.com.pk through an httpx MockTransport, so the
real client, parser, timestamp detection and agent loop are all exercised. Each simulated
day injects a sharp drop, a sustained trend reversal and a volatility burst.
"""
from __future__ import annotations

import json
import math
import random
from datetime import date, timedelta

import httpx

from .clock import SimClock
from .market_calendar import MarketCalendar
from .psx_client import PKT_OFFSET_S

STEP_S = 15
BASE_LEVEL = 260_000.0
SIGMA_1M = 0.0006


class SimulatedPortal:
    def __init__(self, calendar: MarketCalendar, clock: SimClock, symbol: str = "KMI30", seed: int = 7):
        self.calendar = calendar
        self.clock = clock
        self.symbol = symbol
        self.seed = seed
        self._days: dict[date, list[tuple[int, float, float]]] = {}
        self._closes: dict[date, float] = {}

    def transport(self) -> httpx.MockTransport:
        return httpx.MockTransport(self._handle)

    def _trading_days_before(self, d: date, n: int) -> list[date]:
        out, cur = [], d
        while len(out) < n:
            cur -= timedelta(days=1)
            if self.calendar.is_trading_day(cur):
                out.append(cur)
        return list(reversed(out))

    def _close_for(self, d: date) -> float:
        if d not in self._closes:
            rng = random.Random(f"{self.seed}-close-{d.isoformat()}")
            idx = (d - date(2020, 1, 1)).days
            self._closes[d] = BASE_LEVEL * math.exp(0.02 * math.sin(idx / 9) + rng.gauss(0, 0.011))
        return self._closes[d]

    def _path(self, d: date) -> list[tuple[int, float, float]]:
        if d in self._days:
            return self._days[d]
        rng = random.Random(f"{self.seed}-int-{d.isoformat()}")
        prev = self._close_for(self._trading_days_before(d, 1)[0])
        log_v = math.log(prev) + rng.gauss(0, 0.002)
        out: list[tuple[int, float, float]] = []
        sessions = self.calendar.sessions_on(d)
        if not sessions:
            self._days[d] = out
            return out
        open_ts = int(sessions[0].start.timestamp())
        step_sigma = SIGMA_1M * math.sqrt(STEP_S / 60)
        for s in sessions:
            t = int(s.start.timestamp())
            while t < int(s.end.timestamp()):
                m = (t - open_ts) / 60
                drift, sig = 0.0, step_sigma
                if 45 <= m < 48:
                    drift = -0.013 / (3 * 60 / STEP_S)  # sharp drop: -1.3% in 3 minutes
                elif 90 <= m < 150:
                    drift = 0.0004 * STEP_S / 60  # sustained rally: about +2.4% over an hour
                elif 200 <= m < 215:
                    sig = step_sigma * 4  # volatility burst
                log_v += drift + rng.gauss(0, sig)
                out.append((t, round(math.exp(log_v), 2), float(rng.randint(5_000, 60_000))))
                t += STEP_S
        self._days[d] = out
        self._closes[d] = out[-1][1]
        return out

    def _handle(self, request: httpx.Request) -> httpx.Response:
        now = self.clock.now()
        today = self.calendar.local(now).date()
        path = request.url.path
        now_ts = int(now.timestamp())
        if path == f"/timeseries/int/{self.symbol}":
            pts = [p for p in self._path(today) if p[0] <= now_ts]
            data = [[ts + PKT_OFFSET_S, v, vol] for ts, v, vol in reversed(pts)]
            return httpx.Response(200, json={"status": 1, "message": "", "data": data})
        if path == "/indices":
            pts = [p for p in self._path(today) if p[0] <= now_ts]
            prev = self._close_for(self._trading_days_before(today, 1)[0])
            cur = pts[-1][1] if pts else prev
            hi = max((p[1] for p in pts), default=cur)
            lo = min((p[1] for p in pts), default=cur)
            html = (
                "<table><thead><tr><th>Index</th><th>High</th><th>Low</th><th>Current</th>"
                "<th>Change</th><th>% Change</th></tr></thead><tbody>"
                f"<tr><td>{self.symbol}</td><td>{hi:,.2f}</td><td>{lo:,.2f}</td><td>{cur:,.2f}</td>"
                f"<td>{cur - prev:,.2f}</td><td>{(cur / prev - 1) * 100:.2f}%</td></tr></tbody></table>"
            )
            return httpx.Response(200, text=html, headers={"content-type": "text/html"})
        return httpx.Response(404, text=json.dumps({"status": 0, "message": "not found"}))
