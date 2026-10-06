"""Client for the PSX Data Portal (dps.psx.com.pk).

The portal has no published API contract. Known behaviour, from community documentation:

* ``GET /indices`` is an HTML table: index, high, low, current, change, % change. This is the
  default feed: each poll yields the current value, day high/low, and the change vs previous
  close (so previous close = current - change).
* ``GET /timeseries/int/{SYM}`` (opt-in feed) ->
  ``{"status":1,"message":"","data":[[epoch, value, volume], ...]}`` newest first, current trading
  day only. Epochs are Pakistan wall-clock seconds (UTC+5 read as UTC), not true UTC.
* No WAF, but request bursts make connections hang, so callers must stay polite.
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Literal

import httpx
from bs4 import BeautifulSoup

from .config import SourceConfig
from .market_calendar import MarketCalendar
from .models import Tick

log = logging.getLogger(__name__)

PKT_OFFSET_S = 5 * 3600


class PSXError(RuntimeError):
    """Transport-level or HTTP failure talking to the portal."""


class PSXSchemaError(PSXError):
    """The portal answered, but not in the shape we expect. Treat as a contract break."""


@dataclass(frozen=True, slots=True)
class IndexSnapshot:
    symbol: str
    current: float
    high: float | None
    low: float | None
    change: float | None
    change_pct: float | None

    @property
    def prev_close(self) -> float | None:
        return self.current - self.change if self.change is not None else None


def _num(text: str | None) -> float | None:
    if text is None:
        return None
    cleaned = re.sub(r"[^0-9.\-]", "", text.replace("−", "-"))
    if cleaned in ("", "-", ".", "-."):
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


def _norm_symbol(text: str) -> str:
    return re.sub(r"[^A-Z0-9]", "", text.upper())


class PSXClient:
    def __init__(
        self,
        cfg: SourceConfig,
        calendar: MarketCalendar,
        transport: httpx.AsyncBaseTransport | None = None,
    ):
        self.cfg = cfg
        self.calendar = calendar
        self._ts_mode: Literal["pkt_wallclock", "utc"] | None = (
            None if cfg.timestamp_mode == "auto" else cfg.timestamp_mode
        )
        self._http = httpx.AsyncClient(
            base_url=cfg.base_url,
            timeout=httpx.Timeout(cfg.request_timeout_s),
            headers={
                "User-Agent": cfg.user_agent,
                "Accept": "application/json, text/html;q=0.9",
                "X-Requested-With": "XMLHttpRequest",
            },
            transport=transport,
            follow_redirects=True,
        )

    async def aclose(self) -> None:
        await self._http.aclose()

    @property
    def timestamp_mode(self) -> str | None:
        return self._ts_mode

    async def _get(self, path: str) -> httpx.Response:
        try:
            resp = await self._http.get(path)
        except httpx.HTTPError as exc:
            raise PSXError(f"GET {path} failed: {exc!r}") from exc
        if resp.status_code != 200:
            raise PSXError(f"GET {path} returned HTTP {resp.status_code}")
        return resp

    async def _get_series(self, path: str) -> list[list[Any]]:
        resp = await self._get(path)
        try:
            body = resp.json()
        except ValueError as exc:
            raise PSXSchemaError(f"{path}: response is not JSON") from exc
        if not isinstance(body, dict) or "data" not in body:
            raise PSXSchemaError(f"{path}: missing 'data' key")
        if body.get("status") != 1:
            raise PSXError(f"{path}: status={body.get('status')!r} message={body.get('message')!r}")
        data = body["data"]
        if not isinstance(data, list):
            raise PSXSchemaError(f"{path}: 'data' is not a list")
        return data

    def _resolve_ts_mode(self, raw_epochs: list[int]) -> Literal["pkt_wallclock", "utc"]:
        """Pick the timestamp interpretation that puts more ticks inside trading sessions.

        Ties go to ``pkt_wallclock``, the documented portal behaviour. The decision sticks
        once made from a non-trivial sample so it cannot flip mid-session.
        """
        if self._ts_mode is not None:
            return self._ts_mode
        sample = raw_epochs[-200:]

        def in_session(epochs: list[int]) -> int:
            return sum(
                1
                for e in epochs
                if self.calendar.is_open(datetime.fromtimestamp(e, tz=timezone.utc), with_grace=True)
            )

        wall = in_session([e - PKT_OFFSET_S for e in sample])
        utc = in_session(sample)
        mode: Literal["pkt_wallclock", "utc"] = "utc" if utc > wall else "pkt_wallclock"
        if len(sample) >= 10 and max(wall, utc) > 0:
            self._ts_mode = mode
            log.info("timestamp mode resolved to %s (in-session ticks wall=%d utc=%d)", mode, wall, utc)
        return mode

    def _to_utc(self, raw: int, mode: str) -> int:
        return raw - PKT_OFFSET_S if mode == "pkt_wallclock" else raw

    async def intraday(self) -> list[Tick]:
        """Today's intraday series, oldest first, timestamps in true UTC."""
        path = f"/timeseries/int/{self.cfg.symbol}"
        rows = await self._get_series(path)
        parsed: list[tuple[int, float, float]] = []
        for row in rows:
            if not isinstance(row, (list, tuple)) or len(row) < 2:
                raise PSXSchemaError(f"{path}: unexpected row shape {row!r}")
            try:
                ts, value = int(row[0]), float(row[1])
                vol = float(row[2]) if len(row) > 2 and row[2] is not None else 0.0
            except (TypeError, ValueError) as exc:
                raise PSXSchemaError(f"{path}: non-numeric row {row!r}") from exc
            if value <= 0:
                continue
            parsed.append((ts, value, vol))
        if not parsed:
            return []
        mode = self._resolve_ts_mode(sorted(p[0] for p in parsed))
        ticks = {self._to_utc(ts, mode): Tick(self._to_utc(ts, mode), v, vol) for ts, v, vol in parsed}
        return [ticks[k] for k in sorted(ticks)]

    async def indices_snapshot(self) -> IndexSnapshot:
        """Fallback: scrape the /indices HTML table for the configured symbol."""
        resp = await self._get("/indices")
        return parse_indices_html(resp.text, self.cfg.symbol)


_COLS = ("index", "high", "low", "current", "change", "change_pct")


def parse_indices_html(html: str, symbol: str) -> IndexSnapshot:
    soup = BeautifulSoup(html, "html.parser")
    want = _norm_symbol(symbol)
    for table in soup.find_all("table"):
        headers = [th.get_text(" ", strip=True).lower() for th in table.find_all("th")]
        colmap = _map_headers(headers) if headers else None
        for tr in table.find_all("tr"):
            cells = tr.find_all("td")
            if len(cells) < 4:
                continue
            if _norm_symbol(cells[0].get_text(" ", strip=True)) != want:
                continue
            values = [_cell_value(c) for c in cells]
            idx = colmap or {name: i for i, name in enumerate(_COLS)}
            current = values[idx["current"]] if idx.get("current") is not None else None
            if current is None or current <= 0:
                raise PSXSchemaError(f"/indices: row for {symbol} has no usable current value")

            def pick(name: str) -> float | None:
                i = idx.get(name)
                return values[i] if i is not None and i < len(values) else None

            return IndexSnapshot(symbol, current, pick("high"), pick("low"), pick("change"), pick("change_pct"))
    snap = _parse_index_strip(soup.get_text(" ", strip=True), symbol)
    if snap:
        return snap
    raise PSXSchemaError(f"/indices: no row found for {symbol}")


def _parse_index_strip(text: str, symbol: str) -> IndexSnapshot | None:
    """Fallback for the portal's header strip layout: ``KMI30 261,234.56 -1,234.44 (-0.47%)``."""
    sym = r"[\s\-]?".join(re.escape(c) for c in _norm_symbol(symbol))
    m = re.search(rf"\b{sym}\b\s+([\d,]+\.?\d*)\s+([+\-\u2212]?[\d,]+\.?\d*)\s+\(\s*([+\-\u2212]?[\d.]+)\s*%\s*\)", text)
    if not m:
        return None
    current = _num(m.group(1))
    if current is None or current <= 0:
        return None
    return IndexSnapshot(symbol, current, None, None, _num(m.group(2)), _num(m.group(3)))


def _cell_value(cell) -> float | None:
    order = cell.get("data-order")
    v = _num(order) if order is not None else None
    return v if v is not None else _num(cell.get_text(" ", strip=True))


def _map_headers(headers: list[str]) -> dict[str, int] | None:
    m: dict[str, int] = {}
    for i, h in enumerate(headers):
        if "%" in h or "percent" in h:
            m.setdefault("change_pct", i)
        elif "change" in h:
            m.setdefault("change", i)
        elif "current" in h or h in ("last", "value", "close"):
            m.setdefault("current", i)
        elif "high" in h:
            m.setdefault("high", i)
        elif "low" in h:
            m.setdefault("low", i)
        elif "index" in h or "name" in h or "symbol" in h:
            m.setdefault("index", i)
    return m if "current" in m else None
