"""Read-side helpers for stored history, shared by the web API and the CLI."""
from __future__ import annotations

import csv
import io
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from .hub import _bar
from .models import DaySummary, Tick
from .store import Store


def parse_day(value: str) -> date:
    """Strict YYYY-MM-DD parse; raises ValueError otherwise."""
    if len(value) != 10:
        raise ValueError(f"expected YYYY-MM-DD, got {value!r}")
    return date.fromisoformat(value)


def day_bounds(d: date, tz: ZoneInfo) -> tuple[int, int]:
    """UTC epoch range [start, end) of a local calendar day."""
    start = datetime.combine(d, datetime.min.time(), tz)
    end = datetime.combine(d + timedelta(days=1), datetime.min.time(), tz)
    return int(start.timestamp()), int(end.timestamp())


def day_payload(store: Store, d: date, tz: ZoneInfo) -> dict | None:
    """Everything the dashboard needs to redraw a past day, or None when nothing was stored."""
    start, end = day_bounds(d, tz)
    ticks = store.ticks_between(start, end)
    summary = store.daily(d.isoformat())
    if not ticks and summary is None:
        return None
    prev = summary.prev_close if summary and summary.prev_close else store.prev_close(d.isoformat())
    return {
        "day": d.isoformat(),
        "summary": summary.to_dict() if summary else None,
        "prev_close": prev,
        "ticks": [[t.ts, t.value] for t in ticks],
        "bars": [_bar(b) for b in store.bars_between(start, end)],
        "alerts": [a.to_dict() for a in store.alerts_between(start, end)],
    }


READINGS_HEADER = ["time_pkt", "epoch_utc", "value", "day_high", "day_low", "change", "change_pct", "source"]
DAILY_HEADER = ["day", "open", "high", "low", "close", "prev_close", "change", "change_pct",
                "readings", "alerts", "first_reading_pkt", "last_reading_pkt"]


def _r(v: float | None, nd: int = 2) -> str:
    return "" if v is None else f"{v:.{nd}f}"


def _local(ts: int, tz: ZoneInfo) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).astimezone(tz).strftime("%Y-%m-%d %H:%M:%S")


def readings_csv(ticks: list[Tick], tz: ZoneInfo) -> str:
    out = io.StringIO()
    w = csv.writer(out, lineterminator="\n")
    w.writerow(READINGS_HEADER)
    for t in ticks:
        w.writerow([_local(t.ts, tz), t.ts, _r(t.value), _r(t.high), _r(t.low), _r(t.change),
                    _r(t.change_pct, 3), t.source])
    return out.getvalue()


def daily_csv(rows: list[DaySummary], tz: ZoneInfo) -> str:
    """Daily summaries, oldest first."""
    out = io.StringIO()
    w = csv.writer(out, lineterminator="\n")
    w.writerow(DAILY_HEADER)
    for s in sorted(rows, key=lambda r: r.day):
        w.writerow([s.day, _r(s.open), _r(s.high), _r(s.low), _r(s.close), _r(s.prev_close), _r(s.change),
                    _r(s.change_pct, 3), s.readings, s.alerts, _local(s.first_ts, tz), _local(s.last_ts, tz)])
    return out.getvalue()
