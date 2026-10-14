"""SQLite persistence (WAL mode). Single writer: the agent loop."""
from __future__ import annotations

import json
import sqlite3
import threading
from pathlib import Path

from .models import Alert, Bar, DaySummary, Severity, Tick

_SCHEMA = """
CREATE TABLE IF NOT EXISTS ticks (
  ts INTEGER PRIMARY KEY, value REAL NOT NULL, volume REAL NOT NULL DEFAULT 0, source TEXT NOT NULL,
  high REAL, low REAL, change REAL, change_pct REAL
);
CREATE TABLE IF NOT EXISTS daily (
  day TEXT PRIMARY KEY, open REAL NOT NULL, high REAL NOT NULL, low REAL NOT NULL, close REAL NOT NULL,
  prev_close REAL, readings INTEGER NOT NULL, alerts INTEGER NOT NULL, first_ts INTEGER NOT NULL,
  last_ts INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS bars (
  ts INTEGER PRIMARY KEY, open REAL, high REAL, low REAL, close REAL, volume REAL, ticks INTEGER
);
CREATE TABLE IF NOT EXISTS sessions (
  day TEXT PRIMARY KEY, prev_close REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS alerts (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ts INTEGER NOT NULL, detector TEXT NOT NULL, key TEXT NOT NULL, severity INTEGER NOT NULL,
  title TEXT NOT NULL, detail TEXT NOT NULL, value REAL, metrics TEXT, delivered INTEGER NOT NULL DEFAULT 0,
  direction INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS alerts_ts ON alerts(ts);
"""
_TICK_COLS = "ts, value, volume, source, high, low, change, change_pct"
_DAILY_COLS = "day, open, high, low, close, prev_close, readings, alerts, first_ts, last_ts"


class Store:
    def __init__(self, path: str):
        if path != ":memory:":
            Path(path).parent.mkdir(parents=True, exist_ok=True)
        self._db = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
        self._lock = threading.Lock()
        with self._lock:
            self._db.execute("PRAGMA journal_mode=WAL")
            self._db.execute("PRAGMA synchronous=NORMAL")
            self._db.executescript(_SCHEMA)
            self._migrate()

    def _migrate(self) -> None:
        """Add columns introduced after a database was first created. Idempotent."""
        cols = {r[1] for r in self._db.execute("PRAGMA table_info(ticks)")}
        for name in ("high", "low", "change", "change_pct"):
            if name not in cols:
                self._db.execute(f"ALTER TABLE ticks ADD COLUMN {name} REAL")

    def close(self) -> None:
        with self._lock:
            self._db.close()

    def add_ticks(self, ticks: list[Tick]) -> None:
        if not ticks:
            return
        with self._lock:
            self._db.executemany(
                "INSERT OR REPLACE INTO ticks(ts, value, volume, source, high, low, change, change_pct)"
                " VALUES (?,?,?,?,?,?,?,?)",
                [(t.ts, t.value, t.volume, t.source, t.high, t.low, t.change, t.change_pct) for t in ticks],
            )

    def add_bar(self, b: Bar) -> None:
        with self._lock:
            self._db.execute(
                "INSERT OR REPLACE INTO bars VALUES (?,?,?,?,?,?,?)",
                (b.ts, b.open, b.high, b.low, b.close, b.volume, b.ticks),
            )

    def bars_between(self, start_ts: int, end_ts: int) -> list[Bar]:
        with self._lock:
            rows = self._db.execute(
                "SELECT ts, open, high, low, close, volume, ticks FROM bars WHERE ts >= ? AND ts < ? ORDER BY ts",
                (start_ts, end_ts),
            ).fetchall()
        return [Bar(*r) for r in rows]

    def ticks_between(self, start_ts: int, end_ts: int) -> list[Tick]:
        with self._lock:
            rows = self._db.execute(
                f"SELECT {_TICK_COLS} FROM ticks WHERE ts >= ? AND ts < ? ORDER BY ts",
                (start_ts, end_ts),
            ).fetchall()
        return [Tick(*r) for r in rows]

    def set_prev_close(self, day: str, value: float) -> None:
        with self._lock:
            self._db.execute("INSERT OR REPLACE INTO sessions(day, prev_close) VALUES (?,?)", (day, value))

    def prev_close(self, day: str) -> float | None:
        with self._lock:
            row = self._db.execute("SELECT prev_close FROM sessions WHERE day = ?", (day,)).fetchone()
        return row[0] if row else None

    def prev_closes(self, through_day: str, limit: int) -> list[float]:
        """Previous closes recorded for sessions up to and including ``through_day``, oldest first.

        Consecutive sessions' previous closes form a daily close series for volatility estimates.
        """
        with self._lock:
            rows = self._db.execute(
                "SELECT prev_close FROM sessions WHERE day <= ? ORDER BY day DESC LIMIT ?", (through_day, limit)
            ).fetchall()
        return [r[0] for r in reversed(rows)]

    def last_tick_before(self, ts: int) -> Tick | None:
        with self._lock:
            row = self._db.execute(
                f"SELECT {_TICK_COLS} FROM ticks WHERE ts < ? ORDER BY ts DESC LIMIT 1", (ts,)
            ).fetchone()
        return Tick(*row) if row else None

    def add_alert(self, a: Alert, delivered: bool) -> None:
        with self._lock:
            self._db.execute(
                "INSERT INTO alerts(ts, detector, key, severity, title, detail, value, metrics, delivered, direction)"
                " VALUES (?,?,?,?,?,?,?,?,?,?)",
                (a.ts, a.detector, a.key, int(a.severity), a.title, a.detail, a.value,
                 json.dumps(a.metrics), int(delivered), a.direction),
            )

    def alerts_since(self, ts: int) -> list[Alert]:
        return self.alerts_between(ts, 2**62)

    def alerts_between(self, start_ts: int, end_ts: int) -> list[Alert]:
        with self._lock:
            rows = self._db.execute(
                "SELECT detector, key, severity, title, detail, ts, value, metrics, direction FROM alerts"
                " WHERE ts >= ? AND ts < ? ORDER BY ts",
                (start_ts, end_ts),
            ).fetchall()
        return [
            Alert(d, k, Severity(s), t, det, ts_, v or 0.0, json.loads(m or "{}"), dr)
            for d, k, s, t, det, ts_, v, m, dr in rows
        ]

    def upsert_daily(self, d: DaySummary) -> None:
        with self._lock:
            self._db.execute(
                f"INSERT OR REPLACE INTO daily({_DAILY_COLS}) VALUES (?,?,?,?,?,?,?,?,?,?)",
                (d.day, d.open, d.high, d.low, d.close, d.prev_close, d.readings, d.alerts, d.first_ts, d.last_ts),
            )

    def daily(self, day: str) -> DaySummary | None:
        with self._lock:
            row = self._db.execute(f"SELECT {_DAILY_COLS} FROM daily WHERE day = ?", (day,)).fetchone()
        return DaySummary(*row) if row else None

    def daily_range(self, start_day: str = "0000-00-00", end_day: str = "9999-99-99",
                    limit: int | None = None) -> list[DaySummary]:
        """Daily summaries between two days inclusive, newest first."""
        sql = f"SELECT {_DAILY_COLS} FROM daily WHERE day >= ? AND day <= ? ORDER BY day DESC"
        args: tuple = (start_day, end_day)
        if limit is not None:
            sql += " LIMIT ?"
            args += (limit,)
        with self._lock:
            rows = self._db.execute(sql, args).fetchall()
        return [DaySummary(*r) for r in rows]

    def prune(self, before_ts: int) -> None:
        with self._lock:
            self._db.execute("DELETE FROM ticks WHERE ts < ?", (before_ts,))
            self._db.execute("DELETE FROM bars WHERE ts < ?", (before_ts,))
            self._db.execute("DELETE FROM alerts WHERE ts < ?", (before_ts,))
