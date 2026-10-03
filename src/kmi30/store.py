"""SQLite persistence (WAL mode). Single writer: the agent loop."""
from __future__ import annotations

import json
import sqlite3
import threading
from pathlib import Path

from .models import Alert, Bar, EodRow, Severity, Tick

_SCHEMA = """
CREATE TABLE IF NOT EXISTS ticks (
  ts INTEGER PRIMARY KEY, value REAL NOT NULL, volume REAL NOT NULL DEFAULT 0, source TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS bars (
  ts INTEGER PRIMARY KEY, open REAL, high REAL, low REAL, close REAL, volume REAL, ticks INTEGER
);
CREATE TABLE IF NOT EXISTS eod (
  ts INTEGER PRIMARY KEY, close REAL NOT NULL, volume REAL, open REAL
);
CREATE TABLE IF NOT EXISTS alerts (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ts INTEGER NOT NULL, detector TEXT NOT NULL, key TEXT NOT NULL, severity INTEGER NOT NULL,
  title TEXT NOT NULL, detail TEXT NOT NULL, value REAL, metrics TEXT, delivered INTEGER NOT NULL DEFAULT 0,
  direction INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS alerts_ts ON alerts(ts);
"""


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

    def close(self) -> None:
        with self._lock:
            self._db.close()

    def add_ticks(self, ticks: list[Tick]) -> None:
        if not ticks:
            return
        with self._lock:
            self._db.executemany(
                "INSERT OR REPLACE INTO ticks(ts, value, volume, source) VALUES (?,?,?,?)",
                [(t.ts, t.value, t.volume, t.source) for t in ticks],
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
                "SELECT ts, value, volume, source FROM ticks WHERE ts >= ? AND ts < ? ORDER BY ts",
                (start_ts, end_ts),
            ).fetchall()
        return [Tick(*r) for r in rows]

    def upsert_eod(self, rows: list[EodRow]) -> None:
        if not rows:
            return
        with self._lock:
            self._db.executemany(
                "INSERT OR REPLACE INTO eod(ts, close, volume, open) VALUES (?,?,?,?)",
                [(r.ts, r.close, r.volume, r.open) for r in rows],
            )

    def eod(self) -> list[EodRow]:
        with self._lock:
            rows = self._db.execute("SELECT ts, close, volume, open FROM eod ORDER BY ts").fetchall()
        return [EodRow(*r) for r in rows]

    def add_alert(self, a: Alert, delivered: bool) -> None:
        with self._lock:
            self._db.execute(
                "INSERT INTO alerts(ts, detector, key, severity, title, detail, value, metrics, delivered, direction)"
                " VALUES (?,?,?,?,?,?,?,?,?,?)",
                (a.ts, a.detector, a.key, int(a.severity), a.title, a.detail, a.value,
                 json.dumps(a.metrics), int(delivered), a.direction),
            )

    def alerts_since(self, ts: int) -> list[Alert]:
        with self._lock:
            rows = self._db.execute(
                "SELECT detector, key, severity, title, detail, ts, value, metrics, direction FROM alerts"
                " WHERE ts >= ? ORDER BY ts",
                (ts,),
            ).fetchall()
        return [
            Alert(d, k, Severity(s), t, det, ts_, v or 0.0, json.loads(m or "{}"), dr)
            for d, k, s, t, det, ts_, v, m, dr in rows
        ]

    def prune(self, before_ts: int) -> None:
        with self._lock:
            self._db.execute("DELETE FROM ticks WHERE ts < ?", (before_ts,))
            self._db.execute("DELETE FROM bars WHERE ts < ?", (before_ts,))
            self._db.execute("DELETE FROM alerts WHERE ts < ?", (before_ts,))
