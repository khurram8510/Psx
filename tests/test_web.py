import asyncio
from zoneinfo import ZoneInfo

from fastapi.testclient import TestClient

from kmi30.hub import Hub
from kmi30.models import Alert, Bar, DaySummary, Severity, Tick
from kmi30.store import Store
from kmi30.web import create_app

from .conftest import pkt

TZ = ZoneInfo("Asia/Karachi")


class IdleAgent:
    async def run(self, stop: asyncio.Event) -> None:
        await stop.wait()


def seeded_store() -> Store:
    store = Store(":memory:")
    t0 = int(pkt(2026, 9, 14, 10, 0).timestamp())
    store.add_ticks([Tick(t0 + 10 * i, 260_000.0 + i, 0, "indices", 260_100.0, 259_900.0, 500.0 + i, 0.19)
                     for i in range(12)])
    store.add_bar(Bar(t0, 260_000, 260_005, 260_000, 260_005))
    store.add_alert(Alert("level", "level:up", Severity.WARNING, "up", "d", t0 + 30, 260_003.0, {}, 1), True)
    store.upsert_daily(DaySummary("2026-09-14", 260_000, 260_100, 259_900, 260_011, 259_500, 12, 1, t0, t0 + 110))
    store.upsert_daily(DaySummary("2026-09-15", 260_011, 261_000, 259_000, 260_500, 260_011, 3, 0, t0 + 86400,
                                  t0 + 86500))
    return store


def client(hub=None, store=None):
    hub = hub or Hub(18000, simulated=True)
    return TestClient(create_app(IdleAgent(), hub, store=store or Store(":memory:"), tz=TZ))


def test_endpoints_and_websocket_snapshot():
    hub = Hub(18000, simulated=True)
    hub.reset_day("2026-09-14", 260_000.0, [], [])
    with client(hub) as c:
        r = c.get("/")
        assert r.status_code == 200 and "KMI-30 Live" in r.text
        assert c.get("/static/vendor/lightweight-charts.standalone.production.js").status_code == 200
        assert c.get("/healthz").status_code == 200
        hub.heartbeat -= 10_000
        assert c.get("/healthz").status_code == 503  # loop stopped cycling
        assert c.get("/readyz").status_code == 503  # no poll has completed yet
        hub.status = {"market_open": True, "last_tick_age_s": 5}
        assert c.get("/readyz").status_code == 200
        hub.status = {"market_open": True, "last_tick_age_s": 900}
        assert c.get("/readyz").status_code == 503
        assert c.get("/api/state").json()["prev_close"] == 260_000.0
        assert "kmi30_polls_total" in c.get("/metrics").text
        with c.websocket_connect("/ws") as ws:
            msg = ws.receive_json()
            assert msg["type"] == "snapshot" and msg["simulated"] is True


def test_history_days_and_day_payload():
    with client(store=seeded_store()) as c:
        days = c.get("/api/history/days").json()["days"]
        assert [d["day"] for d in days] == ["2026-09-15", "2026-09-14"]
        assert days[1]["change"] == 511.0 and round(days[1]["change_pct"], 3) == 0.197
        assert c.get("/api/history/days?limit=1").json()["days"][0]["day"] == "2026-09-15"

        day = c.get("/api/history/2026-09-14").json()
        assert len(day["ticks"]) == 12 and day["ticks"][0] == [int(pkt(2026, 9, 14, 10, 0).timestamp()), 260_000.0]
        assert len(day["bars"]) == 1 and len(day["alerts"]) == 1
        assert day["prev_close"] == 259_500 and day["summary"]["close"] == 260_011
        assert day["tz_offset_s"] == 18000

        assert c.get("/api/history/2026-09-20").status_code == 404
        assert c.get("/api/history/14-09-2026").status_code == 400
        assert c.get("/api/history/2026-13-01").status_code == 400


def test_history_csv_exports():
    with client(store=seeded_store()) as c:
        r = c.get("/api/history/2026-09-14/readings.csv")
        assert r.status_code == 200 and r.headers["content-type"].startswith("text/csv")
        assert 'filename="kmi30-2026-09-14.csv"' in r.headers["content-disposition"]
        lines = r.text.strip().splitlines()
        assert lines[0].startswith("time_pkt,epoch_utc,value,day_high,day_low,change,change_pct,source")
        assert lines[1].startswith("2026-09-14 10:00:00,") and ",260000.00,260100.00,259900.00,500.00," in lines[1]
        assert len(lines) == 13

        daily = c.get("/api/history/daily.csv").text.strip().splitlines()
        assert daily[1].startswith("2026-09-14,") and daily[2].startswith("2026-09-15,")
        only = c.get("/api/history/daily.csv?from=2026-09-15").text.strip().splitlines()
        assert len(only) == 2
        assert c.get("/api/history/2026-09-20/readings.csv").status_code == 404
        assert c.get("/api/history/daily.csv?from=bad").status_code == 400
