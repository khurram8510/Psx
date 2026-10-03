import asyncio

from fastapi.testclient import TestClient

from kmi30.hub import Hub
from kmi30.web import create_app


class IdleAgent:
    async def run(self, stop: asyncio.Event) -> None:
        await stop.wait()


def test_endpoints_and_websocket_snapshot():
    hub = Hub(18000, simulated=True)
    hub.reset_day("2026-09-14", 260_000.0, [], [])
    with TestClient(create_app(IdleAgent(), hub)) as c:
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
