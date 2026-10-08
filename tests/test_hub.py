import asyncio
import json

from kmi30.hub import MAX_TICKS, Hub
from kmi30.models import Tick


class FakeSocket:
    def __init__(self):
        self.sent = []

    async def send_text(self, data):
        self.sent.append(json.loads(data))


def test_readings_flow_to_snapshot_and_updates_once():
    hub = Hub(18000, simulated=False)
    hub.reset_day("2026-09-14", 100.0, [], [])
    hub.on_tick(Tick(1, 100.0), None)
    hub.on_tick(Tick(2, 101.0), None)
    assert hub.snapshot()["ticks"] == [[1, 100.0], [2, 101.0]]
    hub.mark_synced()  # readings already in a snapshot are not re-sent
    hub.on_tick(Tick(3, 102.0), None)
    ws = FakeSocket()
    hub.clients.add(ws)
    asyncio.run(hub.push_update([], []))
    asyncio.run(hub.push_update([], []))
    assert ws.sent[0]["ticks"] == [[3, 102.0]]
    assert ws.sent[1]["ticks"] == []


def test_reading_history_is_capped_and_reset_daily():
    hub = Hub(18000, simulated=False)
    for i in range(MAX_TICKS + 5):
        hub.on_tick(Tick(i, 1.0), None)
    assert len(hub.ticks) == MAX_TICKS and hub.ticks[0][0] == 5
    hub.reset_day("2026-09-15", None, [], [])
    assert hub.ticks == [] and hub.snapshot()["ticks"] == []
