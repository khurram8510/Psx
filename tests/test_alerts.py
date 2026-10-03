import json
from zoneinfo import ZoneInfo

import httpx
import pytest

from kmi30 import alerts as alerts_mod
from kmi30.alerts import SlackNotifier, build_payload
from kmi30.config import SlackConfig
from kmi30.models import Alert, Severity

from .helpers import START

TZ = ZoneInfo("Asia/Karachi")
URL = "https://hooks.slack.test/services/T/B/X"


def alert(sev=Severity.CRITICAL):
    return Alert("level", "level:down", sev, "KMI-30 down 2.10% vs previous close", "detail", START, 254_540.0,
                 {"change_pct": -2.1}, -1)


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    async def instant(_):
        return None
    monkeypatch.setattr(alerts_mod.asyncio, "sleep", instant)


def test_payload_shape():
    p = build_payload(alert(), SlackConfig(dashboard_url="http://dash"), TZ, 260_000.0, simulated=False)
    assert p["text"].startswith("[CRITICAL]")
    att = p["attachments"][0]
    assert att["color"] == "#d62f2f"
    fields = att["blocks"][1]["fields"]
    assert "254,540.00" in fields[0]["text"]
    assert "-2.10%" in fields[1]["text"]
    assert "10:00" in fields[3]["text"]
    assert att["blocks"][-1]["elements"][0]["url"] == "http://dash"
    assert build_payload(alert(), SlackConfig(), TZ, None, True)["text"].startswith("[SIMULATED]")


async def test_retries_on_429_then_succeeds():
    calls = []

    def handler(req):
        calls.append(json.loads(req.content))
        if len(calls) == 1:
            return httpx.Response(429, headers={"Retry-After": "1"})
        return httpx.Response(200, text="ok")

    n = SlackNotifier(SlackConfig(webhook_url=URL), TZ, transport=httpx.MockTransport(handler))
    assert await n.send(alert(), 260_000.0)
    assert len(calls) == 2
    await n.aclose()


async def test_no_retry_on_4xx_and_gives_up_on_5xx():
    hits = {"n": 0}

    def bad(req):
        hits["n"] += 1
        return httpx.Response(404, text="no_service")

    n = SlackNotifier(SlackConfig(webhook_url=URL), TZ, transport=httpx.MockTransport(bad))
    assert not await n.send(alert(), None)
    assert hits["n"] == 1
    await n.aclose()

    hits["n"] = 0

    def down(req):
        hits["n"] += 1
        return httpx.Response(503)

    n = SlackNotifier(SlackConfig(webhook_url=URL, max_retries=2), TZ, transport=httpx.MockTransport(down))
    assert not await n.send(alert(), None)
    assert hits["n"] == 3
    await n.aclose()


async def test_disabled_and_min_severity():
    n = SlackNotifier(SlackConfig(), TZ)
    assert not n.enabled and not n.wants(alert())
    n2 = SlackNotifier(SlackConfig(webhook_url=URL, min_severity="critical"), TZ)
    assert not n2.wants(alert(Severity.WARNING)) and n2.wants(alert(Severity.CRITICAL))
    await n.aclose()
    await n2.aclose()
