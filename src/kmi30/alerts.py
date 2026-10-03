"""Slack delivery via Incoming Webhook, with retry and Retry-After handling."""
from __future__ import annotations

import asyncio
import logging
import random
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import httpx

from .config import SlackConfig
from .models import Alert, Severity

log = logging.getLogger(__name__)

_COLORS = {Severity.INFO: "#2f6fdb", Severity.WARNING: "#e8a400", Severity.CRITICAL: "#d62f2f"}
_EMOJI = {Severity.INFO: ":information_source:", Severity.WARNING: ":warning:", Severity.CRITICAL: ":rotating_light:"}
_MIN = {"info": Severity.INFO, "warning": Severity.WARNING, "critical": Severity.CRITICAL}


def build_payload(alert: Alert, cfg: SlackConfig, tz: ZoneInfo, prev_close: float | None, simulated: bool) -> dict:
    when = datetime.fromtimestamp(alert.ts, tz=timezone.utc).astimezone(tz)
    change = (
        f"{(alert.value / prev_close - 1) * 100:+.2f}% vs {prev_close:,.2f}" if prev_close else "n/a"
    )
    prefix = "[SIMULATED] " if simulated else ""
    headline = f"{prefix}{_EMOJI[alert.severity]} *{alert.severity.label.upper()}*  {alert.title}"
    blocks: list[dict] = [
        {"type": "section", "text": {"type": "mrkdwn", "text": headline}},
        {"type": "section", "fields": [
            {"type": "mrkdwn", "text": f"*KMI-30*\n{alert.value:,.2f}"},
            {"type": "mrkdwn", "text": f"*Change*\n{change}"},
            {"type": "mrkdwn", "text": f"*Detector*\n`{alert.detector}`"},
            {"type": "mrkdwn", "text": f"*Time (PKT)*\n{when:%Y-%m-%d %H:%M}"},
        ]},
        {"type": "context", "elements": [{"type": "mrkdwn", "text": alert.detail}]},
    ]
    if cfg.dashboard_url:
        blocks.append({"type": "actions", "elements": [{
            "type": "button", "text": {"type": "plain_text", "text": "Open live view"}, "url": cfg.dashboard_url,
        }]})
    return {
        "text": f"{prefix}[{alert.severity.label.upper()}] {alert.title}",
        "attachments": [{"color": _COLORS[alert.severity], "blocks": blocks}],
    }


class SlackNotifier:
    def __init__(self, cfg: SlackConfig, tz: ZoneInfo, transport: httpx.AsyncBaseTransport | None = None,
                 simulated: bool = False):
        self.cfg = cfg
        self.tz = tz
        self.simulated = simulated
        self.min_severity = _MIN[cfg.min_severity]
        self._http = httpx.AsyncClient(timeout=cfg.timeout_s, transport=transport)

    @property
    def enabled(self) -> bool:
        return bool(self.cfg.webhook_url)

    async def aclose(self) -> None:
        await self._http.aclose()

    def wants(self, alert: Alert) -> bool:
        return self.enabled and alert.severity >= self.min_severity

    async def send(self, alert: Alert, prev_close: float | None) -> bool:
        if not self.wants(alert):
            return False
        return await self.post(build_payload(alert, self.cfg, self.tz, prev_close, self.simulated))

    async def post(self, payload: dict) -> bool:
        if not self.cfg.webhook_url:
            log.warning("Slack webhook not configured; dropping message")
            return False
        for attempt in range(self.cfg.max_retries + 1):
            try:
                resp = await self._http.post(self.cfg.webhook_url, json=payload)
            except httpx.HTTPError as exc:
                delay = self._backoff(attempt)
                log.warning("Slack post failed (%r); retry in %.1fs", exc, delay)
            else:
                if resp.status_code == 200:
                    return True
                if resp.status_code == 429:
                    delay = float(resp.headers.get("Retry-After", self._backoff(attempt)))
                    log.warning("Slack rate limited; retry in %.1fs", delay)
                elif resp.status_code >= 500:
                    delay = self._backoff(attempt)
                    log.warning("Slack HTTP %d; retry in %.1fs", resp.status_code, delay)
                else:
                    # 4xx such as invalid_token, no_service, channel_is_archived: retrying will not help.
                    log.error("Slack rejected message: HTTP %d %s", resp.status_code, resp.text[:200])
                    return False
            if attempt < self.cfg.max_retries:
                await asyncio.sleep(delay)
        log.error("Slack delivery failed after %d attempts", self.cfg.max_retries + 1)
        return False

    @staticmethod
    def _backoff(attempt: int) -> float:
        return min(30.0, 2 ** attempt) + random.uniform(0, 0.5)
