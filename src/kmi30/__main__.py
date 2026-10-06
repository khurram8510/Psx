"""CLI entry point: run | check | replay | slack-test."""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
from datetime import date, datetime, timedelta, timezone

from .config import Settings, load_settings
from .market_calendar import MarketCalendar


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        out = {
            "ts": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        if record.exc_info:
            out["exc"] = self.formatException(record.exc_info)
        return json.dumps(out)


def setup_logging(level: str, as_json: bool) -> None:
    h = logging.StreamHandler(sys.stdout)
    h.setFormatter(JsonFormatter() if as_json else logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
    logging.basicConfig(level=level.upper(), handlers=[h], force=True)
    logging.getLogger("httpx").setLevel(logging.WARNING)


def _components(s: Settings):
    from .agent import Agent
    from .alerts import SlackNotifier
    from .clock import Clock, SimClock
    from .hub import Hub
    from .psx_client import PSXClient
    from .simulator import SimulatedPortal
    from .store import Store

    cal = MarketCalendar(s.market)
    simulated = s.source.mode == "simulated"
    transport = None
    clock: Clock = Clock()
    if simulated:
        now = datetime.now(timezone.utc)
        start = cal.next_open(now - timedelta(days=1)) or now
        clock = SimClock(start - timedelta(minutes=1), s.source.sim_speed)
        transport = SimulatedPortal(cal, clock, s.source.symbol).transport()
    store = Store(s.storage.path)
    client = PSXClient(s.source, cal, transport=transport)
    notifier = SlackNotifier(s.slack, cal.tz, simulated=simulated)
    offset = int(datetime.now(cal.tz).utcoffset().total_seconds())
    hub = Hub(offset, simulated)
    agent = Agent(s, cal, store, client, notifier, hub, clock)

    async def shutdown() -> None:
        await client.aclose()
        await notifier.aclose()
        store.close()

    return agent, hub, shutdown


def cmd_run(s: Settings) -> int:
    import uvicorn

    from .web import create_app

    agent, hub, shutdown = _components(s)
    app = create_app(agent, hub, on_shutdown=shutdown)
    logging.getLogger(__name__).info(
        "starting KMI-30 agent: source=%s slack=%s web=%s:%d",
        s.source.mode, "on" if s.slack.webhook_url else "off", s.web.host, s.web.port,
    )
    uvicorn.run(app, host=s.web.host, port=s.web.port, log_config=None, ws_ping_interval=20)
    return 0


async def cmd_check(s: Settings) -> int:
    """One-shot connectivity and contract check against the live PSX portal."""
    from .psx_client import PSXClient, PSXError

    cal = MarketCalendar(s.market)
    client = PSXClient(s.source, cal)
    rc = 0
    try:
        print(f"feed: {s.source.feed}")
        try:
            snap = await client.indices_snapshot()
            print(f"[ OK ] {s.source.base_url}/indices: {snap.symbol} current={snap.current:,.2f} "
                  f"high={snap.high} low={snap.low} change={snap.change} ({snap.change_pct}%) "
                  f"prev_close={snap.prev_close}")
            if snap.prev_close is None:
                print("[WARN] no change figure parsed: level alerts will use the last stored value as previous close")
        except PSXError as exc:
            print(f"[FAIL] {s.source.base_url}/indices: {exc}")
            rc = 1
        if s.source.feed == "intraday":
            try:
                ticks = await client.intraday()
                print(f"[ OK ] intraday: {len(ticks)} rows; newest={ticks[-1] if ticks else None}; "
                      f"timestamp mode={client.timestamp_mode}")
            except PSXError as exc:
                print(f"[FAIL] intraday: {exc}")
                rc = 1
    finally:
        await client.aclose()
    return rc


async def cmd_replay(s: Settings, day: str | None, ticks_file: str | None, notify: bool,
                     prev_close_arg: float | None) -> int:
    """Replay a stored day (or a PSX-shaped intraday JSON file) through the detectors."""
    from .alerts import SlackNotifier
    from .bars import BarBuilder
    from .engine import DetectionEngine, daily_variance_from_closes
    from .models import Tick
    from .psx_client import PKT_OFFSET_S
    from .store import Store

    cal = MarketCalendar(s.market)
    store = Store(s.storage.path)
    if ticks_file:
        with open(ticks_file) as fh:
            body = json.load(fh)
        rows = body["data"] if isinstance(body, dict) else body
        ticks = sorted((Tick(int(r[0]) - PKT_OFFSET_S, float(r[1]), float(r[2]) if len(r) > 2 else 0.0)
                        for r in rows), key=lambda t: t.ts)
        d = cal.local(ticks[0].dt).date() if ticks else date.today()
    else:
        d = date.fromisoformat(day) if day else cal.local(datetime.now(timezone.utc)).date()
        start = int(datetime.combine(d, datetime.min.time(), cal.tz).timestamp())
        ticks = store.ticks_between(start, start + 86400)
    if not ticks:
        print("no ticks to replay")
        return 1

    prev_close = prev_close_arg or store.prev_close(d.isoformat())
    lookback = s.detection.daily_vol_lookback
    daily_var = daily_variance_from_closes(store.prev_closes(d.isoformat(), lookback + 1), lookback)
    if daily_var is None:
        daily_var = (s.detection.default_daily_vol_pct / 100) ** 2
    engine = DetectionEngine(s.detection, cal)
    engine.start_session(prev_close, daily_var, None, cal.session_minutes(d))
    builder = BarBuilder()
    alerts = []
    for t in ticks:
        alerts += engine.on_tick(t)
        for b in builder.add(t):
            alerts += engine.on_bar(b)
    last = builder.flush()
    if last:
        alerts += engine.on_bar(last)

    print(f"replayed {len(ticks)} ticks for {d}; prev_close={prev_close}; {len(alerts)} alerts")
    for a in alerts:
        when = cal.local(datetime.fromtimestamp(a.ts, tz=timezone.utc)).strftime("%H:%M")
        print(f"{when} {a.severity.label:<8} {a.key:<18} {a.title}")
    if notify and alerts:
        n = SlackNotifier(s.slack, cal.tz)
        for a in alerts:
            await n.send(a, prev_close)
        await n.aclose()
    store.close()
    return 0


async def cmd_slack_test(s: Settings) -> int:
    from .alerts import SlackNotifier
    from .models import Alert, Severity

    cal = MarketCalendar(s.market)
    n = SlackNotifier(s.slack, cal.tz)
    if not n.enabled:
        print("SLACK_WEBHOOK_URL is not set")
        return 2
    a = Alert("test", "test", Severity.INFO, "KMI-30 monitor test message",
              "If you can read this, Slack delivery works.", int(datetime.now(timezone.utc).timestamp()), 0.0)
    ok = await n.send(a, None)
    await n.aclose()
    print("delivered" if ok else "delivery failed; see logs")
    return 0 if ok else 1


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="kmi30", description="PSX KMI-30 real-time monitor")
    p.add_argument("-c", "--config", help="YAML config path (or set KMI30_CONFIG)")
    p.add_argument("--json-logs", action="store_true", help="emit structured JSON logs")
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("run", help="run the agent and web dashboard")
    sub.add_parser("check", help="one-shot check that the PSX indices page is reachable and parses")
    r = sub.add_parser("replay", help="replay a day through the detectors")
    r.add_argument("--date", help="YYYY-MM-DD from the local store (default: today)")
    r.add_argument("--ticks-file", help="PSX /timeseries/int JSON file instead of the store")
    r.add_argument("--prev-close", type=float, help="previous close for level alerts (default: from the store)")
    r.add_argument("--notify", action="store_true", help="also send replayed alerts to Slack")
    sub.add_parser("slack-test", help="send a test message to Slack")
    args = p.parse_args(argv)

    s = load_settings(args.config)
    setup_logging(s.log_level, args.json_logs)
    if args.cmd == "run":
        return cmd_run(s)
    if args.cmd == "check":
        return asyncio.run(cmd_check(s))
    if args.cmd == "replay":
        return asyncio.run(cmd_replay(s, args.date, args.ticks_file, args.notify, args.prev_close))
    if args.cmd == "slack-test":
        return asyncio.run(cmd_slack_test(s))
    return 2


if __name__ == "__main__":
    sys.exit(main())
