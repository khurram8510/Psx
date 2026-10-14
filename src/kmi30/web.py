"""FastAPI app: live dashboard, WebSocket push, health and Prometheus metrics."""
from __future__ import annotations

import asyncio
import logging
import time
from contextlib import asynccontextmanager
from importlib.resources import files

from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse, Response
from fastapi.staticfiles import StaticFiles
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from .agent import Agent
from .history import daily_csv, day_bounds, day_payload, parse_day, readings_csv
from .hub import Hub
from .metrics import REGISTRY
from .store import Store

log = logging.getLogger(__name__)
STATIC = files("kmi30") / "static"
LIVENESS_S = 300
STALE_S = 600


def create_app(agent: Agent, hub: Hub, on_shutdown=None, store: Store | None = None, tz=None) -> FastAPI:
    store = store or agent.store
    tz = tz or agent.cal.tz

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        stop = asyncio.Event()
        task = asyncio.create_task(agent.run(stop), name="kmi30-agent")
        try:
            yield
        finally:
            stop.set()
            try:
                await asyncio.wait_for(task, timeout=20)
            except asyncio.TimeoutError:
                task.cancel()
            if on_shutdown:
                await on_shutdown()

    app = FastAPI(title="KMI-30 Monitor", lifespan=lifespan, docs_url=None, redoc_url=None)
    app.mount("/static", StaticFiles(directory=str(STATIC)), name="static")

    @app.get("/", response_class=HTMLResponse)
    async def index() -> str:
        return (STATIC / "index.html").read_text()

    @app.get("/api/state")
    async def state() -> JSONResponse:
        return JSONResponse(hub.snapshot())

    def _day(value: str):
        try:
            return parse_day(value)
        except ValueError:
            raise HTTPException(400, "day must be YYYY-MM-DD") from None

    def _csv(body: str, filename: str) -> PlainTextResponse:
        return PlainTextResponse(body, media_type="text/csv; charset=utf-8",
                                 headers={"Content-Disposition": f'attachment; filename="{filename}"'})

    @app.get("/api/history/days")
    async def history_days(limit: int = Query(365, ge=1, le=10_000)) -> JSONResponse:
        """Daily summaries, newest first, including today's live row."""
        rows = await asyncio.to_thread(store.daily_range, limit=limit)
        return JSONResponse({"today": hub.day, "days": [r.to_dict() for r in rows]})

    @app.get("/api/history/daily.csv")
    async def history_daily_csv(start: str | None = Query(None, alias="from"),
                                end: str | None = Query(None, alias="to")) -> PlainTextResponse:
        lo = _day(start).isoformat() if start else "0000-00-00"
        hi = _day(end).isoformat() if end else "9999-99-99"
        rows = await asyncio.to_thread(store.daily_range, lo, hi)
        return _csv(daily_csv(rows, tz), "kmi30-daily.csv")

    @app.get("/api/history/{day}/readings.csv")
    async def history_readings_csv(day: str) -> PlainTextResponse:
        d = _day(day)
        ticks = await asyncio.to_thread(store.ticks_between, *day_bounds(d, tz))
        if not ticks:
            raise HTTPException(404, f"no readings stored for {d}")
        return _csv(readings_csv(ticks, tz), f"kmi30-{d}.csv")

    @app.get("/api/history/{day}")
    async def history_day(day: str) -> JSONResponse:
        d = _day(day)
        payload = await asyncio.to_thread(day_payload, store, d, tz)
        if payload is None:
            raise HTTPException(404, f"no data stored for {d}")
        payload["tz_offset_s"] = hub.tz_offset_s
        return JSONResponse(payload)

    @app.get("/healthz")
    async def healthz() -> JSONResponse:
        """Liveness: the agent loop is cycling. PSX being down does not fail this."""
        age = time.monotonic() - hub.heartbeat
        ok = age < LIVENESS_S
        return JSONResponse({"ok": ok, "loop_age_s": round(age, 1)}, status_code=200 if ok else 503)

    @app.get("/readyz")
    async def readyz() -> JSONResponse:
        """Readiness: at least one cycle done and, while the market is open, data is fresh."""
        st = hub.status
        stale = bool(st.get("market_open")) and (st.get("last_tick_age_s") or 0) > STALE_S
        ok = st != {} and not stale
        return JSONResponse({"ok": ok, **st}, status_code=200 if ok else 503)

    @app.get("/metrics")
    async def metrics() -> Response:
        return Response(generate_latest(REGISTRY), media_type=CONTENT_TYPE_LATEST)

    @app.websocket("/ws")
    async def ws(socket: WebSocket) -> None:
        await socket.accept()
        await socket.send_json(hub.snapshot())
        hub.clients.add(socket)
        try:
            while True:
                await socket.receive_text()  # keepalive pings from the client; content ignored
        except WebSocketDisconnect:
            pass
        finally:
            hub.clients.discard(socket)

    return app
