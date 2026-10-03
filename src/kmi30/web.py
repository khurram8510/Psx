"""FastAPI app: live dashboard, WebSocket push, health and Prometheus metrics."""
from __future__ import annotations

import asyncio
import logging
import time
from contextlib import asynccontextmanager
from importlib.resources import files

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse, Response
from fastapi.staticfiles import StaticFiles
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from .agent import Agent
from .hub import Hub
from .metrics import REGISTRY

log = logging.getLogger(__name__)
STATIC = files("kmi30") / "static"
LIVENESS_S = 300
STALE_S = 600


def create_app(agent: Agent, hub: Hub, on_shutdown=None) -> FastAPI:
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
