"""Webhook event gate API (FastAPI)."""
from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query, Request

from . import config
from .gate import EventGate
from .schemas import EventIn, EventResult

logger = logging.getLogger("webhook-gate")


def create_app(gate: EventGate | None = None, sweep_interval: float | None = None) -> FastAPI:
    """Build the app.  A custom ``gate``/``sweep_interval`` can be injected (tests)."""
    interval = sweep_interval if sweep_interval is not None else config.SWEEP_INTERVAL_SECONDS

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.gate = gate or EventGate(config.DATABASE_PATH, config.GAP_TIMEOUT_SECONDS)
        stop = asyncio.Event()

        async def sweeper() -> None:
            while not stop.is_set():
                try:
                    expired = await asyncio.to_thread(app.state.gate.sweep_expired)
                    if expired:
                        logger.info("gap timeout expired, force-released streams: %s", expired)
                except Exception:
                    logger.exception("sweeper iteration failed")
                try:
                    await asyncio.wait_for(stop.wait(), timeout=interval)
                except asyncio.TimeoutError:
                    pass

        task = asyncio.create_task(sweeper())
        yield
        stop.set()
        await task
        app.state.gate.close()

    app = FastAPI(
        title="Webhook Event Gate",
        version="1.0.0",
        description="Per-stream ordered event gate: dedup, out-of-order buffering, "
                    "gap timeout, release logging.",
        lifespan=lifespan,
    )

    @app.get("/healthz")
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.post("/events", response_model=EventResult)
    async def submit_event(event: EventIn, request: Request) -> dict:
        return await asyncio.to_thread(
            request.app.state.gate.submit,
            event.stream_id, event.seq, event.event_id, event.payload,
        )

    @app.get("/streams")
    async def list_streams(request: Request) -> dict:
        return {"streams": await asyncio.to_thread(request.app.state.gate.list_streams)}

    @app.get("/streams/{stream_id}")
    async def get_stream(stream_id: str, request: Request) -> dict:
        state = await asyncio.to_thread(request.app.state.gate.get_stream, stream_id)
        if state is None:
            raise HTTPException(status_code=404, detail=f"unknown stream: {stream_id}")
        return state

    @app.get("/streams/{stream_id}/pending")
    async def get_pending(stream_id: str, request: Request) -> dict:
        pending = await asyncio.to_thread(request.app.state.gate.get_pending, stream_id)
        if pending is None:
            raise HTTPException(status_code=404, detail=f"unknown stream: {stream_id}")
        return {"stream_id": stream_id, "pending": pending}

    @app.get("/streams/{stream_id}/releases")
    async def get_releases(
        stream_id: str,
        request: Request,
        limit: int = Query(default=100, ge=1, le=1000),
    ) -> dict:
        releases = await asyncio.to_thread(request.app.state.gate.get_releases, stream_id, limit)
        if releases is None:
            raise HTTPException(status_code=404, detail=f"unknown stream: {stream_id}")
        return {"stream_id": stream_id, "releases": releases}

    return app


app = create_app()
