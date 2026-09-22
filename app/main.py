"""FastAPI 入口：Webhook 事件闸门。"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from typing import Annotated

from fastapi import Depends, FastAPI, HTTPException, Query, Request, Response, status
from fastapi.responses import JSONResponse

from .config import Settings, get_settings
from .db import Database
from .schemas import (
    ErrorBody,
    EventAck,
    EventIn,
    PendingEvent,
    ReleasedItem,
    StreamState,
    StreamSummary,
)
from .service import GateConflict, GateService
from .sweeper import sweeper_loop

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("event_gate")


@contextlib.asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    db = Database(settings.db_path)
    await db.connect()
    service = GateService(db, settings)
    app.state.db = db
    app.state.service = service
    app.state.settings = settings
    task = asyncio.create_task(
        sweeper_loop(service, settings.sweep_interval_seconds), name="sweeper"
    )
    logger.info(
        "gate started: db=%s gap_timeout=%ss sweep=%ss",
        settings.db_path,
        settings.gap_timeout_seconds,
        settings.sweep_interval_seconds,
    )
    try:
        yield
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
        await db.close()


app = FastAPI(
    title="Webhook Event Gate",
    version="1.0.0",
    description="按流维护连续序号窗口：去重、缓存乱序事件、缺口补齐或超时后按序释放。",
    lifespan=lifespan,
)


def get_service(request: Request) -> GateService:
    return request.app.state.service


ServiceDep = Annotated[GateService, Depends(get_service)]


@app.exception_handler(GateConflict)
async def gate_conflict_handler(_: Request, exc: GateConflict) -> JSONResponse:
    return JSONResponse(status_code=409, content={"detail": str(exc)})


@app.post(
    "/events",
    response_model=EventAck,
    status_code=status.HTTP_202_ACCEPTED,
    responses={409: {"model": ErrorBody}},
    tags=["events"],
    summary="提交一个事件",
)
async def submit_event(event: EventIn, service: ServiceDep) -> Response:
    """首次接收返回 **202**；重复提交幂等返回 **200**。"""
    ack = await service.ingest(
        stream_id=event.stream_id,
        seq=event.seq,
        event_id=event.event_id,
        payload=event.payload,
    )
    http_status = (
        status.HTTP_200_OK if ack["status"] == "duplicate" else status.HTTP_202_ACCEPTED
    )
    return JSONResponse(ack, status_code=http_status)


@app.get(
    "/streams",
    response_model=list[StreamSummary],
    tags=["streams"],
    summary="列出全部流",
)
async def list_streams(service: ServiceDep) -> list[dict]:
    return await service.list_streams()


@app.get(
    "/streams/{stream_id}",
    response_model=StreamState,
    responses={404: {"model": ErrorBody}},
    tags=["streams"],
    summary="查询流状态（序号窗口/缺口/待处理/释放统计）",
)
async def get_stream(stream_id: str, service: ServiceDep) -> dict:
    state = await service.get_stream_state(stream_id)
    if state is None:
        raise HTTPException(status_code=404, detail=f"stream {stream_id!r} not found")
    return state


@app.get(
    "/streams/{stream_id}/pending",
    response_model=list[PendingEvent],
    responses={404: {"model": ErrorBody}},
    tags=["streams"],
    summary="查询流的待处理（已缓存、尚未释放）事件",
)
async def get_pending(stream_id: str, service: ServiceDep) -> list[dict]:
    pending = await service.list_pending(stream_id)
    if pending is None:
        raise HTTPException(status_code=404, detail=f"stream {stream_id!r} not found")
    return pending


@app.get(
    "/streams/{stream_id}/releases",
    response_model=list[ReleasedItem],
    responses={404: {"model": ErrorBody}},
    tags=["streams"],
    summary="查询释放结果（事件与超时缺口，最近优先）",
)
async def get_releases(
    stream_id: str,
    service: ServiceDep,
    limit: Annotated[int, Query(ge=1, le=500)] = 100,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> list[dict]:
    releases = await service.list_releases(stream_id, limit, offset)
    if releases is None:
        raise HTTPException(status_code=404, detail=f"stream {stream_id!r} not found")
    return releases


@app.get("/healthz", tags=["system"], summary="健康检查")
async def health() -> dict:
    return {"status": "ok"}
