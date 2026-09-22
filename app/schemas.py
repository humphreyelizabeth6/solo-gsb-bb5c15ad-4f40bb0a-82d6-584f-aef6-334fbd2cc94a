"""Pydantic 请求/响应模型。"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class EventIn(BaseModel):
    stream_id: str = Field(..., min_length=1, max_length=256, examples=["stream-42"])
    seq: int = Field(..., ge=1, examples=[1])
    event_id: str = Field(..., min_length=1, max_length=256, examples=["evt-0001"])
    payload: dict[str, Any] = Field(default_factory=dict)


class EventAck(BaseModel):
    stream_id: str
    seq: int
    event_id: str
    status: str  # accepted（首次接收） / duplicate（幂等重复）
    state: str  # released / buffered
    reason: str  # in_order / gap_filled / gap_waiting / duplicate_*
    next_seq: int
    pending: int
    released_chain: list["ReleasedItem"] = []


class ReleasedItem(BaseModel):
    seq: int
    event_id: str | None = None
    kind: str  # event / gap
    reason: str
    payload: Any | None = None
    released_at: float


class PendingEvent(BaseModel):
    seq: int
    event_id: str
    payload: Any
    created_at: float


class StreamState(BaseModel):
    stream_id: str
    next_seq: int
    gap_since: float | None
    gap_open: bool
    pending_count: int
    created_at: float
    updated_at: float
    released: dict[str, int]


class StreamSummary(BaseModel):
    stream_id: str
    next_seq: int
    gap_open: bool
    pending_count: int
    updated_at: float


class ErrorBody(BaseModel):
    detail: str


EventAck.model_rebuild()
