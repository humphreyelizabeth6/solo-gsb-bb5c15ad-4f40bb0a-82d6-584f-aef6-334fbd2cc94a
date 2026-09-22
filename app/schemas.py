"""Request/response models."""
from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class EventIn(BaseModel):
    stream_id: str = Field(min_length=1, max_length=256, examples=["orders"])
    seq: int = Field(ge=0, description="Monotonic sequence number within the stream")
    event_id: str = Field(min_length=1, max_length=256,
                          description="Unique event id used for idempotent dedup")
    payload: dict[str, Any] = Field(default_factory=dict)


class EventResult(BaseModel):
    status: str = Field(description="released | buffered | duplicate")
    stream_id: str
    seq: int
    event_id: str
    reason: str | None = Field(
        default=None,
        description="in_order for releases; why a submission was deduplicated",
    )
    released_seqs: list[int] | None = Field(
        default=None, description="Seqs released by this submission, in release order"
    )
    next_expected: int | None = None
    waiting_for_seq: int | None = Field(
        default=None, description="For buffered events: the seq the stream is waiting for"
    )
