"""事件闸门核心逻辑。

时序约定
--------
每条流维护 ``next_seq``（下一个期望序号，从 1 开始）：

- 客户端事件的 ``seq == next_seq``：立刻释放，并沿缓存表向后释放所有
  连续序号（``in_order`` / ``gap_filled``）；
- ``seq > next_seq``：缓存乱序事件，并在缺口开始计时；
- ``seq < next_seq``：迟到事件，409 拒绝；
- 同一个 ``event_id`` 或同一 ``seq`` 的重复提交：幂等返回，不重复释放。

超时（后台 sweeper）
---------------------
缺口等待超过 ``GAP_TIMEOUT_SECONDS`` 时，从 ``next_seq`` 起逐个写入
``kind='gap'`` 的释放记录、推进窗口，直到撞上已缓存的事件，再把后续
连续事件一并释放（``timeout_flush``）。
"""

from __future__ import annotations

import asyncio
import json
import time

from .config import Settings
from .db import Database


class GateConflict(Exception):
    """业务冲突（迟到事件 / event_id 冲突），映射为 HTTP 409。"""


class GateService:
    def __init__(self, db: Database, settings: Settings) -> None:
        self.db = db
        self.settings = settings
        # 全局限定写操作顺序；SQLite 只有一个连接，串行化最简单可靠。
        self._write_lock = asyncio.Lock()

    # ---- 内部工具 ---------------------------------------------------------

    @staticmethod
    def _load_payload(raw: str | None) -> object:
        if raw is None:
            return None
        return json.loads(raw)

    async def _flush_chain(
        self,
        conn,
        stream_id: str,
        next_seq: int,
        first_reason: str,
        later_reason: str,
        now: float,
    ) -> tuple[list[dict], int, float | None]:
        """从 next_seq 开始，释放缓存中连续的事件。

        返回 (释放链, 新的 next_seq, 新的 gap_since)。链上首条事件使用
        ``first_reason``，其余使用 ``later_reason``；链结束后若仍有缓存
        事件，说明还有新缺口，从 now 重新计时。
        """
        released: list[dict] = []
        current = next_seq
        rows = await self.db.get_buffered_from(conn, stream_id, current)
        index = {r["seq"]: r for r in rows}
        while current in index:
            reason = first_reason if not released else later_reason
            row = index[current]
            await self.db.release_event(conn, stream_id, current, reason, now)
            await self.db.insert_release(
                conn,
                stream_id=stream_id,
                seq=current,
                event_id=row["event_id"],
                kind="event",
                reason=reason,
                payload=row["payload"],
                now=now,
            )
            released.append(
                {
                    "seq": current,
                    "event_id": row["event_id"],
                    "kind": "event",
                    "reason": reason,
                    "payload": json.loads(row["payload"]),
                    "released_at": now,
                }
            )
            current += 1

        gap_since = now if any(r["seq"] > current for r in rows) else None
        return released, current, gap_since

    # ---- 接收事件 ---------------------------------------------------------

    async def ingest(
        self, stream_id: str, seq: int, event_id: str, payload: dict
    ) -> dict:
        payload_json = json.dumps(payload, ensure_ascii=False)
        now = time.time()

        async with self._write_lock, self.db.transaction() as conn:
            stream = await self.db.upsert_stream_on_first_event(conn, stream_id, now)
            next_seq = stream["next_seq"]

            # 先按 event_id 去重：webhook 可能重试已缓存甚至已释放的事件，
            # 此时必须幂等返回（无论 seq 是否已滑过窗口）。
            same_id = await self.db.get_event_by_event_id(conn, stream_id, event_id)
            if same_id is not None:
                if same_id["seq"] != seq:
                    raise GateConflict(
                        f"event_id={event_id!r} already used with seq={same_id['seq']}"
                    )
                return {
                    "stream_id": stream_id,
                    "seq": seq,
                    "event_id": event_id,
                    "status": "duplicate",
                    "state": same_id["status"],
                    "reason": f"duplicate_{same_id['status']}",
                    "next_seq": next_seq,
                    "pending": len(await self.db.get_buffered_from(conn, stream_id, next_seq)),
                    "released_chain": [],
                }

            if seq < next_seq:
                existing = await self.db.get_event_by_seq(conn, stream_id, seq)
                raise GateConflict(
                    f"late event: seq={seq} already released, next_seq={next_seq}"
                    + (f", event_id={existing['event_id']}" if existing else "")
                )

            same_seq = await self.db.get_event_by_seq(conn, stream_id, seq)
            if same_seq is not None:
                # 同一 seq 已缓存但 event_id 不同：同一序号不允许两个事件。
                raise GateConflict(
                    f"seq={seq} already buffered with event_id={same_seq['event_id']}"
                )

            await self.db.insert_event(conn, stream_id, seq, event_id, payload_json, now)

            if seq == next_seq:
                chain, new_next, gap_since = await self._flush_chain(
                    conn,
                    stream_id,
                    next_seq,
                    "in_order",
                    "gap_filled",
                    now,
                )
                reason = "in_order"
            else:
                chain = []
                new_next = next_seq
                gap_since = stream["gap_since"] if stream["gap_since"] is not None else now
                reason = "gap_waiting"

            await self.db.set_next_seq_and_gap_since(
                conn, stream_id, new_next, gap_since, now
            )
            pending = len(
                await self.db.get_buffered_from(conn, stream_id, new_next)
            )
            return {
                "stream_id": stream_id,
                "seq": seq,
                "event_id": event_id,
                "status": "accepted",
                "state": "released" if chain else "buffered",
                "reason": reason,
                "next_seq": new_next,
                "pending": pending,
                "released_chain": chain,
            }

    # ---- 超时扫描 ---------------------------------------------------------

    async def sweep_once(self) -> int:
        """扫描全部流，推进超时缺口。返回发生推进的流数量。"""
        now = time.time()
        timeout = self.settings.gap_timeout_seconds
        advanced = 0

        streams = await self.db.list_streams()
        for stream in streams:
            gap_since = stream["gap_since"]
            if gap_since is None or now - gap_since < timeout:
                continue

            async with self._write_lock, self.db.transaction() as conn:
                stream_id = stream["stream_id"]
                next_seq = stream["next_seq"]
                buffered = await self.db.get_buffered_from(conn, stream_id, next_seq)
                if not buffered:
                    # 没有任何等待事件，缺口计时无意义，清掉计时。
                    await self.db.set_next_seq_and_gap_since(
                        conn, stream_id, next_seq, None, now
                    )
                    continue

                first_seq = buffered[0]["seq"]
                chain: list[dict] = []
                current = next_seq

                # 1) 为缺失序号写 gap 释放记录，推进到第一个缓存事件。
                while current < first_seq:
                    await self.db.insert_release(
                        conn,
                        stream_id=stream_id,
                        seq=current,
                        event_id=None,
                        kind="gap",
                        reason="timeout_gap",
                        payload=None,
                        now=now,
                    )
                    chain.append(
                        {
                            "seq": current,
                            "event_id": None,
                            "kind": "gap",
                            "reason": "timeout_gap",
                            "payload": None,
                            "released_at": now,
                        }
                    )
                    current += 1

                # 2) 释放此后连续的缓存事件。
                event_chain, new_next, new_gap_since = await self._flush_chain(
                    conn,
                    stream_id,
                    current,
                    "timeout_flush",
                    "timeout_flush",
                    now,
                )
                chain.extend(event_chain)
                await self.db.set_next_seq_and_gap_since(
                    conn, stream_id, new_next, new_gap_since, now
                )
                advanced += 1

        return advanced

    # ---- 查询 -------------------------------------------------------------

    async def list_streams(self) -> list[dict]:
        result = []
        for s in await self.db.list_streams():
            pending_rows = await self.db.list_pending(s["stream_id"])
            result.append(
                {
                    "stream_id": s["stream_id"],
                    "next_seq": s["next_seq"],
                    "gap_open": s["gap_since"] is not None,
                    "pending_count": len(pending_rows),
                    "updated_at": s["updated_at"],
                }
            )
        return result

    async def get_stream_state(self, stream_id: str) -> dict | None:
        s = await self.db.get_stream(stream_id)
        if s is None:
            return None
        pending_rows = await self.db.list_pending(stream_id)
        counts = await self.db.release_counts(stream_id)
        return {
            "stream_id": stream_id,
            "next_seq": s["next_seq"],
            "gap_since": s["gap_since"],
            "gap_open": s["gap_since"] is not None,
            "pending_count": len(pending_rows),
            "created_at": s["created_at"],
            "updated_at": s["updated_at"],
            "released": {
                "total": counts["total_events"],
                "events": counts["events"],
                "gaps": counts["gaps"],
            },
        }

    async def list_pending(self, stream_id: str) -> list[dict] | None:
        if await self.db.get_stream(stream_id) is None:
            return None
        rows = await self.db.list_pending(stream_id)
        return [
            {
                "seq": r["seq"],
                "event_id": r["event_id"],
                "payload": self._load_payload(r["payload"]),
                "created_at": r["created_at"],
            }
            for r in rows
        ]

    async def list_releases(
        self, stream_id: str, limit: int, offset: int
    ) -> list[dict] | None:
        if await self.db.get_stream(stream_id) is None:
            return None
        rows = await self.db.list_releases(stream_id, limit, offset)
        return [
            {
                "seq": r["seq"],
                "event_id": r["event_id"],
                "kind": r["kind"],
                "reason": r["reason"],
                "payload": self._load_payload(r["payload"]),
                "released_at": r["released_at"],
            }
            for r in rows
        ]
