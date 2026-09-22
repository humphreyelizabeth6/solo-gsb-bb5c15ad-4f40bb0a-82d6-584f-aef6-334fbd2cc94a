"""SQLite 异步访问层。

- 单个 aiosqlite 连接 + WAL，配合应用层写锁串行化所有写事务；
- 连接以 autocommit 方式打开（isolation_level=None），事务边界由
  ``transaction()`` 显式控制，避免 aiosqlite 在 execute 前隐式 BEGIN。
"""

from __future__ import annotations

import os
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator

import aiosqlite

SCHEMA = """
CREATE TABLE IF NOT EXISTS streams (
    stream_id        TEXT PRIMARY KEY,
    next_seq         INTEGER NOT NULL DEFAULT 1,
    gap_since        REAL,
    created_at       REAL NOT NULL,
    updated_at       REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS events (
    stream_id        TEXT NOT NULL,
    seq              INTEGER NOT NULL,
    event_id         TEXT NOT NULL,
    payload          TEXT NOT NULL,
    status           TEXT NOT NULL CHECK (status IN ('buffered', 'released')),
    created_at       REAL NOT NULL,
    released_at      REAL,
    release_reason   TEXT,
    PRIMARY KEY (stream_id, seq),
    UNIQUE (stream_id, event_id)
);
CREATE INDEX IF NOT EXISTS idx_events_stream_status
    ON events (stream_id, status, seq);

CREATE TABLE IF NOT EXISTS releases (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    stream_id        TEXT NOT NULL,
    seq              INTEGER NOT NULL,
    event_id         TEXT,
    kind             TEXT NOT NULL CHECK (kind IN ('event', 'gap')),
    reason           TEXT NOT NULL,
    payload          TEXT,
    released_at      REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_releases_stream
    ON releases (stream_id, id);
"""

Row = aiosqlite.Row


class Database:
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self._conn: aiosqlite.Connection | None = None

    async def connect(self) -> None:
        directory = os.path.dirname(os.path.abspath(self.db_path))
        os.makedirs(directory, exist_ok=True)
        conn = await aiosqlite.connect(self.db_path, isolation_level=None)
        conn.row_factory = aiosqlite.Row
        await conn.execute("PRAGMA journal_mode=WAL")
        await conn.execute("PRAGMA foreign_keys=ON")
        await conn.execute("PRAGMA synchronous=NORMAL")
        self._conn = conn
        await self.init_schema()

    async def close(self) -> None:
        if self._conn is not None:
            await self._conn.close()
            self._conn = None

    @property
    def conn(self) -> aiosqlite.Connection:
        if self._conn is None:
            raise RuntimeError("Database is not connected")
        return self._conn

    async def init_schema(self) -> None:
        await self.conn.executescript(SCHEMA)

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[aiosqlite.Connection]:
        """显式写事务：BEGIN IMMEDIATE ... COMMIT / ROLLBACK。"""
        conn = self.conn
        try:
            await conn.execute("BEGIN IMMEDIATE")
            yield conn
            await conn.execute("COMMIT")
        except Exception:
            await conn.execute("ROLLBACK")
            raise

    # ---- streams ----------------------------------------------------------

    async def get_stream(self, stream_id: str) -> Row | None:
        async with self.conn.execute(
            "SELECT * FROM streams WHERE stream_id = ?", (stream_id,)
        ) as cur:
            return await cur.fetchone()

    async def upsert_stream_on_first_event(
        self, conn: aiosqlite.Connection, stream_id: str, now: float
    ) -> Row:
        """取流行，不存在则以 next_seq=1 创建。"""
        await conn.execute(
            """
            INSERT INTO streams (stream_id, next_seq, gap_since, created_at, updated_at)
            VALUES (?, 1, NULL, ?, ?)
            ON CONFLICT(stream_id) DO UPDATE SET updated_at = excluded.updated_at
            """,
            (stream_id, now, now),
        )
        async with conn.execute(
            "SELECT * FROM streams WHERE stream_id = ?", (stream_id,)
        ) as cur:
            return await cur.fetchone()

    async def list_streams(self) -> list[Row]:
        async with self.conn.execute(
            "SELECT * FROM streams ORDER BY updated_at DESC, stream_id"
        ) as cur:
            return list(await cur.fetchall())

    async def set_next_seq_and_gap_since(
        self,
        conn: aiosqlite.Connection,
        stream_id: str,
        next_seq: int,
        gap_since: float | None,
        now: float,
    ) -> None:
        await conn.execute(
            """
            UPDATE streams
               SET next_seq = ?, gap_since = ?, updated_at = ?
             WHERE stream_id = ?
            """,
            (next_seq, gap_since, now, stream_id),
        )

    # ---- events -----------------------------------------------------------

    async def get_event_by_seq(
        self, conn: aiosqlite.Connection, stream_id: str, seq: int
    ) -> Row | None:
        async with conn.execute(
            "SELECT * FROM events WHERE stream_id = ? AND seq = ?",
            (stream_id, seq),
        ) as cur:
            return await cur.fetchone()

    async def get_event_by_event_id(
        self, conn: aiosqlite.Connection, stream_id: str, event_id: str
    ) -> Row | None:
        async with conn.execute(
            "SELECT * FROM events WHERE stream_id = ? AND event_id = ?",
            (stream_id, event_id),
        ) as cur:
            return await cur.fetchone()

    async def insert_event(
        self,
        conn: aiosqlite.Connection,
        stream_id: str,
        seq: int,
        event_id: str,
        payload_json: str,
        now: float,
    ) -> None:
        await conn.execute(
            """
            INSERT INTO events (stream_id, seq, event_id, payload, status, created_at)
            VALUES (?, ?, ?, ?, 'buffered', ?)
            """,
            (stream_id, seq, event_id, payload_json, now),
        )

    async def get_buffered_from(
        self, conn: aiosqlite.Connection, stream_id: str, seq: int
    ) -> list[Row]:
        async with conn.execute(
            """
            SELECT * FROM events
             WHERE stream_id = ? AND seq >= ? AND status = 'buffered'
             ORDER BY seq
            """,
            (stream_id, seq),
        ) as cur:
            return list(await cur.fetchall())

    async def release_event(
        self,
        conn: aiosqlite.Connection,
        stream_id: str,
        seq: int,
        reason: str,
        now: float,
    ) -> None:
        await conn.execute(
            """
            UPDATE events
               SET status = 'released', released_at = ?, release_reason = ?
             WHERE stream_id = ? AND seq = ?
            """,
            (now, reason, stream_id, seq),
        )

    async def list_pending(self, stream_id: str) -> list[Row]:
        async with self.conn.execute(
            """
            SELECT * FROM events
             WHERE stream_id = ? AND status = 'buffered'
             ORDER BY seq
            """,
            (stream_id,),
        ) as cur:
            return list(await cur.fetchall())

    # ---- releases ---------------------------------------------------------

    async def insert_release(
        self,
        conn: aiosqlite.Connection,
        *,
        stream_id: str,
        seq: int,
        event_id: str | None,
        kind: str,
        reason: str,
        payload: str | None,
        now: float,
    ) -> None:
        await conn.execute(
            """
            INSERT INTO releases
                (stream_id, seq, event_id, kind, reason, payload, released_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (stream_id, seq, event_id, kind, reason, payload, now),
        )

    async def list_releases(
        self, stream_id: str, limit: int, offset: int
    ) -> list[Row]:
        async with self.conn.execute(
            """
            SELECT * FROM releases
             WHERE stream_id = ?
             ORDER BY id DESC
             LIMIT ? OFFSET ?
            """,
            (stream_id, limit, offset),
        ) as cur:
            return list(await cur.fetchall())

    async def release_counts(self, stream_id: str) -> dict[str, Any]:
        async with self.conn.execute(
            """
            SELECT COUNT(*) AS total,
                   COALESCE(SUM(CASE WHEN kind = 'event' THEN 1 ELSE 0 END), 0) AS events,
                   COALESCE(SUM(CASE WHEN kind = 'gap' THEN 1 ELSE 0 END), 0) AS gaps
              FROM releases
             WHERE stream_id = ?
            """,
            (stream_id,),
        ) as cur:
            row = await cur.fetchone()
        return {
            "total_events": row["total"],
            "events": row["events"],
            "gaps": row["gaps"],
        }
