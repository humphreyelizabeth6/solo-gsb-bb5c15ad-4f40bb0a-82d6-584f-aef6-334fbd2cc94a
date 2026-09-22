"""Core sequencing gate: per-stream ordering, dedup, gap buffering and release.

State machine per stream
------------------------
* ``next_expected`` — the next sequence number that may be released.
* The first event ever seen for a stream establishes the baseline: it is
  released immediately and ``seq + 1`` becomes ``next_expected``.
* ``seq == next_expected``      → released at once, then any buffered
  followers are drained in order.
* ``seq > next_expected``       → out of order; buffered while the stream
  waits for the gap (``gap_since`` timestamps when the wait started).
* ``seq < next_expected`` or an already-seen ``event_id``/``seq`` → duplicate.

If the gap is not filled within ``gap_timeout``, the missing sequences are
skipped (recorded as ``skipped`` in the release log) and buffered events are
released in order.  Every release and skip is written to the ``releases``
table; every accepted event is stored in ``events`` with its status.
"""
from __future__ import annotations

import json
import sqlite3
import threading
from datetime import datetime, timedelta, timezone
from typing import Any

SCHEMA = """
CREATE TABLE IF NOT EXISTS streams (
    stream_id     TEXT PRIMARY KEY,
    next_expected INTEGER NOT NULL,
    gap_since     TEXT,               -- ISO timestamp when the current gap wait started
    created_at    TEXT NOT NULL,
    updated_at    TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    stream_id   TEXT NOT NULL REFERENCES streams(stream_id),
    seq         INTEGER NOT NULL,
    event_id    TEXT NOT NULL,
    payload     TEXT NOT NULL,        -- JSON
    status      TEXT NOT NULL CHECK (status IN ('buffered', 'released')),
    received_at TEXT NOT NULL,
    released_at TEXT,
    UNIQUE (stream_id, seq),
    UNIQUE (stream_id, event_id)
);

CREATE TABLE IF NOT EXISTS releases (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    stream_id  TEXT NOT NULL,
    seq        INTEGER NOT NULL,
    event_id   TEXT,                  -- NULL for skipped gap sequences
    kind       TEXT NOT NULL CHECK (kind IN ('released', 'skipped')),
    reason     TEXT NOT NULL CHECK (reason IN ('in_order', 'gap_filled', 'gap_timeout')),
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_events_stream_status ON events(stream_id, status);
CREATE INDEX IF NOT EXISTS idx_releases_stream ON releases(stream_id, id);
"""


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _iso(dt: datetime) -> str:
    return dt.isoformat(timespec="milliseconds")


def _parse(ts: str) -> datetime:
    return datetime.fromisoformat(ts)


class EventGate:
    """Serialize event release per stream.  Thread-safe."""

    def __init__(self, db_path: str, gap_timeout_seconds: float = 30.0):
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._conn.executescript(SCHEMA)
        self.gap_timeout = timedelta(seconds=gap_timeout_seconds)

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # ------------------------------------------------------------------ ingest

    def submit(self, stream_id: str, seq: int, event_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        """Accept one event.  Returns a result dict with a ``status`` of
        ``released`` | ``buffered`` | ``duplicate``."""
        now = _utcnow()
        with self._lock:
            self._expire_stream(stream_id, now)  # apply a due timeout first
            with self._conn:
                stream = self._stream_row(stream_id)
                if stream is None:
                    return self._bootstrap_stream(stream_id, seq, event_id, payload, now)

                if self._event_id_seen(stream_id, event_id):
                    return self._result("duplicate", stream_id, seq, event_id, stream,
                                        reason="event_id_already_seen")

                next_expected = stream["next_expected"]
                if seq < next_expected:
                    return self._result("duplicate", stream_id, seq, event_id, stream,
                                        reason="seq_already_passed")

                if self._seq_seen(stream_id, seq):
                    return self._result("duplicate", stream_id, seq, event_id, stream,
                                        reason="seq_already_buffered")

                if seq == next_expected:
                    self._insert_event(stream_id, seq, event_id, payload, "released", now,
                                       released_at=now)
                    self._log_release(stream_id, seq, event_id, "released", "in_order", now)
                    chained, next_seq = self._drain_buffered(stream_id, seq + 1, now, "gap_filled")
                    self._refresh_stream(stream_id, next_seq, now)
                    return self._result("released", stream_id, seq, event_id, None,
                                        released_seqs=[seq] + chained, next_expected=next_seq,
                                        reason="in_order")

                # seq > next_expected → out of order, buffer it and (re)arm the gap timer
                self._insert_event(stream_id, seq, event_id, payload, "buffered", now)
                self._conn.execute(
                    "UPDATE streams SET gap_since = ?, updated_at = ? WHERE stream_id = ?",
                    (stream["gap_since"] or _iso(now), _iso(now), stream_id),
                )
                return self._result("buffered", stream_id, seq, event_id, None,
                                    next_expected=next_expected, waiting_for_seq=next_expected)

    def _bootstrap_stream(self, stream_id: str, seq: int, event_id: str,
                          payload: dict[str, Any], now: datetime) -> dict[str, Any]:
        """First event of a stream defines the baseline and is released at once."""
        self._conn.execute(
            "INSERT INTO streams (stream_id, next_expected, gap_since, created_at, updated_at)"
            " VALUES (?, ?, NULL, ?, ?)",
            (stream_id, seq + 1, _iso(now), _iso(now)),
        )
        self._insert_event(stream_id, seq, event_id, payload, "released", now, released_at=now)
        self._log_release(stream_id, seq, event_id, "released", "in_order", now)
        return self._result("released", stream_id, seq, event_id, None,
                            released_seqs=[seq], next_expected=seq + 1, reason="in_order")

    # ------------------------------------------------------------------ timeout

    def sweep_expired(self) -> list[str]:
        """Expire every stream whose gap wait has timed out.

        Returns the ids of streams for which events were force-released."""
        now = _utcnow()
        with self._lock:
            rows = self._conn.execute(
                "SELECT stream_id FROM streams WHERE gap_since IS NOT NULL"
            ).fetchall()
            return [r["stream_id"] for r in rows
                    if self._expire_stream(r["stream_id"], now)]

    def _expire_stream(self, stream_id: str, now: datetime) -> list[int]:
        """Skip a timed-out gap for one stream; returns released seqs ([] if not due)."""
        stream = self._stream_row(stream_id)
        if stream is None or stream["gap_since"] is None:
            return []
        if now - _parse(stream["gap_since"]) < self.gap_timeout:
            return []
        with self._conn:
            row = self._conn.execute(
                "SELECT MIN(seq) AS m FROM events WHERE stream_id = ? AND status = 'buffered'",
                (stream_id,),
            ).fetchone()
            if row["m"] is None:  # gap flag without buffered events — just clear it
                self._conn.execute(
                    "UPDATE streams SET gap_since = NULL, updated_at = ? WHERE stream_id = ?",
                    (_iso(now), stream_id),
                )
                return []
            first_buffered = row["m"]
            for missing in range(stream["next_expected"], first_buffered):
                self._log_release(stream_id, missing, None, "skipped", "gap_timeout", now)
            released, next_seq = self._drain_buffered(stream_id, first_buffered, now, "gap_timeout")
            self._refresh_stream(stream_id, next_seq, now)
            return released

    # ------------------------------------------------------------------ queries

    def get_stream(self, stream_id: str) -> dict[str, Any] | None:
        with self._lock:
            return self._get_stream(stream_id, _utcnow())

    def _get_stream(self, stream_id: str, now: datetime) -> dict[str, Any] | None:
        """Stream state snapshot.  Caller must hold the lock."""
        stream = self._stream_row(stream_id)
        if stream is None:
            return None
        buffered = self._conn.execute(
            "SELECT seq FROM events WHERE stream_id = ? AND status = 'buffered' ORDER BY seq",
            (stream_id,),
        ).fetchall()
        stats = {r["kind"]: r["c"] for r in self._conn.execute(
            "SELECT kind, COUNT(*) AS c FROM releases WHERE stream_id = ? GROUP BY kind",
            (stream_id,),
        )}
        last = self._conn.execute(
            "SELECT MAX(seq) AS m FROM releases WHERE stream_id = ? AND kind = 'released'",
            (stream_id,),
        ).fetchone()
        gap_since = stream["gap_since"]
        return {
            "stream_id": stream_id,
            "next_expected": stream["next_expected"],
            "last_released_seq": last["m"],
            "buffered_count": len(buffered),
            "buffered_seqs": [r["seq"] for r in buffered],
            "gap_since": gap_since,
            "gap_age_seconds": round((now - _parse(gap_since)).total_seconds(), 3)
            if gap_since else None,
            "gap_timeout_seconds": self.gap_timeout.total_seconds(),
            "released_total": stats.get("released", 0),
            "skipped_total": stats.get("skipped", 0),
            "created_at": stream["created_at"],
            "updated_at": stream["updated_at"],
        }

    def list_streams(self) -> list[dict[str, Any]]:
        now = _utcnow()
        with self._lock:
            rows = self._conn.execute(
                "SELECT stream_id FROM streams ORDER BY created_at"
            ).fetchall()
            return [self._get_stream(r["stream_id"], now) for r in rows]

    def get_pending(self, stream_id: str) -> list[dict[str, Any]] | None:
        """Buffered (out-of-order, not yet released) events of a stream."""
        with self._lock:
            if self._stream_row(stream_id) is None:
                return None
            rows = self._conn.execute(
                "SELECT seq, event_id, payload, received_at FROM events"
                " WHERE stream_id = ? AND status = 'buffered' ORDER BY seq",
                (stream_id,),
            ).fetchall()
            return [
                {
                    "seq": r["seq"],
                    "event_id": r["event_id"],
                    "payload": json.loads(r["payload"]),
                    "received_at": r["received_at"],
                }
                for r in rows
            ]

    def get_releases(self, stream_id: str, limit: int = 100) -> list[dict[str, Any]] | None:
        """Release log (newest first): both released events and skipped gap seqs."""
        with self._lock:
            if self._stream_row(stream_id) is None:
                return None
            rows = self._conn.execute(
                "SELECT seq, event_id, kind, reason, created_at FROM releases"
                " WHERE stream_id = ? ORDER BY id DESC LIMIT ?",
                (stream_id, limit),
            ).fetchall()
            return [dict(r) for r in rows]

    # ------------------------------------------------------------------ helpers

    def _drain_buffered(self, stream_id: str, from_seq: int, now: datetime,
                        reason: str) -> tuple[list[int], int]:
        """Release consecutive buffered events starting at ``from_seq``.

        Returns (released seqs, new next_expected)."""
        released: list[int] = []
        seq = from_seq
        while True:
            row = self._conn.execute(
                "SELECT event_id FROM events"
                " WHERE stream_id = ? AND seq = ? AND status = 'buffered'",
                (stream_id, seq),
            ).fetchone()
            if row is None:
                break
            self._conn.execute(
                "UPDATE events SET status = 'released', released_at = ?"
                " WHERE stream_id = ? AND seq = ?",
                (_iso(now), stream_id, seq),
            )
            self._log_release(stream_id, seq, row["event_id"], "released", reason, now)
            released.append(seq)
            seq += 1
        return released, seq

    def _refresh_stream(self, stream_id: str, next_expected: int, now: datetime) -> None:
        """Persist next_expected; clear the gap timer, or restart it if a new gap remains."""
        remaining = self._conn.execute(
            "SELECT COUNT(*) AS c FROM events WHERE stream_id = ? AND status = 'buffered'",
            (stream_id,),
        ).fetchone()["c"]
        self._conn.execute(
            "UPDATE streams SET next_expected = ?, gap_since = ?, updated_at = ?"
            " WHERE stream_id = ?",
            (next_expected, None if remaining == 0 else _iso(now), _iso(now), stream_id),
        )

    def _stream_row(self, stream_id: str) -> sqlite3.Row | None:
        return self._conn.execute(
            "SELECT * FROM streams WHERE stream_id = ?", (stream_id,)
        ).fetchone()

    def _event_id_seen(self, stream_id: str, event_id: str) -> bool:
        return self._conn.execute(
            "SELECT 1 FROM events WHERE stream_id = ? AND event_id = ?",
            (stream_id, event_id),
        ).fetchone() is not None

    def _seq_seen(self, stream_id: str, seq: int) -> bool:
        return self._conn.execute(
            "SELECT 1 FROM events WHERE stream_id = ? AND seq = ?",
            (stream_id, seq),
        ).fetchone() is not None

    def _insert_event(self, stream_id: str, seq: int, event_id: str,
                      payload: dict[str, Any], status: str, now: datetime,
                      released_at: datetime | None = None) -> None:
        self._conn.execute(
            "INSERT INTO events (stream_id, seq, event_id, payload, status, received_at, released_at)"
            " VALUES (?, ?, ?, ?, ?, ?, ?)",
            (stream_id, seq, event_id, json.dumps(payload), status, _iso(now),
             _iso(released_at) if released_at else None),
        )

    def _log_release(self, stream_id: str, seq: int, event_id: str | None,
                     kind: str, reason: str, now: datetime) -> None:
        self._conn.execute(
            "INSERT INTO releases (stream_id, seq, event_id, kind, reason, created_at)"
            " VALUES (?, ?, ?, ?, ?, ?)",
            (stream_id, seq, event_id, kind, reason, _iso(now)),
        )

    @staticmethod
    def _result(status: str, stream_id: str, seq: int, event_id: str,
                stream: sqlite3.Row | None, **extra: Any) -> dict[str, Any]:
        result = {
            "status": status,
            "stream_id": stream_id,
            "seq": seq,
            "event_id": event_id,
            "next_expected": stream["next_expected"] if stream is not None else None,
            "reason": None,
            "released_seqs": None,
            "waiting_for_seq": None,
        }
        result.update(extra)
        return result
