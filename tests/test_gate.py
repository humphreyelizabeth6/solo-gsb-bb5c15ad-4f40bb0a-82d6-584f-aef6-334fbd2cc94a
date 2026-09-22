"""Unit tests for the core gate logic (in-memory SQLite, manual sweeping)."""
import time

import pytest

from app.gate import EventGate


@pytest.fixture
def gate():
    g = EventGate(":memory:", gap_timeout_seconds=0.05)
    yield g
    g.close()


def test_in_order_release(gate):
    r = gate.submit("s1", 1, "e1", {"a": 1})
    assert r["status"] == "released"
    assert r["released_seqs"] == [1]
    assert r["next_expected"] == 2

    r = gate.submit("s1", 2, "e2", {})
    assert r["status"] == "released"
    assert r["released_seqs"] == [2]


def test_first_event_sets_baseline(gate):
    r = gate.submit("s1", 10, "e10", {})
    assert r["status"] == "released"
    assert gate.get_stream("s1")["next_expected"] == 11


def test_out_of_order_buffered_then_gap_filled(gate):
    gate.submit("s1", 1, "e1", {})
    r = gate.submit("s1", 3, "e3", {})
    assert r["status"] == "buffered"
    assert r["waiting_for_seq"] == 2

    r = gate.submit("s1", 2, "e2", {})
    assert r["status"] == "released"
    assert r["released_seqs"] == [2, 3]  # buffered follower drained in order

    state = gate.get_stream("s1")
    assert state["next_expected"] == 4
    assert state["buffered_count"] == 0
    assert state["gap_since"] is None


def test_duplicate_event_id(gate):
    gate.submit("s1", 1, "e1", {})
    r = gate.submit("s1", 1, "e1", {})
    assert r["status"] == "duplicate"
    assert r["reason"] == "event_id_already_seen"


def test_stale_seq_is_duplicate(gate):
    gate.submit("s1", 1, "e1", {})
    gate.submit("s1", 2, "e2", {})
    r = gate.submit("s1", 1, "e-other", {})
    assert r["status"] == "duplicate"
    assert r["reason"] == "seq_already_passed"


def test_duplicate_buffered_seq(gate):
    gate.submit("s1", 1, "e1", {})
    gate.submit("s1", 5, "e5", {})
    r = gate.submit("s1", 5, "e5b", {})
    assert r["status"] == "duplicate"
    assert r["reason"] == "seq_already_buffered"


def test_gap_timeout_skips_and_releases(gate):
    gate.submit("s1", 1, "e1", {})
    gate.submit("s1", 4, "e4", {})
    gate.submit("s1", 5, "e5", {})
    time.sleep(0.06)

    assert gate.sweep_expired() == ["s1"]

    state = gate.get_stream("s1")
    assert state["next_expected"] == 6
    assert state["released_total"] == 3
    assert state["skipped_total"] == 2  # seq 2 and 3 were skipped

    releases = gate.get_releases("s1")
    skipped = {r["seq"] for r in releases if r["kind"] == "skipped"}
    assert skipped == {2, 3}
    timed_out = [r for r in releases if r["kind"] == "released" and r["reason"] == "gap_timeout"]
    assert {r["seq"] for r in timed_out} == {4, 5}


def test_gap_timer_restarts_for_remaining_gap(gate):
    gate.submit("s1", 1, "e1", {})
    gate.submit("s1", 3, "e3", {})    # gap at seq 2
    gate.submit("s1", 10, "e10", {})  # buffered behind the same gap
    time.sleep(0.06)

    gate.sweep_expired()  # skips 2, releases 3; gap 4..9 remains
    state = gate.get_stream("s1")
    assert state["next_expected"] == 4
    assert state["buffered_seqs"] == [10]

    # timeout window restarted → not due yet
    assert gate.sweep_expired() == []

    time.sleep(0.06)
    gate.sweep_expired()
    state = gate.get_stream("s1")
    assert state["next_expected"] == 11
    assert state["skipped_total"] == 7  # 2 plus 4..9


def test_pending_and_payload_roundtrip(gate):
    gate.submit("s1", 1, "e1", {})
    gate.submit("s1", 3, "e3", {"hello": "world"})
    pending = gate.get_pending("s1")
    assert len(pending) == 1
    assert pending[0]["seq"] == 3
    assert pending[0]["event_id"] == "e3"
    assert pending[0]["payload"] == {"hello": "world"}
    assert pending[0]["received_at"]


def test_streams_are_isolated(gate):
    gate.submit("a", 1, "e1", {})
    gate.submit("b", 1, "e1", {})  # same seq/event_id, different stream → not a dup
    assert gate.get_stream("a")["next_expected"] == 2
    assert gate.get_stream("b")["next_expected"] == 2


def test_unknown_stream_queries(gate):
    assert gate.get_stream("nope") is None
    assert gate.get_pending("nope") is None
    assert gate.get_releases("nope") is None
