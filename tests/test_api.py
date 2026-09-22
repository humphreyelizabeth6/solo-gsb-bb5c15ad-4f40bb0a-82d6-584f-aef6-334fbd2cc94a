"""API integration tests via FastAPI TestClient."""
import time

import pytest
from fastapi.testclient import TestClient

from app.gate import EventGate
from app.main import create_app


def make_client(gap_timeout: float = 30.0, sweep_interval: float = 3600.0) -> TestClient:
    gate = EventGate(":memory:", gap_timeout_seconds=gap_timeout)
    return TestClient(create_app(gate=gate, sweep_interval=sweep_interval))


def post(client, stream, seq, event_id, payload=None):
    return client.post("/events", json={
        "stream_id": stream, "seq": seq, "event_id": event_id, "payload": payload or {},
    })


def test_healthz():
    with make_client() as client:
        assert client.get("/healthz").json() == {"status": "ok"}


def test_full_event_flow():
    with make_client() as client:
        r = post(client, "orders", 1, "e1", {"amount": 100})
        assert r.status_code == 200
        assert r.json()["status"] == "released"

        r = post(client, "orders", 3, "e3")
        assert r.json()["status"] == "buffered"
        assert r.json()["waiting_for_seq"] == 2

        pending = client.get("/streams/orders/pending").json()["pending"]
        assert [p["seq"] for p in pending] == [3]

        state = client.get("/streams/orders").json()
        assert state["next_expected"] == 2
        assert state["buffered_seqs"] == [3]
        assert state["gap_since"] is not None

        r = post(client, "orders", 2, "e2")  # fills the gap
        body = r.json()
        assert body["status"] == "released"
        assert body["released_seqs"] == [2, 3]

        state = client.get("/streams/orders").json()
        assert state["next_expected"] == 4
        assert state["buffered_count"] == 0
        assert state["released_total"] == 3

        releases = client.get("/streams/orders/releases").json()["releases"]
        assert len(releases) == 3
        assert {r["reason"] for r in releases} == {"in_order", "gap_filled"}

        r = post(client, "orders", 3, "e3")  # retry → idempotent duplicate
        assert r.json()["status"] == "duplicate"


def test_list_streams():
    with make_client() as client:
        post(client, "a", 1, "e1")
        post(client, "b", 7, "e7")
        streams = client.get("/streams").json()["streams"]
        assert {s["stream_id"] for s in streams} == {"a", "b"}


def test_unknown_stream_404():
    with make_client() as client:
        assert client.get("/streams/unknown").status_code == 404
        assert client.get("/streams/unknown/pending").status_code == 404
        assert client.get("/streams/unknown/releases").status_code == 404


def test_validation_error():
    with make_client() as client:
        assert post(client, "s", -1, "e1").status_code == 422
        assert post(client, "", 1, "e1").status_code == 422
        assert post(client, "s", 1, "").status_code == 422
        assert client.post("/events", json={"stream_id": "s"}).status_code == 422


def test_background_sweeper_expires_gap():
    with make_client(gap_timeout=0.05, sweep_interval=0.01) as client:
        post(client, "s", 1, "e1")
        post(client, "s", 3, "e3")

        for _ in range(200):
            state = client.get("/streams/s").json()
            if state["next_expected"] == 4:
                break
            time.sleep(0.02)
        else:
            pytest.fail("background sweeper did not expire the gap")

        releases = client.get("/streams/s/releases").json()["releases"]
        assert any(r["kind"] == "skipped" and r["seq"] == 2 for r in releases)
        assert any(r["kind"] == "released" and r["seq"] == 3
                   and r["reason"] == "gap_timeout" for r in releases)
