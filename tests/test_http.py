"""End-to-end HTTP behaviour across every entry point."""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

import main as main_module
from observability.models import EventType
from tests._support import explain


@pytest.fixture()
def client():
    main_module.service.reset()
    return TestClient(main_module.app)


NOW = main_module.service.clock.now_ms()


def event_body(run, att, et, ts, eid=None, dur=None, err=None):
    return {
        "pipeline": "p", "run_id": run, "attempt": att,
        "event_type": et, "ts_ms": ts, "event_id": eid,
        "duration_ms": dur, "error": err,
    }


def test_full_flow_http(client):
    r = client.post("/pipelines/p", json={"schedule_interval_ms": 1000, "grace_ms": 200,
                                          "failure_alert_threshold": 2})
    assert r.status_code == 200 and r.json()["revision"] == 1

    # start + failure
    for body in [
        event_body("r", 1, "started", NOW, "e1"),
        event_body("r", 1, "failed", NOW + 20, "e2", err="boom"),
    ]:
        resp = client.post("/events", json=body)
        explain(f"POST /events {body['event_id']}", body, resp.json())
        assert resp.json()["accepted"] is True

    # duplicate event via HTTP: still 200, rejected by reason, not double counted
    resp = client.post("/events", json=event_body("r", 1, "failed", NOW + 20, "e2"))
    assert resp.json()["accepted"] is False
    assert resp.json()["reason"] == "duplicate_event"

    # unknown pipeline 404 at freshness/stats
    assert client.get("/pipelines/ghost/freshness").status_code == 404

    # stats: one failed outcome
    r = client.get("/pipelines/p/stats", params={"window_start_ms": NOW - 1000, "window_end_ms": NOW + 100_000})
    stats = r.json()
    explain("GET stats", {"window_around": NOW}, stats)
    assert stats["total_finished"] == 1 and stats["failed"] == 1

    # second failure trips the alert; third is suppressed
    client.post("/events", json=event_body("r2", 1, "started", NOW + 30, "e3"))
    r = client.post("/events", json=event_body("r2", 1, "failed", NOW + 40, "e4"))
    assert r.json()["alert"] and r.json()["alert"]["suppressed"] is False
    r = client.post("/events", json=event_body("r3", 1, "failed", NOW + 50, "e5"))
    assert r.json()["alert"]["suppressed"] is True
    alerts = client.get("/alerts", params={"pipeline": "p"}).json()
    explain("GET /alerts", {"pipeline": "p"}, alerts)
    assert len(alerts) == 1


def test_freshness_endpoint_statuses(client):
    client.post("/pipelines/p", json={"schedule_interval_ms": 1000, "grace_ms": 200})
    f = client.get("/pipelines/p/freshness").json()
    explain("GET freshness no-signal", {}, f)
    assert f["status"] == "no_signal"

    client.post("/events", json=event_body("ok", 1, "started", NOW, "s1"))
    client.post("/events", json=event_body("ok", 1, "succeeded", NOW + 3, "s2", dur=3))
    f = client.get("/pipelines/p/freshness").json()
    explain("GET freshness fresh", {}, f)
    assert f["status"] == "fresh"


def test_invalid_window_rejected(client):
    client.post("/pipelines/p", json={"schedule_interval_ms": 1000, "grace_ms": 200})
    r = client.get("/pipelines/p/stats", params={"window_start_ms": 100, "window_end_ms": 100})
    assert r.status_code == 422
