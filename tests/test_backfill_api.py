"""HTTP 入口：批量回补（含部分失败/幂等）与追溯重算端到端。"""
import logging
import threading

import pytest
from fastapi.testclient import TestClient

import pipelineobs.api as api_mod
from main import app
from pipelineobs.store import PipelineStore

log = logging.getLogger("test.backfill_api")


@pytest.fixture()
def client():
    api_mod.store = PipelineStore()
    return TestClient(app)


def test_batch_endpoint_partial_failure_and_per_item_reasons(client):
    r = client.post("/api/v1/events:batch", json={"events": [
        {"pipeline_id": "p", "run_id": "r1", "event_type": "started",
         "event_ts": 100},
        {"pipeline_id": "p", "run_id": "r1", "event_type": "succeeded",
         "event_ts": 110},
        {"pipeline_id": "p", "run_id": "r1", "event_type": "succeeded",
         "event_ts": 110},                       # 重复
        {"pipeline_id": "p", "run_id": "r2", "event_type": "bogus",
         "event_ts": 120},                      # 非法类型
        {"pipeline_id": "p", "run_id": "r3", "attempt": 0,
         "event_type": "failed", "event_ts": 130},  # 非法 attempt
        {"pipeline_id": "p", "run_id": "r4", "event_type": "failed",
         "event_ts": 140},                      # 合法
    ]})
    assert r.status_code == 200, r.text
    body = r.json()
    assert (body["total"], body["accepted"], body["duplicate"],
            body["invalid"]) == (6, 3, 1, 2)
    statuses = [(x["index"], x["status"], x.get("reason"))
                for x in body["results"]]
    assert statuses[2][1] == "duplicate"
    assert statuses[3] == (3, "invalid", "invalid_event_type")
    assert statuses[4] == (4, "invalid", "invalid_attempt")

    # 合法事件经同一入口可查询，状态与单条上报一致
    r = client.get("/api/v1/pipelines/p/runs/r1")
    assert r.json()["status"] == "succeeded"


def test_batch_endpoint_idempotent_retry(client):
    payload = {"events": [
        {"pipeline_id": "p", "run_id": "r1", "event_type": "failed",
         "event_ts": 100, "attempt": 1},
        {"pipeline_id": "p", "run_id": "r1", "event_type": "succeeded",
         "event_ts": 130, "attempt": 2},
    ]}
    r1 = client.post("/api/v1/events:batch", json=payload).json()
    assert r1["invalid"] == 0
    r2 = client.post("/api/v1/events:batch", json=payload).json()
    assert r2["accepted"] == 0 and r2["duplicate"] == 2
    agg = client.post("/api/v1/pipelines/p/aggregate",
                      json={"end_ts": 200, "window_seconds": 200}).json()
    assert agg["total_runs"] == 1 and agg["succeeded_runs"] == 1


def test_replay_endpoint_shows_diff_without_side_effects(client):
    client.post("/api/v1/events:batch", json={"events": [
        {"pipeline_id": "p", "run_id": "r1", "event_type": "succeeded",
         "event_ts": 110},
        {"pipeline_id": "p", "run_id": "f1", "event_type": "failed",
         "event_ts": 220},
        {"pipeline_id": "p", "run_id": "f2", "event_type": "failed",
         "event_ts": 230},
    ]})
    r = client.post("/api/v1/pipelines/p/replay",
                    json={"end_ts": 200, "window_seconds": 200,
                          "threshold": 2})
    assert r.status_code == 200, r.text
    rp = r.json()
    assert rp["historical"]["window"]["total_runs"] == 1
    assert rp["online"]["window"]["total_runs"] == 1
    assert sorted(c["run_id"] for c in rp["run_changes"]) == ["f1", "f2"]
    assert rp["historical"]["alert"]["would_alert"] is False
    assert rp["online"]["alert"]["would_alert"] is True
    assert rp["alert_changed"] is True

    # 重算不产生告警副作用：随后真实检查仍会首次触发
    alert = client.post("/api/v1/pipelines/p/alerts/check",
                        json={"end_ts": 300, "threshold": 2,
                              "cooldown_seconds": 100}).json()
    assert alert["suppressed"] is False


def test_batch_concurrent_with_single_event_endpoint(client):
    def single_worker(t):
        for i in range(100):
            r = client.post("/api/v1/events", json={
                "pipeline_id": "rt", "run_id": f"r-{t}-{i}",
                "event_type": "succeeded",
                "event_ts": float(10_000 + t * 1000 + i)})
            assert r.status_code == 200

    def batch_worker():
        events = [{"pipeline_id": "bf", "run_id": f"b{i}",
                   "event_type": "failed" if i % 2 else "succeeded",
                   "event_ts": float(i)} for i in range(800)]
        r = client.post("/api/v1/events:batch", json={"events": events})
        assert r.status_code == 200
        assert r.json()["invalid"] == 0

    threads = [threading.Thread(target=batch_worker)]
    threads += [threading.Thread(target=single_worker, args=(t,))
                for t in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    rt = client.get("/api/v1/pipelines/rt/runs").json()["runs"]
    bf = client.post("/api/v1/pipelines/bf/aggregate",
                     json={"end_ts": 1_000_000,
                           "window_seconds": 1_000_000}).json()
    assert len(rt) == 400
    assert bf["total_runs"] == 800
    assert bf["succeeded_runs"] + bf["failed_runs"] == 800


def test_single_event_endpoint_still_works_as_before(client):
    r = client.post("/api/v1/events", json={
        "pipeline_id": "p", "run_id": "r1", "event_type": "started",
        "event_ts": 100})
    assert r.status_code == 200
    assert r.json()["run"]["status"] == "running"
