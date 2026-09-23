"""HTTP 入口端到端：乱序、重试、时钟回拨、窗口边界、并发上报一致性。"""
import logging
import threading

import pytest
from fastapi.testclient import TestClient

import pipelineobs.api as api_mod
from main import app

log = logging.getLogger("test.api")


@pytest.fixture()
def client():
    # 每个用例使用全新的内存状态，保证相互隔离
    from pipelineobs.store import PipelineStore
    api_mod.store = PipelineStore()
    return TestClient(app)


def post(client, **kw):
    r = client.post("/api/v1/events", json=kw)
    assert r.status_code == 200, r.text
    body = r.json()
    log.info("POST event %s => status=%s lifecycle=%s aggregate=%s",
             kw, body["run"]["status"], body["lifecycle_reason"],
             body["aggregate_reason"])
    return body


def test_duplicate_and_out_of_order_over_http(client):
    post(client, pipeline_id="p", run_id="r1", attempt=1,
         event_type="started", event_ts=100)
    post(client, pipeline_id="p", run_id="r1", attempt=1,
         event_type="succeeded", event_ts=110)
    dup = post(client, pipeline_id="p", run_id="r1", attempt=1,
               event_type="succeeded", event_ts=110)
    assert dup["lifecycle_reason"] == "duplicate_event"
    late_fail = post(client, pipeline_id="p", run_id="r1", attempt=1,
                     event_type="failed", event_ts=120)
    assert late_fail["run"]["status"] == "succeeded"
    assert late_fail["lifecycle_reason"] == "attempt_terminal_held"


def test_retry_success_counts_as_one_succeeded_run(client):
    post(client, pipeline_id="p", run_id="r1", attempt=1,
         event_type="started", event_ts=100)
    post(client, pipeline_id="p", run_id="r1", attempt=1,
         event_type="failed", event_ts=110)
    post(client, pipeline_id="p", run_id="r1", attempt=2,
         event_type="started", event_ts=120)
    post(client, pipeline_id="p", run_id="r1", attempt=2,
         event_type="succeeded", event_ts=130)
    r = client.post("/api/v1/pipelines/p/aggregate",
                    json={"end_ts": 200, "window_seconds": 100})
    agg = r.json()
    log.info("aggregate => %s", agg)
    assert agg["total_runs"] == 1
    assert agg["succeeded_runs"] == 1
    assert agg["failed_runs"] == 0


def test_freshness_clock_rollback_and_missing_ts(client):
    # 缺失时间戳的成功 => missing_event_ts
    post(client, pipeline_id="p", run_id="r0", attempt=1,
         event_type="succeeded")
    r = client.get("/api/v1/pipelines/p/freshness",
                   params={"evaluated_at": 100, "interval_seconds": 10})
    assert r.json()["state"] == "unknown"
    assert r.json()["reason"] == "missing_event_ts"

    # 正常成功后时钟回拨
    post(client, pipeline_id="p", run_id="r1", attempt=1,
         event_type="succeeded", event_ts=100)
    r = client.get("/api/v1/pipelines/p/freshness",
                   params={"evaluated_at": 90, "interval_seconds": 10})
    body = r.json()
    log.info("freshness rollback => %s", body)
    assert body["state"] == "unknown"
    assert body["reason"] == "clock_rollback"


def test_window_boundary_and_alert_suppression(client):
    # 三个失败落在 [100,110) 与之后；连续 3 次触发一次告警
    post(client, pipeline_id="p", run_id="f1", attempt=1,
         event_type="failed", event_ts=100)
    post(client, pipeline_id="p", run_id="f2", attempt=1,
         event_type="failed", event_ts=104)
    post(client, pipeline_id="p", run_id="f3", attempt=1,
         event_type="failed", event_ts=108)

    r = client.post("/api/v1/pipelines/p/alerts/check",
                    json={"end_ts": 110, "threshold": 3,
                          "cooldown_seconds": 100})
    first = r.json()
    assert first["suppressed"] is False and first["reason"] == "threshold_reached"

    r = client.post("/api/v1/pipelines/p/alerts/check",
                    json={"end_ts": 110, "threshold": 3,
                          "cooldown_seconds": 100})
    second = r.json()
    assert second["suppressed"] is True
    assert second["reason"] == "already_alerted_streak"

    # 窗口边界 [100,110)：100 计入，110 不计入
    r = client.post("/api/v1/pipelines/p/aggregate",
                    json={"end_ts": 110, "window_seconds": 10})
    agg = r.json()
    log.info("window [100,110) => %s", agg)
    assert agg["window_start"] == 100 and agg["window_end"] == 110
    assert agg["total_runs"] == 3 and agg["failed_runs"] == 3


def test_concurrent_event_submission_consistent(client):
    def worker(t):
        for i in range(50):
            post(client, pipeline_id="p", run_id=f"r-{t}-{i}", attempt=1,
                 event_type="succeeded", event_ts=float(100 + t * 1000 + i))

    threads = [threading.Thread(target=worker, args=(t,)) for t in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    r = client.get("/api/v1/pipelines/p/runs")
    runs = r.json()["runs"]
    assert len(runs) == 300
    assert all(x["status"] == "succeeded" for x in runs)

    r = client.post("/api/v1/pipelines/p/aggregate",
                    json={"end_ts": 100_000, "window_seconds": 100_000})
    agg = r.json()
    assert agg["total_runs"] == 300
    assert agg["succeeded_runs"] == 300


def test_concurrent_duplicate_same_run(client):
    def worker():
        for _ in range(40):
            post(client, pipeline_id="p", run_id="dup", attempt=1,
                 event_type="succeeded", event_ts=50)

    threads = [threading.Thread(target=worker) for _ in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    r = client.post("/api/v1/pipelines/p/aggregate",
                    json={"end_ts": 100, "window_seconds": 100})
    assert r.json()["total_runs"] == 1


def test_interval_shrink_flow(client):
    post(client, pipeline_id="p", run_id="r1", attempt=1,
         event_type="succeeded", event_ts=100)
    client.put("/api/v1/pipelines/p/interval",
               json={"interval_seconds": 10})
    r = client.get("/api/v1/pipelines/p/freshness",
                   params={"evaluated_at": 108, "interval_seconds": 5})
    assert r.json()["reason"] == "interval_shrunk"
    # 显式确认
    r = client.get("/api/v1/pipelines/p/freshness",
                   params={"evaluated_at": 105, "interval_seconds": 5,
                           "confirm_shrink": True})
    assert r.json()["state"] == "fresh"
