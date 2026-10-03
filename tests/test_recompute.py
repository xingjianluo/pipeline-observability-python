"""追溯重算：历史窗口复盘、与在线对比、不改在线状态、不影响告警抑制。"""
import logging
import threading

import pytest
from fastapi.testclient import TestClient

import pipelineobs.api as api_mod
from main import app
from pipelineobs.store import PipelineStore

log = logging.getLogger("test.recompute")


def build_store():
    """窗口 [100,200) 内 r1/r2 失败；窗口后 r1 重试成功、r3 成功。"""
    s = PipelineStore()
    s.ingest("p", "r1", 1, "started", 100)
    s.ingest("p", "r1", 1, "failed", 150)
    s.ingest("p", "r2", 1, "started", 160)
    s.ingest("p", "r2", 1, "failed", 180)
    s.ingest("p", "r1", 2, "started", 500)
    s.ingest("p", "r1", 2, "succeeded", 510)
    s.ingest("p", "r3", 1, "started", 600)
    s.ingest("p", "r3", 1, "succeeded", 620)
    return s


def online_snapshot(store):
    runs = {(s.pipeline_id, s.run_id): (s.status, dict(s.attempts and {
        n: (a.status, a.started_ts, a.finished_ts, a.latency)
        for n, a in s.attempts.items()}))
        for s in store.list_runs()}
    window = store.window("p", 200, 100)
    freshness = store.evaluate_freshness("p", 1000, 100)
    return runs, window, freshness


def test_recompute_window_stats_and_alert_as_of_then():
    store = build_store()
    report = store.recompute_window("p", 100, 200, threshold=2)
    log.info("INPUT window=[100,200) => DECISION stats=%s alert=%s diffs=%s",
             report.stats, report.alert, report.differences)
    # 当时可见：两次失败、无成功，耗时按当时记录
    assert report.replayed_events == 4
    assert (report.stats.total_runs, report.stats.succeeded_runs,
            report.stats.failed_runs) == (2, 0, 2)
    assert report.stats.avg_latency == 35.0
    assert report.alert.consecutive_failures == 2
    assert report.alert.would_alert is True
    assert report.alert.reason == "threshold_reached"
    # 在线现在：r1 已重试成功，结论不同 => 差异可见
    assert report.online_stats.succeeded_runs == 1
    assert report.online_would_alert is False
    assert any("would_alert" in d for d in report.differences)
    assert any("failed_runs" in d for d in report.differences)


def test_recompute_without_difference_has_empty_diff():
    store = PipelineStore()
    store.ingest("p", "r1", 1, "started", 100)
    store.ingest("p", "r1", 1, "succeeded", 150)
    report = store.recompute_window("p", 100, 200, threshold=2)
    log.info("stats=%s online=%s diffs=%s", report.stats,
             report.online_stats, report.differences)
    assert report.stats == report.online_stats
    assert report.differences == ()


def test_recompute_does_not_mutate_online_state():
    store = build_store()
    store.set_interval("p", 100)
    before = online_snapshot(store)
    store.recompute_window("p", 100, 200, threshold=2)
    after = online_snapshot(store)
    log.info("before=%s after=%s", before, after)
    assert before == after


def test_recompute_does_not_fire_or_suppress_alerts():
    store = PipelineStore()
    store.ingest("p", "r1", 1, "started", 100)
    store.ingest("p", "r1", 1, "failed", 150)
    store.ingest("p", "r2", 1, "started", 160)
    store.ingest("p", "r2", 1, "failed", 180)
    # 重算窗口内“当时该告警”，但不能真的发告警/消费抑制
    report = store.recompute_window("p", 100, 200, threshold=2)
    assert report.alert.would_alert is True
    # 在线第一次真实检查仍应触发（说明重算没有写入抑制状态）
    a1 = store.check_alert("p", 200, 2, 300)
    log.info("after recompute: check_alert => suppressed=%s reason=%s",
             a1.suppressed, a1.reason)
    assert not a1.suppressed and a1.reason == "threshold_reached"
    # 重算也不读坏抑制：同一序列再次检查被正常抑制
    a2 = store.check_alert("p", 200, 2, 300)
    assert a2.suppressed and a2.reason == "already_alerted_streak"
    # 抑制期间做重算，不影响已有抑制记录
    store.recompute_window("p", 100, 200, threshold=2)
    a3 = store.check_alert("p", 200, 2, 300)
    assert a3.suppressed and a3.reason == "already_alerted_streak"


def test_recompute_after_backfill_uses_imported_history():
    store = PipelineStore()
    # 在线只见到 r3 成功；历史事件后来批量回补进来
    store.ingest("p", "r3", 1, "succeeded", 620)
    store.ingest_batch([
        {"pipeline_id": "p", "run_id": "r1", "event_type": "started",
         "event_ts": 100},
        {"pipeline_id": "p", "run_id": "r1", "event_type": "failed",
         "event_ts": 150},
        {"pipeline_id": "p", "run_id": "r2", "event_type": "started",
         "event_ts": 160},
        {"pipeline_id": "p", "run_id": "r2", "event_type": "failed",
         "event_ts": 180},
    ], received_ts=1000.0)
    report = store.recompute_window("p", 100, 200, threshold=2)
    log.info("after backfill: stats=%s alert=%s", report.stats, report.alert)
    assert report.stats.failed_runs == 2
    assert report.alert.would_alert is True


def test_recompute_concurrent_with_realtime_ingest():
    store = build_store()
    errors = []

    def realtime():
        try:
            for i in range(200):
                store.ingest("p", f"rt{i}", 1, "started", 700 + i)
                store.ingest("p", f"rt{i}", 1, "succeeded", 700 + i + 0.5)
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    def recompute():
        try:
            for _ in range(50):
                store.recompute_window("p", 100, 200, threshold=2)
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=realtime),
               threading.Thread(target=recompute),
               threading.Thread(target=recompute)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert errors == []
    # 实时事件全部生效，重算未干扰在线
    assert store.get_run("p", "rt199").status.value == "succeeded"
    report = store.recompute_window("p", 100, 200, threshold=2)
    assert report.stats.failed_runs == 2  # 窗口外实时事件不进入历史窗口


@pytest.fixture()
def client():
    api_mod.store = PipelineStore()
    return TestClient(app)


def test_recompute_over_http(client):
    r = client.post("/api/v1/events/batch", json={"events": [
        {"pipeline_id": "p", "run_id": "r1", "event_type": "started",
         "event_ts": 100},
        {"pipeline_id": "p", "run_id": "r1", "event_type": "failed",
         "event_ts": 150},
        {"pipeline_id": "p", "run_id": "r2", "event_type": "failed",
         "event_ts": 180},
    ]})
    assert r.status_code == 200
    assert r.json()["summary"] == {"total": 3, "applied": 3,
                                   "duplicates": 0, "rejected": 0}
    resp = client.post("/api/v1/pipelines/p/recompute",
                       json={"window_start": 100, "window_end": 200,
                             "threshold": 2})
    body = resp.json()
    log.info("HTTP recompute => %s", body)
    assert resp.status_code == 200
    assert body["stats"]["failed_runs"] == 2
    assert body["alert"]["would_alert"] is True
    # 非法窗口：可区分的 422
    bad = client.post("/api/v1/pipelines/p/recompute",
                      json={"window_start": 200, "window_end": 100})
    assert bad.status_code == 422
