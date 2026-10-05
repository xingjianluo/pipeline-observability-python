"""批量回补健壮性回归：

- 真实入口混入坏数据（非对象条目、缺事件类型）逐条给结论，不拖垮整批；
- 乱序回补的耗时统计（avg / p95）与到达顺序无关，且与按时间顺序逐条上报一致；
- 回补与实时上报并发、乱序批次重试多遍后，运行状态 / 窗口（含耗时）/ 告警
  都是同一份稳定结果。
"""
import logging
import random
import threading

import pytest
from fastapi.testclient import TestClient

import pipelineobs.api as api_mod
from main import app
from pipelineobs.models import BackfillItem
from pipelineobs.store import PipelineStore

log = logging.getLogger("test.backfill_robustness")


@pytest.fixture()
def client():
    api_mod.store = PipelineStore()
    return TestClient(app)


def _item(pid, rid, etype, ts, attempt=1):
    return BackfillItem(pid, rid, attempt, etype, ts)


def _runs_snapshot(store):
    """运行级快照：状态 + 每个尝试的开始/结束/耗时。"""
    snap = {}
    for st in store.list_runs():
        snap[(st.pipeline_id, st.run_id)] = (
            st.status.value,
            tuple(sorted((n, a.status.value, a.started_ts, a.finished_ts,
                          a.latency) for n, a in st.attempts.items())))
    return snap


def _window_snapshot(store, pid, end_ts=100_000, window=100_000):
    w = store.window(pid, end_ts, window)
    return (w.total_runs, w.succeeded_runs, w.failed_runs,
            w.avg_latency, w.p95_latency, tuple(sorted(w.latencies)))


def _reference_store(events):
    """参照实现：把同一批事件按时间顺序逐条单报到全新 store。"""
    ref = PipelineStore()
    for e in sorted(events, key=lambda e: (e.event_ts, e.run_id, e.attempt)):
        ref.ingest(e.pipeline_id, e.run_id, e.attempt, e.event_type,
                   e.event_ts, e.detail, received_ts=e.event_ts)
    return ref


def _latency_events():
    """一批带开始/结束的历史事件：单尝试成功、失败后重试成功、纯失败。"""
    events = []
    for i in range(20):  # 单尝试成功，耗时各异
        events += [_item("p", f"ok{i}", "started", 1000 + 10 * i),
                   _item("p", f"ok{i}", "succeeded", 1000 + 10 * i + (i % 7) + 1)]
    for i in range(10):  # 失败重试后成功
        events += [_item("p", f"retry{i}", "started", 2000 + 10 * i),
                   _item("p", f"retry{i}", "failed", 2000 + 10 * i + 2),
                   _item("p", f"retry{i}", "started", 2000 + 10 * i + 4,
                         attempt=2),
                   _item("p", f"retry{i}", "succeeded", 2000 + 10 * i + 9,
                         attempt=2)]
    for i in range(10):  # 纯失败
        events += [_item("p", f"bad{i}", "started", 3000 + 10 * i),
                   _item("p", f"bad{i}", "failed", 3000 + 10 * i + 3)]
    return events


# ---------------------------------------------------------------------------
# 1. 真实入口混入坏数据：逐条结论，合法条目照常生效
# ---------------------------------------------------------------------------

def test_batch_endpoint_mixed_bad_items_per_item_conclusions(client):
    r = client.post("/api/v1/events:batch", json={"events": [
        {"pipeline_id": "p", "run_id": "r1", "event_type": "started",
         "event_ts": 100},
        "not-an-object",                                  # 非对象：字符串
        42,                                               # 非对象：数字
        ["list", "item"],                                 # 非对象：数组
        {"pipeline_id": "p", "run_id": "r2",
         "event_ts": 110},                                # 缺 event_type
        {"pipeline_id": "p", "run_id": "r3",
         "event_type": None, "event_ts": 120},            # event_type 为 null
        {"pipeline_id": "p", "run_id": "r4",
         "event_type": "bogus", "event_ts": 130},         # 非法事件类型
        {"pipeline_id": "p", "run_id": "r1",
         "event_type": "succeeded", "event_ts": 105},     # 合法
    ]})
    assert r.status_code == 200, r.text
    body = r.json()
    log.info("mixed batch => total=%d accepted=%d duplicate=%d invalid=%d",
             body["total"], body["accepted"], body["duplicate"],
             body["invalid"])
    assert (body["total"], body["accepted"], body["invalid"]) == (8, 2, 6)
    reasons = {x["index"]: x.get("reason") for x in body["results"]}
    assert reasons[1] == reasons[2] == reasons[3] == "item_not_object"
    assert reasons[4] == reasons[5] == "missing_event_type"
    assert reasons[6] == "invalid_event_type"

    # 合法条目照常生效，效果与逐条单报完全一致
    run = client.get("/api/v1/pipelines/p/runs/r1").json()
    assert run["status"] == "succeeded"
    assert run["attempts"][0]["latency"] == 5
    assert client.get("/api/v1/pipelines/p/runs/r2").status_code == 404
    agg = client.post("/api/v1/pipelines/p/aggregate",
                      json={"end_ts": 1000, "window_seconds": 1000}).json()
    assert (agg["total_runs"], agg["succeeded_runs"]) == (1, 1)
    assert agg["avg_latency"] == 5


def test_batch_endpoint_bad_items_do_not_change_valid_equivalence(client):
    """走真实入口的合法事件效果 == 把同样事件逐条走 POST /events。"""
    good = [
        {"pipeline_id": "eq", "run_id": "a", "event_type": "started",
         "event_ts": 100},
        {"pipeline_id": "eq", "run_id": "a", "event_type": "failed",
         "event_ts": 110},
        {"pipeline_id": "eq", "run_id": "a", "event_type": "started",
         "event_ts": 120, "attempt": 2},
        {"pipeline_id": "eq", "run_id": "a", "event_type": "succeeded",
         "event_ts": 130, "attempt": 2},
    ]
    r = client.post("/api/v1/events:batch", json={"events":
        [good[1], "garbage", good[3], {"run_id": "x"}, good[0], good[2]]})
    assert r.status_code == 200
    assert (r.json()["accepted"], r.json()["invalid"]) == (4, 2)
    batch_run = client.get("/api/v1/pipelines/eq/runs/a").json()

    api_mod.store = PipelineStore()  # 换全新 store 逐条单报同一批
    for e in good:
        resp = client.post("/api/v1/events", json=e)
        assert resp.status_code == 200
    single_run = client.get("/api/v1/pipelines/eq/runs/a").json()
    assert batch_run == single_run
    log.info("batch-vs-single run state identical: %s", batch_run["status"])


# ---------------------------------------------------------------------------
# 2. 乱序回补：耗时统计与到达顺序无关，且与按时间顺序逐条上报一致
# ---------------------------------------------------------------------------

def test_out_of_order_backfill_latency_matches_in_order_ingestion():
    events = _latency_events()
    ref = _reference_store(events)
    ref_runs = _runs_snapshot(ref)
    ref_win = _window_snapshot(ref, "p")
    log.info("reference window: total=%d avg=%.3f p95=%.3f",
             ref_win[0], ref_win[3], ref_win[4])

    permutations = [list(reversed(events))]
    rnd = random.Random(2026)
    for _ in range(5):
        shuffled = events[:]
        rnd.shuffle(shuffled)
        permutations.append(shuffled)

    for i, perm in enumerate(permutations):
        store = PipelineStore()
        res = store.ingest_batch(perm)
        assert res.invalid == 0
        runs = _runs_snapshot(store)
        win = _window_snapshot(store, "p")
        assert runs == ref_runs, f"permutation {i}: run states differ"
        assert win == ref_win, f"permutation {i}: window stats differ"
        log.info("permutation %d => window(total=%d ok=%d fail=%d avg=%.3f "
                 "p95=%.3f) matches in-order reference",
                 i, win[0], win[1], win[2], win[3], win[4])


def test_terminal_before_started_still_gets_latency():
    """结束事件先于开始事件到达：耗时必须补齐而不是留空。"""
    store = PipelineStore()
    store.ingest_batch([
        _item("p", "r1", "succeeded", 110),
        _item("p", "r1", "started", 100),
    ])
    attempt = store.get_run("p", "r1").attempts[1]
    assert attempt.latency == 10
    w = store.window("p", 1000, 1000)
    assert (w.avg_latency, w.p95_latency) == (10.0, 10.0)
    log.info("terminal-first backfill => latency=%s avg=%s p95=%s",
             attempt.latency, w.avg_latency, w.p95_latency)


# ---------------------------------------------------------------------------
# 3. 回补与实时上报并发：最终状态 / 窗口（含耗时）/ 告警结论稳定
# ---------------------------------------------------------------------------

def test_backfill_concurrent_with_realtime_stable_latency_and_alert():
    events = _latency_events()
    # 追加连续失败，让告警阈值必然触发
    for i in range(3):
        events += [_item("p", f"alert{i}", "started", 9000 + 10 * i),
                   _item("p", f"alert{i}", "failed", 9000 + 10 * i + 1)]
    ref = _reference_store(events)
    ref_runs = _runs_snapshot(ref)
    ref_win = _window_snapshot(ref, "p")
    ref_alert = ref.peek_alert("p", 100_000, 3, 300.0)

    store = PipelineStore()
    shuffled = events[:]
    random.Random(99).shuffle(shuffled)
    halves = [shuffled[:len(shuffled) // 2], shuffled[len(shuffled) // 2:]]

    def batch_worker(part):
        store.ingest_batch(part)

    def realtime_worker(seed):
        rnd = random.Random(seed)
        for _ in range(300):
            e = rnd.choice(events)  # 实时线程反复重放同一事件池（重复/乱序）
            store.ingest(e.pipeline_id, e.run_id, e.attempt, e.event_type,
                         e.event_ts, received_ts=e.event_ts)

    threads = [threading.Thread(target=batch_worker, args=(h,))
               for h in halves]
    threads += [threading.Thread(target=realtime_worker, args=(s,))
                for s in range(3)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    runs = _runs_snapshot(store)
    win = _window_snapshot(store, "p")
    alert = store.peek_alert("p", 100_000, 3, 300.0)
    log.info("concurrent => window(total=%d ok=%d fail=%d avg=%.3f p95=%.3f) "
             "alert(would_fire=%s reason=%s)",
             win[0], win[1], win[2], win[3], win[4],
             not alert.suppressed, alert.reason)
    assert runs == ref_runs
    assert win == ref_win
    assert (alert.suppressed, alert.reason, alert.consecutive_failures) == (
        ref_alert.suppressed, ref_alert.reason,
        ref_alert.consecutive_failures)


# ---------------------------------------------------------------------------
# 4. 乱序批次重试多遍：幂等，耗时/计数/告警不被重试弄乱
# ---------------------------------------------------------------------------

def test_shuffled_batch_retries_are_idempotent_including_latency():
    events = _latency_events()
    store = PipelineStore()
    first = store.ingest_batch(events)
    assert first.invalid == 0
    base_runs = _runs_snapshot(store)
    base_win = _window_snapshot(store, "p")
    base_alert = store.check_alert("p", 100_000, 3, 300.0)

    rnd = random.Random(7)
    for round_no in range(5):
        shuffled = events[:]
        rnd.shuffle(shuffled)
        res = store.ingest_batch(shuffled)
        assert res.accepted == 0 and res.invalid == 0
        assert res.duplicate == len(events)
        assert _runs_snapshot(store) == base_runs
        assert _window_snapshot(store, "p") == base_win
        # 重试不推进在线告警抑制、不多发告警
        alert = store.peek_alert("p", 100_000, 3, 300.0)
        assert (alert.suppressed, alert.reason) == (
            True, "already_alerted_streak")
        log.info("retry round %d => duplicate=%d window avg=%.3f p95=%.3f "
                 "alert=%s (unchanged)", round_no, res.duplicate,
                 base_win[3], base_win[4], alert.reason)
    assert base_alert.suppressed is False  # 首轮确实触发过告警
