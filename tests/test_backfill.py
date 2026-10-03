"""批量回补：逐条结论、部分失败、重复批次幂等、与逐条单报等价、并发与量级。"""
import logging
import random
import threading
import time

from pipelineobs.store import PipelineStore

log = logging.getLogger("test.backfill")


def make_events():
    """构造一批跨流水线、乱序、含重复的历史事件。"""
    return [
        {"pipeline_id": "p1", "run_id": "r2", "attempt": 1,
         "event_type": "failed", "event_ts": 130},
        {"pipeline_id": "p2", "run_id": "r9", "attempt": 1,
         "event_type": "started", "event_ts": 100},
        {"pipeline_id": "p1", "run_id": "r1", "attempt": 1,
         "event_type": "succeeded", "event_ts": 120},
        {"pipeline_id": "p1", "run_id": "r1", "attempt": 1,
         "event_type": "started", "event_ts": 100},
        {"pipeline_id": "p1", "run_id": "r2", "attempt": 1,
         "event_type": "started", "event_ts": 110},
        {"pipeline_id": "p2", "run_id": "r9", "attempt": 1,
         "event_type": "succeeded", "event_ts": 140},
        # 批内重复
        {"pipeline_id": "p1", "run_id": "r1", "attempt": 1,
         "event_type": "succeeded", "event_ts": 120},
        # r2 重试成功
        {"pipeline_id": "p1", "run_id": "r2", "attempt": 2,
         "event_type": "started", "event_ts": 150},
        {"pipeline_id": "p1", "run_id": "r2", "attempt": 2,
         "event_type": "succeeded", "event_ts": 160},
    ]


def snapshot(store):
    """提取可对比的收敛状态：运行状态/尝试、窗口统计、连续失败。"""
    runs = {}
    for s in store.list_runs():
        runs[(s.pipeline_id, s.run_id)] = (
            s.status,
            {n: (a.status, a.started_ts, a.finished_ts, a.latency)
             for n, a in s.attempts.items()})
    windows = {pid: store.window(pid, 1000, 1000)
               for pid in ("p1", "p2")}
    streaks = {pid: store.aggregator.consecutive_failures(pid, 1000)
               for pid in ("p1", "p2")}
    return runs, windows, streaks


def test_batch_ingest_per_item_outcomes():
    store = PipelineStore()
    result = store.ingest_batch(make_events(), received_ts=1000.0)
    log.info("INPUT batch=%d events => summary=%s", len(make_events()),
             result.summary)
    for item in result.items:
        log.info("item=%d => DECISION status=%s lifecycle=%s aggregate=%s",
                 item.index, item.status, item.lifecycle_reason,
                 item.aggregate_reason)
    assert result.summary.total == 9
    assert result.summary.rejected == 0
    assert result.summary.duplicates == 1  # 批内重复
    assert result.summary.applied == 8
    # 每条都有处理结论，且下标与输入一一对应
    assert [i.index for i in result.items] == list(range(9))
    assert result.items[6].status == "duplicate"
    assert result.items[6].lifecycle_reason == "duplicate_event"
    # 重试成功：运行级收敛为 succeeded，聚合就地更新
    run = store.get_run("p1", "r2")
    assert run.status.value == "succeeded"


def test_batch_equivalent_to_single_ingest_any_order():
    events = make_events()
    batch_store = PipelineStore()
    batch_store.ingest_batch(events, received_ts=1000.0)

    single_store = PipelineStore()
    shuffled = list(events)
    random.Random(42).shuffle(shuffled)
    for e in shuffled:
        single_store.ingest(e["pipeline_id"], e["run_id"], e["attempt"],
                            e["event_type"], e["event_ts"],
                            received_ts=1000.0)
    b_runs, b_win, b_streak = snapshot(batch_store)
    s_runs, s_win, s_streak = snapshot(single_store)
    log.info("batch=%s single=%s", b_win, s_win)
    assert b_runs == s_runs
    assert b_win == s_win
    assert b_streak == s_streak


def test_partial_failure_valid_events_still_apply():
    store = PipelineStore()
    events = [
        {"pipeline_id": "p", "run_id": "r1", "event_type": "started",
         "event_ts": 100},
        "not-an-object",
        {"pipeline_id": "p", "run_id": "r1", "event_type": "bogus"},
        {"pipeline_id": "", "run_id": "r2", "event_type": "started"},
        {"pipeline_id": "p", "run_id": "r2", "event_type": "started",
         "attempt": 0},
        {"pipeline_id": "p", "run_id": "r2", "event_type": "started",
         "event_ts": -5},
        {"pipeline_id": "p", "run_id": "r1", "event_type": "succeeded",
         "event_ts": 110},
    ]
    result = store.ingest_batch(events, received_ts=1000.0)
    errors = {i.index: i.error for i in result.items if i.status == "rejected"}
    log.info("INPUT %d events => rejected=%s", len(events), errors)
    assert result.summary.rejected == 5
    assert result.summary.applied == 2
    # 原因码可区分
    assert errors[1] == "payload_not_object"
    assert errors[2] == "invalid_event_type"
    assert errors[3] == "missing_pipeline_id"
    assert errors[4] == "invalid_attempt"
    assert errors[5] == "invalid_event_ts"
    # 合法事件照常生效
    assert store.get_run("p", "r1").status.value == "succeeded"
    w = store.window("p", 1000, 1000)
    assert (w.total_runs, w.succeeded_runs) == (1, 1)


def test_duplicate_batch_is_idempotent():
    store = PipelineStore()
    # p3 尾部两次连续失败，用于验证重复批次不会重复告警
    events = make_events() + [
        {"pipeline_id": "p3", "run_id": "r10", "event_type": "failed",
         "event_ts": 300},
        {"pipeline_id": "p3", "run_id": "r11", "event_type": "failed",
         "event_ts": 310},
    ]
    first = store.ingest_batch(events, received_ts=1000.0)
    before = snapshot(store)
    second = store.ingest_batch(events, received_ts=2000.0)
    log.info("first=%s second=%s", first.summary, second.summary)
    # 第二次：全部判重，不多算
    assert second.summary.applied == 0
    assert second.summary.duplicates == len(events)
    assert snapshot(store) == before
    # 不重复告警：失败序列只告警一次
    a1 = store.check_alert("p3", 1000, 2, 300)
    a2 = store.check_alert("p3", 1000, 2, 300)
    log.info("alert first=%s/%s second=%s/%s", a1.suppressed, a1.reason,
             a2.suppressed, a2.reason)
    assert not a1.suppressed and a1.reason == "threshold_reached"
    assert a2.suppressed and a2.reason == "already_alerted_streak"


def test_batch_and_realtime_concurrent_deterministic():
    events = make_events()
    realtime = [
        {"pipeline_id": "p1", "run_id": "r3", "attempt": 1,
         "event_type": "started", "event_ts": 200},
        {"pipeline_id": "p1", "run_id": "r3", "attempt": 1,
         "event_type": "failed", "event_ts": 210},
        {"pipeline_id": "p2", "run_id": "r8", "attempt": 1,
         "event_type": "started", "event_ts": 220},
        {"pipeline_id": "p2", "run_id": "r8", "attempt": 1,
         "event_type": "succeeded", "event_ts": 230},
    ]
    store = PipelineStore()

    def do_batch():
        store.ingest_batch(events, received_ts=1000.0)

    def do_realtime():
        for e in realtime:
            store.ingest(e["pipeline_id"], e["run_id"], e["attempt"],
                         e["event_type"], e["event_ts"], received_ts=1000.0)

    t1 = threading.Thread(target=do_batch)
    t2 = threading.Thread(target=do_realtime)
    t1.start(); t2.start(); t1.join(); t2.join()

    # 参考：同一批事件按某顺序逐条处理
    ref = PipelineStore()
    for e in events + realtime:
        ref.ingest(e["pipeline_id"], e["run_id"], e["attempt"],
                   e["event_type"], e["event_ts"], received_ts=1000.0)
    got, ref_snap = snapshot(store), snapshot(ref)
    log.info("concurrent=%s reference=%s", got[1], ref_snap[1])
    assert got == ref_snap


def test_batch_scale_thousands_of_events():
    store = PipelineStore()
    n_runs = 1500
    events = []
    for i in range(n_runs):
        pid = f"p{i % 3}"
        events.append({"pipeline_id": pid, "run_id": f"r{i}",
                       "event_type": "started", "event_ts": float(100 + i)})
        events.append({"pipeline_id": pid, "run_id": f"r{i}",
                       "event_type": "succeeded" if i % 2 else "failed",
                       "event_ts": float(100 + i) + 0.5})
    random.Random(7).shuffle(events)
    start = time.monotonic()
    result = store.ingest_batch(events, received_ts=1000.0)
    elapsed = time.monotonic() - start
    log.info("INPUT %d events => summary=%s elapsed=%.3fs",
             len(events), result.summary, elapsed)
    assert result.summary.total == 2 * n_runs
    assert result.summary.applied == 2 * n_runs
    assert result.summary.rejected == 0
    assert elapsed < 2.0  # 几千条量级应在亚秒级完成
    total = sum(store.window(f"p{k}", 10000, 10000).total_runs
                for k in range(3))
    assert total == n_runs
    # 乱序重试同一批：幂等
    again = store.ingest_batch(events, received_ts=3000.0)
    assert again.summary.applied == 0
    assert again.summary.duplicates == 2 * n_runs
