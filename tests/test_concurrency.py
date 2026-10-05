"""批量回补与实时上报并发：最终结论顺序无关、乱序重试幂等、几千条不阻塞。"""
import logging
import random
import threading
import time

from pipelineobs.models import BackfillItem
from pipelineobs.store import PipelineStore

log = logging.getLogger("test.concurrency")


def _window_sig(w):
    """窗口签名：计数 + 耗时，任一被并发/重试弄乱都会被发现。"""
    return (w.total_runs, w.succeeded_runs, w.failed_runs,
            tuple(sorted(w.latencies)), w.avg_latency, w.p95_latency)


def _reference_snapshot(events):
    """参照：把全部事件逐条喂给全新 store（随机到达顺序）。"""
    ref = PipelineStore()
    shuffled = events[:]
    random.Random(7).shuffle(shuffled)
    for e in shuffled:
        ref.ingest(e.pipeline_id, e.run_id, e.attempt, e.event_type,
                   e.event_ts, received_ts=e.event_ts)
    state = {}
    for st in ref.list_runs():
        state[(st.pipeline_id, st.run_id)] = st.status.value
    windows = {}
    for pid in sorted({e.pipeline_id for e in events}):
        windows[pid] = _window_sig(ref.window(pid, 1_000_000, 1_000_000))
    return state, windows


def _snapshot(store, pipelines):
    state = {}
    for st in store.list_runs():
        state[(st.pipeline_id, st.run_id)] = st.status.value
    windows = {}
    for pid in pipelines:
        windows[pid] = _window_sig(store.window(pid, 1_000_000, 1_000_000))
    return state, windows


def _build_events():
    events = []
    # p1：started + succeeded，每条运行都有耗时
    for i in range(400):
        events.append(BackfillItem("p1", f"r{i}", 1, "started",
                                   float(1000 + i)))
        events.append(BackfillItem("p1", f"r{i}", 1, "succeeded",
                                   float(1000 + i) + 0.5))
    # p2：先失败后重试成功（同一 run 四条事件，顺序无关地应收敛为成功 1 条，
    # 耗时取成功尝试的 started->succeeded）
    for i in range(200):
        events.append(BackfillItem("p2", f"r{i}", 1, "started",
                                   float(2000 + 2 * i)))
        events.append(BackfillItem("p2", f"r{i}", 1, "failed",
                                   float(2000 + 2 * i) + 0.5))
        events.append(BackfillItem("p2", f"r{i}", 2, "started",
                                   float(2000 + 2 * i) + 0.75))
        events.append(BackfillItem("p2", f"r{i}", 2, "succeeded",
                                   float(2000 + 2 * i + 1)))
    # p3：纯失败，带 started，失败运行也有耗时
    for i in range(300):
        events.append(BackfillItem("p3", f"r{i}", 1, "started",
                                   float(3000 + i)))
        events.append(BackfillItem("p3", f"r{i}", 1, "failed",
                                   float(3000 + i) + 0.25))
    return events


def test_backfill_concurrent_with_realtime_ingest():
    events = _build_events()
    ref_state, ref_win = _reference_snapshot(events)

    store = PipelineStore()
    # 切成多个批次，与实时单条上报线程交错
    batches = [events[0:300], events[300:600], events[600:]]

    def batch_worker():
        for b in batches:
            store.ingest_batch(b)

    stop = threading.Event()

    def realtime_worker(seed):
        rnd = random.Random(seed)
        pool = events[:]
        while not stop.is_set():
            e = rnd.choice(pool)  # 实时线程多为重复事件
            store.ingest(e.pipeline_id, e.run_id, e.attempt, e.event_type,
                         e.event_ts, received_ts=e.event_ts)

    rt_threads = [threading.Thread(target=realtime_worker, args=(s,))
                  for s in range(4)]
    bw = threading.Thread(target=batch_worker)
    bw.start()
    for t in rt_threads:
        t.start()
    bw.join()
    stop.set()
    for t in rt_threads:
        t.join()

    state, win = _snapshot(store, ("p1", "p2", "p3"))
    assert state == ref_state
    assert win == ref_win
    # 计数与耗时都与参照一致（参照本身即“任意逐条顺序”的一种）
    assert win["p1"][:3] == (400, 400, 0)
    assert win["p1"][4:] == (0.5, 0.5)          # avg / p95
    assert len(win["p1"][3]) == 400             # 每条运行都有耗时
    assert win["p2"][:3] == (200, 200, 0)
    assert win["p2"][4:] == (0.25, 0.25)
    assert win["p3"][:3] == (300, 0, 300)
    assert win["p3"][4:] == (0.25, 0.25)


def test_backfill_shuffled_retry_idempotent_under_concurrency():
    # 全部事件（p1 成功 / p2 失败→重试成功 / p3 纯失败），覆盖耗时与告警场景
    events = _build_events()
    store = PipelineStore()
    store.ingest_batch(events)
    expected_state, expected_win = _snapshot(store, ("p1", "p2", "p3"))
    # 基线里每条运行都必须已有耗时，否则“重试后耗时一致”无从谈起
    assert all(sig[4] is not None for sig in expected_win.values())
    # 告警结论基线（只读判定，不消费抑制状态）
    alert_before = store.peek_alert("p3", 1_000_000, 3, 300.0)
    assert alert_before.suppressed is False  # 300 连续失败，理应触发

    def worker(seed):
        rnd = random.Random(seed)
        for _ in range(3):
            b = events[:]
            rnd.shuffle(b)
            res = store.ingest_batch(b)
            assert res.accepted == 0

    threads = [threading.Thread(target=worker, args=(s,)) for s in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    state, win = _snapshot(store, ("p1", "p2", "p3"))
    assert state == expected_state
    assert win == expected_win
    # 乱序重试多遍后告警结论（含连续失败数与序列）不变
    alert_after = store.peek_alert("p3", 1_000_000, 3, 300.0)
    assert alert_after == alert_before


def test_large_batch_is_fast_and_does_not_block_reads():
    store = PipelineStore()
    big = [BackfillItem("p", f"r{i}", 1,
                        "failed" if i % 3 == 0 else "succeeded",
                        float(i)) for i in range(3000)]

    read_latencies = []
    stop = threading.Event()

    def reader():
        while not stop.is_set():
            t0 = time.perf_counter()
            store.window("p", 1_000_000, 1_000_000)
            read_latencies.append(time.perf_counter() - t0)
            time.sleep(0.0005)

    rt = threading.Thread(target=reader)
    rt.start()
    t0 = time.perf_counter()
    res = store.ingest_batch(big)
    elapsed = time.perf_counter() - t0
    stop.set()
    rt.join()

    log.info("3000-event batch took %.3fs; %d concurrent reads, max=%.4fs",
             elapsed, len(read_latencies),
             max(read_latencies, default=0.0))
    assert res.invalid == 0 and res.accepted == 3000
    assert elapsed < 2.0  # 几千条不应有明显不可接受的耗时
    # 回补期间读请求一直能拿到响应（批次在锁内分条，读锁等待仅为单条粒度）
    assert read_latencies, "reader never ran"
    w = store.window("p", 1_000_000, 1_000_000)
    assert w.total_runs == 3000
    assert w.succeeded_runs + w.failed_runs == 3000
