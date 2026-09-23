"""聚合与告警：窗口边界、成功率/延迟、连续失败、抑制、并发一致。"""
import logging
import threading

import pytest

from pipelineobs.aggregation import Aggregator

log = logging.getLogger("test.aggregation")


def win(a, pid, end, w):
    r = a.window(pid, end, w)
    log.info("INPUT end=%s window=%s => total=%d ok=%d fail=%d rate=%.3f "
             "avg=%s p95=%s", end, w, r.total_runs, r.succeeded_runs,
             r.failed_runs, r.success_rate, r.avg_latency, r.p95_latency)
    return r


def test_window_boundary_half_open_assignment():
    a = Aggregator()
    a.add_run("p", "r0", True, 0.0, 1.0)    # [0,10)
    a.add_run("p", "r1", False, 10.0, 2.0)  # 边界归 [10,20)
    a.add_run("p", "r2", True, 19.999, 3.0)
    a.add_run("p", "r3", True, 20.0, 4.0)   # 边界归 [20,30)
    w1 = win(a, "p", 20, 10)
    assert (w1.window_start, w1.window_end) == (10, 20)
    assert (w1.total_runs, w1.succeeded_runs, w1.failed_runs) == (2, 1, 1)
    assert abs(w1.success_rate - 0.5) < 1e-9
    w0 = win(a, "p", 10, 10)
    assert w0.total_runs == 1 and w0.succeeded_runs == 1
    w2 = win(a, "p", 30, 10)
    assert w2.total_runs == 1
    # 所有窗口计数之和 == 去重后的 run 总数（不重复不遗漏）
    total = sum(win(a, "p", e, 10).total_runs for e in (10, 20, 30))
    assert total == 4


def test_latency_stats_nearest_rank_p95():
    a = Aggregator()
    for i in range(20):
        a.add_run("p", f"r{i}", True, 0.1 + i * 0.1, latency=float(i + 1))
    w = win(a, "p", 10, 10)
    assert w.total_runs == 20
    assert w.avg_latency == 10.5
    # nearest-rank: ceil(0.95*20)=19 => 第 19 小 = 19.0
    assert w.p95_latency == 19.0


def test_idempotent_run_not_double_counted():
    a = Aggregator()
    assert a.add_run("p", "r1", False, 5, 1) == "added"
    assert a.add_run("p", "r1", False, 5, 1) == "duplicate_run"
    assert win(a, "p", 10, 10).total_runs == 1


def test_alert_threshold_streak_and_dedup():
    a = Aggregator()
    for i in range(3):
        a.add_run("p", f"f{i}", False, float(i), None)
    al = a.check_alert("p", 2, 3, 100)
    log.info("alert => suppressed=%s reason=%s streak=%d",
             al.suppressed, al.reason, al.consecutive_failures)
    assert not al.suppressed and al.reason == "threshold_reached"
    # 同一失败序列重复判定 => 抑制
    al = a.check_alert("p", 2, 3, 100)
    assert al.suppressed and al.reason == "already_alerted_streak"


def test_alert_cooldown_suppresses_new_streak():
    a = Aggregator()
    for i in range(3):
        a.add_run("p", f"f{i}", False, float(i), None)
    a.check_alert("p", 2, 3, cooldown_seconds=100)
    # 冷却窗口内出现新的连续失败序列
    a.add_run("p", "f3", False, 3.0, None)
    al = a.check_alert("p", 3, 3, 100)
    assert al.suppressed and al.reason == "cooldown_active"
    # 冷却结束后，新序列可再次告警
    al = a.check_alert("p", 102, 3, 100)
    assert not al.suppressed and al.reason == "threshold_reached"


def test_success_resets_streak():
    a = Aggregator()
    a.add_run("p", "f1", False, 1, None)
    a.add_run("p", "f2", False, 2, None)
    a.add_run("p", "s1", True, 3, None)
    a.add_run("p", "f3", False, 4, None)
    al = a.check_alert("p", 4, 3, 100)
    assert al.suppressed and al.reason == "below_threshold"
    assert al.consecutive_failures == 1


def test_concurrent_ingestion_consistent_count():
    a = Aggregator()
    n_threads, per_thread = 8, 100

    def worker(t):
        for i in range(per_thread):
            rid = f"t{t}-r{i}"
            a.add_run("p", rid, i % 2 == 0, float(t * 1000 + i),
                      latency=1.0)

    threads = [threading.Thread(target=worker, args=(t,))
               for t in range(n_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    # 用一个足够宽的窗口统计全部
    w = win(a, "p", 1_000_000, 1_000_000)
    assert w.total_runs == n_threads * per_thread
    assert w.succeeded_runs + w.failed_runs == w.total_runs
    assert w.avg_latency == 1.0


def test_concurrent_duplicate_same_run_counts_once():
    a = Aggregator()

    def worker():
        for _ in range(50):
            a.add_run("p", "same", False, 5.0, None)

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert win(a, "p", 10, 10).total_runs == 1
