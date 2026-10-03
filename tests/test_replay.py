"""历史窗口追溯重算：截至时刻截断、与在线对比、不改在线状态、不真发告警。

重算语义：窗口为对齐半开区间 [end-W, end)；“当时”= 仅使用 order_ts <= end
的事件重建。历史与在线出现差异只可能来自：
1. 窗口结束之后才发生/导入的运行（在线可见、当时不可见）；
2. 运行在窗口结束后才收敛（当时失败在窗内，后来重试成功、终态时间移出窗口）。
"""
import logging

from pipelineobs.models import BackfillItem
from pipelineobs.store import PipelineStore

log = logging.getLogger("test.replay")


def batch(store, rows):
    store.ingest_batch(
        [BackfillItem("p", rid, att, etype, ts)
         for rid, att, etype, ts in rows])


def test_replay_as_of_cutoff_reconstructs_past():
    store = PipelineStore()
    batch(store, [
        ("r1", 1, "succeeded", 110),  # 窗内，当时已可见
        ("r2", 1, "failed", 220),     # 窗口结束后才发生
        ("r3", 1, "failed", 230),
    ])
    rp = store.replay_window("p", end_ts=200, window_seconds=200,
                             threshold=2)
    assert rp.window_start == 0 and rp.window_end == 200
    # 当时窗口：只有 r1；在线：窗口统计仍是 r1（r2/r3 在窗口外）
    assert (rp.historical.window.total_runs,
            rp.historical.window.succeeded_runs,
            rp.historical.window.failed_runs) == (1, 1, 0)
    assert rp.online.window.total_runs == 1
    # 但总体运行差异与告警结论可看出后来发生了什么
    added = sorted((c.run_id, c.kind.value) for c in rp.run_changes)
    assert added == [("r2", "added"), ("r3", "added")]
    assert rp.historical.alert.would_alert is False
    assert rp.online.alert.would_alert is True  # 尾部两连失败
    assert rp.alert_changed is True
    assert rp.changed is True


def test_replay_detects_failure_to_success_change():
    store = PipelineStore()
    batch(store, [
        ("r1", 1, "failed", 110),      # 当时：窗内一次失败
        ("r1", 2, "succeeded", 250),   # 窗口结束后重试成功，终态移出窗口
    ])
    rp = store.replay_window("p", 200, 200, threshold=1)
    assert (rp.historical.window.total_runs,
            rp.historical.window.succeeded_runs,
            rp.historical.window.failed_runs) == (1, 0, 1)
    assert rp.online.window.total_runs == 0
    kinds = {c.run_id: c.kind.value for c in rp.run_changes}
    assert kinds == {"r1": "changed"}
    assert rp.historical.runs[0].succeeded is False
    assert rp.historical.alert.would_alert is True
    assert rp.online.alert.would_alert is False
    assert rp.alert_changed is True


def test_replay_does_not_mutate_online_state_or_alert_suppression():
    store = PipelineStore()
    batch(store, [
        ("f1", 1, "failed", 100),
        ("f2", 1, "failed", 101),
        ("f3", 1, "failed", 102),
    ])
    online_before = {r.run_id: r.status.value for r in store.list_runs("p")}
    snapshot_before = store.aggregator.last_alert_snapshot("p")
    assert snapshot_before is None

    # 历史侧判定“当时该告”，但不允许真发/留下抑制痕迹
    rp = store.replay_window("p", 200, 200, threshold=3,
                             cooldown_seconds=1000)
    assert rp.historical.alert.would_alert is True
    assert rp.historical.alert.reason == "threshold_reached"
    assert store.aggregator.last_alert_snapshot("p") is None

    # 在线真实告警检查仍然会触发（重算没有消费这次告警）
    alert = store.check_alert("p", 200, threshold=3, cooldown_seconds=1000)
    assert alert.suppressed is False and alert.reason == "threshold_reached"

    # 再重算：在线侧应识别为已告警（只读），且仍不推进状态
    rp2 = store.replay_window("p", 200, 200, threshold=3,
                              cooldown_seconds=1000)
    assert rp2.online.alert.would_alert is False
    assert rp2.online.alert.reason == "already_alerted_streak"
    assert rp2.historical.alert.would_alert is True  # 历史侧永远干净
    online_after = {r.run_id: r.status.value for r in store.list_runs("p")}
    assert online_after == online_before


def test_replay_no_diff_when_nothing_changed():
    store = PipelineStore()
    batch(store, [("r1", 1, "succeeded", 100)])
    rp = store.replay_window("p", 200, 200)
    assert (rp.historical.window.total_runs,
            rp.online.window.total_runs) == (1, 1)
    assert rp.run_changes == [] and rp.alert_changed is False
    assert rp.changed is False


def test_replay_matches_ingestion_at_that_moment():
    """重算窗口统计必须与“在当时用同样事件逐条上报”得到的窗口一致。"""
    events = [
        ("r1", 1, "started", 100), ("r1", 1, "failed", 110),
        ("r1", 2, "started", 120), ("r1", 2, "succeeded", 130),
        ("r2", 1, "failed", 140),
        ("late", 1, "failed", 250),
    ]
    store = PipelineStore()
    batch(store, events)
    rp = store.replay_window("p", 200, 200, threshold=1)

    reference = PipelineStore()
    batch(reference, [e for e in events if e[3] <= 200])
    ref_win = reference.window("p", 200, 200)
    assert (rp.historical.window.total_runs,
            rp.historical.window.succeeded_runs,
            rp.historical.window.failed_runs) == (
        ref_win.total_runs, ref_win.succeeded_runs, ref_win.failed_runs)
    assert rp.historical.window.avg_latency == ref_win.avg_latency
