"""历史窗口追溯重算：基于已导入事件复盘，不改动在线状态、不真发告警。

事实来源是生命周期的事件留痕（journal）：只保留 order_ts <= end_ts 的事件，
按 (order_ts, received_ts) 全序喂给一个全新的生命周期管理器重建当时状态，
再用与在线完全相同的规范终态规则推导运行结论与窗口统计。

告警判定分两侧：
- historical：没有任何“已发告警”快照，只回答“当时该不该告”，不写状态；
- online：读取当前抑制快照做只读判定（peek），不推进、不消费在线抑制。

因此重算对在线状态、真实告警与告警抑制均无副作用。
"""
from __future__ import annotations

import logging

from .aggregation import _Outcome
from .lifecycle import LifecycleManager, canonical_outcome
from .models import (
    AlertDecision,
    ReplayResult,
    ReplayView,
    RunChangeKind,
    RunEvent,
    RunOutcomeChange,
    RunOutcomeView,
)

logger = logging.getLogger("pipelineobs.replay")


def _to_view(o: _Outcome) -> RunOutcomeView:
    return RunOutcomeView(run_id=o.run_id, succeeded=o.succeeded,
                          finished_ts=o.finished_ts, latency=o.latency)


class Replayer:
    def __init__(self, store) -> None:
        self._store = store

    def _historical_outcomes(self, pipeline_id: str,
                             end_ts: float) -> list[_Outcome]:
        """重建 end_ts 时刻可见事件推导出的全部运行终态结论。"""
        records = [r for r in self._store.journal(pipeline_id)
                   if r.order_ts <= end_ts]
        records.sort(key=lambda r: (r.order_ts, r.received_ts))

        scratch = LifecycleManager()
        for rec in records:
            event = RunEvent(
                pipeline_id=rec.pipeline_id, run_id=rec.run_id,
                attempt=rec.attempt, event_type=rec.event_type,
                event_ts=rec.order_ts if rec.ts_present else None,
                received_ts=rec.received_ts, detail=rec.detail)
            scratch.ingest(event)

        outcomes: list[_Outcome] = []
        for state in scratch.all_runs(pipeline_id):
            oc = canonical_outcome(state)
            if oc is not None:
                succeeded, finished_ts, latency = oc
                outcomes.append(_Outcome(state.run_id, succeeded,
                                         float(finished_ts), latency))
        outcomes.sort(key=lambda o: (o.finished_ts, o.run_id))
        return outcomes

    def replay_window(self, pipeline_id: str, end_ts: float,
                      window_seconds: float, threshold: int = 3,
                      cooldown_seconds: float = 300.0) -> ReplayResult:
        if window_seconds <= 0:
            raise ValueError("window_seconds must be positive")
        agg = self._store.aggregator
        with self._store.lock:
            hist = self._historical_outcomes(pipeline_id, end_ts)
            online = agg.outcomes(pipeline_id)

            hist_win = agg._window_from_outcomes(
                pipeline_id, end_ts, window_seconds, hist)
            online_win = agg._window_from_outcomes(
                pipeline_id, end_ts, window_seconds, online)

            # 历史侧：无已发告警快照、只看 end_ts 前可见事件；
            # 在线侧：当前全部已知终态（不按 end 截断）+ 当前抑制快照。
            # 两侧都不写状态。
            hist_alert = agg.decide_alert(
                pipeline_id, end_ts, threshold, cooldown_seconds,
                last_alert=None, ordered=hist)
            online_cutoff = max((o.finished_ts for o in online),
                                default=end_ts)
            online_alert = agg.decide_alert(
                pipeline_id, online_cutoff, threshold, cooldown_seconds,
                last_alert=agg.last_alert_snapshot(pipeline_id),
                ordered=online)

            hist_view = ReplayView(
                window=hist_win,
                alert=AlertDecision(
                    consecutive_failures=hist_alert.consecutive_failures,
                    threshold=threshold,
                    would_alert=not hist_alert.suppressed,
                    reason=hist_alert.reason),
                runs=[_to_view(o) for o in hist
                      if hist_win.window_start <= o.finished_ts
                      < hist_win.window_end])
            online_view = ReplayView(
                window=online_win,
                alert=AlertDecision(
                    consecutive_failures=online_alert.consecutive_failures,
                    threshold=threshold,
                    would_alert=not online_alert.suppressed,
                    reason=online_alert.reason),
                runs=[_to_view(o) for o in online
                      if online_win.window_start <= o.finished_ts
                      < online_win.window_end])

            changes = self._diff_runs(hist, online)
            result = ReplayResult(
                pipeline_id=pipeline_id,
                window_start=hist_win.window_start,
                window_end=hist_win.window_end,
                historical=hist_view, online=online_view,
                run_changes=changes,
                total_delta=online_win.total_runs - hist_win.total_runs,
                succeeded_delta=online_win.succeeded_runs
                - hist_win.succeeded_runs,
                failed_delta=online_win.failed_runs - hist_win.failed_runs,
                alert_changed=(hist_view.alert.would_alert
                               != online_view.alert.would_alert
                               or hist_view.alert.reason
                               != online_view.alert.reason))

        logger.info(
            "[replay] pipeline=%s window=[%.3f,%.3f) hist(total=%d ok=%d "
            "fail=%d alert=%s/%s) online(total=%d ok=%d fail=%d alert=%s/%s) "
            "changes=%d alert_changed=%s",
            pipeline_id, result.window_start, result.window_end,
            hist_win.total_runs, hist_win.succeeded_runs,
            hist_win.failed_runs, hist_view.alert.would_alert,
            hist_view.alert.reason, online_win.total_runs,
            online_win.succeeded_runs, online_win.failed_runs,
            online_view.alert.would_alert, online_view.alert.reason,
            len(changes), result.alert_changed)
        return result

    @staticmethod
    def _diff_runs(hist: list[_Outcome],
                   online: list[_Outcome]) -> list[RunOutcomeChange]:
        h = {o.run_id: o for o in hist}
        o = {x.run_id: x for x in online}
        changes: list[RunOutcomeChange] = []
        for run_id in sorted(set(h) | set(o)):
            old, new = h.get(run_id), o.get(run_id)
            if old is None and new is not None:
                kind = RunChangeKind.ADDED
            elif old is not None and new is None:
                kind = RunChangeKind.REMOVED
            elif (old.succeeded, old.finished_ts, old.latency) != (
                    new.succeeded, new.finished_ts, new.latency):
                kind = RunChangeKind.CHANGED
            else:
                continue
            changes.append(RunOutcomeChange(
                run_id=run_id, kind=kind,
                historical=_to_view(old) if old else None,
                online=_to_view(new) if new else None))
        return changes
