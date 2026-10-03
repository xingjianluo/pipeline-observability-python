"""按流水线与时间窗口聚合，并在连续失败达到阈值时触发告警。

确定性约定：
- 窗口按 epoch 边界对齐：window_start = floor(finished_ts / W) * W，
  区间半开 [window_start, window_start + W)；恰好落在边界的产出归入
  起点所在窗口，保证边界归属唯一、不重复不遗漏。
- 同一 run_id 只计一次，重复/并发上报幂等。
- 连续失败按 (finished_ts, run_id) 全序排序后从尾部回数。
- 告警抑制：同一失败序列只告警一次（already_alerted_streak）；
  冷却窗口内新序列不重复告警（cooldown_active）。
- p95 采用 nearest-rank 法，结果确定。
"""
from __future__ import annotations

import math
import threading
from dataclasses import dataclass

from .models import Alert, WindowAggregate


@dataclass(frozen=True)
class _Outcome:
    run_id: str
    succeeded: bool
    finished_ts: float
    latency: float | None


class Aggregator:
    def __init__(self, lock: threading.RLock | None = None) -> None:
        self._lock = lock or threading.RLock()
        self._runs: dict[str, dict[str, _Outcome]] = {}
        # pipeline_id -> (最后告警时间, 失败序列键)
        self._last_alert: dict[str, tuple[float, tuple[str, ...]]] = {}

    @property
    def lock(self) -> threading.RLock:
        return self._lock

    def add_run(self, pipeline_id: str, run_id: str, succeeded: bool,
                finished_ts: float, latency: float | None) -> str:
        """登记一个进入终态的运行。幂等：返回 added 或 duplicate_run。"""
        with self._lock:
            runs = self._runs.setdefault(pipeline_id, {})
            if run_id in runs:
                return "duplicate_run"
            runs[run_id] = _Outcome(run_id, succeeded, float(finished_ts),
                                    latency)
            return "added"

    def upsert_run(self, pipeline_id: str, run_id: str, succeeded: bool,
                   finished_ts: float, latency: float | None) -> str:
        """登记或按最终收敛结论更新一个运行。

        - 首次见到该 run：added；
        - 结论与终态时间均未变：duplicate_run；
        - 失败后重试成功：updated_to_success（成功不回退为失败）；
        - 成功后收到成功（可能时间更新）：保持成功，时间取较早者不回退。
        """
        with self._lock:
            runs = self._runs.setdefault(pipeline_id, {})
            existing = runs.get(run_id)
            if existing is None:
                runs[run_id] = _Outcome(run_id, succeeded,
                                        float(finished_ts), latency)
                return "added"
            if existing.succeeded and not succeeded:
                return "duplicate_run"
            if existing.succeeded == succeeded and \
                    existing.finished_ts == float(finished_ts):
                return "duplicate_run"
            if succeeded and not existing.succeeded:
                runs[run_id] = _Outcome(
                    run_id, True,
                    min(existing.finished_ts, float(finished_ts)), latency)
                return "updated_to_success"
            return "duplicate_run"

    def put_run(self, pipeline_id: str, run_id: str, succeeded: bool,
                finished_ts: float, latency: float | None) -> str:
        """按“运行全部尝试的规范终态结论”幂等写入，结论与到达顺序无关。

        调用方（store/重算）每次都传入由生命周期状态推导出的同一规范结论，
        因此同一批事件乱序重试只得到 added / updated / unchanged 三态，
        不存在重复计数，也不存在因到达顺序不同导致终态时间不同。
        成功结论不回退为失败。
        """
        with self._lock:
            runs = self._runs.setdefault(pipeline_id, {})
            outcome = _Outcome(run_id, succeeded, float(finished_ts), latency)
            existing = runs.get(run_id)
            if existing is None:
                runs[run_id] = outcome
                return "added"
            if existing.succeeded and not outcome.succeeded:
                # 成功是运行级终态，不被失败覆盖
                outcome = _Outcome(run_id, True, existing.finished_ts,
                                   existing.latency)
            if (existing.succeeded, existing.finished_ts,
                    existing.latency) == (outcome.succeeded,
                                          outcome.finished_ts,
                                          outcome.latency):
                return "unchanged"
            runs[run_id] = outcome
            return "updated"

    def outcomes(self, pipeline_id: str) -> list[_Outcome]:
        """返回该流水线全部运行终态的有序副本。"""
        with self._lock:
            return list(self._ordered(pipeline_id))

    def outcome(self, pipeline_id: str, run_id: str) -> _Outcome | None:
        with self._lock:
            return self._runs.get(pipeline_id, {}).get(run_id)

    def _ordered(self, pipeline_id: str) -> list[_Outcome]:
        return sorted(self._runs.get(pipeline_id, {}).values(),
                      key=lambda o: (o.finished_ts, o.run_id))

    @staticmethod
    def _p95(latencies: list[float]) -> float | None:
        if not latencies:
            return None
        ordered = sorted(latencies)
        rank = math.ceil(0.95 * len(ordered))
        return ordered[max(0, rank - 1)]

    def window(self, pipeline_id: str, end_ts: float,
               window_seconds: float) -> WindowAggregate:
        if window_seconds <= 0:
            raise ValueError("window_seconds must be positive")
        with self._lock:
            return self._window_from_outcomes(
                pipeline_id, end_ts, window_seconds,
                self._ordered(pipeline_id))

    @staticmethod
    def _window_from_outcomes(pipeline_id: str, end_ts: float,
                              window_seconds: float,
                              ordered: list[_Outcome]) -> WindowAggregate:
        w_end = window_seconds * math.floor(end_ts / window_seconds)
        w_start = w_end - window_seconds
        total = succ = fail = 0
        latencies: list[float] = []
        for o in ordered:
            if w_start <= o.finished_ts < w_end:
                total += 1
                if o.succeeded:
                    succ += 1
                else:
                    fail += 1
                if o.latency is not None:
                    latencies.append(o.latency)
        rate = succ / total if total else 0.0
        avg = sum(latencies) / len(latencies) if latencies else None
        return WindowAggregate(
            pipeline_id=pipeline_id, window_start=w_start, window_end=w_end,
            total_runs=total, succeeded_runs=succ, failed_runs=fail,
            success_rate=rate, latencies=list(latencies),
            avg_latency=avg, p95_latency=Aggregator._p95(latencies))

    def consecutive_failures(self, pipeline_id: str,
                             end_ts: float,
                             ordered: list[_Outcome] | None = None
                             ) -> tuple[int, tuple[str, ...]]:
        """返回截至 end_ts 的尾部连续失败次数与失败 run_id 序列（全序确定）。"""
        if ordered is None:
            ordered = self._ordered(pipeline_id)
        streak: list[str] = []
        for o in reversed(ordered):
            if o.finished_ts > end_ts:
                continue
            if o.succeeded:
                break
            streak.append(o.run_id)
        streak.reverse()
        return len(streak), tuple(streak)

    def check_alert(self, pipeline_id: str, end_ts: float,
                    threshold: int, cooldown_seconds: float) -> Alert:
        with self._lock:
            count, streak = self.consecutive_failures(pipeline_id, end_ts)
            if count < threshold:
                return Alert(pipeline_id, end_ts, count, threshold,
                             suppressed=True, reason="below_threshold")
            last = self._last_alert.get(pipeline_id)
            if last is not None and last[1] == streak:
                return Alert(pipeline_id, end_ts, count, threshold,
                             suppressed=True,
                             reason="already_alerted_streak")
            if last is not None and end_ts - last[0] < cooldown_seconds:
                return Alert(pipeline_id, end_ts, count, threshold,
                             suppressed=True, reason="cooldown_active")
            self._last_alert[pipeline_id] = (end_ts, streak)
            return Alert(pipeline_id, end_ts, count, threshold,
                         suppressed=False, reason="threshold_reached")

    def decide_alert(self, pipeline_id: str, end_ts: float,
                     threshold: int, cooldown_seconds: float,
                     *, last_alert: tuple[float, tuple[str, ...]] | None,
                     ordered: list[_Outcome] | None = None) -> Alert:
        """纯告警判定：给定失败序列与“上次告警”快照得出结论，不写入任何状态。

        追溯重算用它对历史（无任何已发告警快照）和在线（当前抑制快照）分别
        判定，保证复盘既不真发告警、也不影响在线抑制。
        """
        count, streak = self.consecutive_failures(
            pipeline_id, end_ts, ordered)
        if count < threshold:
            return Alert(pipeline_id, end_ts, count, threshold,
                         suppressed=True, reason="below_threshold")
        if last_alert is not None and last_alert[1] == streak:
            return Alert(pipeline_id, end_ts, count, threshold,
                         suppressed=True, reason="already_alerted_streak")
        if last_alert is not None and end_ts - last_alert[0] < cooldown_seconds:
            return Alert(pipeline_id, end_ts, count, threshold,
                         suppressed=True, reason="cooldown_active")
        return Alert(pipeline_id, end_ts, count, threshold,
                     suppressed=False, reason="threshold_reached")

    def peek_alert(self, pipeline_id: str, end_ts: float,
                   threshold: int, cooldown_seconds: float) -> Alert:
        """只读告警判定：基于当前在线抑制快照，但不消费、不推进抑制状态。"""
        with self._lock:
            return self.decide_alert(
                pipeline_id, end_ts, threshold, cooldown_seconds,
                last_alert=self._last_alert.get(pipeline_id))

    def last_alert_snapshot(self, pipeline_id: str
                            ) -> tuple[float, tuple[str, ...]] | None:
        with self._lock:
            return self._last_alert.get(pipeline_id)
