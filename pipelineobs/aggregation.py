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
            w_end = window_seconds * math.floor(end_ts / window_seconds)
            w_start = w_end - window_seconds
            total = succ = fail = 0
            latencies: list[float] = []
            for o in self._ordered(pipeline_id):
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
                avg_latency=avg, p95_latency=self._p95(latencies))

    def consecutive_failures(self, pipeline_id: str,
                             end_ts: float) -> tuple[int, tuple[str, ...]]:
        """返回截至 end_ts 的尾部连续失败次数与失败 run_id 序列（全序确定）。"""
        streak: list[str] = []
        for o in reversed(self._ordered(pipeline_id)):
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
