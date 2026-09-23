"""线程安全的本地状态门面：串联生命周期、新鲜度与聚合。

三个子系统共享同一把可重入锁，保证“上报 -> 状态收敛 -> 聚合/新鲜度更新”
对外部表现为原子操作，多个上报并发到达时结论确定一致。
"""
from __future__ import annotations

import threading
import time
from dataclasses import dataclass

from .aggregation import Aggregator
from .freshness import FreshnessMonitor
from .lifecycle import LifecycleManager
from .models import (Alert, EventType, FreshnessResult, RunEvent, RunState,
                     RunStatus, WindowAggregate)


@dataclass(frozen=True)
class IngestResult:
    run: RunState
    lifecycle_reason: str
    aggregate_reason: str | None


class PipelineStore:
    def __init__(self, clock=time.time) -> None:
        self._clock = clock
        self._lock = threading.RLock()
        self._lifecycle = LifecycleManager(self._lock)
        self._freshness = FreshnessMonitor(self._lock)
        self._aggregator = Aggregator(self._lock)

    @property
    def freshness(self) -> FreshnessMonitor:
        return self._freshness

    @property
    def aggregator(self) -> Aggregator:
        return self._aggregator

    def ingest(self, pipeline_id: str, run_id: str, attempt: int,
               event_type: str, event_ts: float | None,
               detail: str = "", received_ts: float | None = None) -> IngestResult:
        recv = received_ts if received_ts is not None else self._clock()
        event = RunEvent(
            pipeline_id=pipeline_id, run_id=run_id, attempt=max(1, attempt),
            event_type=EventType(event_type), event_ts=event_ts,
            received_ts=recv, detail=detail)
        with self._lock:
            state, lc_reason = self._lifecycle.ingest(event)

            agg_reason: str | None = None
            applied = lc_reason not in ("duplicate_event",
                                        "attempt_terminal_held")
            if state.status is RunStatus.SUCCEEDED and applied:
                self._freshness.record_success(pipeline_id, event_ts)
            # 聚合单元是“运行”：一个 run 只出现一次，取其最终收敛结论。
            # 失败先登记；若之后重试成功则就地更新（不新增、不遗漏）。
            # 旧尝试迟到的终态事件不影响聚合。
            if applied and state.status in (RunStatus.SUCCEEDED, RunStatus.FAILED) \
                    and event.event_type in (EventType.SUCCEEDED, EventType.FAILED) \
                    and event.attempt == max(state.attempts):
                latest = state.attempts[event.attempt]
                agg_reason = self._aggregator.upsert_run(
                    pipeline_id, run_id,
                    succeeded=state.status is RunStatus.SUCCEEDED,
                    finished_ts=event_ts if event_ts is not None else recv,
                    latency=latest.latency)
            return IngestResult(state, lc_reason, agg_reason)

    def get_run(self, pipeline_id: str, run_id: str) -> RunState | None:
        return self._lifecycle.get(pipeline_id, run_id)

    def list_runs(self, pipeline_id: str | None = None) -> list[RunState]:
        return self._lifecycle.all_runs(pipeline_id)

    def evaluate_freshness(self, pipeline_id: str,
                           evaluated_at: float | None = None,
                           interval_seconds: float | None = None) -> FreshnessResult:
        at = evaluated_at if evaluated_at is not None else self._clock()
        return self._freshness.evaluate(pipeline_id, at, interval_seconds)

    def set_interval(self, pipeline_id: str, interval_seconds: float) -> None:
        self._freshness.set_interval(pipeline_id, interval_seconds)

    def window(self, pipeline_id: str, end_ts: float,
               window_seconds: float) -> WindowAggregate:
        return self._aggregator.window(pipeline_id, end_ts, window_seconds)

    def check_alert(self, pipeline_id: str, end_ts: float | None,
                    threshold: int, cooldown_seconds: float) -> Alert:
        at = end_ts if end_ts is not None else self._clock()
        return self._aggregator.check_alert(
            pipeline_id, at, threshold, cooldown_seconds)
