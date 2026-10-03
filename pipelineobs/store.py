"""线程安全的本地状态门面：串联生命周期、新鲜度与聚合。

三个子系统共享同一把可重入锁，保证“上报 -> 状态收敛 -> 聚合/新鲜度更新”
对外部表现为原子操作，多个上报并发到达时结论确定一致。
"""
from __future__ import annotations

import threading
import time
from dataclasses import dataclass

from .aggregation import Aggregator
from .backfill import Backfiller
from .freshness import FreshnessMonitor
from .lifecycle import LifecycleManager, canonical_outcome
from .models import (
    Alert,
    EventRecord,
    EventType,
    FreshnessResult,
    RunEvent,
    RunState,
    RunStatus,
    WindowAggregate,
)
from .replay import Replayer


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
        self._backfiller = Backfiller(self)
        self._replayer = Replayer(self)

    @property
    def lock(self) -> threading.RLock:
        return self._lock

    @property
    def lifecycle(self) -> LifecycleManager:
        return self._lifecycle

    @property
    def freshness(self) -> FreshnessMonitor:
        return self._freshness

    @property
    def aggregator(self) -> Aggregator:
        return self._aggregator

    def apply_event(self, event: RunEvent) -> IngestResult:
        """与 ingest 相同的处理路径，但接受已构造的事件（批量回补共用）。

        必须在 self._lock 已持有时调用（RLock 可重入）。
        """
        state, lc_reason = self._lifecycle.ingest(event)

        agg_reason: str | None = None
        applied = lc_reason not in ("duplicate_event",
                                    "attempt_terminal_held")
        if state.status is RunStatus.SUCCEEDED and applied:
            self._freshness.record_success(event.pipeline_id, event.event_ts)
        # 聚合写入顺序无关的“规范终态结论”：每次都由全部尝试重新推导，
        # 因此同一批事件乱序/并发/重试得到的窗口统计完全一致。
        if lc_reason != "duplicate_event":
            outcome = canonical_outcome(state)
            if outcome is not None:
                succeeded, finished_ts, latency = outcome
                agg_reason = self._aggregator.put_run(
                    event.pipeline_id, event.run_id, succeeded=succeeded,
                    finished_ts=finished_ts, latency=latency)
        return IngestResult(state, lc_reason, agg_reason)

    def ingest(self, pipeline_id: str, run_id: str, attempt: int,
               event_type: str, event_ts: float | None,
               detail: str = "", received_ts: float | None = None) -> IngestResult:
        recv = received_ts if received_ts is not None else self._clock()
        event = RunEvent(
            pipeline_id=pipeline_id, run_id=run_id, attempt=max(1, attempt),
            event_type=EventType(event_type), event_ts=event_ts,
            received_ts=recv, detail=detail)
        with self._lock:
            return self.apply_event(event)

    def get_run(self, pipeline_id: str, run_id: str) -> RunState | None:
        return self._lifecycle.get(pipeline_id, run_id)

    def list_runs(self, pipeline_id: str | None = None) -> list[RunState]:
        return self._lifecycle.all_runs(pipeline_id)

    def journal(self, pipeline_id: str) -> list[EventRecord]:
        return self._lifecycle.journal(pipeline_id)

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

    def peek_alert(self, pipeline_id: str, end_ts: float | None,
                   threshold: int, cooldown_seconds: float) -> Alert:
        """只读告警判定：不真发告警、不推进在线抑制状态。"""
        at = end_ts if end_ts is not None else self._clock()
        return self._aggregator.peek_alert(
            pipeline_id, at, threshold, cooldown_seconds)

    def ingest_batch(self, items):
        """历史事件批量回补，每条给出独立结论，等价于逐条单报。"""
        return self._backfiller.ingest_batch(items)

    def replay_window(self, pipeline_id: str, end_ts: float,
                      window_seconds: float, threshold: int = 3,
                      cooldown_seconds: float = 300.0):
        """追溯重算历史窗口，与在线结论对比，不产生任何副作用。"""
        return self._replayer.replay_window(
            pipeline_id, end_ts, window_seconds, threshold,
            cooldown_seconds)
