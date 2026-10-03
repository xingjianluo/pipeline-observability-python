"""线程安全的本地状态门面：串联生命周期、新鲜度与聚合。

三个子系统共享同一把可重入锁，保证“上报 -> 状态收敛 -> 聚合/新鲜度更新”
对外部表现为原子操作，多个上报并发到达时结论确定一致。
"""
from __future__ import annotations

import threading
import time
from dataclasses import dataclass

from .aggregation import Aggregator
from .backfill import (
    BatchItemOutcome,
    BatchResult,
    BatchSummary,
    InvalidEventError,
    parse_event,
)
from .freshness import FreshnessMonitor
from .lifecycle import LifecycleManager
from .models import (
    Alert,
    EventType,
    FreshnessResult,
    RunEvent,
    RunState,
    RunStatus,
    WindowAggregate,
)
from .recompute import RecomputeReport, RetroAlert, build_report


@dataclass(frozen=True)
class IngestResult:
    run: RunState
    lifecycle_reason: str
    aggregate_reason: str | None


def _apply_event(lifecycle: LifecycleManager, freshness: FreshnessMonitor,
                 aggregator: Aggregator,
                 event: RunEvent) -> tuple[RunState, str, str | None]:
    """单条事件的在线处理链路：生命周期收敛 -> 新鲜度/聚合更新。

    在线单报、批量回补、追溯重算的重放共用同一条链路，
    保证同一事件内容从任何入口进入结论一致。
    """
    state, lc_reason = lifecycle.ingest(event)

    agg_reason: str | None = None
    applied = lc_reason not in ("duplicate_event",
                                "attempt_terminal_held")
    if state.status is RunStatus.SUCCEEDED and applied:
        freshness.record_success(event.pipeline_id, event.event_ts)
    # 聚合单元是“运行”：一个 run 只出现一次，取其最终收敛结论。
    # 失败先登记；若之后重试成功则就地更新（不新增、不遗漏）。
    # 旧尝试迟到的终态事件不影响聚合。
    if state.status in (RunStatus.SUCCEEDED, RunStatus.FAILED) \
            and event.attempt == max(state.attempts):
        latest = state.attempts[event.attempt]
        if applied and event.event_type in (EventType.SUCCEEDED,
                                            EventType.FAILED):
            agg_reason = aggregator.upsert_run(
                event.pipeline_id, event.run_id,
                succeeded=state.status is RunStatus.SUCCEEDED,
                finished_ts=event.event_ts
                if event.event_ts is not None else event.received_ts,
                latency=latest.latency)
        elif event.event_type is EventType.STARTED \
                and latest.finished_ts is not None:
            # 迟到 started 回填了该尝试的耗时：同步修正聚合记录，
            # 保证窗口统计与事件到达顺序无关。
            agg_reason = aggregator.upsert_run(
                event.pipeline_id, event.run_id,
                succeeded=state.status is RunStatus.SUCCEEDED,
                finished_ts=latest.finished_ts,
                latency=latest.latency)
    return state, lc_reason, agg_reason


class PipelineStore:
    def __init__(self, clock=time.time) -> None:
        self._clock = clock
        self._lock = threading.RLock()
        self._lifecycle = LifecycleManager(self._lock)
        self._freshness = FreshnessMonitor(self._lock)
        self._aggregator = Aggregator(self._lock)
        # 已受理事件的追加日志（按到达顺序），供追溯重算重放。
        self._event_log: list[RunEvent] = []

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
            state, lc_reason, agg_reason = _apply_event(
                self._lifecycle, self._freshness, self._aggregator, event)
            self._event_log.append(event)
            return IngestResult(state, lc_reason, agg_reason)

    def ingest_batch(self, raw_events: list[object],
                     received_ts: float | None = None) -> BatchResult:
        """批量回补一批历史事件。

        - 每条独立校验：不合法的条目单独给出可区分原因码，合法条目照常生效；
        - 与逐条单报完全同链路、同一把锁，结论与任意顺序逐条处理一致；
        - 重复提交同一批幂等（生命周期去重键 + 聚合 upsert）。
        """
        recv = received_ts if received_ts is not None else self._clock()
        outcomes: list[BatchItemOutcome] = []
        parsed: list[tuple[int, RunEvent]] = []
        for idx, raw in enumerate(raw_events):
            try:
                parsed.append((idx, parse_event(raw, recv)))
            except InvalidEventError as exc:
                outcomes.append(BatchItemOutcome(
                    index=idx, status="rejected", error=exc.code))
        applied = duplicates = 0
        with self._lock:
            for idx, event in parsed:
                _, lc_reason, agg_reason = _apply_event(
                    self._lifecycle, self._freshness, self._aggregator, event)
                self._event_log.append(event)
                if lc_reason == "duplicate_event":
                    duplicates += 1
                    status = "duplicate"
                else:
                    applied += 1
                    status = "applied"
                outcomes.append(BatchItemOutcome(
                    index=idx, status=status, lifecycle_reason=lc_reason,
                    aggregate_reason=agg_reason))
        outcomes.sort(key=lambda o: o.index)
        rejected = sum(1 for o in outcomes if o.status == "rejected")
        summary = BatchSummary(total=len(outcomes), applied=applied,
                               duplicates=duplicates, rejected=rejected)
        return BatchResult(items=tuple(outcomes), summary=summary)

    def recompute_window(self, pipeline_id: str, window_start: float,
                         window_end: float, threshold: int = 3,
                         cooldown_seconds: float = 300.0) -> RecomputeReport:
        """追溯重算历史窗口，并与当前在线结论对比。

        只读复盘：重放使用全新的临时子系统，不改动在线运行状态、
        不触发告警、不影响告警抑制；在线侧读取也用只读的
        evaluate_alert，不会消费抑制状态。
        """
        if window_end <= window_start:
            raise ValueError("window_end must be greater than window_start")
        with self._lock:
            snapshot = [e for e in self._event_log
                        if e.pipeline_id == pipeline_id]
            online_stats = self._aggregator.range_window(
                pipeline_id, window_start, window_end)
            online_failures, _ = self._aggregator.consecutive_failures(
                pipeline_id, window_end)
        # 重放在锁外进行：临时子系统不共享状态，几千条事件重放为毫秒级，
        # 不阻塞在线上报与查询。
        lifecycle = LifecycleManager()
        freshness = FreshnessMonitor()
        aggregator = Aggregator()
        replayed = 0
        for event in snapshot:
            order_ts = event.event_ts if event.event_ts is not None \
                else event.received_ts
            if order_ts > window_end:
                continue  # 只重放“当时可见”的事件
            _apply_event(lifecycle, freshness, aggregator, event)
            replayed += 1
        stats = aggregator.range_window(pipeline_id, window_start, window_end)
        failures, _ = aggregator.consecutive_failures(pipeline_id, window_end)
        alert = RetroAlert(
            consecutive_failures=failures, threshold=threshold,
            would_alert=failures >= threshold,
            reason="threshold_reached" if failures >= threshold
            else "below_threshold")
        return build_report(
            pipeline_id, window_start, window_end, replayed, stats, alert,
            online_stats, online_failures, online_failures >= threshold)

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
