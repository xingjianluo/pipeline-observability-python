"""领域类型定义：运行事件、运行状态、新鲜度结论、聚合结果、告警。"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class RunStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class EventType(str, Enum):
    STARTED = "started"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class FreshnessState(str, Enum):
    FRESH = "fresh"
    STALE = "stale"
    LAGGING = "lagging"
    UNKNOWN = "unknown"


class InvalidateReason(str, Enum):
    MISSING_LAST_SUCCESS = "missing_last_success"
    MISSING_EVENT_TS = "missing_event_ts"
    MISSING_INTERVAL = "missing_interval"
    NON_POSITIVE_INTERVAL = "non_positive_interval"
    CLOCK_ROLLBACK = "clock_rollback"
    INTERVAL_SHRUNK = "interval_shrunk"


@dataclass(frozen=True)
class RunEvent:
    pipeline_id: str
    run_id: str
    attempt: int
    event_type: EventType
    event_ts: float | None
    received_ts: float
    detail: str = ""


@dataclass
class AttemptRecord:
    attempt: int
    status: RunStatus
    started_ts: float | None = None
    finished_ts: float | None = None
    latency: float | None = None
    detail: str = ""


@dataclass
class RunState:
    pipeline_id: str
    run_id: str
    attempts: dict[int, AttemptRecord] = field(default_factory=dict)
    status: RunStatus = RunStatus.PENDING
    first_seen_ts: float | None = None
    last_event_ts: float | None = None
    dedup_keys: set[str] = field(default_factory=set)


@dataclass(frozen=True)
class FreshnessResult:
    pipeline_id: str
    state: FreshnessState
    evaluated_at: float
    last_success_ts: float | None = None
    interval_seconds: float | None = None
    age_seconds: float | None = None
    lag_seconds: float | None = None
    reason: str = ""


@dataclass(frozen=True)
class WindowAggregate:
    pipeline_id: str
    window_start: float
    window_end: float
    total_runs: int = 0
    succeeded_runs: int = 0
    failed_runs: int = 0
    success_rate: float = 0.0
    latencies: list[float] = field(default_factory=list)
    avg_latency: float | None = None
    p95_latency: float | None = None


@dataclass(frozen=True)
class Alert:
    pipeline_id: str
    window_end: float
    consecutive_failures: int
    threshold: int
    suppressed: bool
    reason: str


# ---------------------------------------------------------------------------
# 历史事件批量回补
# ---------------------------------------------------------------------------


class BackfillItemStatus(str, Enum):
    ACCEPTED = "accepted"    # 合法且生效（含首次登记、重试收敛等）
    DUPLICATE = "duplicate"  # 合法但与既有数据重复，未产生新效果
    INVALID = "invalid"      # 不合法，未处理，原因见 reason


@dataclass(frozen=True)
class EventRecord:
    """已通过去重、实际进入生命周期的事件留痕（追溯重算的事实来源）。"""
    pipeline_id: str
    run_id: str
    attempt: int
    event_type: EventType
    order_ts: float          # event_ts 缺失时取 received_ts
    ts_present: bool
    received_ts: float
    detail: str = ""


@dataclass(frozen=True)
class BackfillItem:
    # 字段在 Backfiller.validate_item 中做显式校验与归一化，
    # 因此这里允许原始 JSON 值（int/str/bool/dict 等任意形状）
    pipeline_id: object
    run_id: object
    attempt: object
    event_type: object
    event_ts: object
    detail: str = ""
    received_ts: object = None


@dataclass(frozen=True)
class BackfillItemResult:
    index: int
    status: BackfillItemStatus
    run_status: str | None = None
    lifecycle_reason: str | None = None
    aggregate_reason: str | None = None
    reason: str | None = None


@dataclass(frozen=True)
class BackfillResult:
    total: int
    accepted: int
    duplicate: int
    invalid: int
    results: list[BackfillItemResult] = field(default_factory=list)


# ---------------------------------------------------------------------------
# 历史窗口追溯重算
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class RunOutcomeView:
    run_id: str
    succeeded: bool
    finished_ts: float
    latency: float | None


class RunChangeKind(str, Enum):
    ADDED = "added"          # 当时不存在/未终态，现在在线有终态
    REMOVED = "removed"      # 当时有终态，现在在线无终态
    CHANGED = "changed"      # 结论或时间发生变化（如失败后重试成功）


@dataclass(frozen=True)
class RunOutcomeChange:
    run_id: str
    kind: RunChangeKind
    historical: RunOutcomeView | None
    online: RunOutcomeView | None


@dataclass(frozen=True)
class AlertDecision:
    """只读的告警判定结论，不消费、不影响在线抑制状态。"""
    consecutive_failures: int
    threshold: int
    would_alert: bool
    reason: str


@dataclass(frozen=True)
class ReplayView:
    window: WindowAggregate
    alert: AlertDecision
    runs: list[RunOutcomeView] = field(default_factory=list)


@dataclass(frozen=True)
class ReplayResult:
    pipeline_id: str
    window_start: float
    window_end: float
    historical: ReplayView
    online: ReplayView
    run_changes: list[RunOutcomeChange] = field(default_factory=list)
    total_delta: int = 0
    succeeded_delta: int = 0
    failed_delta: int = 0
    alert_changed: bool = False

    @property
    def changed(self) -> bool:
        return bool(self.run_changes) or self.alert_changed
