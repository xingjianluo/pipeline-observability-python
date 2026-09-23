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
