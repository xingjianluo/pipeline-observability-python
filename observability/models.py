"""Domain types: events, states, freshness and aggregation result models."""
from __future__ import annotations

from enum import Enum

from pydantic import BaseModel, Field


class EventType(str, Enum):
    STARTED = "started"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class RunStatus(str, Enum):
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class AttemptStatus(str, Enum):
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class IngestionReason(str, Enum):
    """Deterministic explanation of how an ingested event was handled."""
    ACCEPTED = "accepted"
    DUPLICATE_EVENT = "duplicate_event"
    DUPLICATE_LATE_EVENT = "duplicate_late_event"
    UNKNOWN_PIPELINE = "unknown_pipeline"
    OUT_OF_ORDER_SUPERSEDED = "out_of_order_superseded"
    ATTEMPT_TERMINAL = "attempt_already_terminal"
    RUN_TERMINAL_OLDER_ATTEMPT = "run_terminal_older_attempt"


class IngestEventIn(BaseModel):
    model_config = {"extra": "forbid"}

    pipeline: str = Field(..., min_length=1)
    run_id: str = Field(..., min_length=1)
    event_type: EventType
    attempt: int = Field(..., ge=1)
    ts_ms: int | None = Field(None, description="event epoch milliseconds; null = missing")
    event_id: str | None = Field(None, min_length=1)
    duration_ms: int | None = Field(None, ge=0)
    error: str | None = None


class IngestEventOut(BaseModel):
    accepted: bool
    reason: IngestionReason
    pipeline: str
    run_id: str
    attempt: int
    run_status: RunStatus | None = None
    attempts: int
    first_seen_ts_ms: int | None = None
    alert: "Alert | None" = None


class PipelineConfigIn(BaseModel):
    model_config = {"extra": "forbid"}

    schedule_interval_ms: int = Field(..., gt=0)
    grace_ms: int = Field(
        default=60_000,
        gt=0,
        description="documented explicit default: 60s beyond interval counts as lag",
    )
    failure_alert_threshold: int = Field(
        default=3,
        ge=1,
        description="documented explicit default: alert on the 3rd consecutive failed finish",
    )


class PipelineConfigOut(PipelineConfigIn):
    registered_at_ms: int
    updated_at_ms: int
    revision: int


class FreshnessStatus(str, Enum):
    FRESH = "fresh"
    STALE = "stale"
    LAGGED = "lagged"
    NO_SIGNAL = "no_signal"
    MISSING_TIMESTAMP = "missing_timestamp"
    CLOCK_ROLLBACK = "clock_rollback"


class FreshnessReason(str, Enum):
    WITHIN_INTERVAL = "within_interval"
    BEYOND_INTERVAL_WITHIN_GRACE = "beyond_interval_within_grace"
    BEYOND_GRACE = "beyond_grace"
    NO_SUCCESS_OUTPUT = "no_success_output"
    LAST_SUCCESS_TIMESTAMP_MISSING = "last_success_timestamp_missing"
    CLOCK_ROLLBACK_DETECTED = "clock_rollback_detected"
    PIPELINE_UNKNOWN = "pipeline_unknown"


class FreshnessOut(BaseModel):
    pipeline: str
    status: FreshnessStatus
    reason: FreshnessReason
    now_ms: int | None = None
    last_success_ts_ms: int | None = None
    schedule_interval_ms: int | None = None
    grace_ms: int | None = None
    elapsed_ms: int | None = None
    detail: str = ""


class WindowStatsOut(BaseModel):
    pipeline: str
    window_start_ms: int
    window_end_ms: int
    total_finished: int
    succeeded: int
    failed: int
    success_rate: float | None = None
    avg_duration_ms: float | None = None
    p95_duration_ms: int | None = None


class Alert(BaseModel):
    pipeline: str
    triggered_at_ms: int
    failure_streak: int
    threshold: int
    suppressed: bool = False
    suppression_key: str = ""
    reason: str = ""


Alert.model_rebuild()
