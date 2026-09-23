"""In-memory, thread-safe state store simulating local persistence.

One process-wide RLock serializes every state transition. Aggregation reads
take a snapshot under the lock and compute outside of it. All decisions are
based on the injected server clock; client timestamps are kept as evidence
but never drive ordering.
"""
from __future__ import annotations

import threading
from dataclasses import dataclass, field

from observability.models import (
    AttemptStatus,
    PipelineConfigOut,
    RunStatus,
)


@dataclass
class AttemptRecord:
    attempt: int
    status: AttemptStatus = AttemptStatus.RUNNING
    started_ts_ms: int | None = None
    started_ingest_ms: int | None = None
    finish_ts_ms: int | None = None          # client-reported terminal timestamp
    finish_ingest_ms: int | None = None      # server-side terminal ingestion time
    duration_ms: int | None = None
    error: str | None = None
    event_ids: set[str] = field(default_factory=set)
    seen_event_types: set[str] = field(default_factory=set)


@dataclass
class RunRecord:
    pipeline: str
    run_id: str
    attempts: dict[int, AttemptRecord] = field(default_factory=dict)
    latest_attempt: int = 0
    status: RunStatus = RunStatus.RUNNING
    first_seen_ms: int | None = None
    first_event_ts_ms: int | None = None

    # exactly-once outcome bookkeeping, keyed by (run_id, attempt)
    outcome_inserted: set[tuple[str, int]] = field(default_factory=set)

    # dedupe across retries of the same logical report
    seen_event_ids: set[str] = field(default_factory=set)

    def get_attempt(self, attempt: int) -> AttemptRecord:
        rec = self.attempts.get(attempt)
        if rec is None:
            rec = AttemptRecord(attempt=attempt)
            self.attempts[attempt] = rec
        return rec


@dataclass
class OutcomeEntry:
    """A terminal attempt finish: the unit counted by window aggregation."""
    pipeline: str
    run_id: str
    attempt: int
    status: RunStatus
    finish_ingest_ms: int
    reported_ts_ms: int | None
    duration_ms: int | None
    seq: int


@dataclass
class RollbackEvidence:
    previous_ts_ms: int
    observed_ts_ms: int
    detected_at_ms: int
    run_id: str
    attempt: int


@dataclass
class PipelineRecord:
    config: PipelineConfigOut
    name: str = ""
    runs: dict[str, RunRecord] = field(default_factory=dict)

    # terminal outcomes in insertion order; seq makes ordering total
    outcomes: list[OutcomeEntry] = field(default_factory=list)

    # consecutive failed attempt finishes; reset by any success
    failure_streak: int = 0
    alert_firing: bool = False
    alerts: list = field(default_factory=list)          # list[Alert]
    suppressed_alert_count: int = 0

    # freshness evidence
    last_success_ts_ms: int | None = None               # max reported ts, never decreases
    last_success_ingest_ms: int | None = None
    last_success_had_missing_ts: bool = False
    rollback: RollbackEvidence | None = None
    success_count: int = 0

    def get_run(self, run_id: str) -> RunRecord:
        rec = self.runs.get(run_id)
        if rec is None:
            rec = RunRecord(pipeline=self.name, run_id=run_id)
        return rec


class Store:
    def __init__(self) -> None:
        self.lock = threading.RLock()
        self.pipelines: dict[str, PipelineRecord] = {}

    # -- pipelines -------------------------------------------------------
    def get_pipeline_record(self, name: str) -> PipelineRecord | None:
        return self.pipelines.get(name)

    def put_pipeline_record(self, name: str, record: PipelineRecord) -> None:
        self.pipelines[name] = record

    def get_run(self, pipeline: str, run_id: str) -> RunRecord | None:
        rec = self.pipelines.get(pipeline)
        if rec is None:
            return None
        run = rec.runs.get(run_id)
        return run

    def reset(self) -> None:
        with self.lock:
            self.pipelines.clear()
