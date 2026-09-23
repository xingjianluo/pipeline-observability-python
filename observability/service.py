"""Application facade wiring the clock, store and domain modules.

Pipeline registration is also configuration revision control: changing the
schedule interval creates a new revision and stamps the change time; the
freshness rule then uses the current revision deterministically (no silent
default substitution).
"""
from __future__ import annotations

from observability.clock import Clock, SystemClock
from observability.aggregation import list_alerts, window_stats
from observability.freshness import evaluate_freshness
from observability.models import (
    Alert,
    FreshnessOut,
    IngestEventIn,
    IngestEventOut,
    PipelineConfigIn,
    PipelineConfigOut,
    WindowStatsOut,
)
from observability.runs import ingest_event
from observability.store import PipelineRecord, Store


class ObservabilityService:
    def __init__(self, clock: Clock | None = None) -> None:
        self.clock: Clock = clock or SystemClock()
        self.store = Store()

    def reset(self) -> None:
        self.store.reset()

    def register_pipeline(self, name: str, config: PipelineConfigIn) -> PipelineConfigOut:
        now = self.clock.now_ms()
        with self.store.lock:
            existing = self.store.get_pipeline_record(name)
            if existing is None:
                out = PipelineConfigOut(
                    schedule_interval_ms=config.schedule_interval_ms,
                    grace_ms=config.grace_ms,
                    failure_alert_threshold=config.failure_alert_threshold,
                    registered_at_ms=now,
                    updated_at_ms=now,
                    revision=1,
                )
                self.store.put_pipeline_record(name, PipelineRecord(config=out, name=name))
                return out

            old = existing.config
            out = PipelineConfigOut(
                schedule_interval_ms=config.schedule_interval_ms,
                grace_ms=config.grace_ms,
                failure_alert_threshold=config.failure_alert_threshold,
                registered_at_ms=old.registered_at_ms,
                updated_at_ms=now,
                revision=old.revision + 1,
            )
            existing.config = out
            return out

    def get_pipeline(self, name: str) -> PipelineConfigOut | None:
        rec = self.store.get_pipeline_record(name)
        return rec.config if rec is not None else None

    def ingest(self, event: IngestEventIn) -> IngestEventOut:
        return ingest_event(self.store, event, self.clock.now_ms())

    def freshness(self, pipeline: str) -> FreshnessOut:
        return evaluate_freshness(self.store, pipeline, self.clock.now_ms())

    def stats(
        self, pipeline: str, window_start_ms: int, window_end_ms: int
    ) -> WindowStatsOut:
        return window_stats(self.store, pipeline, window_start_ms, window_end_ms)

    def alerts(self, pipeline: str | None = None) -> list[Alert]:
        return list_alerts(self.store, pipeline)
