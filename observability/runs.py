"""Run lifecycle ingestion.

Guarantees:
  * identical reports (same event_id, or same attempt+event_type on a run)
    are deduplicated and never counted twice;
  * out-of-order reports never move the run backwards: run status is always
    derived from the *highest* attempt number, and an attempt that reached a
    terminal state stays terminal;
  * a retry is a new attempt (higher ``attempt``) with its own start/finish
    record, but stays attached to the same ``run_id``;
  * terminal finishes are recorded as outcomes exactly once
    (key = (run_id, attempt)), so aggregation never double counts.
"""
from __future__ import annotations

from observability.clock import monotonic_event_order
from observability.models import (
    Alert,
    AttemptStatus,
    EventType,
    IngestEventIn,
    IngestEventOut,
    IngestionReason,
    RunStatus,
)
from observability.store import (
    AttemptRecord,
    OutcomeEntry,
    PipelineRecord,
    RollbackEvidence,
    RunRecord,
    Store,
)

_TERMINAL_ATTEMPT = {AttemptStatus.SUCCEEDED, AttemptStatus.FAILED}


def _fresh_run_status(run: RunRecord) -> None:
    """Derive run status from its highest-numbered attempt. Never regresses."""
    latest = run.attempts.get(run.latest_attempt)
    run.status = latest.status if latest is not None else RunStatus.RUNNING


def _record_success_evidence(
    prec: PipelineRecord,
    run: RunRecord,
    attempt: AttemptRecord,
    server_ms: int,
) -> None:
    """Update last-success freshness evidence.

    A missing timestamp is recorded explicitly (never silently replaced by a
    default). A reported timestamp smaller than the previously accepted one
    is clock rollback and is recorded as evidence; the stored value is the
    monotonic maximum and never decreases.
    """
    prec.success_count += 1
    prec.last_success_ingest_ms = server_ms
    if attempt.finish_ts_ms is None:
        prec.last_success_had_missing_ts = True
        return
    ts = attempt.finish_ts_ms
    if prec.last_success_ts_ms is not None and ts < prec.last_success_ts_ms:
        prec.rollback = RollbackEvidence(
            previous_ts_ms=prec.last_success_ts_ms,
            observed_ts_ms=ts,
            detected_at_ms=server_ms,
            run_id=run.run_id,
            attempt=attempt.attempt,
        )
        # monotonic max: do not roll the accepted timestamp backwards
        return
    if ts >= (prec.last_success_ts_ms if prec.last_success_ts_ms is not None else ts):
        prec.last_success_ts_ms = ts
        # a later success with a valid timestamp clears the missing flag
        prec.last_success_had_missing_ts = False


def _update_failure_alert(
    prec: PipelineRecord,
    server_ms: int,
    outcome_status: RunStatus,
) -> Alert | None:
    """Consecutive-failure alert with suppression.

    Fires exactly once when the streak reaches the threshold; while the alert
    stays firing, further failures are suppressed; any success resets it.
    """
    threshold = prec.config.failure_alert_threshold
    if outcome_status == RunStatus.SUCCEEDED:
        prec.failure_streak = 0
        prec.alert_firing = False
        return None

    prec.failure_streak += 1
    if prec.failure_streak < threshold:
        return None
    if prec.alert_firing:
        prec.suppressed_alert_count += 1
        return Alert(
            pipeline=prec.name,
            triggered_at_ms=server_ms,
            failure_streak=prec.failure_streak,
            threshold=threshold,
            suppressed=True,
            suppression_key=f"{prec.name}:firing",
            reason="alert_already_firing_suppressed",
        )

    prec.alert_firing = True
    alert = Alert(
        pipeline=prec.name,
        triggered_at_ms=server_ms,
        failure_streak=prec.failure_streak,
        threshold=threshold,
        suppressed=False,
        suppression_key=f"{prec.name}:firing",
        reason="failure_streak_reached_threshold",
    )
    prec.alerts.append(alert)
    return alert


def _finish_attempt(
    prec: PipelineRecord,
    run: RunRecord,
    attempt: AttemptRecord,
    event: IngestEventIn,
    server_ms: int,
) -> Alert | None:
    new_status = (
        AttemptStatus.SUCCEEDED
        if event.event_type is EventType.SUCCEEDED
        else AttemptStatus.FAILED
    )
    run_status = (
        RunStatus.SUCCEEDED if new_status is AttemptStatus.SUCCEEDED else RunStatus.FAILED
    )
    attempt.status = new_status
    attempt.finish_ts_ms = event.ts_ms
    attempt.finish_ingest_ms = server_ms
    if event.duration_ms is not None:
        attempt.duration_ms = event.duration_ms
    elif attempt.started_ts_ms is not None and event.ts_ms is not None:
        delta = event.ts_ms - attempt.started_ts_ms
        attempt.duration_ms = delta if delta >= 0 else None
    if event.error is not None:
        attempt.error = event.error

    _fresh_run_status(run)

    alert: Alert | None = None
    key = (run.run_id, attempt.attempt)
    if key not in run.outcome_inserted:
        run.outcome_inserted.add(key)
        outcome = OutcomeEntry(
            pipeline=prec.name,
            run_id=run.run_id,
            attempt=attempt.attempt,
            status=run_status,
            finish_ingest_ms=server_ms,
            reported_ts_ms=event.ts_ms,
            duration_ms=attempt.duration_ms,
            seq=monotonic_event_order(),
        )
        prec.outcomes.append(outcome)
        if run_status is RunStatus.SUCCEEDED:
            _record_success_evidence(prec, run, attempt, server_ms)
        alert = _update_failure_alert(prec, server_ms, run_status)
    return alert


def ingest_event(store: Store, event: IngestEventIn, server_ms: int) -> IngestEventOut:
    prec = store.get_pipeline_record(event.pipeline)
    if prec is None:
        return IngestEventOut(
            accepted=False,
            reason=IngestionReason.UNKNOWN_PIPELINE,
            pipeline=event.pipeline,
            run_id=event.run_id,
            attempt=event.attempt,
            run_status=None,
            attempts=0,
        )

    with store.lock:
        run = prec.runs.get(event.run_id)
        if run is None:
            run = RunRecord(pipeline=event.pipeline, run_id=event.run_id)
            run.first_seen_ms = server_ms
            run.first_event_ts_ms = event.ts_ms
            prec.runs[event.run_id] = run

        attempt_rec = run.get_attempt(event.attempt)

        # 1) exact duplicate: same event id anywhere on the run
        if event.event_id is not None and event.event_id in run.seen_event_ids:
            return IngestEventOut(
                accepted=False,
                reason=IngestionReason.DUPLICATE_EVENT,
                pipeline=event.pipeline,
                run_id=event.run_id,
                attempt=event.attempt,
                run_status=run.status,
                attempts=len(run.attempts),
                first_seen_ts_ms=run.first_event_ts_ms,
            )

        # 2) semantic duplicate: same (attempt, event_type) already processed
        if event.event_type.value in attempt_rec.seen_event_types:
            return IngestEventOut(
                accepted=False,
                reason=IngestionReason.DUPLICATE_LATE_EVENT,
                pipeline=event.pipeline,
                run_id=event.run_id,
                attempt=event.attempt,
                run_status=run.status,
                attempts=len(run.attempts),
                first_seen_ts_ms=run.first_event_ts_ms,
            )

        # 3) stale attempt: a newer retry exists, never regress the run
        if event.attempt < run.latest_attempt:
            return IngestEventOut(
                accepted=False,
                reason=IngestionReason.OUT_OF_ORDER_SUPERSEDED,
                pipeline=event.pipeline,
                run_id=event.run_id,
                attempt=event.attempt,
                run_status=run.status,
                attempts=len(run.attempts),
                first_seen_ts_ms=run.first_event_ts_ms,
            )

        # 4) same attempt already in a terminal state
        if attempt_rec.status in _TERMINAL_ATTEMPT:
            return IngestEventOut(
                accepted=False,
                reason=IngestionReason.ATTEMPT_TERMINAL,
                pipeline=event.pipeline,
                run_id=event.run_id,
                attempt=event.attempt,
                run_status=run.status,
                attempts=len(run.attempts),
                first_seen_ts_ms=run.first_event_ts_ms,
            )

        # --- accept the event ---
        if event.event_id is not None:
            run.seen_event_ids.add(event.event_id)
        attempt_rec.seen_event_types.add(event.event_type.value)
        attempt_rec.event_ids.add(event.event_id or f"anon:{event.event_type.value}")

        if event.attempt > run.latest_attempt:
            run.latest_attempt = event.attempt

        alert: Alert | None = None
        if event.event_type is EventType.STARTED:
            attempt_rec.status = AttemptStatus.RUNNING
            attempt_rec.started_ts_ms = event.ts_ms
            attempt_rec.started_ingest_ms = server_ms
            _fresh_run_status(run)
        else:
            alert = _finish_attempt(prec, run, attempt_rec, event, server_ms)

        return IngestEventOut(
            accepted=True,
            reason=IngestionReason.ACCEPTED,
            pipeline=event.pipeline,
            run_id=event.run_id,
            attempt=event.attempt,
            run_status=run.status,
            attempts=len(run.attempts),
            first_seen_ts_ms=run.first_event_ts_ms,
            alert=alert,
        )
