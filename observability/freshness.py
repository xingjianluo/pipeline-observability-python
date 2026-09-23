"""Data freshness and schedule lag evaluation.

Rules (all times epoch ms; the injected server clock is authoritative):

    elapsed = now - last_success_ts
    elapsed <= interval                 -> FRESH      / within_interval
    interval < elapsed <= interval+grace -> STALE     / beyond_interval_within_grace
    elapsed > interval + grace          -> LAGGED     / beyond_grace

Distinguishable abnormal conclusions (never silently defaulted):
    * no successful output ever          -> NO_SIGNAL / no_success_output
    * latest success carried no timestamp -> MISSING_TIMESTAMP
    * observed success timestamp moved backwards relative to an earlier
      accepted success                  -> CLOCK_ROLLBACK
Also if the server clock itself moved backwards since the last accepted
success (now < last_success_ingest_ms) that is reported as CLOCK_ROLLBACK.
"""
from __future__ import annotations

from observability.models import (
    FreshnessOut,
    FreshnessReason,
    FreshnessStatus,
)
from observability.store import Store


def evaluate_freshness(
    store: Store,
    pipeline: str,
    now_ms: int | None = None,
) -> FreshnessOut:
    prec = store.get_pipeline_record(pipeline)
    if prec is None:
        return FreshnessOut(
            pipeline=pipeline,
            status=FreshnessStatus.NO_SIGNAL,
            reason=FreshnessReason.PIPELINE_UNKNOWN,
            detail="pipeline is not registered",
        )

    cfg = prec.config
    with store.lock:
        if prec.rollback is not None:
            rb = prec.rollback
            return FreshnessOut(
                pipeline=pipeline,
                status=FreshnessStatus.CLOCK_ROLLBACK,
                reason=FreshnessReason.CLOCK_ROLLBACK_DETECTED,
                now_ms=now_ms,
                last_success_ts_ms=prec.last_success_ts_ms,
                schedule_interval_ms=cfg.schedule_interval_ms,
                grace_ms=cfg.grace_ms,
                detail=(
                    f"reported success timestamp moved backwards: "
                    f"{rb.observed_ts_ms} < previous {rb.previous_ts_ms} "
                    f"(run={rb.run_id}, attempt={rb.attempt}, detected_at={rb.detected_at_ms})"
                ),
            )

        if (
            prec.last_success_ingest_ms is not None
            and now_ms is not None
            and now_ms < prec.last_success_ingest_ms
        ):
            return FreshnessOut(
                pipeline=pipeline,
                status=FreshnessStatus.CLOCK_ROLLBACK,
                reason=FreshnessReason.CLOCK_ROLLBACK_DETECTED,
                now_ms=now_ms,
                last_success_ts_ms=prec.last_success_ts_ms,
                schedule_interval_ms=cfg.schedule_interval_ms,
                grace_ms=cfg.grace_ms,
                detail=(
                    f"server clock moved backwards: now={now_ms} < "
                    f"last_success_ingest_ms={prec.last_success_ingest_ms}"
                ),
            )

        if prec.success_count == 0:
            return FreshnessOut(
                pipeline=pipeline,
                status=FreshnessStatus.NO_SIGNAL,
                reason=FreshnessReason.NO_SUCCESS_OUTPUT,
                now_ms=now_ms,
                schedule_interval_ms=cfg.schedule_interval_ms,
                grace_ms=cfg.grace_ms,
                detail="no successful output has ever been reported",
            )

        if prec.last_success_had_missing_ts or prec.last_success_ts_ms is None:
            return FreshnessOut(
                pipeline=pipeline,
                status=FreshnessStatus.MISSING_TIMESTAMP,
                reason=FreshnessReason.LAST_SUCCESS_TIMESTAMP_MISSING,
                now_ms=now_ms,
                last_success_ts_ms=prec.last_success_ts_ms,
                schedule_interval_ms=cfg.schedule_interval_ms,
                grace_ms=cfg.grace_ms,
                detail=(
                    "latest successful output was reported without ts_ms; "
                    "refusing to substitute a default timestamp"
                ),
            )

        elapsed = now_ms - prec.last_success_ts_ms
        if elapsed < 0:
            return FreshnessOut(
                pipeline=pipeline,
                status=FreshnessStatus.CLOCK_ROLLBACK,
                reason=FreshnessReason.CLOCK_ROLLBACK_DETECTED,
                now_ms=now_ms,
                last_success_ts_ms=prec.last_success_ts_ms,
                schedule_interval_ms=cfg.schedule_interval_ms,
                grace_ms=cfg.grace_ms,
                detail=f"now ({now_ms}) precedes last success ({prec.last_success_ts_ms})",
            )

        interval = cfg.schedule_interval_ms
        grace_limit = interval + cfg.grace_ms
        if elapsed <= interval:
            status, reason = FreshnessStatus.FRESH, FreshnessReason.WITHIN_INTERVAL
            detail = f"elapsed {elapsed}ms <= interval {interval}ms"
        elif elapsed <= grace_limit:
            status, reason = FreshnessStatus.STALE, FreshnessReason.BEYOND_INTERVAL_WITHIN_GRACE
            detail = f"elapsed {elapsed}ms > interval {interval}ms but within grace {grace_limit}ms"
        else:
            status, reason = FreshnessStatus.LAGGED, FreshnessReason.BEYOND_GRACE
            detail = f"elapsed {elapsed}ms > interval+grace {grace_limit}ms"

        return FreshnessOut(
            pipeline=pipeline,
            status=status,
            reason=reason,
            now_ms=now_ms,
            last_success_ts_ms=prec.last_success_ts_ms,
            schedule_interval_ms=interval,
            grace_ms=cfg.grace_ms,
            elapsed_ms=elapsed,
            detail=detail,
        )
