"""Shared test helpers: deterministic service construction and input logging."""
from __future__ import annotations

import logging

from observability.clock import ManualClock
from observability.models import PipelineConfigIn
from observability.service import ObservabilityService

log = logging.getLogger("observability.tests")


def build_service(start_ms: int = 1_000, **cfg) -> tuple[ObservabilityService, ManualClock]:
    clock = ManualClock(start_ms)
    svc = ObservabilityService(clock=clock)
    params = {"schedule_interval_ms": 1_000, "grace_ms": 200, "failure_alert_threshold": 3}
    params.update(cfg)
    svc.register_pipeline("p", PipelineConfigIn(**params))
    return svc, clock


def explain(title: str, payload: dict, result) -> None:
    """Print the raw input and the deterministic decision basis."""
    log.info("CASE %s | INPUT %s", title, payload)
    if hasattr(result, "model_dump_json"):
        log.info("CASE %s | DECISION %s", title, result.model_dump_json())
    else:
        log.info("CASE %s | DECISION %s", title, result)
