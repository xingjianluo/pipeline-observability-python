"""HTTP service exposing local pipeline observability capabilities."""
from __future__ import annotations

from fastapi import FastAPI, HTTPException, Query

from observability.clock import SystemClock
from observability.models import (
    Alert,
    FreshnessOut,
    IngestEventIn,
    IngestEventOut,
    PipelineConfigIn,
    PipelineConfigOut,
    WindowStatsOut,
)
from observability.service import ObservabilityService

app = FastAPI(title="pipeline-observability")
service = ObservabilityService(clock=SystemClock())


@app.get("/")
def read_root() -> dict:
    return {"service": "pipeline-observability", "status": "ok"}


@app.post("/pipelines/{name}", response_model=PipelineConfigOut)
def register_pipeline(name: str, config: PipelineConfigIn) -> PipelineConfigOut:
    return service.register_pipeline(name, config)


@app.post("/events", response_model=IngestEventOut)
def ingest_event(event: IngestEventIn) -> IngestEventOut:
    # Idempotent endpoint: duplicates / ordering rejections are reported in
    # the body with a deterministic reason rather than as HTTP errors.
    return service.ingest(event)


@app.get("/pipelines/{name}/freshness", response_model=FreshnessOut)
def pipeline_freshness(name: str) -> FreshnessOut:
    if service.get_pipeline(name) is None:
        raise HTTPException(status_code=404, detail="pipeline not registered")
    return service.freshness(name)


@app.get("/pipelines/{name}/stats", response_model=WindowStatsOut)
def pipeline_stats(
    name: str,
    window_start_ms: int = Query(..., ge=0),
    window_end_ms: int = Query(..., gt=0),
) -> WindowStatsOut:
    if service.get_pipeline(name) is None:
        raise HTTPException(status_code=404, detail="pipeline not registered")
    try:
        return service.stats(name, window_start_ms, window_end_ms)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@app.get("/alerts", response_model=list[Alert])
def list_alerts(pipeline: str | None = None) -> list[Alert]:
    return service.alerts(pipeline)
