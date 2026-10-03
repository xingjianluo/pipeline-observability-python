"""FastAPI 路由：本地流水线可观测与告警入口。"""
from __future__ import annotations

import logging
import time
from dataclasses import asdict
from enum import Enum
from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from .models import BackfillItem, FreshnessState
from .store import PipelineStore

logger = logging.getLogger("pipelineobs")

router = APIRouter(prefix="/api/v1", tags=["observability"])

# 单进程本地状态；注入时钟便于测试。
store = PipelineStore()


class EventIn(BaseModel):
    pipeline_id: str
    run_id: str
    attempt: int = Field(default=1, ge=1)
    event_type: str = Field(description="started | succeeded | failed")
    event_ts: float | None = Field(default=None, description="事件时间戳；缺失用接收时间")
    detail: str = ""


class IntervalIn(BaseModel):
    interval_seconds: float
    confirm_shrink: bool = False


def _run_dict(state: Any) -> dict:
    return {
        "pipeline_id": state.pipeline_id,
        "run_id": state.run_id,
        "status": state.status.value,
        "attempts": [
            {"attempt": a.attempt, "status": a.status.value,
             "started_ts": a.started_ts, "finished_ts": a.finished_ts,
             "latency": a.latency, "detail": a.detail}
            for n, a in sorted(state.attempts.items())
        ],
        "last_event_ts": state.last_event_ts,
    }


@router.post("/events")
def post_event(payload: EventIn) -> dict:
    if payload.event_type not in ("started", "succeeded", "failed"):
        raise HTTPException(status_code=422, detail="invalid event_type")
    received = time.time()
    logger.info("[event] input=%s received_ts=%.3f",
                payload.model_dump(), received)
    try:
        result = store.ingest(
            payload.pipeline_id, payload.run_id, payload.attempt,
            payload.event_type, payload.event_ts, payload.detail,
            received_ts=received)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    logger.info("[event] run=%s/%s decision status=%s lifecycle=%s aggregate=%s",
                payload.pipeline_id, payload.run_id,
                result.run.status.value, result.lifecycle_reason,
                result.aggregate_reason)
    return {"run": _run_dict(result.run),
            "lifecycle_reason": result.lifecycle_reason,
            "aggregate_reason": result.aggregate_reason}


@router.get("/pipelines/{pipeline_id}/runs/{run_id}")
def get_run(pipeline_id: str, run_id: str) -> dict:
    state = store.get_run(pipeline_id, run_id)
    if state is None:
        raise HTTPException(status_code=404, detail="run not found")
    return _run_dict(state)


@router.get("/pipelines/{pipeline_id}/runs")
def list_runs(pipeline_id: str) -> dict:
    return {"runs": [_run_dict(s) for s in store.list_runs(pipeline_id)]}


@router.put("/pipelines/{pipeline_id}/interval")
def put_interval(pipeline_id: str, payload: IntervalIn) -> dict:
    store.set_interval(pipeline_id, payload.interval_seconds)
    if payload.confirm_shrink:
        store.freshness.confirm_interval_shrink(pipeline_id)
    return {"pipeline_id": pipeline_id,
            "interval_seconds": payload.interval_seconds,
            "shrink_confirmed": payload.confirm_shrink}


@router.get("/pipelines/{pipeline_id}/freshness")
def get_freshness(pipeline_id: str, evaluated_at: float | None = None,
                  interval_seconds: float | None = None,
                  confirm_shrink: bool = False) -> dict:
    if confirm_shrink:
        store.freshness.confirm_interval_shrink(pipeline_id)
    result = store.evaluate_freshness(pipeline_id, evaluated_at,
                                      interval_seconds)
    logger.info("[freshness] pipeline=%s decision=%s reason=%s age=%s lag=%s",
                pipeline_id, result.state.value, result.reason,
                result.age_seconds, result.lag_seconds)
    if result.state is FreshnessState.UNKNOWN:
        return {"state": result.state.value, "reason": result.reason,
                "evaluated_at": result.evaluated_at,
                "last_success_ts": result.last_success_ts,
                "interval_seconds": result.interval_seconds,
                "age_seconds": result.age_seconds}
    return asdict(result)


class WindowIn(BaseModel):
    end_ts: float
    window_seconds: float = Field(gt=0)


@router.post("/pipelines/{pipeline_id}/aggregate")
def post_aggregate(pipeline_id: str, payload: WindowIn) -> dict:
    agg = store.window(pipeline_id, payload.end_ts, payload.window_seconds)
    logger.info("[aggregate] pipeline=%s window=[%.3f,%.3f) total=%d ok=%d fail=%d",
                pipeline_id, agg.window_start, agg.window_end,
                agg.total_runs, agg.succeeded_runs, agg.failed_runs)
    return asdict(agg)


class AlertIn(BaseModel):
    end_ts: float | None = None
    threshold: int = Field(default=3, ge=1)
    cooldown_seconds: float = Field(default=300.0, ge=0)


@router.post("/pipelines/{pipeline_id}/alerts/check")
def post_alert_check(pipeline_id: str, payload: AlertIn) -> dict:
    alert = store.check_alert(pipeline_id, payload.end_ts,
                              payload.threshold, payload.cooldown_seconds)
    logger.info("[alert] pipeline=%s decision suppressed=%s reason=%s streak=%d",
                pipeline_id, alert.suppressed, alert.reason,
                alert.consecutive_failures)
    return asdict(alert)


# ---------------------------------------------------------------------------
# 历史事件批量回补
# ---------------------------------------------------------------------------


class BackfillEventIn(BaseModel):
    # 放宽入口约束：批次内坏数据必须由回补器逐条给出可区分原因，
    # 而不是被整体 422 拒掉（模型层只负责接住任意 JSON 形状）
    pipeline_id: Any = ""
    run_id: Any = ""
    attempt: Any = 1
    event_type: Any = Field(description="started | succeeded | failed")
    event_ts: Any = Field(default=None, description="事件时间戳；缺失用接收时间")
    detail: Any = ""
    received_ts: Any = None


class BackfillIn(BaseModel):
    events: list[BackfillEventIn] = Field(default_factory=list)


def _jsonable(obj: Any) -> Any:
    """dataclass/枚举/列表递归转为可 JSON 序列化的普通结构。"""
    if isinstance(obj, Enum):
        return obj.value
    if hasattr(obj, "__dataclass_fields__"):
        return {k: _jsonable(v) for k, v in asdict(obj).items()}
    if isinstance(obj, (list, tuple)):
        return [_jsonable(v) for v in obj]
    if isinstance(obj, dict):
        return {k: _jsonable(v) for k, v in obj.items()}
    return obj


@router.post("/events:batch")
def post_events_batch(payload: BackfillIn) -> dict:
    """历史事件批量回补。

    与 POST /events 共用同一处理路径：乱序、跨流水线/运行、重复、坏数据
    混在一批里也逐条给结论；非法事件单独标记，不影响其余事件生效。
    """
    items = [
        BackfillItem(pipeline_id=e.pipeline_id, run_id=e.run_id,
                     attempt=e.attempt, event_type=e.event_type,
                     event_ts=e.event_ts, detail=e.detail,
                     received_ts=e.received_ts)
        for e in payload.events]
    logger.info("[backfill] batch input size=%d", len(items))
    result = store.ingest_batch(items)
    logger.info("[backfill] batch result total=%d accepted=%d duplicate=%d "
                "invalid=%d", result.total, result.accepted,
                result.duplicate, result.invalid)
    return _jsonable(result)


# ---------------------------------------------------------------------------
# 历史窗口追溯重算
# ---------------------------------------------------------------------------


class ReplayIn(BaseModel):
    end_ts: float = Field(description="复盘截至时刻（按事件时间可见性截断）")
    window_seconds: float = Field(gt=0)
    threshold: int = Field(default=3, ge=1)
    cooldown_seconds: float = Field(default=300.0, ge=0)


@router.post("/pipelines/{pipeline_id}/replay")
def post_replay(pipeline_id: str, payload: ReplayIn) -> dict:
    """追溯重算历史窗口并与当前在线结论对比。只读，不改动在线状态、不发告警。"""
    try:
        result = store.replay_window(
            pipeline_id, payload.end_ts, payload.window_seconds,
            payload.threshold, payload.cooldown_seconds)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    return _jsonable(result)
