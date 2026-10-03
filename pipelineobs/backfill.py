"""历史事件批量回补。

设计约定：
- 一批事件可横跨多条流水线、多个运行，顺序任意、可含重复；
- 每条事件独立校验、独立给出处理结论（applied / duplicate / rejected），
  个别事件不合法不影响其余事件生效；
- 整批结果与同样内容逐条单报完全一致：复用与在线相同的
  “生命周期收敛 -> 聚合/新鲜度更新”链路，且共享同一把锁；
- 重复提交同一批：生命周期去重键保证幂等，不多算、不重复告警。
"""
from __future__ import annotations

from dataclasses import dataclass

from .models import EventType, RunEvent


class InvalidEventError(ValueError):
    """单条事件校验失败；code 为可区分的原因码。"""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True)
class BatchItemOutcome:
    """批次中单条事件的处理结论。"""

    index: int
    status: str  # applied | duplicate | rejected
    lifecycle_reason: str | None = None
    aggregate_reason: str | None = None
    error: str | None = None


@dataclass(frozen=True)
class BatchSummary:
    total: int
    applied: int
    duplicates: int
    rejected: int


@dataclass(frozen=True)
class BatchResult:
    items: tuple[BatchItemOutcome, ...]
    summary: BatchSummary


def parse_event(raw: object, received_ts: float) -> RunEvent:
    """把原始输入校验并转换为 RunEvent；不合法时抛 InvalidEventError。

    原因码可区分：payload_not_object / missing_pipeline_id /
    missing_run_id / invalid_event_type / invalid_attempt / invalid_event_ts /
    invalid_detail。校验规则与单条上报入口保持一致（attempt>=1、
    event_type 三选一、event_ts 非负）。
    """
    if not isinstance(raw, dict):
        raise InvalidEventError("payload_not_object",
                                f"event payload must be an object, got {type(raw).__name__}")
    pipeline_id = raw.get("pipeline_id")
    if not isinstance(pipeline_id, str) or not pipeline_id:
        raise InvalidEventError("missing_pipeline_id",
                                "pipeline_id must be a non-empty string")
    run_id = raw.get("run_id")
    if not isinstance(run_id, str) or not run_id:
        raise InvalidEventError("missing_run_id",
                                "run_id must be a non-empty string")
    event_type = raw.get("event_type")
    if event_type not in ("started", "succeeded", "failed"):
        raise InvalidEventError("invalid_event_type",
                                f"event_type must be started|succeeded|failed, got {event_type!r}")
    attempt = raw.get("attempt", 1)
    if isinstance(attempt, bool) or not isinstance(attempt, int) or attempt < 1:
        raise InvalidEventError("invalid_attempt",
                                f"attempt must be an integer >= 1, got {attempt!r}")
    event_ts = raw.get("event_ts")
    if event_ts is not None:
        if isinstance(event_ts, bool) or not isinstance(event_ts, (int, float)):
            raise InvalidEventError("invalid_event_ts",
                                    f"event_ts must be a number, got {event_ts!r}")
        event_ts = float(event_ts)
        if event_ts < 0:
            raise InvalidEventError("invalid_event_ts",
                                    f"event_ts must be >= 0, got {event_ts!r}")
    detail = raw.get("detail", "")
    if not isinstance(detail, str):
        raise InvalidEventError("invalid_detail",
                                f"detail must be a string, got {detail!r}")
    return RunEvent(pipeline_id=pipeline_id, run_id=run_id, attempt=attempt,
                    event_type=EventType(event_type), event_ts=event_ts,
                    received_ts=received_ts, detail=detail)
