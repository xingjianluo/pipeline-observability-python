"""历史窗口追溯重算（事后复盘）。

- 基于已导入的事件日志，按“截至 window_end 可见的事件”重放，
  重建当时的运行收敛、窗口统计与告警判定；
- 与当前在线结论逐字段对比，列出差异；
- 全程只读：使用全新的临时子系统重放，不触碰在线状态，
  不触发告警、不影响告警抑制。
"""
from __future__ import annotations

from dataclasses import dataclass

from .models import WindowAggregate


@dataclass(frozen=True)
class RetroAlert:
    """重算出的“当时该不该告警”结论（不产生任何副作用）。"""

    consecutive_failures: int
    threshold: int
    would_alert: bool
    reason: str  # threshold_reached | below_threshold


@dataclass(frozen=True)
class RecomputeReport:
    pipeline_id: str
    window_start: float
    window_end: float
    replayed_events: int
    stats: WindowAggregate
    alert: RetroAlert
    online_stats: WindowAggregate
    online_consecutive_failures: int
    online_would_alert: bool
    differences: tuple[str, ...]


def _diff_lines(stats: WindowAggregate, alert: RetroAlert,
                online_stats: WindowAggregate,
                online_consecutive_failures: int,
                online_would_alert: bool) -> tuple[str, ...]:
    """逐字段对比重算结论与在线结论，返回可读的差异描述。"""
    diffs: list[str] = []
    for name in ("total_runs", "succeeded_runs", "failed_runs",
                 "success_rate", "avg_latency", "p95_latency"):
        recomputed = getattr(stats, name)
        online = getattr(online_stats, name)
        if recomputed != online:
            diffs.append(f"{name}: recomputed={recomputed} online={online}")
    if alert.consecutive_failures != online_consecutive_failures:
        diffs.append(
            f"consecutive_failures: recomputed={alert.consecutive_failures} "
            f"online={online_consecutive_failures}")
    if alert.would_alert != online_would_alert:
        diffs.append(
            f"would_alert: recomputed={alert.would_alert} "
            f"online={online_would_alert}")
    return tuple(diffs)


def build_report(pipeline_id: str, window_start: float, window_end: float,
                 replayed_events: int, stats: WindowAggregate,
                 alert: RetroAlert, online_stats: WindowAggregate,
                 online_consecutive_failures: int,
                 online_would_alert: bool) -> RecomputeReport:
    """汇总重算结论与在线结论，并计算差异（纯函数，无副作用）。"""
    return RecomputeReport(
        pipeline_id=pipeline_id, window_start=window_start,
        window_end=window_end, replayed_events=replayed_events,
        stats=stats, alert=alert, online_stats=online_stats,
        online_consecutive_failures=online_consecutive_failures,
        online_would_alert=online_would_alert,
        differences=_diff_lines(stats, alert, online_stats,
                                online_consecutive_failures,
                                online_would_alert))
