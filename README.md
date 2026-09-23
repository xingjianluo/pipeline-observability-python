# 本地流水线可观测与告警

在现有 FastAPI 服务之上，用**本地内存状态**模拟多条流水线运行的上报、判定与告警。
重点保证：重复、乱序与时序异常的数据，无论从哪个入口进入，都得到**确定、一致、可解释**的结论。

## 模块结构

| 文件 | 职责 |
| --- | --- |
| `pipelineobs/models.py` | 领域类型：事件、尝试、运行状态、新鲜度结论、窗口聚合、告警 |
| `pipelineobs/lifecycle.py` | 运行生命周期上报、去重、乱序收敛、重试区分 |
| `pipelineobs/freshness.py` | 数据新鲜度与调度滞后判定 |
| `pipelineobs/aggregation.py` | 对齐时间窗口聚合、连续失败统计、告警触发与抑制 |
| `pipelineobs/store.py` | 线程安全门面，一把锁串联三个子系统 |
| `pipelineobs/api.py` | HTTP 入口 |

## 一、运行状态模型

- 一个**逻辑运行** = `(pipeline_id, run_id)`；同一运行的每次**重试**用递增的
  `attempt` 区分，仍归属同一 `run_id`。
- 单个尝试状态：`pending → running → succeeded / failed`。
- 运行级状态是**全部尝试的纯函数**，因此与事件到达顺序无关：
  1. 任一尝试 `succeeded` ⇒ 运行 `succeeded`（终态，永不再变）；
  2. 否则以最大 `attempt` 的尝试状态为准（`running` / `failed`）。
- **重复上报**：同一 `(run, attempt, event_type)` 只计一次，返回
  `duplicate_event`，不重复计数。
- **乱序上报**：
  - 迟到事件不能让已进入终态的尝试回退（`attempt_terminal_held`）；
  - 同一尝试出现冲突终态时，按**事件时间戳**决胜；时间戳缺失时按**接收顺序**
    决胜（先到的终态成立）。
  - `failed` 后收到新尝试的 `started` 是合法重试（`retry_started`），运行
    重新 `running`；旧尝试的迟到事件不会影响新结论。
- **聚合单元是“运行”**：一个 `run_id` 在窗口中只出现一次，取最终收敛结论。
  先失败登记，重试成功则就地 `updated_to_success`（不新增、不遗漏、不回退）。

## 二、新鲜度与调度滞后规则

设 `age = evaluated_at - last_success_ts`，调度间隔为 `interval`：

| 条件 | 结论 | 理由 |
| --- | --- | --- |
| `age <= interval` | `fresh` | `age_within_interval` |
| `interval < age <= 2*interval` | `lagging` | `age_exceeds_one_interval` |
| `age > 2*interval` | `stale` | `age_exceeds_two_intervals` |

边界取半开区间：`age == interval` 仍为 fresh，`age == 2*interval` 仍为 lagging。

**异常输入不静默套用默认值**，统一返回 `state=unknown` 且 `reason` 可区分：

- `missing_last_success`：该流水线从未成功产出；
- `missing_event_ts`：最近成功缺少时间戳（无法计算年龄）；
- `missing_interval`：未配置调度间隔；
- `non_positive_interval`：调度间隔 `<= 0`；
- `clock_rollback`：评估时间早于最近成功时间（时钟回拨），`age_seconds` 为负；
- `interval_shrunk`：调度间隔较上次调小，需调用方**显式确认**
  （`confirm_interval_shrink` / 接口 `confirm_shrink=true`）后才按新间隔判定；
  调大间隔无需确认。

最近成功时间单调不回退：更早（乱序/回拨）的成功时间戳不会覆盖已记录的最近产出。

## 三、窗口聚合与告警抑制

- **窗口对齐**：`window_start = floor(end_ts / W) * W - W`，统计区间为半开
  `[window_start, window_end)`。恰好在边界的产出唯一归入起点所在窗口，
  **不重复不遗漏**。
- **指标**：运行总数、成功/失败数、成功率、延迟列表、平均延迟、p95
  （nearest-rank，结果确定）。
- **连续失败**：按 `(finished_ts, run_id)` 全序排序后从尾部回数，成功即中断。
- **告警触发**：连续失败数达到 `threshold` 时触发一次。
- **告警抑制**（短时间不重复告警）：
  - `already_alerted_streak`：同一个失败序列只告警一次；
  - `cooldown_active`：距上次告警不足 `cooldown_seconds` 时，即使出现新的
    连续失败序列也抑制；冷却结束后新序列可再次告警；
  - `below_threshold`：未达阈值。
- **并发一致**：三个子系统共享同一把可重入锁，“上报 → 收敛 → 聚合/新鲜度”
  对外部是原子操作；`run_id` 级 upsert 幂等，多个上报并发到达计数确定。

## HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/v1/events` | 上报 `started/succeeded/failed` 事件 |
| GET | `/api/v1/pipelines/{p}/runs` | 列出流水线全部运行及尝试 |
| GET | `/api/v1/pipelines/{p}/runs/{run}` | 查询单个运行状态 |
| PUT | `/api/v1/pipelines/{p}/interval` | 配置调度间隔 |
| GET | `/api/v1/pipelines/{p}/freshness` | 新鲜度/滞后判定 |
| POST | `/api/v1/pipelines/{p}/aggregate` | 指定窗口聚合 |
| POST | `/api/v1/pipelines/{p}/alerts/check` | 连续失败告警检查 |

事件上报示例：

```bash
curl -X POST localhost:8000/api/v1/events -H 'Content-Type: application/json' -d '{
  "pipeline_id": "p", "run_id": "r1", "attempt": 1,
  "event_type": "started", "event_ts": 100
}'
```

响应中 `lifecycle_reason` / `aggregate_reason` 给出该次上报的判定依据；
`unknown` 新鲜度的 `reason` 字段给出可区分的失败原因。

## 验证方法

```bash
uv sync
uv run pytest                       # 运行全部单测（日志实时打印输入与判定依据）
uv run uvicorn main:app --reload    # 启动服务
```

测试覆盖（共 28 个用例）：

- `tests/test_lifecycle.py`：重复上报、乱序上报、顺序无关收敛、重试区分、
  旧尝试迟到事件、时间戳缺失；
- `tests/test_freshness.py`：fresh/lagging/stale 边界、时钟回拨、时间戳/间隔
  缺失、间隔调小需确认、调大免确认；
- `tests/test_aggregation.py`：半开窗口边界、成功率与 p95、幂等、告警阈值/
  去重/冷却、成功重置、多线程并发与并发重复；
- `tests/test_api.py`：经 HTTP 入口复现上述场景，含时钟回拨、窗口边界与
  并发上报一致性。

单测以 `log_cli` 实时输出形如
`INPUT ... => DECISION state=... reason=...` 的输入与判定依据。
