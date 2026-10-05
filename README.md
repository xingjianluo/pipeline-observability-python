# 本地流水线可观测与告警

在现有 FastAPI 服务之上，用**本地内存状态**模拟多条流水线运行的上报、判定与告警。
重点保证：重复、乱序与时序异常的数据，无论从哪个入口进入，都得到**确定、一致、可解释**的结论。

## 模块结构

| 文件 | 职责 |
| --- | --- |
| `pipelineobs/models.py` | 领域类型：事件、尝试、运行状态、新鲜度结论、窗口聚合、告警、回补/重算结论 |
| `pipelineobs/lifecycle.py` | 运行生命周期上报、去重、乱序收敛、重试区分、事件留痕 |
| `pipelineobs/freshness.py` | 数据新鲜度与调度滞后判定 |
| `pipelineobs/aggregation.py` | 对齐时间窗口聚合、连续失败统计、告警触发与抑制（含只读判定） |
| `pipelineobs/backfill.py` | 历史事件批量回补：逐条复用单条上报路径，部分失败可区分 |
| `pipelineobs/replay.py` | 历史窗口追溯重算：只读复盘并与在线结论对比 |
| `pipelineobs/store.py` | 线程安全门面，一把锁串联三个子系统与回补/重算 |
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
  - 终态之后迟到的 `started` 同样不能回退状态，但它携带的“开始时间”事实
    仍会被记录（取最早者）并据此重算耗时（`attempt_started_late`）——
    只要 `started` 与终态两条事件都到齐，**耗时必然有值且与到达顺序无关**。
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

为了让“批量回补 / 实时上报 / 并发”三种入口的结论完全一致，聚合写入采用
**顺序无关的规范终态结论**（`lifecycle.canonical_outcome`）：每次事件生效后都由
运行的全部尝试重新推导——成功取最早成功尝试的终态，否则取最大尝试号的失败终态。
耗时同样顺序无关：`started_ts` 取该尝试全部 `started` 事件的最早时间，终态时间
按冲突决胜规则确定，`latency = max(0, finished_ts - started_ts)` 由两者推导，
即使终态事件先于 `started` 到达，迟到的 `started` 也会把耗时补上。
因此同一组事件无论按什么顺序、经哪个入口、重试多少遍，窗口统计（含 avg/p95
耗时）、运行状态与告警结论都相同。

## 四、历史事件批量回补

`POST /api/v1/events:batch`（`store.ingest_batch` / `Backfiller`）一次接收一批
事件，事件可以横跨多条流水线、多个运行，顺序可乱、内容可重复。

- **逐条独立结论**：每条事件返回
  `accepted | duplicate | invalid` 及判定依据（`lifecycle_reason` /
  `aggregate_reason` / `reason`）；批次汇总为 `total / accepted / duplicate /
  invalid`。
- **与单条上报完全等价**：合法事件一律走与 `POST /events` 相同的
  `store.apply_event` 处理路径，整批最终结果等同于把同样事件一条条单独上报。
- **坏数据不拖垮整批**：入口只要求 `events` 是数组，元素可以是任意 JSON
  值；不合法条目不进入状态机，其余事件照常生效，且原因可区分：
  - `item_not_object`：条目不是对象（字符串/数字/null/数组等）；
  - `missing_pipeline_id` / `missing_run_id`：缺标识字段；
  - `missing_event_type`：缺 `event_type` 字段；
  - `invalid_event_type` / `invalid_attempt` / `invalid_event_ts` /
    `invalid_received_ts` / `invalid_detail`：字段存在但取值非法。
  模型层不会对单条坏数据返回 422；只有请求体整体不是
  `{"events": [...]}` 形状时才整体拒绝。
- **乱序耗时一致**：同一批历史事件换任意提交顺序回补，窗口的 avg/p95
  耗时与计数、成功失败数一样稳定，且与按时间顺序逐条上报的结论相同
  （由上节的顺序无关耗时推导保证）。
- **幂等**：重复提交同一批（哪怕乱序重试）只会得到 `duplicate`，不多算、
  不重复告警——去重仍由生命周期的 `(run, attempt, event_type)` 与聚合的规范终态
  写入共同保证。
- **不卡住其他请求**：批次**逐条**获取共享锁（而非整批长事务），单条处理与实时
  上报互斥，但读请求和其他上报可在事件之间穿插；实测 3000 条约 0.1~0.2s，
  期间并发读请求单次等待约十毫秒级。

## 五、历史窗口追溯重算

`POST /api/v1/pipelines/{p}/replay`（`store.replay_window` / `Replayer`）基于
**已经导入的事件留痕**（生命周期 journal）复盘过去某个窗口，并与当前在线结论对比。

- **复盘语义**：窗口仍是与在线一致的 epoch 对齐半开区间 `[end-W, end)`；
  “当时”只使用 `order_ts <= end`（事件时间戳缺失时取接收时间）的可见事件，
  喂给一个全新的生命周期管理器重建，再用同一套规范终态规则推导运行结论、
  窗口统计（总数/成功/失败/成功率/耗时）与连续失败告警判定。
- **告警复盘不真发**：历史侧没有任何“已发告警”快照，只回答“当时该不该告”
  （`would_alert` + `reason`）；在线侧读取当前抑制快照做**只读**判定（peek），
  两者都不写状态——重算不会真的发告警，也不会推进或影响在线抑制。
- **差异一目了然**：返回
  - `historical` / `online` 两侧的窗口统计、告警判定与窗口内运行明细；
  - `run_changes`：`added`（当时未见/未终态，现在在线有）、`removed`、
    `changed`（如当时失败、后来重试成功导致终态移出窗口），含两侧明细；
  - `total_delta / succeeded_delta / failed_delta` 与 `alert_changed`。
- **纯只读**：不修改任何在线运行状态、窗口数据、新鲜度记录与告警抑制。

## HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/v1/events` | 上报 `started/succeeded/failed` 事件 |
| POST | `/api/v1/events:batch` | 历史事件批量回补（乱序/重复/跨流水线，逐条结论、部分失败） |
| GET | `/api/v1/pipelines/{p}/runs` | 列出流水线全部运行及尝试 |
| GET | `/api/v1/pipelines/{p}/runs/{run}` | 查询单个运行状态 |
| PUT | `/api/v1/pipelines/{p}/interval` | 配置调度间隔 |
| GET | `/api/v1/pipelines/{p}/freshness` | 新鲜度/滞后判定 |
| POST | `/api/v1/pipelines/{p}/aggregate` | 指定窗口聚合 |
| POST | `/api/v1/pipelines/{p}/alerts/check` | 连续失败告警检查（消费抑制状态） |
| POST | `/api/v1/pipelines/{p}/replay` | 历史窗口追溯重算，与在线对比，纯只读 |

**兼容范围**：批量回补与单条上报只是两个入口，内部是同一条处理路径；原有
`/events` 及其余接口的请求/响应结构、判定规则均保持不变。`replay` 与
`alerts/check` 的区别：前者只读、不真发告警、不改变抑制；后者是在线的真实
判定，会记录“已发告警”并参与去重/冷却。

事件上报示例：

```bash
curl -X POST localhost:8000/api/v1/events -H 'Content-Type: application/json' -d '{
  "pipeline_id": "p", "run_id": "r1", "attempt": 1,
  "event_type": "started", "event_ts": 100
}'
```

响应中 `lifecycle_reason` / `aggregate_reason` 给出该次上报的判定依据；
`unknown` 新鲜度的 `reason` 字段给出可区分的失败原因。

批量回补示例（顺序可乱、可重复、可夹带坏数据，包括非对象条目）：

```bash
curl -X POST localhost:8000/api/v1/events:batch \
  -H 'Content-Type: application/json' -d '{"events": [
  {"pipeline_id":"p","run_id":"r1","event_type":"started","event_ts":100},
  {"pipeline_id":"p","run_id":"r1","event_type":"succeeded","event_ts":110},
  {"pipeline_id":"p","run_id":"r1","event_type":"succeeded","event_ts":110},
  "oops-not-an-object",
  {"pipeline_id":"p","run_id":"r2","event_ts":120},
  {"pipeline_id":"p","run_id":"x","attempt":0,"event_type":"weird"}
]}'
# => {"total":6,"accepted":2,"duplicate":1,"invalid":3,
#     "results":[...,
#       {"index":3,"status":"invalid","reason":"item_not_object"},
#       {"index":4,"status":"invalid","reason":"missing_event_type"},
#       {"index":5,"status":"invalid","reason":"invalid_attempt"}]}
# 合法条目照常生效：GET /api/v1/pipelines/p/runs/r1 => succeeded
```

追溯重算示例（复盘 `end=200` 之前的 `[0,200)` 窗口，阈值 2）：

```bash
curl -X POST localhost:8000/api/v1/pipelines/p/replay \
  -H 'Content-Type: application/json' \
  -d '{"end_ts":200,"window_seconds":200,"threshold":2,"cooldown_seconds":300}'
# historical/online 各自给出 window 与 alert（would_alert+reason），
# run_changes/total_delta/.../alert_changed 指出当时与现在的差异。
```

## 验证方法

```bash
uv sync
uv run pytest                       # 运行全部单测（日志实时打印输入与判定依据）
uv run uvicorn main:app --reload    # 启动服务
```

测试覆盖（共 51 个用例）：

- `tests/test_lifecycle.py`：重复上报、乱序上报、顺序无关收敛、重试区分、
  旧尝试迟到事件（迟到 `started` 补耗时、状态不回退）、耗时顺序无关、
  时间戳缺失；
- `tests/test_freshness.py`：fresh/lagging/stale 边界、时钟回拨、时间戳/间隔
  缺失、间隔调小需确认、调大免确认；
- `tests/test_aggregation.py`：半开窗口边界、成功率与 p95、幂等、告警阈值/
  去重/冷却、成功重置、多线程并发与并发重复；
- `tests/test_api.py`：经 HTTP 入口复现上述场景，含时钟回拨、窗口边界与
  并发上报一致性；
- `tests/test_backfill.py`：乱序批量与逐条单报等价、逐条结论、部分失败原因
  可区分且不丢合法事件、重复/乱序批次幂等、跨流水线、失败后重试成功只计一次、
  **乱序回补耗时一致**（逆序/交错/200 种全排列抽查的 avg/p95 与逐条上报相同）、
  终态先到时 `started` 后补耗时；
- `tests/test_replay.py`：截至时刻截断重建、失败→重试成功的差异、
  重算不改在线状态/不真发告警/不消费抑制、无差异场景、与“当时逐条上报”一致；
- `tests/test_concurrency.py`：回补与实时上报并发最终结论（含耗时）与任意
  逐条顺序一致、乱序重试并发幂等（含耗时与告警结论稳定）、3000 条批次耗时
  与并发读不被长时间阻塞；
- `tests/test_backfill_api.py`：批量/重算的 HTTP 端到端，含**非对象条目与缺
  `event_type` 条目逐条标记不拖垮整批**、夹坏数据的批量入口与逐条单报等价、
  幂等、重算无告警副作用、与单条上报入口并发及原有用法兼容。

单测以 `log_cli` 实时输出形如
`INPUT ... => DECISION state=... reason=...` 的输入与判定依据；
批量回补场景另输出 `[backfill] idx=N accepted/duplicate/invalid ... reason=...`
的逐条结论，可直接从日志确认每个用例覆盖了哪些业务路径。

### 本地验证这几类场景

```bash
uv sync
# 坏数据逐条结论（真实 HTTP 入口：非对象条目 + 缺 event_type）
uv run pytest tests/test_backfill_api.py -k "non_object or bad_data"
# 乱序回补耗时一致性（含全排列抽查与终态先到）
uv run pytest tests/test_backfill.py -k "latency"
# 回补与实时并发、乱序重试幂等（含耗时与告警稳定性）
uv run pytest tests/test_concurrency.py
# 全量回归
uv run pytest
```

也可以启动服务后用上面的 `curl` 示例手工验证：夹带坏数据的批量请求返回
200 且逐条给出 `reason`；把同一批事件换个顺序再发一遍，第二次全部
`duplicate`，窗口统计（含 `avg_latency` / `p95_latency`）不变。
