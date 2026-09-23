# 本地流水线可观测与告警（pipeline-observability）

基于 FastAPI 的本地服务，用进程内状态模拟多条流水线的运行上报、状态收敛、
新鲜度判定、窗口聚合与连续失败告警。所有判定都是确定性的：重复不重计、乱序不
回退、异常时间不静默套用默认值。

## 1. 运行状态模型

### 概念层级

* **Pipeline（流水线）**：注册时携带调度间隔 `schedule_interval_ms`、
  宽限 `grace_ms`（显式文档化默认 60s）与连续失败告警阈值
  `failure_alert_threshold`（显式文档化默认 3）。重复注册产生新的
  `revision`，并记录 `registered_at_ms / updated_at_ms`。
* **Run（逻辑运行，`run_id`）**：同一逻辑运行的所有尝试（重试）都挂在同一个
  `run_id` 下。
* **Attempt（尝试，`attempt` 从 1 递增）**：每次重试是独立尝试，拥有独立的
  started / succeeded / failed 记录与延迟；在聚合中作为独立的“终局结果”计数，
  因此“原始尝试”和“重试尝试”可区分但归属同一运行。

### 事件与状态收敛

事件：`started | succeeded | failed`，携带 `attempt`、`ts_ms`（客户端毫秒时间
戳，可缺失）、可选 `event_id`、`duration_ms`。

接收顺序无关，规则按优先级依次判定，且每种情况都有**可区分的 reason**：

1. `duplicate_event`：相同 `event_id` 在该运行上已出现过（跨乱序重放也识别）。
2. `duplicate_late_event`：同一 `(attempt, event_type)` 已处理过（无 event_id
   的语义去重）。
3. `out_of_order_superseded`：事件属于比当前最新尝试更旧的尝试——新重试已存在，
   旧事件被忽略，运行状态不回退。
4. `attempt_already_terminal`：同一尝试已进入终态（succeeded/failed），后来的
   冲突终态事件被忽略。
5. `accepted`：正常接受。
6. `unknown_pipeline`：流水线未注册（HTTP `/events` 返回 200，在响应体中用
   `accepted=false` 表达；查询类接口返回 404）。

关键不变量：

* **终态不回退**：Run 状态始终由“最高 attempt 编号”的尝试派生。已失败的尝试
  不会被迟到的成功翻转；但一次新的重试（更高 attempt）可以让运行重新进入
  running，再收敛到新终态。
* **终局结果恰好一次**：每个 `(run_id, attempt)` 只产生一条 outcome
  （带进程内单调 `seq`），聚合不重不漏。

## 2. 新鲜度与调度滞后规则

时间一律以注入的服务端时钟（epoch ms）为准；`ts_ms` 只作为证据，不参与排序。
令 `elapsed = now - last_success_ts`：

| 条件 | status | reason |
| --- | --- | --- |
| `elapsed <= interval` | `fresh` | `within_interval` |
| `interval < elapsed <= interval + grace` | `stale` | `beyond_interval_within_grace` |
| `elapsed > interval + grace` | `lagged` | `beyond_grace` |
| 从无成功产出 | `no_signal` | `no_success_output` |
| 最近一次成功缺少 `ts_ms` | `missing_timestamp` | `last_success_timestamp_missing` |
| 成功时间戳比此前接受值更小（客户端时钟回拨） | `clock_rollback` | `clock_rollback_detected` |
| 服务端 now 早于上次成功的摄取时间（服务端时钟回拨） | `clock_rollback` | `clock_rollback_detected` |
| 流水线未注册 | `no_signal` | `pipeline_unknown` |

确定性约定：

* **时间戳缺失**不会被替换成 now 或任何默认值，而是直接给出
  `missing_timestamp`；之后到来带合法时间戳的成功会恢复正常判定。
* **时钟回拨**被显式记录为证据（previous/observed/run/attempt/检测时间），
  已接受的最近成功时间保持**单调最大值**，不随回拨减小。
* **调度间隔调整**通过注册接口形成新 `revision`；判定始终使用当前 revision 的
  间隔与宽限，响应中回显所用参数与 elapsed，便于解释，绝不静默套默认值。

## 3. 窗口聚合与告警抑制

### 聚合

`GET /pipelines/{name}/stats?window_start_ms&window_end_ms`

* 固定窗口、**半开区间** `[start, end)`，边界确定；`end <= start` 返回 422。
* 分桶依据是**服务端摄取时间** `finish_ingest_ms`，因此客户端时钟偏移、乱序
  上报都无法把终局结果在窗口间挪动。
* 输出：`total_finished / succeeded / failed / success_rate`、
  `avg_duration_ms`、`p95_duration_ms`（nearest-rank）。空窗口成功率为 null。
* 并发安全：所有状态变更在同一把进程锁内完成；先在锁内快照、锁外计算。
  多线程并发上报 + 重复重放时计数精确（见并发测试）。

### 连续失败告警与抑制

* 每来一条**失败终局结果**，该流水线 `failure_streak += 1`；任意成功立即清零。
* streak **恰好达到**阈值时触发一条告警；阈值不变而告警仍在 firing 期间，
  后续失败只返回 `suppressed=true` 的抑制结果（带 suppression_key
  `"<pipeline>:firing"`），**不重复落库告警**。
* 一次成功清除 firing；之后再次累计到阈值会重新触发一条新告警。
* `GET /alerts?pipeline=` 查看已落库告警（抑制事件计数但不入列表）。

## 4. HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/pipelines/{name}` | 注册/修订流水线配置（body：间隔、宽限、阈值） |
| POST | `/events` | 上报生命周期事件（幂等，拒绝原因在响应体） |
| GET  | `/pipelines/{name}/freshness` | 新鲜度/滞后判定（未注册 404） |
| GET  | `/pipelines/{name}/stats` | 窗口聚合 |
| GET  | `/alerts?pipeline=` | 告警列表 |

交互式文档：启动后访问 `/docs`。

## 5. 验证方法

```bash
uv sync --group dev          # 安装运行与测试依赖
uv run pytest                # 全量单测（-s 已默认开启，打印输入与判定依据）
uv run uvicorn main:app      # 启动服务
```

测试覆盖（共 24 个用例）：

* `tests/test_runs.py`：event_id 去重、无 id 语义去重、终态不回退、重试与原始
  尝试区分、乱序旧事件被忽略、未知管道、告警触发一次/抑制/成功后重新 armed。
* `tests/test_freshness.py`：fresh/stale/lagged 全部边界（含 `==interval`、
  `==interval+grace`）、无信号、时间戳缺失、客户端与服务端时钟回拨、间隔调整
  revision 生效。
* `tests/test_aggregation.py`：半开窗口边界、重试分计、8×200 并发上报不重不漏、
  8 线程并发重复重放只计一次、成功率与 p95 延迟。
* `tests/test_http.py`：通过 FastAPI TestClient 覆盖各入口的一致性。

每个用例通过 `observability.tests` logger 打印 `INPUT`（原始上报内容）与
`DECISION`（接受与否、reason、收敛状态、告警等判定依据）。
