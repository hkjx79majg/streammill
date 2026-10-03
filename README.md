# StreamMill

这是一个面向实时流式分析的流式仓库与实时分析引擎。长期目标是提供事件时间与水位线、窗口聚合、乱序与去重、流表连接与物化视图、状态后端与快照恢复、精确一次写入、背压与查询优化，把实时分析沉淀为可复用引擎。

仓库采用 Python，当前冻结基线只提供进程健康检查。后续能力必须通过独立题目逐步实现；每个题目都应定义可观察的公共行为、兼容边界和失败语义，不得依赖未公开内部 API。

## 启动

```bash
PYTHONPATH=src python3 -m streammill.server --host 127.0.0.1 --port 8080
```

服务默认监听 `127.0.0.1:8080`，可通过 `STREAMMILL_ADDR` 修改。`GET /healthz` 返回 JSON 健康状态。

## 事件时间滚动窗口

- `POST /streams` 创建具名流，请求体为 `{"name", "window_ms", "allowed_lateness_ms"}`：`window_ms` 为正整数，`allowed_lateness_ms` 为非负整数。重名返回 409 `stream_exists`。
- `POST /streams/{name}/events` 提交事件 `{"timestamp_ms", "value"}`：`timestamp_ms` 为整数，`value` 为有限数值。事件归入左闭右开窗口 `[floor(timestamp_ms/window_ms)*window_ms, +window_ms)`。若 `timestamp_ms < 当前水位线 - allowed_lateness_ms`，响应成功且 `dropped: true`，聚合不变。
- `POST /streams/{name}/watermark` 推进水位线 `{"watermark_ms"}`，不得回退（回退返回 409 `watermark_regression`，重复提交相同值成功且幂等）。水位线达到 `window_end_ms + allowed_lateness_ms` 的窗口成为最终结果，随响应的 `finalized` 返回且只产生一次，按窗口结束时间递增。
- `GET /streams/{name}/results` 返回该流当前全部最终窗口（`stream`、`window_start_ms`、`window_end_ms`、`count`、`sum`），未关闭的窗口不暴露。各流的配置、水位线、事件与结果彼此隔离。

## 可选的事件标识去重

- `POST /streams` 可额外携带 `dedup_retention_ms`（正整数，且不小于 `allowed_lateness_ms`），提供时在创建响应中回显；不提供时流行为与上述基线完全一致，`event_id` 视为未声明字段。
- 启用去重的流要求事件携带非空字符串 `event_id`。新标识参与聚合，返回 `dropped: false, duplicate: false`；保留期内以相同 `timestamp_ms` 与数值相等的 `value` 重试，返回 `dropped: false, duplicate: true`，窗口不再变化；同一标识的时间戳或数值不同则返回 409 `event_id_conflict`，聚合与去重状态均不变。
- 去重判断先于迟到判断：已保留标识的精确重试即使越过迟到边界仍是 `duplicate: true`；未见过的过迟事件返回 `dropped: true, duplicate: false` 且不记录标识。
- 已接收标识按原 `timestamp_ms` 保留；水位线成功推进后仅淘汰 `timestamp_ms < watermark_ms - dedup_retention_ms` 的标识（等于边界仍保留，无水位线不淘汰）。淘汰后的标识可重用，按新事件处理。各流标识空间相互隔离，并发提交同一标识至多聚合一次。

错误统一为 `{"error": {"code", "message"}}`：非法 JSON 返回 400 `invalid_json`；缺字段、类型错误、配置越界、非有限数值或含未声明字段返回 422 `invalid_request`，且不留部分状态；访问不存在的流返回 404 `stream_not_found`。

## 已接收事件时间驱动的自动水位线

- `POST /streams` 可额外携带 `auto_watermark_lag_ms`（非负整数），提供时在创建响应中回显；不提供时流保持手工模式，公开行为与快照结构与基线完全一致。
- 自动流维护成功接收事件的最大 `timestamp_ms`。新事件先依据处理前的有效水位线执行既有迟到与去重判断；只有既未丢弃也非重复的事件完成聚合与去重登记后，才更新最大事件时间，并把有效水位线原子推进到 `max(当前水位线, 最大事件时间 - auto_watermark_lag_ms)`（尚无水位线时直接取后者）。
- 由此关闭的窗口沿用既有结束边界（`window_end_ms + allowed_lateness_ms`）、允许迟到量与排序规则，只在该事件响应的 `finalized` 中出现一次。相同或更旧但仍可接收的事件不导致回退；精确重复、过迟丢弃与 `event_id_conflict` 都不改变最大事件时间或水位线，前两者的 `finalized` 为空。
- 自动流的事件响应额外包含处理后的 `watermark_ms`（尚无有效水位线时为 `null`）与本次 `finalized`；其余字段（`dropped`、启用去重时的 `duplicate`）不变。
- 手工 `POST /streams/{name}/watermark` 在自动流上仍可推进水位线，`watermark_regression` 与幂等规则不变；后续自动推进只取较大值，不回退手工结果。自动流上的手工水位线响应同样返回 `watermark_ms` 与 `finalized`。

## 可选的固定步长滑动窗口

- `POST /streams` 可额外携带 `slide_ms`：正整数、不大于 `window_ms` 且能整除 `window_ms`，提供时在创建响应中回显；不提供时请求、响应与快照形状与滚动窗口基线完全一致。类型或范围不合法返回 422 `invalid_request`，流不会被创建。
- 滑动模式下，每个成功接收的事件计入所有满足 `start <= timestamp_ms < start + window_ms` 且 `start` 为 `slide_ms` 整数倍的窗口（负时间戳按相同数学边界处理，Python 向下取整），同一事件在每个重叠窗口中各计一次；窗口宽度仍为 `window_ms`。
- 每个窗口继续输出 `stream`、`window_start_ms`、`window_end_ms`、`count`、`sum`，按窗口起点递增返回；水位线达到该窗口自己的 `window_end_ms + allowed_lateness_ms` 时才最终化，每个窗口只在触发它的响应 `finalized` 中出现一次。
- 迟到判断仍先针对事件时间（去重判断先于迟到判断的规则不变）：过迟事件成功返回且 `dropped` 为 `true`，不改动任何重叠窗口。
- 启用去重时，一个 `event_id` 代表整次事件而非某个窗口成员关系：保留期内的精确重试返回 `duplicate: true` 且不重复计入任一重叠窗口；内容冲突返回 409 `event_id_conflict`，所有窗口与去重状态不变。自动水位线只由首次成功接收的事件推进，重复、冲突与过迟事件不推进最大事件时间/水位线，也不产生最终结果。

## 全量状态快照与恢复

- `GET /snapshot` 返回一致时点的全量状态文档 `{"format_version": 1, "streams": [...]}`。导出在同一把状态锁内完成，期间的创建流、提交事件与推进水位线要么整体包含、要么整体不包含；普通接口不产生任何落盘副作用。
- `streams` 按流名升序。每个流对象包含 `name`、`window_ms`、`allowed_lateness_ms`、`dedup_retention_ms`（未启用去重时为 `null`，且无 `dedup_records`）、`watermark_ms`（未推进时为 `null`）、尚未最终关闭的 `windows`、已经最终化的 `finalized`；启用去重时还包含保留期内的 `dedup_records`（`event_id`、`timestamp_ms`、`value`）。自动流额外包含成对出现的 `auto_watermark_lag_ms` 与 `max_event_timestamp_ms`（尚无成功接收事件时后者为 `null`）；不含这两个字段的文档按手工流恢复。滑动流额外包含 `slide_ms`；不含该字段的旧文档按滚动窗口恢复。`windows` 与 `finalized` 的窗口对象为 `window_start_ms`、`window_end_ms`、`count`、`sum`，`finalized` 行另含 `stream`；两个数组均按 `window_start_ms` 升序（滑动流按 `slide_ms` 网格对齐），`dedup_records` 按 `event_id` 升序。
- `POST /snapshot/restore` 仅允许在尚未创建任何流的实例上调用，成功返回 200 `{"restored_streams": N}` 并一次性发布全部状态，其他请求不会观察到部分流；空快照 `{"format_version": 1, "streams": []}` 合法并返回零。恢复后结果查询与导出前一致，开放窗口可继续接收合规事件并在后续水位线下正确最终化，已最终化窗口不会再次进入 `finalized`，保留的标识继续执行重复/冲突判断且淘汰边界不变；自动流恢复后的自动推进、去重与最终结果与导出前一致；滑动流恢复后的继续写入（事件计入全部重叠窗口）、去重淘汰、手工或自动推进及再次导出与未中断实例等价；无写入的再次导出在语义与数组顺序上完全相同。
- 恢复严格校验且不静默修正：请求体不是合法 JSON 返回 400 `invalid_json`；对象结构、字段类型、`format_version`（仅支持 1，其他版本同样拒绝）、流名唯一性与升序、配置约束（含 `dedup_retention_ms >= allowed_lateness_ms`，以及滑动流的 `slide_ms` 为正整数、不大于 `window_ms` 且整除 `window_ms`）、数组排序、窗口宽度、起点与网格步长对齐、窗口开闭关系、最终结果与水位线/迟到配置的关系、重复窗口或重复 `event_id`、去重记录的保留期边界与所属各窗口计数关系任一不合法，均返回 422 `invalid_snapshot`，实例保持完全为空。自动流的两个字段必须成对出现且类型合法（`auto_watermark_lag_ms` 为非负整数，`max_event_timestamp_ms` 为整数或 `null`）；非空最大事件时间聚合到的每个窗口都必须是已有开放或最终窗口，且水位线不得低于最大事件时间减去滞后量。实例中已存在任意流时，对任何可解析的快照文档都返回 409 `restore_conflict`（请求体本身不是合法 JSON 时仍按请求格式错误返回 400 `invalid_json`），原状态不变。健康检查、手工流、滑动/滚动流间隔离以及其他既有错误优先级均不改变。

## 验证

```bash
PYTHONPATH=src python3 -m unittest discover -s tests -v
```

快照覆盖进程内全量状态，可用于跨实例/重启的人工恢复；连接与落盘持久化仍不在当前范围，由后续任务从已冻结事实出发独立设计并验证。
