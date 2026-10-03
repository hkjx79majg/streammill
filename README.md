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

## 由事件时间驱动的自动水位线

- `POST /streams` 可额外携带 `auto_watermark_lag_ms`（非负整数，含 0），提供时在创建响应中回显；不提供时流为手工模式，全部既有行为与快照结构保持不变。取值类型错误、布尔值、负数或携带未声明字段均返回 422 `invalid_request`，且不创建部分状态。
- 自动流维护已成功接收事件的最大 `timestamp_ms`（`max_event_timestamp_ms`）。新事件仍先依据处理前的有效水位线执行既有的迟到与去重判断；只有既未丢弃也非重复的事件完成聚合（启用去重时还包括标识登记）后，才更新最大事件时间，并把水位线原子推进到 `max(当前水位线, max_event_timestamp_ms - auto_watermark_lag_ms)`。由此关闭的窗口沿用既有结束边界、允许迟到量与排序规则，只在该事件响应的 `finalized` 中出现一次。
- 自动流的事件响应额外包含 `watermark_ms` 与本次 `finalized`：尚无有效水位线（推导出的目标水位线为负）时 `watermark_ms` 为 `null`；`finalized` 为数组。相同或更旧但仍可接收的事件不使水位线回退；精确重复、过迟丢弃与 409 `event_id_conflict` 均不改变最大事件时间或水位线，前两者的 `finalized` 为空。并发事件呈现某个串行顺序，水位线单调且窗口至多最终化一次。
- 手工 `POST /streams/{name}/watermark` 在自动流上仍然可用：`watermark_regression` 与相同值幂等规则不变，手工推进后的水位线不会被后续自动推进回退。
- 自动流可与去重组合使用，去重判断与淘汰规则不变。

## 全量状态快照与恢复

- `GET /snapshot` 返回一致时点的全量状态文档 `{"format_version": 1, "streams": [...]}`。导出在同一把状态锁内完成，期间的创建流、提交事件与推进水位线要么整体包含、要么整体不包含；普通接口不产生任何落盘副作用。
- `streams` 按流名升序。每个流对象包含 `name`、`window_ms`、`allowed_lateness_ms`、`dedup_retention_ms`（未启用去重时为 `null`，且无 `dedup_records`）、`watermark_ms`（未推进时为 `null`）、尚未最终关闭的 `windows`、已经最终化的 `finalized`；启用去重时还包含保留期内的 `dedup_records`（`event_id`、`timestamp_ms`、`value`）。自动流还额外包含成对出现的 `auto_watermark_lag_ms` 与 `max_event_timestamp_ms`（尚无有效事件时后者为 `null`）；不含这两个字段的流文档按手工流恢复并原样导出。`windows` 与 `finalized` 的窗口对象为 `window_start_ms`、`window_end_ms`、`count`、`sum`，`finalized` 行另含 `stream`；两个数组均按 `window_start_ms` 升序，`dedup_records` 按 `event_id` 升序。
- `POST /snapshot/restore` 仅允许在尚未创建任何流的实例上调用，成功返回 200 `{"restored_streams": N}` 并一次性发布全部状态，其他请求不会观察到部分流；空快照 `{"format_version": 1, "streams": []}` 合法并返回零。恢复后结果查询与导出前一致，开放窗口可继续接收合规事件并在后续水位线下正确最终化，已最终化窗口不会再次进入 `finalized`，保留的标识继续执行重复/冲突判断且淘汰边界不变；自动流恢复后的自动推进、去重与最终结果与导出前一致；无写入的再次导出在语义与数组顺序上完全相同。
- 恢复严格校验且不静默修正：请求体不是合法 JSON 返回 400 `invalid_json`；对象结构、字段类型、`format_version`（仅支持 1，其他版本同样拒绝）、流名唯一性与升序、配置约束（含 `dedup_retention_ms >= allowed_lateness_ms`）、数组排序、窗口边界对齐与开闭关系、最终结果与水位线/迟到配置的关系、重复窗口或重复 `event_id`、去重记录的保留期边界与所属窗口计数关系任一不合法，均返回 422 `invalid_snapshot`，实例保持完全为空。自动流还要求 `auto_watermark_lag_ms` 与 `max_event_timestamp_ms` 成对出现且类型正确；非空最大事件时间必须落入某个已存在的开放或最终窗口，水位线不得低于 `max_event_timestamp_ms - auto_watermark_lag_ms`，且最大事件时间已产生有效水位线时 `watermark_ms` 不得为 `null`。实例中已存在任意流时，对任何可解析的快照文档都返回 409 `restore_conflict`（请求体本身不是合法 JSON 时仍按请求格式错误返回 400 `invalid_json`），原状态不变。

## 验证

```bash
PYTHONPATH=src python3 -m unittest discover -s tests -v
```

快照覆盖进程内全量状态，可用于跨实例/重启的人工恢复；连接与落盘持久化仍不在当前范围，由后续任务从已冻结事实出发独立设计并验证。
