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

## 状态快照与恢复

- `GET /snapshot` 在服务锁内一次性导出一致时点的全量状态文档：`format_version` 为 1，`streams` 按流名升序。每个流对象包含创建配置（`name`、`window_ms`、`allowed_lateness_ms`，启用去重时含 `dedup_retention_ms`）、`watermark_ms`（无水位线时为 `null`）、尚未最终关闭的窗口聚合 `windows`、已最终化结果 `finalized`，以及启用去重时仍在保留期内的 `dedup` 记录（`event_id`、`timestamp_ms`、`value`）。窗口与最终结果按 `window_start_ms` 升序，去重记录按 `event_id` 升序。导出期间的并发写入要么完整包含、要么完整排除。
- `POST /snapshot/restore` 在尚未创建任何流的实例上恢复快照：校验通过后状态一次性可见，返回 200 与 `restored_streams` 数量；空快照（`streams: []`）允许恢复并返回 0。恢复后最终结果立即可查，开放窗口继续接收合规事件并在后续水位线下最终化，已最终化窗口不会重复产生，保留标识的重复/冲突判断与淘汰边界不变；中间无写入时再次导出得到相同文档。
- 恢复失败语义：请求体非法 JSON 返回 400 `invalid_json`；结构、字段类型、`format_version`（仅支持 1）、流名唯一性、配置约束、排序要求或状态内在关系（窗口边界对齐、最终结果与水位线和迟到配置一致、开放窗口尚未可最终化、无重复窗口或重复 `event_id`、去重记录未越过淘汰边界等）不合法时返回 422 `invalid_snapshot`，实例保持完全为空，输入不会被静默修正；实例已存在任意流时无论快照内容如何都返回 409 `restore_conflict`，原状态不变。
- 快照仅作为 JSON 文档经 HTTP 导出/导入，普通接口不新增任何落盘副作用。

## 验证

```bash
PYTHONPATH=src python3 -m unittest discover -s tests -v
```

连接、持久化与自动水位线仍不在当前范围，由后续任务从已冻结事实出发独立设计并验证。
