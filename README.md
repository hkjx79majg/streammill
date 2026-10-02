# StreamMill

这是一个面向实时流式分析的流式仓库与实时分析引擎。长期目标是提供事件时间与水位线、窗口聚合、乱序与去重、流表连接与物化视图、状态后端与快照恢复、精确一次写入、背压与查询优化，把实时分析沉淀为可复用引擎。

仓库采用 Python，当前冻结基线只提供进程健康检查。后续能力必须通过独立题目逐步实现；每个题目都应定义可观察的公共行为、兼容边界和失败语义，不得依赖未公开内部 API。

## 启动

```bash
PYTHONPATH=src python3 -m streammill.server --host 127.0.0.1 --port 8080
```

服务默认监听 `127.0.0.1:8080`，可通过 `STREAMMILL_ADDR` 修改。`GET /healthz` 返回 JSON 健康状态。

## 事件时间滚动窗口

- `POST /streams`：创建具名流，请求体为 `{"name": ..., "window_ms": 正整数, "allowed_lateness_ms": 非负整数}`。
- `POST /streams/{name}/events`：提交事件 `{"timestamp_ms": 整数, "value": 有限数值}`。事件归入左闭右开窗口，起点为 `timestamp_ms` 对 `window_ms` 向下取整。晚于水位线容忍边界的事件返回 `{"dropped": true}` 且不改变聚合。
- `POST /streams/{name}/watermark`：显式推进水位线 `{"watermark_ms": 整数}`，不得回退。当水位线达到 `window_end_ms + allowed_lateness_ms` 时窗口最终化，响应中按窗口结束时间递增返回本次结果（`stream`、`window_start_ms`、`window_end_ms`、`count`、`sum`）；空窗口不产生结果，重复水位线不重复结果。
- `GET /streams/{name}/results`：返回该流当前全部最终窗口。

错误码：`stream_exists`（409）、`stream_not_found`（404）、`watermark_regression`（409）、`invalid_json`（400）、`invalid_request`（422）、未知路由 `not_found`（404），均沿用 `{"error": {"code", "message"}}` 结构。各流状态彼此隔离；本版本不包含重启恢复、去重、持久化与自动水位线。

## 验证

```bash
PYTHONPATH=src python3 -m unittest discover -s tests -v
```

当前基线刻意不包含事件时间、窗口聚合与状态恢复的实现，以便后续任务从已冻结事实出发独立设计并验证这些能力。
