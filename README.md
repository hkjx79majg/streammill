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
- 恢复严格校验且不静默修正：请求体不是合法 JSON 返回 400 `invalid_json`；对象结构、字段类型、`format_version`（支持 1 与 2，其他版本同样拒绝）、流名唯一性与升序、配置约束（含 `dedup_retention_ms >= allowed_lateness_ms`，以及滑动流的 `slide_ms` 为正整数、不大于 `window_ms` 且整除 `window_ms`）、数组排序、窗口宽度、起点与网格步长对齐、窗口开闭关系、最终结果与水位线/迟到配置的关系、重复窗口或重复 `event_id`、去重记录的保留期边界与所属各窗口计数关系任一不合法，均返回 422 `invalid_snapshot`，实例保持完全为空。自动流的两个字段必须成对出现且类型合法（`auto_watermark_lag_ms` 为非负整数，`max_event_timestamp_ms` 为整数或 `null`）；非空最大事件时间聚合到的每个窗口都必须是已有开放或最终窗口，且水位线不得低于最大事件时间减去滞后量。实例中已存在任意流时，对任何可解析的快照文档都返回 409 `restore_conflict`（请求体本身不是合法 JSON 时仍按请求格式错误返回 400 `invalid_json`），原状态不变。健康检查、手工流、滑动/滚动流间隔离以及其他既有错误优先级均不改变。

## 进程内维表与可选当前值连接

- `POST /tables` 以非空 `name` 创建维表，成功返回 201；重名返回 409 `table_exists`。
- `POST /tables/{name}/rows` 以非空字符串 `key`、`label` 写行（覆盖式 upsert）：新增或改值返回 `changed: true`，相同重试返回 `changed: false`；未知表返回 404 `table_not_found`。
- `POST /streams` 可额外携带 `lookup_table`（非空字符串），只能引用已存在的表并在创建响应中回显；格式与字段错误沿用 400 `invalid_json` 与 422 `invalid_request`。
- 连接流的事件必须携带非空字符串 `lookup_key`（普通流仍将其视为未声明字段）。事件先按既有规则去重、判断迟到，再原子读取当前 `label`；键不存在返回 409 `lookup_key_not_found` 且状态不变。成功事件同时进入总量窗口与按 `(lookup_key, 当时 label)` 的分组聚合（`count`、`sum`）；改表仅影响后续事件。去重内容包含 `lookup_key`：同一 `event_id` 换键返回 409 `event_id_conflict`。滑动流计入全部重叠窗口，窗口关闭时同步最终化分组。
- `GET /streams/{name}/joined-results` 仅返回最终分组（`stream`、`window_start_ms`、`window_end_ms`、`lookup_key`、`label`、`count`、`sum`），按窗口起点、`lookup_key`、`label` 升序；普通流返回 409 `join_not_enabled`，未知流返回 404 `stream_not_found`。
- 存在任意维表时 `GET /snapshot` 导出 `format_version: 2`：`tables` 按名称、其行按 `key` 排序，连接流额外携带 `lookup_table`、`joined_windows`、`joined_finalized`（去重记录含 `lookup_key`）。恢复兼容 version 1，严格校验 version 2：未知表引用、重复或乱序键、分组与基础窗口不一致、非有限聚合值或错误排序均返回 422 `invalid_snapshot` 且不发布部分状态；已有流或表时返回 409 `restore_conflict`。未使用连接时，既有接口、错误优先级、响应与 version 1 快照不变。

## 可选的窗口物化变更流

- `POST /streams` 可额外携带 `change_retention`（正整数），限定变更流最多保留的记录条数并在创建响应中回显；不提供时不启用变更流，既有接口、响应与快照行为完全不变。
- 启用后 `GET /streams/{name}/changes?after_seq=N&limit=L` 返回 `seq` 大于 `N` 的保留记录：`N` 为非负整数，`L` 为 1 至 1000；响应为 `{"stream", "latest_seq", "changes"}`，`changes` 按 `seq` 递增，初始 `latest_seq` 为 0。缺失、重复、未知或越界参数返回 422 `invalid_request`（参数校验先于流存在性检查）。
- `seq` 从 1 连续递增。每个成功聚合的事件为其影响的每个基础窗口产生一条 `kind: "upsert"` 记录，携带 `seq`、`window_start_ms`、`window_end_ms` 与更新后的 `count`、`sum`；滑动窗口按起点递增产生多条。水位线推进最终化窗口时为每个新最终窗口产生一条 `kind: "final"` 记录（含最终 `count`、`sum`）；自动最终化时同一次提交先排列全部 upsert 再排列全部 final，手工水位线只产生 final，无新最终结果则不产生记录。精确重复、过迟丢弃、`event_id_conflict`、`lookup_key_not_found` 与校验失败都不消耗序号；连接流只发布基础窗口变更，分组结果不变。并发写入在同一把状态锁内形成与聚合一致的全序，读取只能看到完整提交。
- 每次提交后裁掉最旧记录，最多保留 `change_retention` 条，`latest_seq` 不回退。`after_seq` 大于 `latest_seq` 返回 409 `change_cursor_ahead`；仍有保留记录且 `after_seq` 小于最早保留 `seq` 减一时返回 410 `change_cursor_expired`；未知流返回 404 `stream_not_found`，未启用变更流的流返回 409 `change_feed_not_enabled`，错误体沿用 `{"error": {"code", "message"}}`。
- 存在任意启用变更流的流时 `GET /snapshot` 导出 `format_version: 3`：文档始终携带 `tables` 数组（含既有表与连接状态），启用变更流的流对象额外包含 `change_retention`、`latest_seq` 与保留的 `changes` 记录。恢复兼容 version 1 与 2，并严格校验 version 3：序号连续性（保留记录为以 `latest_seq` 结尾的连续序号，条数不超过 `change_retention` 且在序号超过上限后恰好等于上限）、排序、窗口起点与网格对齐及宽度、记录引用的窗口必须存在、每个窗口至多一条 final 且其后无记录、各窗口最后一条保留记录的 kind 与 `count`/`sum` 必须和该窗口聚合状态一致，任一不合法均返回 422 `invalid_snapshot` 且不发布任何状态。恢复后游标与下一序号连续，继续写入、裁剪与再次导出和未中断实例等价；未启用变更流的实例继续导出版本 1 或 2 的原始形状。

## 可恢复的原子批次写入

- `POST /streams` 可额外携带 `batch_retention`（正整数），限定保留的批次记录条数并在创建响应中回显；不提供时既有响应与快照不变，`POST /streams/{name}/batches` 返回 409 `batch_ingest_not_enabled`。
- 启用后 `POST /streams/{name}/batches` 接收 `{"batch_id", "events"}`：`batch_id` 为非空字符串，`events` 含 1 至 1000 个元素，各元素遵循该流的单事件字段规则（去重流要求 `event_id`，连接流要求 `lookup_key`，其余字段同样视为未声明）。非法 JSON 返回 400 `invalid_json`；结构错误（含任一元素）返回 422 `invalid_request`；结构合法但流不存在返回 404 `stream_not_found`。
- 服务先校验全部元素，再在同一状态锁内按输入顺序应用既有的去重、迟到、维表查询、窗口、自动水位线、最终化与变更序号语义，其他请求不会看到中间状态；自动水位线逐项推进，后续元素使用前项处理后的水位线。成功响应为 `{"stream", "batch_id", "outcomes"}`，`outcomes` 与输入同序，每项为对应单事件响应去除 `stream` 后的内容；过迟丢弃与精确重复仍是成功 outcome。
- 处理中首个标识冲突或维表键缺失分别返回 409 `event_id_conflict`、409 `lookup_key_not_found`，并回滚整个批次：聚合、去重、水位线、最终结果与变更序号均不变，失败请求不占用 `batch_id`。
- 成功批次保留 `batch_id`、请求与完整响应。保留期内以字段值与事件顺序相同的请求重试，原样返回首次响应且不再写入（重试不刷新保留顺序）；同一 `batch_id` 内容不同返回 409 `batch_id_conflict`。仅保留最近 `batch_retention` 条，淘汰后的标识可复用，并发同标识至多提交一次。
- 存在任意批次流时 `GET /snapshot` 导出 `format_version: 4`（始终携带 `tables` 数组），对应流额外携带 `batch_retention` 与按提交顺序排列的 `batches` 记录（`batch_id`、`request`、`response`）。恢复兼容 version 1 至 3，并严格校验 version 4：重复标识、记录条数超过 `batch_retention`、请求或响应形状不合法均返回 422 `invalid_snapshot` 且不发布部分状态；恢复后重放与淘汰顺序和未中断实例一致。健康检查、单事件入口、水位线、结果查询、连接与变更流的既有行为不变。

## 可选的事件时间版本维表

- `POST /tables` 可额外携带 `event_time_versioned: true`（布尔值），声明该表为事件时间版本维表并在创建响应中回显；未声明（或为 `false`）时创建、覆盖写入与快照行为与当前值维表完全一致。类型或字段错误沿用 400 `invalid_json` 与 422 `invalid_request`。
- 版本维表的 `POST /tables/{name}/rows` 接收 `{"key", "label", "effective_from_ms"}`：`effective_from_ms` 为整数，同一 `key` 的每个版本自其 `effective_from_ms` 生效到该键下一版本之前，写入允许乱序。首次写入或新增生效时间返回 `changed: true`；字段完全相同的重试返回 `changed: false`；同一 `(key, effective_from_ms)` 已存在但 `label` 不同返回 409 `dimension_version_conflict`，历史不变。当前值维表收到 `effective_from_ms` 视为未声明字段返回 422；未知表仍按基础形状先校验再返回 404 `table_not_found`。
- `lookup_table` 引用版本维表的连接流中，每个事件以自身 `timestamp_ms` 选择不晚于该时间的最新版本参与分组聚合；键不存在或事件时间早于该键首个版本返回 409 `lookup_version_not_found`，聚合、去重、水位线、变更序号与批次均不变。之后补写的维度历史只影响事件时间不早于它的事件，不重算已接收事件。去重内容仍包含 `lookup_key`；批次中任一元素找不到版本时整批回滚，失败请求不占用 `batch_id`。
- 只要存在版本维表，`GET /snapshot` 即导出 `format_version: 5`（始终携带 `tables` 数组）：版本维表对象携带 `event_time_versioned: true` 与 `versions`（`key`、`label`、`effective_from_ms`），版本点按 `key`、`effective_from_ms` 严格升序；当前值维表保持 `rows` 形状，流对象字段与 version 4 相同。恢复兼容 version 1 至 4，并严格校验 version 5：模式字段组合错误（`rows` 与版本标记混用、`versions` 缺少标记或标记非 `true`）、重复或乱序版本点、无效生效时间及同一点冲突均返回 422 `invalid_snapshot` 且不发布部分状态；恢复后按事件时间的连接解析、批次重放与变更游标与未中断实例一致。普通维表、未连接流、已有结果与 joined-results 排序、迟到与自动水位线规则、去重淘汰、变更流游标、批次重放、健康检查及既有错误优先级均保持不变。

## 验证

```bash
PYTHONPATH=src python3 -m unittest discover -s tests -v
```

快照覆盖进程内全量状态（含维表与连接分组），可用于跨实例/重启的人工恢复；落盘持久化仍不在当前范围，由后续任务从已冻结事实出发独立设计并验证。
