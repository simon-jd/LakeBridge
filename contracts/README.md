# 输入契约 v1

业务键为 `(tenant_id,event_id)`，不是 `event_time`。源版本为正整数 `source_version`，
同键同版本必须有完全一致的 `event_time,ingested_at,units,is_deleted`。不一致记录连同键和冲突
原因写入 quarantine，本批失败，不自动挑一个值发布。版本不同按最大源版本保留；删除以 tombstone
保留在 detail 中，消费者和 daily 排除 `is_deleted=true`。

`event_time` 在同一业务键的所有修订中不可更改。需要纠正时间时，源 adapter 应把旧键发出较新
tombstone、将新时间作为新的事件键输入。本版会拒绝原键时间变更，避免范围补数隐式移动日期。
时间统一为 UTC 无时区微秒值，日期边界按 UTC；单位是 `decimal(18,6)`，汇总为 `decimal(28,6)`。
不接收浮点 units，不做隐式 schema 转换，不接受负用量、空键和不合法版本。

套餐维度为 `tenant_id,plan_id,effective_from,effective_to,attributes`。区间左闭右开；
`effective_to=NULL` 表示无穷远。相同维度记录可去重，重叠和无法匹配的事件都阻断发布。
初版 plan_id 枚举 basic/pro，attributes 是保留的 JSON 字符串。

规则 v1 保留原始 units；v2 对 pro 的 billable_units 使用精确十进制 0.9 系数，basic 不变。
这是可复现的演示规则，不是实际产品计费政策。范围补数允许不同日期保留不同的 rule_version，
release 中 rule_scope 明确标记规则生效范围，不能把它误读为整表统一规则。

Manifest 字段参见 [JSON Schema](manifest.schema.json)，物理 Arrow schema 在
`src/lakebridge/contracts.py`。文件路径相对 manifest，不允许目录逃逸、重复路径；batch 连续且
代表到达顺序。晚到文件必须新建 batch，不能追加进已消费的旧 batch。批次 H0/H1 与事件日期无关。
处理前校验字节数、SHA-256、Parquet 行数和精确字段类型，发布前再次验证源文件。

源输入必须是不可变快照或只读 staging。校验摘要不能防止有权限的外部进程在读的瞬间修改文件。
S3 输入通过 `lakebridge stage-s3` 以事先固定的 manifest 摘要下载精确文件集合，校验后再执行迁移。
新增批次可以使用新的累计 manifest；旧文件项及其摘要必须原样保留。

