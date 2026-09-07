# 日期范围补数手册

请求必须包含 UTC 起止日期（含两端）、规则 v1/v2、原因、固定 manifest、到达 HWM。
第一步运行 `dry-run`。检查 plan.json 及分布式 affected_keys、partitions Parquet：范围、键数、
输入边界应符合业务意图。dry-run 是规划证据，执行时仍重新校验输入并加锁，不能当作提前获得写权限。

使用 `backfill --start YYYY-MM-DD --end YYYY-MM-DD --run-id <稳定名称>` 提交。
同 run_id 重试必须使用相同参数；Airflow 从 DAG run_id 派生稳定运行名，重试不创建第二个业务运行。
同表日常更新在建分支至验收发布期间排队；记录 queue_seconds，牺牲新鲜度是明确取舍。
本版对三表 release 使用一个数据集级写入锁，比单表锁更保守。

如果新的 HWM 包含范围外的未消费修订，本次被拒绝；先扩大范围或完整追赶。不能悄悄跳过范围外
修订却推进 HWM。维度历史变化要求完整重算；范围任务不能发布新维度而保留不匹配的范围外明细。

检查 runs/<run_id>/validation/report.json：keys/detail/daily 的 missing 与 unexpected 必须为 0；
duplicate_keys/null_keys 必须为 0；tombstones 单列；outside_range 比较包括 source_file 的所有列。
失败差异以 Parquet 保存，不收集到 driver。检查 release.rule_scope，范围外可以保留旧 rule_version。

失败后：

1. `status` 查看 writer_run，读取 runs/<run_id>/failure.json。
2. 提交间隙用 `recover <run_id>`；分支 MERGE 可重试，已验收 candidate 不重复计算。
3. 源契约错误且 main 未改变时，`abort <run_id> --reason ...`，生成新的不可变输入后用新 run_id 执行。
4. main 已改变时禁止 abort。恢复完整发布或在停用外部写者后核对分叉并作前向修正。

注入点：after_detail_commit、after_fast_forward、before_audit、after_audit。演示的失败是实验注入，
不是曾经发生过的生产事故。

