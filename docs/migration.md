# 迁移方案与发布协议

旧输入是按日期目录组织的 Parquet，来源本身没有跨文件原子替换或跨表读版本。本项目采取
“读旧文件，写独立 Iceberg 路径”，不直接 add_files 接管旧文件，迁移复制与中间 staging 都有额外存储成本。

1. 固定 contract、累计 manifest 与 H0。manifest 逐文件记录 SHA-256、大小、行数、schema 和到达 batch。
2. Spark 先拒绝空键、负单位、同源版本冲突、时间移动和维度区间重叠，再按最大源版本归并。
3. 三表 detail/plans/daily 建立独立 audit branch；所有业务写者共用持久所有权及执行锁。
4. 源数据读取后落入 run 的 incoming Parquet，保存来源文件路径；分支 MERGE 比较 source_version。
5. DuckDB 独立构建 oracle；Spark 双向 EXCEPT ALL 验证键、精确值、tenant/date 聚合及范围外数据。
6. 固定三张表的 snapshot_id，先将完整 candidate 写入审计，再为每个发布快照创建 retention tag。
7. 检查 main 仍是初始头或本次已提交的头，逐表 fast-forward。任意分叉都阻断，不盲目重试。
8. PostgreSQL 单事务写 releases 与 active_release/generation。此时才对消费者可见；完成运行并释放锁。
9. H0 后原始接入继续形成批次，目标以新 manifest 追赶 H1。旧文件项不可变化，进度不得回退。

运行状态为 RUNNING → WRITING → VALIDATED → COMMITTED → COMPLETED；异常记录 FAILED 和
last_error，保留 writer_run。恢复使用原 run_id、原 manifest 摘要、规则和范围；已经存在的分支和 candidate
帮助区分写出文件、分支提交、主分支推进和发布审计。COMPLETED 与解锁同一事务，避免完成后卡锁。

关键不变量：未验收 candidate 不能发布；同一发布绑定全部表版本；消费者回退不回退 writer 高水位；
半完成的单表 fast-forward 不改变活动 release。Iceberg 原子性是单表提交，跨表一致读取由消费者遵循
发布清单实现，不是跨表数据库事务。

本版的性能取舍：每次重算仍扫描截至 HWM 的完整输入，用于独立归并和对账；明细只合并选定日期，
汇总根据固定明细分支全量重建。没有宣称整个补数作业只扫描范围内源文件。后续可在保持相同验收
协议的前提下增加可索引源状态、受影响键增量汇总和更短切换窗口。

参考：[Iceberg JDBC Catalog](https://iceberg.apache.org/docs/1.8.1/jdbc/)、
[Spark Writes](https://iceberg.apache.org/docs/1.8.1/spark-writes/)、
[表格式规范](https://iceberg.apache.org/spec/)。

