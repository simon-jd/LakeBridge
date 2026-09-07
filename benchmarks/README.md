# 性能实验方法

`lakebridge benchmark` 实际生成有不同 ID、日期、单位与热点租户的固定记录，先保存为旧 Parquet。
所有变体从同一个旧数据快照构建，逻辑结果逐项相等才报告耗时。
旧 Parquet 与全部 Iceberg 统一使用 Zstandard；旧 Parquet/无分区 Iceberg 均按 event_id hash
分配到 4 个写入分区，避免压缩算法和基线文件数不同造成归因混乱。

实验一只改变 Iceberg 是否使用 days(event_time) 分区，固定一天时间谓词，与无分区表比较。
旧 Parquet 同逻辑查询另列，避免把格式转换与分区变化的收益混为一谈。
实验二在同一表上先故意以 16 个写入分区形成小文件，量测全扫描聚合，然后 rewrite_data_files，
以相同查询再测。记录真实合并前后文件数/压缩字节/大小分布及合并耗时。

每项预热 1 次，至少重复 3 次，记录全部样本、中位数、标准差、最小/最大。执行前清除 Spark cache；
JVM/OS cache 已预热，不能宣称 cold cache。变体执行次序固定，所以仍存在顺序和 OS 缓存影响。
报告固定 master、driver memory、shuffle 分区、AQE、Spark/Python 版本、application_id。
计时覆盖已启动 Spark 内查询规划到结果收集；explain 另记，Spark JVM 启动不计入单查询计时。
独立启动到结束的作业时间应由外层计时器记录，不能将这里的秒数当完整迁移耗时。

`report.json` 包括执行过的物理计划 metrics（例如 resultDataFiles、totalFileSize、shuffle）。
这些扫描文件字节是引擎指标，不能自动等同于操作系统或 S3 实际传输字节；无指标时留空，不编造。
完整 event logs 可用 `python scripts/event_metrics.py <log> --output <json>` 汇总 task input bytes、
shuffle、spill、GC、任务最大/中位耗时；它包括预热和重试，不能直接套给单次查询。

128/256 MiB 通过 --file-mib 控制目标大小，不保证小数据能形成目标尺寸文件。
合并可能令查询变慢；报告保留实测负收益。总成本应同时包含迁移复制、staging、维护、长期 tags
保留和查询；没有账单就不能报告真实成本节省。原 local[2] 微实验为 100,000 条；另已完成
三 Worker、19,773,338 条、10.157 GiB 的宽事件专项，见 [10 GiB 报告](../docs/performance-local-10g.md)。
50–100 GiB 仍未执行，不能机械放大当前结果。

后续尚未完成的专项量测：大维度 broadcast 门槛、executor GC/内存极限、两阶段倾斜聚合、
不同合并频率的总资源成本。当前生成器、固定查询和 event logs 可作为这些实验的起点。

已保存两项报告：[日期分区](../docs/performance-partition.md)、
[文件合并](../docs/performance-compaction.md)，原始样本与执行计划在
[results/local-100k](results/local-100k/report.json)。

三 Worker 的独立模拟入口、请求计量和成本口径见 [复现说明](../docs/local-simulation.md)，
正式证据在 [results/local-10g](results/local-10g/report.json)，
估算使用 [cloud-cost-assumptions.json](cloud-cost-assumptions.json) 的显式参数。
