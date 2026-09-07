# 性能实验一：日期分区裁剪

实验日期 2026-09-06；实验 ID `bench_b93f24c96e`；固定 100,000 条、14 天、热点租户数据。
Spark 3.5.6、Java 17、local[2]、driver 2 GB、shuffle 4、AQE 开启，所有变体使用 Zstandard。
比较同一天 count/sum 查询，结果完全一致。每项预热后测量 3 次，清空 Spark cache，OS/JVM 已预热。

| 指标 | 无分区 Iceberg | days(event_time) 分区 |
|---|---:|---:|
| 表文件总数 | 4 | 56 |
| 此查询匹配文件数 | 4 | 4 |
| 跳过的文件数 | 0 | 52 |
| 匹配文件总字节（引擎指标） | 3,880,819 | 290,836 |
| Scan 输出行数 | 100,000 | 7,143 |
| 查询中位数（秒） | 0.050287 | 0.043552 |
| 三次范围（秒） | 0.046047–0.057345 | 0.042175–0.044185 |

日期分区减少了被选中文件的总大小，虽然文件数同为 4，每个文件覆盖范围更小。
这证明本次谓词获得了分区裁剪；匹配文件总字节不等于 S3 网络实际传输量。
中位耗时有所降低，但只有 3 次、小数据和预热缓存，不能据此证明在 10 GB 上有稳定提速。
代价是表文件数从 4 增至 56，小样本按日期切分可能增加调度与维护成本。

源 Parquet 查询中位数 0.061655 秒，单独列示，不能将格式、分区、文件布局多种变化合并归因。
旧 Parquet 与无分区 Iceberg 都用 4 个 event_id hash 写入分区和相同压缩算法。

证据：[全部样本和 SQL metrics](../benchmarks/results/local-100k/report.json)、
[无分区计划](../benchmarks/results/local-100k/unpartitioned.plan.txt)、
[日期分区计划](../benchmarks/results/local-100k/partitioned.plan.txt)、
[事件日志汇总](../benchmarks/results/local-100k/event-metrics.json)。

复现：`lakebridge --root artifacts/new_benchmark --namespace new_benchmark benchmark --rows 100000 --repetitions 3`。
环境、缓存与变体执行顺序限制参见 [实验方法](../benchmarks/README.md)。
