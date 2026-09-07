# 本地性能实验实测

数据量：100,000 条；每项重复 3 次。
预热 JVM/OS 缓存，清空 Spark cache；单机结果不能外推至 10 GB 或云端。

| 场景 | 中位数（秒） | 最小–最大（秒） | 文件数 |
|---|---:|---:|---:|
| legacy | 0.0617 | 0.0497–0.0618 | Parquet source |
| unpartitioned | 0.0503 | 0.0460–0.0573 | 4 |
| partitioned | 0.0436 | 0.0422–0.0442 | 56 |
| before_compaction | 0.1707 | 0.1670–0.1915 | 224 |
| after_compaction | 0.0522 | 0.0518–0.0531 | 14 |

## 日期分区

前后查询结果完全一致。基线耗时 / 改动后耗时 = 1.155。
此数值小于 1 时表示变慢；小样本调度和缓存开销可能大于扫描收益。
执行计划见 `unpartitioned.plan.txt` 和 `partitioned.plan.txt`。

## 文件合并

前后查询结果完全一致。基线耗时 / 改动后耗时 = 3.271。
此数值小于 1 时表示变慢；小样本调度和缓存开销可能大于扫描收益。
执行计划见 `before_compaction.plan.txt` 和 `after_compaction.plan.txt`。

合并作业额外耗时 0.4907 秒，必须计入总成本。
未采集云账单，不推导成本节省；详细 SQL 指标与样本见 report.json，task 指标见 event logs。
