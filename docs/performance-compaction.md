# 性能实验二：小文件合并

实验日期 2026-09-06；与日期实验共用固定 100,000 条数据及相同本机资源。
先用 16 个 event_id hash 写入分区制造小文件，再对同一表执行 rewrite_data_files；
分区、压缩和查询逻辑不变。每项预热后测量 3 次，全表 count/sum/payload_length 结果完全一致。

| 指标 | 合并前 | 合并后 |
|---|---:|---:|
| 文件数 | 224 | 14 |
| 压缩存储字节 | 4,540,531 | 4,015,288 |
| 文件中位大小（字节） | 20,276 | 286,871 |
| 查询中位数（秒） | 0.170750 | 0.052201 |

本次中位查询耗时下降约 69.4%（约 3.27 倍），合并作业额外耗时 0.490650 秒。
按本次中位查询节省约 0.118549 秒粗算，至少约 5 次相同查询才能覆盖这一轮合并的墙钟时间；
这只是本机耗时比较，不是云账单模型，不能将墙钟秒直接当计算资源成本。

目标文件 128 MiB，而实际每个日期只有少量数据，合并后的文件远小于目标大小，这是正常结果。
底层 compaction 产生新快照；旧 release 仍通过 tag 读取原文件，所以活跃表文件减少不代表总物理
存储立刻按比例下降。发布保留和旧快照过期必须一起评估。

证据：[完整报告](../benchmarks/results/local-100k/report.json)、
[合并前计划](../benchmarks/results/local-100k/before_compaction.plan.txt)、
[合并后计划](../benchmarks/results/local-100k/after_compaction.plan.txt)、
[压缩保存的真实 Spark event log](../benchmarks/results/local-100k/spark-event-log.json.gz)。

本实验未测 10 GB、不同合并频率、云端账单或 executor 内存极限。缓存、顺序与重复次数限制同
[实验方法](../benchmarks/README.md)。负收益也会原样保存在 benchmark 自动报告中。
