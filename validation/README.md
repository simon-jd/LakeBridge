# 独立验收

生产转换使用 Spark DataFrame；oracle 使用独立 DuckDB SQL，位于 `src/lakebridge/oracle.py`，
不调用 Spark 的 prepare 或 aggregate。生成器在 expected/h{1,2,3}_v1 保存独立预期结果；
每次运行再按固定输入和规则构建期望快照。DuckDB 限制 512 MB 并允许落盘，结果通过 COPY 写 Parquet。

Spark 分布式执行双向 EXCEPT ALL，分别核验主键多重集合、精确业务字段和 tenant/date 汇总，
另列重复、空键、tombstone、活跃行数。范围外对比所有列，包括 lineage；失败差异写 Parquet，
driver 不收集整张错误明细。发布只有在全部校验通过后执行。

运行：`make test`；真实 Spark 检查：`make integration`。
演练的 JSON 在 `artifacts/<root>/demo-report.json`；每次运行证据在 `runs/<run_id>/validation/`。
