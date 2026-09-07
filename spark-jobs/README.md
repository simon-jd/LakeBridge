# Spark 作业

作业使用安装后的 `lakebridge` CLI，避免再维护一份复制的转换代码。

| 命令 | 实现与职责 |
|---|---|
| migrate | `src/lakebridge/pipeline.py`，固定输入、分支写入、校验、发布 |
| backfill / dry-run | 明确日期范围、受影响键清单、范围外保护 |
| recover / abort | 失败运行的恢复；主分支未改变时允许有原因地终止 |
| query / rollback | 固定发布清单读取；消费者切换、写入头不回退 |
| maintain | `maintenance.py`，发布引用保护、合并、过期、孤儿候选清单 |
| compatibility | 真实 Iceberg 操作和分支分叉验证 |
| benchmark | 同一数据与查询的分区/文件实验，真实 SQL 计划和运行指标 |

先启用 `JAVA_HOME` 指向 JDK 17。容器镜像已包含此配置。

