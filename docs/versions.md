# 冻结版本与复现

| 组件 | 版本 / 选择 |
|---|---|
| Python | 项目支持 3.11–3.12；实测 3.11.11 |
| Spark / Scala | 3.5.9 / 2.12.18（runtime 二进制名使用 2.12） |
| Iceberg | 1.10.2；iceberg-spark-runtime-3.5_2.12 |
| Java | 17；本机 17.0.20，容器随 Debian 的 17 更新，实际值保存在 compatibility.json |
| JDBC | org.postgresql:postgresql:42.7.5 |
| FileIO | 本地 HadoopFileIO；对象存储 S3FileIO + iceberg-aws-bundle:1.10.2 |
| Catalog | 本机快速路径 Hadoop Catalog；完整 Compose 使用 JDBC Catalog/PostgreSQL |
| PostgreSQL | 17.6-alpine；catalog/audit 不同 schema 与角色 |
| MinIO / mc | RELEASE.2025-04-22T22-12-26Z / RELEASE.2025-04-16T18-13-26Z |
| Airflow | 2.10.5-python3.11；LocalExecutor |
| Arrow / DuckDB | 19.0.1 / 1.2.2 |
| psycopg | 3.2.6 |
| Terraform | 验证使用 1.11.2；AWS provider 锁定于 .terraform.lock.hcl |

Python 完整依赖见 uv.lock；Docker 的 requirements.lock 由其导出并含各平台 hash。
JVM 三个 jar 的 SHA-256 在 src/lakebridge/config.py 中冻结；bootstrap 验证成功才就位，
已有正确 jar 可离线复用。版本不是“当前最新”承诺，升级时应重新跑 compatibility、业务集成与 S3 维护。

已验证操作包括 CREATE/INSERT、MERGE、分支隔离、tag、版本读取、fast-forward、schema/分区演进、
compaction、expire_snapshots、protected snapshot、S3 orphan dry-run、rollback 与分叉拒绝/重建。
建表成功不能替代这些操作的兼容性测试。

查询来源：[Iceberg Spark Getting Started](https://iceberg.apache.org/docs/1.10.2/spark-getting-started/)、
[Spark Procedures](https://iceberg.apache.org/docs/1.10.2/spark-procedures/)、
[Branching](https://iceberg.apache.org/docs/1.10.2/branching/)。

## 2026-09 维护更新

本轮版本选择与官方 release/迁移来源见 [工程取舍](design-decisions.md)。
MinIO 原仓库已于 2026-04-25 归档；Airflow 2.x 于 2026-04-22 EOL，
这里保留的是本地历史实验依赖，不是当前生产选型。Airflow 3 与存储替换尚未完成。

此次 PostgreSQL 执行锁键从数据库全局常数改为审计表 OID 范围。升级必须先停掉旧作业、
恢复或审查未完成 run，再统一切换应用与 Spark 镜像；不支持新旧锁协议混跑。
不修改既有审计表结构和 release payload；已发布 release 的恢复仅忽略两项耗时 telemetry。
重新构建 `docker compose build spark` 才会把新代码和版本带入容器，已有镜像不会自动变化。

`benchmarks/results/local-10g` 与之前的 docs/evidence 根目录报告保留原版本记录，
不得以此声称新版本已有 10 GiB 性能验证。本轮报告单独位于 docs/evidence/market-refresh。
