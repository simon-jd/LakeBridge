# 2026-09 更新验收

执行日期：2026-09-06 America/Los_Angeles / 2026-09-07 UTC。
这是本地实验验证，不是生产或真实云运行。

## 结果

| 检查 | 实际结果与证据 |
|---|---|
| 修复前负向基线 | 4 failed、8 passed；`evidence/market-refresh/baseline.xml`。四项故障涉及准入竞争、跨 schema 锁、并发激活错误类型、release 内容冲突 |
| 修复后完整测试 | **64 passed，36.98 秒**；`evidence/market-refresh/tests.xml` / tests.log。含 7 项真实 Spark 集成、6 项真实 PostgreSQL 测试；其余为单元/本地集成测试，不把用例数当吞吐量 |
| Spark/Iceberg S3 + JDBC | **18 项兼容性操作通过**；`evidence/market-refresh/runtime-verification.json`。Spark 3.5.9、Iceberg 1.10.2、Scala 2.12.18、Python 3.11.11、Java 17.0.20 |
| S3 上业务闭环 | 40 行生成基数的四项演示通过，5 个审计 release、最终 generation 6，writer_run 为空；不是 10 GiB 业务迁移 |
| 维护 | 独立兼容性表执行 rewrite-all/过期/保留快照检查；业务小表 compact 正常完成但实际重写 0 文件，orphan dry-run 候选 0；不能声称此次业务小表获得性能收益 |
| Prometheus | promtool v3.6.0 实际校验成功及失败两种文本输出，退出码均 0；对应 audit.prom / audit-unavailable.prom。未部署长期采集服务 |
| 容器与既有数据 | 重建 Spark 镜像；新容器固定版本读取原 Compose 活动 release 的 5 行样本。构建与读取日志归档；不等于全量历史数据重验 |
| 静态与配置 | Ruff、冻结依赖检查、两份 Compose 配置验证；机器可读结果见 `evidence/market-refresh/checks.json` |

新测试首次全量执行出现两项 fixture 错误（生成行数小于项目要求的 20），修正为 20 后完整重跑通过。
首次 S3 初始化在复用数据库的新 catalog schema 中失败，错误为 iceberg_tables 不存在；
另一次新库未显式选择 audit search_path，正确地被 PostgreSQL 拒绝在 public 建表。
最终使用独立数据库 `lakebridge_refresh_0906`，并显式选择 catalog/audit schema。
首次日志保留在 `artifacts/market-refresh/*first-attempt*` 和 `*search-path-attempt.log`。
没有通过扩大 public 权限、覆盖原演示表或吞掉异常来让验证通过。

## 复现

```bash
export JAVA_HOME=/path/to/jdk17
uv sync --frozen --extra dev --python 3.11
.venv/bin/python scripts/bootstrap.py
export LAKEBRIDGE_TEST_POSTGRES_DSN='postgresql://user:password@127.0.0.1:5432/test_database'
LAKEBRIDGE_INTEGRATION=1 .venv/bin/pytest -q --junitxml=artifacts/test-results.xml
```

测试 DSN 需要在可丢弃的本地数据库中创建独立 schema，连接终止用例需要终止自己的其他会话。
每个测试清理自己创建的 schema。未配置 DSN 的普通 pytest 会明确跳过 PostgreSQL 项；
GitHub workflow 已添加 PostgreSQL service 和测试 DSN，该轮验收发生在 Git 初始化之前，未执行远端 CI。

运行时验证使用现有 Settings 环境变量（与 compose.yaml 相同），配置独立库、独立 warehouse 前缀：

```bash
.venv/bin/python scripts/verify_runtime.py \
  --root artifacts/new_runtime_check --namespace new_runtime_check
```

默认本地路径为 Hadoop Catalog + SQLite；要验证 S3/JDBC，必须同时设置
LAKEBRIDGE_CATALOG_URI、CATALOG_USER/PASSWORD、AUDIT_DSN、WAREHOUSE、S3_ENDPOINT 和 AWS 凭据。
建议为资格验证使用全新数据库；在含其他 JDBC catalog 的数据库新增 schema 时需单独验证初始化。
脚本拒绝复用已有 release 的审计库，结果、事件日志与校验数据留在指定 root。

## 环境与范围

单台 Linux 工作站，Intel i9-13900K、32 logical CPUs、约 123 GiB 内存；本轮 Spark 为 local[2]，
PostgreSQL 17.6 与 MinIO 由本机 Docker 提供。完整环境快照与文件摘要见 checks.json 和
source-manifest.json；当时尚无 Git commit，历史证据按 SHA-256 绑定。
原 Spark 容器保留为 `lakebridge-spark:baseline-3.5.6`，新默认镜像需统一切换后使用。

先前 10.157 GiB / 19,773,338 行的三 worker 实验未重跑，仍属于 Spark 3.5.6 / Iceberg 1.8.1。
720→90 文件与查询中位数 9.715→8.407 秒仅可作为注明环境的历史本地合成数据结果。
本轮没有新性能提升声明。云费用是有假设的模型，不是真实账单或已实现节省。

仍未完成：真实 AWS 部署/账单、生产负载、Airflow 3、维护中的 S3 模拟器替代、存储层 fencing、
跨主机故障/网络分区、混合新旧锁协议滚动升级，以及新版本大规模性能验证。
本轮经历库条目应把这些列为缺口，不得写成已验证经历。
