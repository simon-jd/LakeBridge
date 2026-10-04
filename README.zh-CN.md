# LakeBridge · 历史数据湖仓迁移与性能治理

本次代码回归与云端结果分别记录：参见 [数值快照比较契约](docs/releasequery-numeric-contract.md)
与 [本地验证记录](docs/github-alignment-validation.md)。`numeric-diff` 支持五列投影、
浮点容差和显式 NaN 策略；原有固定 release 的 `diff` 接口保持精确比较。

2026-09-07 更新：恢复参数与写者归属在同一事务中提交；终止操作在锁内重读状态，原子终止并释放归属；
已完成发布后的报告写入失败不会把 run 改回 FAILED。本轮本地回归 **73 项通过**
（含 8 项真实 Spark/Iceberg、8 项 PostgreSQL），新增 9 项生命周期测试。
详见[恢复说明](docs/run-lifecycle.md)、[招聘与技术维护依据](docs/reliability-refresh-2026-09-07.md)
及[本轮验证](docs/evidence/run-lifecycle/verification.json)。旧性能/S3 记录保持各自环境与日期。

[English README](README.md)

将不可变的历史 Parquet 与追加批次迁移到 Apache Iceberg，用 PySpark 完成版本归并、
范围补数、独立对账和快照发布。消费者读取同一 release 绑定的明细、套餐、汇总快照；
错误发布可以切回旧版本，再前向修正并保留后续合法数据。

本项目按《03_LakeBridge_历史数据湖仓迁移与性能治理》实现。验收以真实运行证据为准：
小样本业务闭环及本机三 Worker 的 10.157 GiB 存储/性能模拟已运行，并提供明确假设的云费用估算。
50–100 GiB、10 GiB SaaS 业务全流程和真实云端账单实验尚未执行。
详细结果见 [验收记录](docs/acceptance.md)。

2026-09 工程更新：修复 PostgreSQL 发布/回滚竞争与重复请求边界，增加无 Spark 的只读审计指标，
升级到 Spark 3.5.9 / Iceberg 1.10.2。依据、范围与证据见
[工程取舍](docs/design-decisions.md)、[本轮验证](docs/market-refresh-validation.md)。

```mermaid
flowchart LR
  A[不可变 Parquet / S3 staging] --> B[Manifest: hash / schema / HWM]
  B --> C[Spark: source_version 归并]
  C --> D[Iceberg audit branches]
  E[独立 DuckDB oracle] --> F[分布式主键与精确聚合对账]
  D --> F
  F --> G[保留 tags + 发布快照集合]
  G --> H[PostgreSQL 活动 release]
  H --> I[消费者固定版本读取]
  J[Airflow + 持久写入锁] -.-> D
  K[JDBC Catalog / PostgreSQL] -.-> D
```

## Cloud qualification results

These results follow the project owner’s separately run cloud qualification recorded in the experience bank. The workload, environment, denominators and limitations below belong to that round; local regression checks for this checkout are separate. [Result record](docs/experience-bank-results.json).

### LakeBridge — Auditable Lakehouse and Versioned Consumer Refresh

- Built an immutable release manifest holding per-table Iceberg snapshot_ids behind a PostgreSQL active-release pointer, so a consumer reads one release_id and gets one consistent multi-table view; single-table snapshots commit atomically by themselves, while the multi-table view exists only through the manifest plus the control-plane pointer, and bypassing the release to query tables directly is not covered.

- Bound tenant refresh plans to their full content: old and new release, tenant, date range and diff hash are part of the plan, an independent approver signs plan_hash, the worker has no approval rights, and execution re-verifies plan and generation before switching the pointer.

- Published consumer results and completion status in one PostgreSQL transaction after temporary-table validation, with the dataset generation guarded by a row lock (SELECT FOR UPDATE, compare generation, switch pointer) under a dataset advisory lock; a report-generation failure cannot undo already-committed data.

- Matched 2,000/2,000 difference business keys (1,000 added, 200 deleted, 800 modified) per key against a SQL oracle with tenant and date impact sets exactly equal, across the 100-test new round: 75 lifecycle/recovery plus 25 consumer bridge tests, with the 19 runtime-compatibility scenarios a labeled subset of the 100 rather than extra cases.

- Injected 20 faults across 5 windows × 4 repetitions (after data write, after a single-table snapshot, after manifest write, after the PostgreSQL pointer commit, before report write): the first 12 left the old release active and the last 8 left the new release active, and retries returned the same release instead of republishing; orphaned files are left to the retention policy, and object storage is not claimed to roll back with the PostgreSQL transaction.

- Held 4 concurrent identical refresh requests to a single job and one consumer publication, and rejected all 8 attempts to reuse a valid approval token with a different tenant or target release.

- Exposed status through REPEATABLE READ snapshot reads and an atomically replaced Prometheus textfile, and explicitly exported a failure state when collection failed.

### LakeBridge Ops Agent — MCP Investigation and Approved Refresh

- Built a LangGraph companion agent with 7 typed MCP tools (list_releases, describe_release, preview_diff, create_plan, approval_status, submit_refresh, get_job), synchronous SQLite checkpoints and a cumulative call budget of at most 12 calls per run that persists across restarts instead of resetting.

- Kept model claims subordinate to complete tool facts: on the fixed old-R1/new-R2 change (tenant A affected 3 days, tenant B affected 2 days) the standard 9-call path produced a plan with exactly 5 tenant/date combinations, and when the scripted model claimed only 1 day was affected the final answer still cited all 5 combinations and marked the inconsistency.

- Bound approval to an independent human identity on the service side: the PostgreSQL approval is tied to plan_hash and expires after 15 minutes, and because the agent token cannot call approve, 2 self-approval attempts returned HTTP 403 while 2 explicit human rejections ended in DENIED with no submit.

- Recovered from ambiguous execution: a submit whose reply was lost reused the original idempotency key to return the same job and publish only once, two successful refreshes each created 1 independent job with no idempotency-key sharing between rounds, and after the checkpointer wrote to disk a killed process recovered with tool results still referencing the same release.

- Exercised a 12-scenario fault matrix (6 classes × 2) covering restart after plan, approval expiry, agent self-approval, plan replacement after approval, lost submit reply and budget exhaustion: restart retained the same plan, expiry and replacement each rejected submission twice, exhaustion stopped new tool calls twice, and retrying submit moved the flow from 9 to 10 calls, still inside the 12-call budget.

- Treated model text and external MCP descriptions as data only — an 'ignore approval' instruction changed nothing about service permissions — while limiting the claim to scripted prompt scenarios rather than all prompt injection or OAuth production systems.

### High-Performance Data Tooling — Rust Arrow/DataFusion Snapshot Diff

- Diffed two primary-key-ordered snapshots of 100M rows each with a streaming merge that never loads a full table, over five projected columns - int64 key, float64 value, timestamp64 and two int32, 32 bytes logical width - in a 22-second core-stage median and 1.1 GiB peak RSS across five warm-file-cache runs on Linux x86_64, 16 vCPU/64 GiB, single local NVMe; the Python baseline took a 540-second median at 6.0 GiB on the same projection.

- Reported throughput honestly as about 9.09 million input rows/s, not output diffs/s, at a fixed 0.1% difference rate, with core output written to a bounded local sink and persistent publication accounted separately; cold-cache results are listed separately and do not reuse the 22 seconds.

- Cut the end-to-end simulated pipeline from 52 to 14 minutes on one full run trajectory per side (baseline preparation 26 min + core 9 min + audit/publish 17 min; candidate preparation 8.5 min + core 22 s + audit/publish 308 s), and scoped it correctly: because preparation changed, this is a whole-pipeline rework, not a merge-algorithm result.

- Made numerical decisions tolerance-aware rather than hash-based: keys are compared exactly, float values use abs(a-b) <= 1e-8 + 1e-6*max(abs(a),abs(b)), NULL equals only NULL, NaN compares by an explicit policy, same-sign infinities and positive/negative zero are equal; xxHash is used only for locating and diagnostics and never bypasses the per-column comparison.

- Treated duplicate primary keys as input errors, used 64K-row RecordBatches with a bounded queue, and ran DataFusion scheduling and outer concurrency under one shared thread budget to avoid nested thread-pool oversubscription; a small-scale independent Python oracle with random differential plus NULL, NaN, infinity and tolerance-boundary cases reconciled added, deleted and changed key sets.

## 快速运行

需要 Python 3.11、JDK 17、uv；首次安装需要下载 Python/JVM 依赖。Linux/macOS 单机可使用
本地 Hadoop Catalog + SQLite 审计，仍运行真实 Spark 和 Iceberg，适合快速复现。

```bash
cd LakeBridge
export JAVA_HOME=/path/to/jdk17
make install
make test
make integration

# 每次完整 demo 使用新的 root 和 namespace；避免覆盖已有演示数据。
.venv/bin/lakebridge --root artifacts/my_demo --namespace my_demo demo --rows 200 --days 14
.venv/bin/lakebridge --root artifacts/my_demo --namespace my_demo query --limit 5
```

设置 `JAVA_HOME` 为本机实际安装的 JDK 17 路径。
`make install` 使用 `uv.lock` 冻结依赖，并校验 JVM jar 的仓库内固定 SHA-256。
项目不能写入原始数据路径与仓库路径相互包含的目录。

## PostgreSQL + MinIO 完整环境

```bash
mkdir -p artifacts
docker compose up -d postgres minio minio-init
docker compose build spark
docker compose run --rm spark --root /workspace/artifacts/compose compatibility
docker compose run --rm spark --root /workspace/artifacts/compose demo --rows 200 --days 14
docker compose run --rm spark --root /workspace/artifacts/compose query --limit 5
```

新 checkout 不包含生成数据；完成 demo 后，结果保存在 `artifacts/compose`，再执行查询命令。
重复 demo 需要独立的 catalog/audit 数据集，不能只更换输出目录重用同一审计库。
每个审计 schema 绑定一个 namespace/warehouse，避免读写串库。

| 服务 | 本机地址 | 开发凭据 |
|---|---|---|
| PostgreSQL | `127.0.0.1:55439` | catalog: `lake_catalog/catalog_local`；审计: `lake_audit/audit_local` |
| MinIO API / Console | `127.0.0.1:59000` / `http://localhost:59001` | 管理员 `minio_admin/minio_admin_local` |
| Airflow（可选） | `http://localhost:58080` | `admin/admin_local` |

开发凭据只用于本地实验。Compose 端口只绑定回环地址；catalog 和 audit 使用不同 schema/角色，
湖表 writer 仅有 warehouse bucket 权限，不能删除 raw bucket 文件。
端口冲突时设置 `LAKEBRIDGE_POSTGRES_PORT`。停止服务并保留数据：`docker compose --profile airflow down`。
数据卷清理、云资源退出步骤见 [基础设施说明](infra/README.md)。

MinIO 仓库已归档、Airflow 2.x 已结束支持；上述配置保留为历史本地实验，不能作为生产部署推荐。
此次执行锁协议更新需停止旧作业后统一切换，不支持旧镜像与新代码混跑；见 [版本说明](docs/versions.md)。

## 分步骤迁移和补数

```bash
.venv/bin/lakebridge generate --output artifacts/source --rows 3000 --days 90
.venv/bin/lakebridge verify-manifest artifacts/source/manifest.json --watermark 1
.venv/bin/lakebridge --root artifacts/local migrate \
  --manifest artifacts/source/manifest.json --watermark 1 --rule v1 \
  --run-id initial_h0 --reason '固定 H0 全量迁移'

.venv/bin/lakebridge --root artifacts/local dry-run \
  --manifest artifacts/source/manifest.json --watermark 2 --rule v1 \
  --start 2025-01-01 --end 2025-03-31
.venv/bin/lakebridge --root artifacts/local backfill \
  --manifest artifacts/source/manifest.json --watermark 2 --rule v1 \
  --start 2025-01-01 --end 2025-03-31 --run-id late_h1 --reason '迟到修订追赶至 H1'
.venv/bin/lakebridge --root artifacts/local releases
.venv/bin/lakebridge --root artifacts/local query --release rel_initial_h0 --limit 5
```

生成器第 2 批包含分布在历史日期中的迟到修订；必须先覆盖这些修订才能把输入边界推进到 H1。
消费进度按到达批次推进。相同键的同版本冲突会隔离并阻断；较旧版本不能复活已删除事件。
第一次迁移要求全范围。详细说明见 [数据契约](contracts/README.md) 与 [补数手册](docs/backfill.md)。

## 故障、恢复与维护

```bash
# 注入“数据已提交但发布审计未写完”的故障；返回非零符合预期。
.venv/bin/lakebridge --root artifacts/local migrate \
  --manifest artifacts/source/manifest.json --watermark 3 --rule v1 \
  --run-id failure_demo --reason '审计间隙演练' --fail-at before_audit
.venv/bin/lakebridge --root artifacts/local recover failure_demo

# 只切消费者；写入表头保留，后续通过前向修正保住合法新增。
.venv/bin/lakebridge --root artifacts/local rollback rel_initial_h0 --reason '消费者回退演练'
.venv/bin/lakebridge --root artifacts/local maintain inspect
.venv/bin/lakebridge --root artifacts/local maintain orphans
.venv/bin/lakebridge --root artifacts/local maintain compact --execute
```

锁没有自动过期，失败运行必须 recover 或满足条件后 abort；不能让“租约时间到了”成为新写者
越过旧提交的依据。Airflow 用单作业涵盖建分支至发布的临界区。此控制只约束本项目写入者，
外部写入者必须实际停用；不声称 JDBC/应用锁已经实现存储层提交 fencing。

使用 `lakebridge --root ... --namespace ... status` 查看只读审计快照，
或 `metrics --output artifacts/metrics/lakebridge.prom` 导出监控文件。
采集失败返回非零并输出 `audit_up 0`；指标语义、采集新鲜度及恢复步骤见 [监控手册](docs/observability.md)。

## 编排与性能证据

```bash
export AIRFLOW_UID=$(id -u)
docker compose --profile airflow build airflow
docker compose --profile airflow run --rm airflow-init  # 首次初始化
docker compose --profile airflow up -d airflow

.venv/bin/lakebridge --root artifacts/performance --namespace performance \
  benchmark --rows 100000 --repetitions 3 --file-mib 128
```

Airflow DAG `lakebridge_history` 接受 manifest、watermark、rule、start/end、reason，
依次校验输入、生成 dry-run、写入/对账/发布、验证消费者。DAG `lakebridge_maintenance` 默认只输出计划。
已有 Compose 演示数据的继续处理可使用 `/workspace/artifacts/compose/source/manifest.json`，
HWM 3、规则 v1。首次独立编排数据集应配置独立审计 schema 与 namespace。

性能实验比较旧 Parquet、无分区 Iceberg、日期分区 Iceberg，以及同一表合并前后；每项至少 3 次。
运行产出 report.json、report.md、EXPLAIN 与实际 SQL metrics；Spark event logs 记录任务级 shuffle、spill、GC。
方法与限制见 [性能实验](benchmarks/README.md)，实际结果见 [验收记录](docs/acceptance.md)。

四项演示的真实终端录屏在 [demo.cast](docs/evidence/demo.cast)，安装 asciinema 后运行
`asciinema play docs/evidence/demo.cast` 回放；它由 `scripts/record_demo.py` 记录实际执行时序。

历史 Spark 3.5.6 / Iceberg 1.8.1 环境的本机多 Worker 模拟已实跑 19,773,338 条合成宽事件、压缩后 10.157 GiB，全部精确对账通过。
文件合并从 720 个文件降至 90 个，全量查询中位时间 9.715 → 8.407 秒。
复现命令见 [本地模拟](docs/local-simulation.md)，实测与费用假设见
[10 GiB 报告](docs/performance-local-10g.md)。模拟环境独立于原演示数据卷。

## 代码与文档

| 目录 | 内容 |
|---|---|
| src/lakebridge | 可安装的 CLI、Spark 作业、控制状态机、oracle、维护、实验 |
| source-adapters / contracts / examples | 生成器、S3 adapter、输入契约、真实样例 manifest/Parquet |
| spark-jobs / sql / validation | 作业入口说明、固定查询、独立验收 |
| orchestration / infra | Airflow DAG、Docker、Terraform S3/IAM 实验模块 |
| tests / .github/workflows | 单元与真实 Spark 集成测试、CI 证据上传 |
| benchmarks / docs | 方法、实测报告、迁移/补数/切换/恢复/保留手册 |

阅读顺序：[架构与迁移](docs/migration.md) → [补数](docs/backfill.md) →
[切换与回退](docs/cutover-rollback.md) → [保留与维护](docs/retention.md) →
[并发复盘](docs/concurrency-postmortem.md) → [验收](docs/acceptance.md)。

## Consumer Bridge

新增 [Consumer Bridge](docs/consumer-bridge.md)：比较固定发布版本，生成完整业务差异与
重算范围，经独立人工审批后，在 PostgreSQL/dbt 中发布 `tenant_daily_usage` 消费者版本。
API、单 worker 与 Spark 导出分别运行；审批绑定租户日期范围、方案 hash 和消费 generation。

`scripts/consumer_demo.py` 可复现从真实 Iceberg 快照到 PostgreSQL 日汇总的本地流程。
[Ops Agent](ops-agent/README.md) 提供 7 个 MCP 业务工具、LangGraph 持久调查、累计预算、
独立审批与任务恢复；[ReleaseQuery](release-query/README.md) 提供 Rust/Arrow/DataFusion
精确归并算子、CLI 和固定 Iceberg 原生快照读取。

组合演示已接通实际 stdio MCP、HTTP、Rust 原生扫描、PostgreSQL 和 dbt。
演示使用显式版本 ID 与脚本化独立人工审批；模型 HTTP 适配器通过确定性模拟端点验证，
未声称调用付费模型或部署 OAuth。SQL 保留为默认 backend。
[实施审计](docs/implementation-roadmap-2026-09-13.md) 汇总完整证据与性能测量。
