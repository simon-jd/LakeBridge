# 开发验收记录 · 2026-09-06

本记录区分已经执行的验证与未开展的规模实验。所有数据为独立生成的实验数据；没有把实验故障
写成生产事故，也没有填写未经测量的云成本收益。

## 已执行

| 验收项 | 实际结果 | 证据 |
|---|---|---|
| 自动化单元 + Spark 集成测试 | 新增模拟计价测试后 49 passed，35.18 秒 | [测试日志](evidence/simulation-regression.log)、[JUnit](evidence/simulation-regression.xml) |
| 本地真实 Spark/Iceberg 四项业务闭环 | 100 条基础事件；迁移、迟到补数、并发阻断、回退后前向修正通过 | [本地报告](evidence/local-demo.json) |
| PostgreSQL JDBC Catalog + MinIO | 相同四项闭环通过，持久审计与 S3 FileIO 实际运行 | [Compose 报告](evidence/compose-demo.json) |
| JVM/Iceberg 兼容性 | MERGE、branch、tag、版本读取、schema/分区演进、维护、回滚、分叉恢复 | [兼容性结果](evidence/compose-compatibility.json) |
| 契约及质量阻断 | 摘要/缺文件/schema、空键/负值、同版本冲突、时间移动、维度重叠、范围外遗漏和错误汇总被阻断 | [测试](../tests/) |
| 幂等与恢复 | 原 run_id 不重复发布；detail 提交后失败及 before_audit 失败都安全恢复 | [录制演示结果](evidence/recorded-demo.json) |
| Airflow | DAG 无导入错误；verify → dry-run → 写入/验收/发布 → query 实际执行成功 | [DAG 日志](evidence/airflow-e2e.log)、[导入检查](evidence/airflow-import-check.log) |
| S3 原始输入 adapter | 按固定 manifest SHA-256 下载并验证 12 份明确文件 | [S3 adapter](evidence/s3-adapter.json) |
| 发布引用下的 S3 维护 | orphan dry-run 候选 0；合并后仍按原 release 读取 | [孤儿候选](evidence/s3-orphan-plan.log)、[合并](evidence/s3-compaction.log)、[查询](evidence/compose-final-query.log) |
| Catalog/控制服务中断 | 新控制连接失败；服务重启后发布数据重新可读，总实验时间约 5.90 秒 | [故障报告](evidence/catalog-drill.json) |
| Terraform | fmt、init -backend=false、validate 通过；没有 apply | [验证日志](evidence/terraform-validation.log) |
| 工程检查 | Ruff lint/format 通过；Compose config 通过；Spark/Airflow 镜像构建成功 | Makefile、compose.yaml、Dockerfile |
| 可回放终端演示 | 实际录制四项业务场景，没有拼接成虚假输出 | [asciicast](evidence/demo.cast) |

业务演示的 base events 为 100，batch 2 带 5 个键的修订、精确重复和乱序旧版本，batch 3 有 2 条
合法新增。最终 detail 有 102 个业务键，其中 1 条 tombstone、101 条活跃记录；合法新增在回退后
前向修正中保留。明细、套餐和汇总的快照集合见 [发布记录样例](evidence/sample-release.json)。

Catalog 演练暂停的是本项目同一主机上的 PostgreSQL 服务，验证服务不可用时的连接失败与恢复读取。
没有将其报告为跨节点灾备 RTO，也没有声称已经测试运行中每一个 catalog 提交中断点。

## 性能实验

正式实验 ID `bench_b93f24c96e`，100,000 条固定记录；源 Parquet 和各 Iceberg 变体统一 Zstandard。
其他本项目 Spark 验收作业结束后单独运行，local[2]、driver 2 GB、shuffle 4、AQE 开启。
每个查询预热 1 次后量测 3 次，Spark cache 清空；OS/JVM 已预热，执行顺序固定。

| 实验 | 基线 | 改动后 | 解释 |
|---|---:|---:|---|
| 日期谓词匹配文件总字节 | 3,880,819 | 290,836 | 证明分区裁剪；不是网络传输字节 |
| 日期查询中位数 | 0.050287 秒 | 0.043552 秒 | 仅 3 次小样本，不外推稳定大规模收益 |
| 小文件数量 | 224 | 14 | 同一表真实 rewrite_data_files |
| 全扫描聚合中位数 | 0.170750 秒 | 0.052201 秒 | 本次约下降 69.4%；合并另花 0.490650 秒 |

两份独立实验报告：[日期分区](performance-partition.md)、[文件合并](performance-compaction.md)。
[原始样本与 SQL metrics](../benchmarks/results/local-100k/report.json)、
[查询计划与汇总](../benchmarks/results/local-100k/report.md)、
[完整压缩事件日志](../benchmarks/results/local-100k/spark-event-log.json.gz) 均已保存。
10 万条是性能微实验数据，不能写成已经迁移 10 万条 SaaS 业务历史，更不是 10 GB。

新增本机三 Worker 模拟 `sim_10g_20260906`：19,773,338 条合成宽事件，实际压缩源大小
10.157 GiB，587.108 秒完成生成、两个目标表、全量双向对账、文件合并及重复查询。
文件数 720 → 90，全量查询中位耗时 9.715 → 8.407 秒；3,812 个 task attempts，失败 0。
按明示的四节点、30 天批次运行假设，1 倍云运行时间的费用小计 $45.24 → $42.39/月；
它是情景估算，没有真实云账单。完整方法、资源占用与限制见 [10 GiB 报告](performance-local-10g.md)。

## 明确未执行或存在的边界

- 已执行本机三个容器 Worker 的 10.157 GiB 存储/性能专项；未执行 50–100 GiB、10 GiB SaaS 全业务闭环、跨物理节点或真实云端计费实验。Terraform 未部署云集群。
- 未执行 Driver/Executor OOM、长时间任务与外部清理器竞态等资源极限演练。
- 全范围源扫描、全量汇总、数据集级串行写入会限制吞吐；已明确记录该取舍，不宣称无影响在线重算。
- 应用锁不等于存储提交 fencing。接入真实旧任务前必须实际停写或隔离所有外部写者。
- Manifest 的文件条目驻 driver，大规模应扩展分页/分片；S3 staging 和源只读快照需要相同路径与权限管理。
- 发布 tags、audit branches 和中间证据尚无自动退役；成本报告必须计入长期保留。
- GitHub Actions 工作流已编写，本机运行了对应检查；该轮验收时尚无 Git 仓库元数据；后续提交与远端状态以 Git 记录为准。

## 复现与定位

首次使用按 [README](../README.md) 安装，运行 `make test`、`make integration`、`lakebridge demo`。
容器复现使用 `docker compose run --rm spark`，复用 catalog 时先查询现有 release，避免用新 root
冒充一个全新的审计数据集。`artifacts/` 是可再生成的原始运行目录，关键证据已另存入本仓库 docs/evidence
与 benchmarks/results；运行输出不会被假装成尚未执行的生产指标。

终端录屏回放：`asciinema play docs/evidence/demo.cast`。

## 2026-09 后续工程更新

本次发布控制、监控与维护版本的验证独立记录在
[更新验收](market-refresh-validation.md)：64 项测试通过，S3/JDBC 18 项兼容性操作和四项业务演示通过。
旧报告中的版本、测试数量和 10 GiB 性能数据仍为历史结果，不随此次升级改写。
