# 本机多 Worker 湖仓与成本模拟

本实验使用同一台机器上的 Docker 容器，实际执行 Spark 3.5.6、Iceberg 1.8.1、
PostgreSQL JDBC Catalog 和 MinIO。`SPARK_MASTER=spark://master:7077`，3 个独立 executor
分别运行在 worker1/2/3；它具有进程和资源隔离，但没有模拟跨物理节点网络、磁盘或可用区故障。
云端费用由本机观测值和明示参数计算，没有访问 AWS 账户或创建计费资源。

## 复现

先按主 README 安装 Python/JVM 依赖、构建 `lakebridge-spark:latest` 镜像。额外的 S3A jar
与 Spark 内嵌 Hadoop 3.3.4 匹配；下载后按 `infra/docker/simulation-jars.json` 校验 SHA-256。

```bash
.venv/bin/python scripts/bootstrap_simulation.py
mkdir -p artifacts/simulation/scratch
docker compose -f compose.simulation.yaml up -d postgres minio minio-init master worker1 worker2 worker3 s3proxy
.venv/bin/python scripts/run_simulation.py --run-id my_10g --target-gib 10
.venv/bin/python scripts/summarize_simulation.py artifacts/simulation/runs/my_10g
.venv/bin/python -m lakebridge.simulation_cost artifacts/simulation/runs/my_10g
.venv/bin/python scripts/archive_simulation.py artifacts/simulation/runs/my_10g --output benchmarks/results/my_10g
docker compose -f compose.simulation.yaml down
```

每次必须换一个 `run-id`，避免覆盖证据或重用表路径。`--target-gib` 接受 0.01–100，
正式执行前可用 0.02 冒烟测试。生成器至少生成 120,000 行，所以最小测试实际会大于所填容量。
50–100 GiB 会增加数据副本与 shuffle 空间，修改规模后需要重新量测，不能机械放大 10 GiB 结果。

Compose 项目名是 `lakebridge-simulation`，独立 PostgreSQL/MinIO 数据卷不与原演示环境混用。
`down` 停止容器并保留数据卷与证据。MinIO API 为 `127.0.0.1:59100`，Spark Master UI 为
`http://127.0.0.1:58180`。该实验使用独立的本地管理凭据，以便生成源数据和量测 S3A；
原业务演示的 raw/warehouse 分角色权限仍由原 Compose 提供。

## 固定资源

| 服务 | CPU 配额 | 容器内存上限 | Spark 参数 |
|---|---:|---:|---|
| 每个 Worker，共 3 个 | 4 | 16 GiB | 向 Master 声明可分配内存 14 GiB，executor heap 12 GiB，4 cores |
| Driver | 2 | 10 GiB | heap 8 GiB |
| Master | 1 | 1 GiB | daemon 512 MiB |
| MinIO | 3 | 6 GiB | 本机独立数据卷 |
| PostgreSQL | 1 | 2 GiB | 独立 catalog |
| S3 请求日志代理 | 2 | 1 GiB | 两个 Nginx worker，上游连接复用 |

主实验容器上限合计 21 个逻辑 CPU、68 GiB；实际占用由每 2 秒的 Docker cgroup 采样记录。
Spark 总 executor cores 为 12，shuffle partitions 为 96，AQE 开启。配额限制用量，
没有 CPU 绑核或独占物理磁盘；同机其他应用与 OS page cache 仍可能影响结果。

## 数据与比较方法

先生成 120,000 行校准压缩字节/行，再确定正式行数，并以源 Parquet 对象的真实字节数验收
至少 10 GiB。每行有唯一 event_id、30 天内的事件时间、60% 热点租户、精确小数 units，
以及 16 个由 ID 独立派生的 SHA-256 trace 字段。1024 字符宽列提供不可轻易压缩的负载。
这是合成宽事件的存储/性能专项，未将 10 GiB 全部套入 SaaS 套餐关联、迟到修订及发布 oracle。

源文件按日期目录写入 MinIO，两个独立 Iceberg 表分别无分区和 `days(event_time)` 分区，
全部使用 Zstandard，写入并行度 24，Iceberg 文件目标 128 MiB。
比较日期查询时，旧 Parquet 显式带 `business_date` 谓词以支持原有目录裁剪；
分区收益主要比较两个 Iceberg 表，不能将格式、目录分布和 SDK 差异全归因于 Iceberg。

文件合并比较同一个日期分区表：保留 `before_compaction` tag，执行 `rewrite_data_files`，
然后重跑完全相同的全扫描聚合。该查询对整条 trace_payload 求 xxhash64 聚合，确保实际读取宽列。
文件目标大小是参数，报告采用 `.files` 元数据的实际数量和字节，不把 128 MiB 当成实际结果。

每张迁移表和合并结果均检查行数、ID 范围、精确 units 和全语义列指纹，另用双向
`EXCEPT ALL` 检查所有列的精确多重集一致性。查询结果在所有重复和可比较变体之间必须相同。
每项查询预热一次、重复三次、报告中位数与所有样本；清 Spark cache，未清 JVM/OS/MinIO 缓存。
执行顺序固定，属于已预热的初步基线，不是 cold-cache 或多轮随机顺序实验。

## 观测与证据

`report.json`、`stages.json` 和各查询 JSON 保存时间、执行计划、SQL metrics、请求数量、
源大小与表统计。`runner.json` 另记容器启动到 JVM 结束的全实验时间，包含生成、两份迁移、
完整精确对账、预热、重复查询、维护和停止；不能把它称为一次生产迁移的耗时。

`resources.jsonl` 是每 2 秒的 cgroup CPU 累计值、内存、网络和块设备计数；
`resource-summary.json` 使用各容器首尾观测差值。启动/退出尾部和亚秒峰值可能漏采，
这些 CPU 秒不代替云实例分配时长。容器网络流量相加会重复计算内部通信。
`event-summary.json` 汇总真实 executor、task attempts、shuffle、spill 和 GC；
SQL 扫描文件字节、task input bytes、HTTP 响应字节属于不同口径。

所有 S3A 与 Iceberg S3FileIO 都经过相同 Nginx 代理。逐请求 JSON 日志无缓冲落盘，
记录方法、URI、状态、字节与耗时，阶段边界读取新增行。`s3-summary.json` 还计入阶段间调用，
因此与阶段计数求和略有差异。正常的对象存在性检查会返回 HEAD 404；单列错误计数，不重复收费。
代理与 MinIO 本身的开销也是本机实验的一部分，不能当成 AWS S3 的延迟或请求账单。

## 成本口径

费率和业务假设在 [cloud-cost-assumptions.json](../benchmarks/cloud-cost-assumptions.json)，
可修改参数后仅重算估算；若改资源、数据分布或查询，需要重新实跑。
按 4 台 m6i.xlarge 的参考价格 $0.192/台时映射三个 Worker 和一台 Driver/Master/自管 PostgreSQL，
云端 S3 替代本机 MinIO。该价格来自 [AWS 官方参考表](https://docs.aws.amazon.com/prescriptive-guidance/latest/optimize-costs-microsoft-workloads/right-size-selection.html)，
不表示已取得账户报价或确认本机与该实例性能相等。

S3 Standard 假设 $0.023/GiB 月、GET/HEAD $0.0004/千次、LIST $0.005/千次，
参考 [AWS 官方实验说明](https://aws.amazon.com/blogs/storage/run-spark-31-faster-and-optimize-compute-costs-with-amazon-s3-express-one-zone-on-amazon-emr/)。
PUT/POST 假设 $0.005/千次，参考 [AWS S3 存储示例](https://aws.amazon.com/blogs/database/automate-postgresql-audit-log-extraction-and-analysis-with-amazon-s3/)。
S3 的 GB 按二进制字节计算，见 [S3 计价说明](https://aws.amazon.com/s3/pricing/)。
四块 100 GiB gp3 持续保留，按 [EBS 官方示例](https://aws.amazon.com/ebs/pricing/) 的
$0.08/GiB 月假设计费；没有把本机 NVMe 性能映射成 gp3 实测性能。

主要公式：

```text
计算费用 = 分配给整个集群的秒数 / 3600 × 4 × 每台小时费率
请求费用 = (GET + HEAD) / 1000 × 读费率 + (PUT + POST + LIST) / 1000 × 写/列表费率
存储费用 = 实际数据字节 / 2^30 × 保留天数 / 30 × 存储月费率
```

敏感性参数让云端耗时为本机的 0.5/1/2 倍，它们是情景值，不是预测置信区间。
月度场景使用固定数据集，每天 100 次全扫描、一次批次、一次预热，启动预算 600 秒/批次。
首月计入一次分区表加载及精确对账；优化方案另计月初的一次合并及对账。
源文件保留，旧 tag 保留整月，因此优化后要同时计入源、当前文件和旧文件，不能只报压缩收益。
整月常开场景对计算资源收取完整 720 小时，查询提速不会自动减少这部分费用。

这是计算、EBS、S3 数据文件与观测请求的费用小计。元数据小对象、监控、备份、NAT、
跨 AZ 流量、公网 IPv4、出网、Airflow、托管 RDS/EMR、税费及本机电费折旧不包含在内。
批次场景的自管 catalog 在停机时不可服务；该预算不代表高可用、全天在线的生产部署。
