# 审计状态与监控

`status` 与 `metrics` 只读取审计数据库，不导入 PySpark、不启动 JVM、不扫描 Parquet。
PostgreSQL 使用只读、REPEATABLE READ 事务；SQLite 使用只读打开及读事务。
配置的 namespace/warehouse 必须与现有审计绑定一致。缺库、缺表、连接错误或串库配置会失败，
不会自动创建一个看起来健康的空库。`releases` 也使用只读连接。

```bash
.venv/bin/lakebridge --root artifacts/my_demo --namespace my_demo status
.venv/bin/lakebridge --root artifacts/my_demo --namespace my_demo metrics
mkdir -p artifacts/metrics
.venv/bin/lakebridge --root artifacts/my_demo --namespace my_demo \
  metrics --output artifacts/metrics/lakebridge.prom
```

PostgreSQL 使用与作业相同的 `LAKEBRIDGE_AUDIT_DSN` / `LAKEBRIDGE_WAREHOUSE` 配置；
部署时可为采集器单独授予 schema USAGE 和审计表 SELECT，凭据通过环境提供。
应用层也强制只读事务。连接超时 5 秒，PostgreSQL 单条采集查询超时 5 秒；不把它说成整体 5 秒 SLA。
测试用真实 PostgreSQL 验证只读事务拒写；没有部署长期运行的只读监控账户。

## 指标解释

| 指标 | 意义 |
|---|---|
| `lakebridge_audit_up` | 当前配置审计库是否成功采样；不证明 Spark、对象存储或消费者健康 |
| `lakebridge_audit_observed_timestamp_seconds` | 本次采集时间，必须用于识别停止更新的文件 |
| `lakebridge_release_generation` / `lakebridge_releases` | 当前消费代次、累计不可变发布记录数 |
| `lakebridge_active_release_validated` | 当前发布是否携带通过的验证结果；不是本次重新扫描数据 |
| `lakebridge_last_publish_timestamp_seconds` | 最近审计发布的时间；不是上游数据新鲜度 |
| `lakebridge_writer_owned` | 是否保留 writer 所有权；可能是活跃任务或等待人工恢复的失败任务 |
| `lakebridge_writer_state_age_seconds` | 距最后状态更新的时间；不是心跳、运行时长或租约倒计时 |
| `lakebridge_writer_state_missing` | 存在所有权但没有相应 run 记录，可出现在准入刚完成或维护任务中 |
| `lakebridge_runs{state="…"}` | 固定 7 种状态加 OTHER；历史 FAILED 不等于当前需要报警 |

指标不带 run_id、release_id、错误详情或文件路径标签；这些诊断信息只保留在 JSON status 和审计中。
每个采集目标对应一个数据集；多数据集应配置不同的采集目标/静态 target labels，
不能把同名无标签指标的多个文件放到同一 collector 中。

文件写入采用目标目录临时文件 + flush/fsync + rename，文件权限为 0644。
目标目录需预先创建。读取失败时仍写入 `audit_up 0` 和新采集时间，并返回退出码 2；
写入失败同样非零，旧文件可能仍存在，因此不可省略采集时间报警。

## 接入与故障处置

可由用户管理的 timer/cron 每 30–60 秒调用一次，再让 node_exporter
`--collector.textfile.directory` 读取输出目录。本项目未安装系统服务或长期运行的 Prometheus。
仅做了文本格式校验，不能把它写成上线监控/值班经历。
官方接口依据：[node_exporter textfile collector](https://github.com/prometheus/node_exporter#textfile-collector)。

在对应的 scrape job 上配置 `lakebridge_audit_up == 0`，以及
`time() - lakebridge_audit_observed_timestamp_seconds > 180`；同时使用 `absent(...)`
检测从未产生过数据的目标。180 秒为示例阈值，不是已经测定的服务目标。
writer 长时间未更新先结合作业日志判断，不能因指标超时就清空 writer_run。

恢复步骤：保存 status → 确认旧 executor 已停止 → 检查固定输入与已提交 snapshots →
对原 run_id 执行 recover；符合未更改主分支的条件才可 abort。数据库连接丢失释放 session lock，
但持久所有权保留；这不提供外部 Spark JVM 的存储提交 fencing。
