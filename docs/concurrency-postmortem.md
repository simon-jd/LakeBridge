# 并发冲突实验复盘

这是独立实验数据上的故障演练，不是生产经历。

## 场景一：补数期间日常写者进入

补数进程持有执行锁和持久 writer_run；另一连接使用不同 run_id 请求写入。
检测信号为 ConflictError，第二个写者不能进入临界区。第一个释放后，第二个可正常获得锁。
同 run_id 的并行重试也被阻断。SQLite 使用 flock；PostgreSQL 使用会话 advisory lock，
并叠加不自动过期的持久 writer_run，避免仅依赖超时租约让旧作业重新提交。

## 场景二：audit 和 main 分叉

compatibility 表在共同快照创建 conflict 分支；分支修改 value，main 写入合法值。
实际调用 fast_forward 被 Iceberg 拒绝，main 的合法值仍存在。
恢复动作是删除实验 conflict 分支，基于新 main 重建分支，再应用已审查的 annotation 修正。
最终 fast-forward 成功，合法 value 没有丢失。业务 pipeline 也在每张表提交边界检查初始 main
与 candidate head，并拒绝未解释的分叉。

## 场景三：提交成功但发布审计失败

在 after_detail_commit / before_audit 注入异常，旧活动 release 保持不变。
失败记录保存 run_id、manifest 摘要、Spark application_id、规则、范围、base snapshots、candidate。
同 run_id 恢复时复用原分支和已验收 candidate，跳过已经推进的头，仅补全剩余审计。
最终用独立 oracle 再验证业务数据并恢复可读性。实测耗时在 demo-report.json 中，属于本机恢复时间，
不是跨节点灾备 RTO。

限制：单表 Iceberg 提交、PostgreSQL 发布和对象存储不是同一个事务；清单读取协议保证消费者
不混用半完成的单表更新。外部写者、失联后仍运行的任意 JVM 没有存储级 fencing，不能声称无锁在线补数。

## 2026-09：PostgreSQL 控制行竞争

旧 SQLite 测试使用 BEGIN IMMEDIATE，隐含串行化了读状态与写状态；PostgreSQL 原实现只有
普通 SELECT，activate 可读到 writer_run 为空后暂停，另一个 writer 进入，再由 activate
基于旧状态切换消费者。长任务 advisory lock 只有 writer 获取，无法约束这条 activate 路径。

修复对 writer 准入、publish 与 activate 的短事务统一 SELECT FOR UPDATE，再校验 generation
和 writer_run。行锁不覆盖整个 Spark 作业；长任务仍持有独立执行锁和不会过期的持久所有权。
PostgreSQL 执行锁根据数据库内的 control 表 OID 区分审计 schema，避免不同数据集互相排队。
绑定身份不能仅由调用者传入的 run_id 决定。同一连接嵌套执行也被拒绝。

重复 release_id 只有不可变内容一致才被接纳；execution_seconds 和 queue_seconds 两项重试
遥测不参与身份比较。重复发布不会重新激活已被消费者回退的版本。
排队请求在获得执行锁后重新读取 run，已完成请求直接返回原 release，参数冲突仍被拒绝。

旧实现的负向基线为 4 failed / 8 passed，见 `docs/evidence/market-refresh/baseline.xml`；
修复后的完整结果见本轮 tests.xml。真实 PostgreSQL 测试覆盖：激活/准入竞争、两个独立 schema、
同一数据集互斥、并发代次校验、pg_terminate_backend 中断后保留所有权、监控的一致性快照。
强制终止的是 PostgreSQL 会话，不是网络分区、Spark executor 崩溃或生产宕机。

升级必须先停止旧作业再统一更换应用/镜像，旧常数锁与新 OID 锁不能混跑。
`--lock-timeout` 约束执行锁排队，不是全部 SQL/作业耗时上限。
