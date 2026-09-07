# 快照保留、文件维护与恢复边界

常规实验快照默认保留最近 7 天且至少 5 个；每次发布创建同名 tag，保护三表快照。
发布 tag 没有自动到期，当前版本所有发布都保留，故存储增长是预期成本。暂不自动退役发布，
需要人工核验消费者、回退窗口与备份后再设计 release/tag 退役流程。

audit branch 也保留用于故障证据。因此快照过期未必释放很多文件；不能把执行过维护等同于省钱。
原始输入、incoming staging、oracle、错误差异、事件日志与 Iceberg 各有独立保留需求，
`maintain` 不删除这些外部证据。已结束实验可在归档后按独立实验目录清理。

所有修改性维护与业务写者共用同一锁。维护前逐个检查 release 的 tag 与 snapshot_id 一致。
`maintain inspect` 输出文件数/压缩字节/大小分布/快照数。
`compact`/`expire` 默认输出计划，显式 `--execute` 才调用真实程序。

`orphans` 默认调用 dry_run，候选清单写入 maintenance/。候选年龄取
`max(72小时, 最长作业时长×2+24小时)`，覆盖在途写入与安全余量。
S3 使用 Iceberg S3FileIO 的 listPrefix 分页迭代，按 1000 项写本地清单，作为 file_list_view
交给 remove_orphan_files；避免默认 Hadoop 目录枚举缺少 s3 文件系统的问题。
显式 execute 时先保存候选清单，再在同一受控写入锁内重跑删除程序；必须实际排除外部写者。

安全边界：应用锁约束不了持有对象存储凭据的外部进程。目录权限、独立角色及停写协议是前提。
仍引用的数据被误删后，光有 snapshot_id 无法恢复；必须恢复对应对象版本/备份和 catalog 元数据。
原始 bucket 不授予湖表 writer 删除权限，warehouse 不配置按对象年龄盲目清理的数据生命周期。

保留 tag 保护快照可读性，但如果中间祖先快照已过期，`rollback_to_snapshot` 的祖先链校验可能
失败。兼容性实验先验证完整祖先链上的 rollback，再强制过期未保护快照，验证 tag 仍可读，
并在已隔离的实验表上用 set_current_snapshot 重选该版本。业务消费者回退只使用数值 snapshot_id，
不依赖这条祖先链；不可把实验中的表头重选直接用于有合法新增的业务表。

参考：[Iceberg Maintenance](https://iceberg.apache.org/docs/1.8.1/maintenance/)、
[Spark Procedures](https://iceberg.apache.org/docs/1.8.1/spark-procedures/)。
