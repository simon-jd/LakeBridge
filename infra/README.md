# 本地基础设施与云端实验边界

Compose 包括 PostgreSQL 17.6、MinIO、一次性 bucket/权限初始化、Spark job 容器及可选 Airflow 2.10.5。
`spark` 采用 Python 3.11 + Java 17；依赖从 requirements.lock 安装并验证 hash，JVM jar SHA-256 固定在源码中。
Spark 的 event logs、run 审计证据和输入 staging 共享 `./artifacts`。仅在本机回环端口提供服务。

PostgreSQL catalog 与 audit 由不同角色拥有，角色 search_path 指向各自 schema。每个审计 schema
对应一个业务数据集，持久化 namespace/warehouse 绑定；多个独立实验需要另建数据库或 schema/角色，
不能让两个不相干 namespace 共用 active_release。Airflow metadata 使用独立数据库和角色。

宿主机使用 Compose catalog 的示例配置（开发环境）：

```bash
export LAKEBRIDGE_CATALOG_URI='jdbc:postgresql://127.0.0.1:55439/lakebridge?currentSchema=catalog'
export LAKEBRIDGE_CATALOG_USER=lake_catalog
export LAKEBRIDGE_CATALOG_PASSWORD=catalog_local
export LAKEBRIDGE_AUDIT_DSN='postgresql://lake_audit:audit_local@127.0.0.1:55439/lakebridge'
export LAKEBRIDGE_WAREHOUSE='s3://lakebridge-warehouse/warehouse'
export LAKEBRIDGE_S3_ENDPOINT='http://127.0.0.1:59000'
export AWS_ACCESS_KEY_ID=lake_writer
export AWS_SECRET_ACCESS_KEY=lake_writer_local
export AWS_REGION=us-east-1
```

容器内 manifest 的绝对路径与宿主机不同；跨运行环境恢复要保持相同挂载路径。
不要在宿主机随意恢复引用 `/workspace/...` 的容器运行。优先用相同 `docker compose run spark` 命令。

## 备份与退出

先让作业结束并停止写入，再分别备份 catalog 与 audit schema、数据库角色配置和 warehouse 对象。
对象 versioning 有助于恢复误删，但不是独立异地备份，也不代替 catalog。

```bash
docker compose exec -T postgres pg_dump -U postgres -d lakebridge --schema=catalog > artifacts/catalog.sql
docker compose exec -T postgres pg_dump -U postgres -d lakebridge --schema=audit > artifacts/audit.sql
docker compose --profile airflow down
```

确认仅为可销毁实验且证据已归档后，可执行 `docker compose --profile airflow down --volumes` 删除
该 Compose 项目数据卷；会永久丢失本地 catalog 与对象数据。默认 Makefile 停止命令不会删卷。
不操作机器上的其他项目、镜像或数据卷。

## Terraform

`terraform/` 提供 AWS 原始 bucket、warehouse bucket、版本控制、加密、阻止公开访问及最小 writer
IAM policy。没有 apply 到云端，没有创建付费 Spark/数据库集群；云端计算、VPC、catalog 数据库和
调度器必须按实验预算单独配置。这里的 `force_destroy=false` 防止销毁资源时顺带清空历史数据。

```bash
cd infra/terraform
terraform init
terraform validate
terraform plan -var 'name=你的唯一实验前缀'
```

只在明确预算与账户后 apply。退出实验先归档并按批准的保留期清理对象所有版本，确认空桶，再
terraform destroy；versioning 的旧版本也占空间。未配置按年龄删除 warehouse data 的生命周期。

参考：[Airflow Docker 文档](https://airflow.apache.org/docs/apache-airflow/2.10.5/howto/docker-compose/index.html)、
[Iceberg S3 FileIO](https://iceberg.apache.org/docs/1.8.1/aws/)。
