# Source adapters

第一版只使用 SaaS usage/plan 契约，独立生成器在 `src/lakebridge/generate.py`。
`lakebridge generate` 生成 Parquet、累计 manifest、实际分布统计及独立 DuckDB oracle。
同一个 seed 和参数生成完全相同的文件内容；分块生成原始数据，包含季节性、热点、乱序修订和删除。

S3 adapter 实现在 `src/lakebridge/adapters.py`，入口：

```bash
lakebridge stage-s3 s3://raw-bucket/batch-set/manifest.json \
  --sha256 <事先记录的manifest摘要> --output artifacts/staged-input
lakebridge verify-manifest artifacts/staged-input/manifest.json
```

AWS 凭据通过环境/运行角色传入；MinIO 另外设置 `LAKEBRIDGE_S3_ENDPOINT=http://localhost:59000`。
原始文件只下载不删除；worker 使用 staging 中已校验的文件集合，不能直接发现并消费新文件。
LedgerFlow 的具体外部字段和路径不在方向文档中，因此不猜测其接口；实际接入时将字段映射到
上述契约即可。AdPulse 不在第一版范围内。

