# Backend benchmark

Run an optimized binary and a new output directory:

```bash
cargo build --release --locked --manifest-path release-query/Cargo.toml
.venv/bin/python scripts/releasequery_benchmark.py \
  --root artifacts/rq-benchmark-new --rows 50000 --repeats 3
```

The 2×2×2 matrix varies sorted/unsorted inputs, 1%/50% requested updates and
16/512-byte plan strings, with two tenants and a fixed scope. Each cell runs both
backends in alternating order in fresh processes. Fixed seeds reproduce the
inputs; fixture construction is excluded. Local filesystem caches are left warm,
and all repeats are retained. No production or distributed scale claim is implied.

`backend_ns` measures the configured backend call; the Rust bridge includes CLI
startup, source verification and full artifact validation. `process_wall_ns`
additionally includes Python startup/imports and the common parity validation.
Every full row stream is hashed independently after computation and required to
match the SQL reference. RSS is a 5 ms sampled sum of the worker process and its
children, separate from DataFusion reservations. It can miss short peaks and is
not a hard upper bound. Raw artifact phase metrics and per-repeat results remain
available for inspection.

The output report records medians, individual measurements, counts, provenance
and binary SHA-256. The measured result and default-backend decision are recorded below. SQL stays available
regardless of the result; this project has no required speedup target.

## Measured local result (2026-09-13)

50,000 source rows per side, two tenants, three repeats per cell; all 48 complete
outputs matched the SQL reference. Medians below are configured backend calls,
including Rust CLI/provenance and its independent full-artifact validation.

| Input | Updates | String bytes | SQL ms | Rust bridge ms |
| --- | ---: | ---: | ---: | ---: |
| sorted | 1% | 16 | 61.3 | 99.1 |
| sorted | 1% | 512 | 123.9 | 161.4 |
| sorted | 50% | 16 | 97.2 | 1541.8 |
| sorted | 50% | 512 | 198.8 | 1725.5 |
| unsorted | 1% | 16 | 63.3 | 99.8 |
| unsorted | 1% | 512 | 124.3 | 198.4 |
| unsorted | 50% | 16 | 101.0 | 1581.8 |
| unsorted | 50% | 512 | 195.1 | 1769.7 |

SQL remains the default in all cases. The Rust CLI internal task was faster in
this sample, while Python artifact-validation and integration costs removed the
advantage. The ratios above describe the current adapter, not the merge algorithm.
Fresh-process total medians include common validation for both backends; see the
raw report for those costs, scan/sort/merge/write metrics and sampled RSS.

A separate resource probe on the wide unsorted case failed cleanly at 32/48 MiB
pool budgets without a complete artifact. At 64 MiB it completed with six recorded
DataFusion sort spills and full SQL parity. Pool budgets are not RSS limits.

Saved [report](../../docs/evidence/releasequery/benchmark.json),
[raw observations](../../docs/evidence/releasequery/benchmark-raw.json) and
[resource probe](../../docs/evidence/releasequery/resource-probe.json).
