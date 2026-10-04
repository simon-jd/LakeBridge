# ReleaseQuery

Rust/Arrow/DataFusion component for exact row changes between two fixed LakeBridge
release pins. `releasequery-core` supplies `ReleaseDiffExec`; the LakeBridge crate
adapts immutable exports or a native Iceberg snapshot provider; the CLI publishes
complete Parquet artifacts. It never approves jobs or updates consumer pointers.

The separate `numeric-diff` command compares an ordered int64 key plus float64,
timestamp64 and two int32 fields, with explicit tolerance and NaN rules. See the
[numeric contract and reproduction commands](../docs/releasequery-numeric-contract.md)
and [current local verification](../docs/github-alignment-validation.md). Its
profile is selected explicitly; the pinned-release `diff` protocol stays exact.

The tested dependency set is Rust 1.94.0, DataFusion 53.1.0, Arrow/Parquet 58.4.0,
Iceberg Rust 0.10.0 and iceberg-datafusion 0.10.1. `Cargo.lock` pins transitive
versions. Parquet 58.0.0 did not compile with the selected Iceberg writer APIs;
58.4.0 is the actually compiled combination.

Run from the repository root with Rust 1.94 installed:

```bash
cargo build --manifest-path release-query/Cargo.toml --release --locked
release-query/target/release/release-query inspect release-query/fixtures/export-request.json
release-query/target/release/release-query explain release-query/fixtures/export-request.json
release-query/target/release/release-query diff release-query/fixtures/export-request.json \
  --output artifacts/my-release-diff
cargo test --manifest-path release-query/Cargo.toml --locked
LAKEBRIDGE_RELEASE_QUERY=release-query/target/release/release-query \
  .venv/bin/pytest -q tests/test_releasequery.py
```

This workspace's optional local toolchain uses
`CARGO_HOME=$PWD/.runtime/cargo RUSTUP_HOME=$PWD/.runtime/rustup` and
`.runtime/cargo/bin/cargo`; it does not change the user's PATH or Rust installation.
Use a fresh output directory. Relative descriptor paths are resolved from the
process working directory. `inspect` and `explain` validate descriptors and build
the plan; they do not claim to have completed the data scan.

The request wrapper contains the shared `request`, `left_source`, `right_source`
and optional `options`. Only the shared request participates in `request_hash`;
a separate `source_hash` binds physical descriptors. Snapshot IDs remain decimal
strings through both languages. The included fixture reads the two checked-in
[real snapshot exports](../examples/consumer/pinned-exports).

Source kinds:

- `parquet_export`: registered complete manifest path/hash; validates exact pin,
  Parquet schema, file hashes and row counts before scanning.
- `iceberg_native`: fixed metadata path/hash, full pin and explicit
  `lakebridge.spark-utc-us.v1` adapter. Uses
  `IcebergStaticTableProvider::try_new_from_table_snapshot`, with no current lookup
  or hand-listed Parquet substitute. Local format-v2 tables with the known
  LakeBridge field IDs/types and one schema are supported. Delete files, schema
  evolution, name mappings, absent field IDs and unsupported types are rejected.
  The export adapter remains available for explicitly normalized snapshots.

Native scans honor Iceberg manifests and field IDs. The adapter checks both
snapshot delete counters and planned task delete lists, then verifies physical
Parquet field IDs. Native delete application is deliberately outside this v1
adapter's verified surface; rejection fixtures do not claim delete-file execution.

```bash
.venv/bin/python scripts/releasequery_demo.py --root artifacts/rq-demo-new \
  --source-root artifacts/consumer-demo-20260913
lakebridge-consumer --registry artifacts/consumer-demo-20260913/exports \
  --artifacts artifacts/consumer/diffs --diff-backend rust \
  --native-sources artifacts/rq-demo-new/native-sources.json \
  compare-worker COMPARISON_ID
```

The service's native descriptor mapping and executable are operator configuration;
Agent inputs cannot supply either. `RustDiffBackend` invokes a fixed argument list,
checks exit status and validates the complete artifact independently. SQL remains
the default; either backend produces the same consumer protocol.

See [execution semantics and resource accounting](docs/execution.md) and the
[benchmark procedure](docs/benchmarks.md). The full combined demonstration accepts
`--diff-backend rust --native-sources ...` in `scripts/ops_agent_demo.py`.
