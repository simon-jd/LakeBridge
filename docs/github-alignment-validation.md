# Local validation: 2026-10-04

This checkout preserves the owner's independent cloud qualification in
[experience-bank-results.json](experience-bank-results.json). The following
local checks verify the implementation; they do not rerun the cloud performance
numbers or replace the historical evidence in other folders.

| Check | Result | Scope |
|---|---|---|
| Python unit selection, with compiled Rust CLI | 82 passed, 2 skipped, 25 deselected | `pytest -m 'not integration and not postgres' -q -ra` |
| PostgreSQL 16.8 | 13 passed, 3 skipped, 93 deselected | Real temporary database; `pytest -m postgres -q -ra` |
| Ops Agent | 10 passed | Durable agent and typed-tool regressions |
| Rust workspace | 11 passed | Merge correctness, resource limits, schema/key failures and numeric policy |
| Python/Rust bridge | 13 passed, 1 skipped | Subset of the 82 Python passes above, not additional tests |
| Independent numeric oracle | 43 cases passed | 40 seeded random/NaN cases plus boundary, no-overwrite and key rejection cases |
| Rust build, rustfmt, Clippy and oracle Ruff | Passed | Locked dependencies; Clippy denies warnings |

The Rust workspace used toolchain 1.94.0 and locked Arrow/Parquet 58.4.0 and
DataFusion 53.1.0. Python checks used Python 3.12. The compiled CLI lived in an
isolated temporary Cargo target directory; the reproduction commands below use
the normal checkout path. Raw results and exact invocation details are retained
in [the evidence folder](evidence/github-alignment-20261004/verification.json).

```bash
cargo fmt --manifest-path release-query/Cargo.toml --all --check
cargo clippy --manifest-path release-query/Cargo.toml --locked --workspace \
  --all-targets -- -D warnings
cargo test --manifest-path release-query/Cargo.toml --locked --workspace
cargo build --manifest-path release-query/Cargo.toml --locked --bin release-query
LAKEBRIDGE_RELEASE_QUERY=release-query/target/debug/release-query \
  .venv/bin/python -m pytest -m 'not integration and not postgres' -q -ra
.venv/bin/python release-query/tests/numeric_oracle.py \
  --binary release-query/target/debug/release-query \
  --output artifacts/numeric-oracle.json
ops-agent/.venv/bin/python -m pytest -q ops-agent/tests
# Set LAKEBRIDGE_TEST_POSTGRES_DSN to an isolated test database first.
.venv/bin/python -m pytest -q -m postgres -ra
```

The two unit-selection skips are a PostgreSQL-dependent API case (covered in
the separate PostgreSQL selection) and a native Iceberg request requiring a
generated demo artifact. Three PostgreSQL cases require the optional dbt
environment and `LAKEBRIDGE_TEST_DBT`; they were skipped. Existing September
Spark/MinIO/native-provider evidence remains historical. This round did not
repeat the full external stack, 100M-row cloud benchmark or cloud RSS sampling.

The new [numeric profile](releasequery-numeric-contract.md) is implemented by
`ReleaseDiffExec::try_new_numeric`, the `numeric-diff` CLI and a separate Python
oracle. It preserves exact key comparisons, explicit NaN policy, pairwise
tolerance comparisons, batch-boundary duplicate rejection, bounded pull-based
merge state, atomic non-overwriting file publication, and one shared Tokio
executor budget. The legacy `diff REQUEST --output DIRECTORY` bridge continues
to use exact business-field comparison and its existing artifact protocol.
