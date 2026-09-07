# LakeBridge

**Auditable Parquet-to-Iceberg migration, recoverable release control, and measured local performance.**

[中文说明](README.zh-CN.md) · [Architecture](docs/migration.md) · [Verification](docs/market-refresh-validation.md) · [Benchmark](docs/performance-local-10g.md)

LakeBridge migrates immutable historical Parquet batches into Apache Iceberg, reconciles them against an independent DuckDB oracle, and publishes a release binding detail, plan, and daily aggregate tables to specific snapshots. Readers can switch back to a previous release while a later correction preserves legitimate data that arrived afterward.

This is a locally verified data-platform engineering project with synthetic data. It runs real Spark, Iceberg, PostgreSQL and Docker; MinIO provides a local S3-compatible endpoint.

## Engineering problems it addresses

- **Incomplete multi-table publication:** validate isolated Iceberg branches, retain snapshots, and publish one audited snapshot set for readers to resolve.
- **Late corrections and repeatable backfills:** pin manifests and watermarks; reject conflicting source versions, unsafe scopes, and reused run IDs with different parameters.
- **Failure between data commit and audit publication:** preserve the original run's ownership and candidate snapshots, then resume the unfinished steps.
- **Concurrent rollback and writers:** serialize control decisions with PostgreSQL row locks, enforce release generations, and scope execution locks to each audit table.
- **Invisible failed jobs:** expose read-only JSON status and bounded-cardinality Prometheus metrics without starting Spark; report failed collection as `audit_up 0`.

## Architecture

```mermaid
flowchart LR
  A[Immutable Parquet batches] --> B[Manifest, hashes, watermark]
  B --> C[PySpark version merge and backfill]
  C --> D[Iceberg audit branches]
  A --> E[Independent DuckDB oracle]
  D --> F[Exact reconciliation]
  E --> F
  F --> G[Protected snapshot set]
  G --> H[PostgreSQL release ledger]
  H --> I[Readers resolve one release]
  H --> J[Read-only status and metrics]
```

An Iceberg commit is atomic for one table. LakeBridge's release ledger is the publication boundary across tables: consumers must use its snapshot pointers. PostgreSQL and object storage are not one distributed transaction. See [publication and rollback](docs/cutover-rollback.md).

## Quick start

Requires **Linux/macOS, Python 3.11, JDK 17, and uv**. The default path uses local files, Hadoop Catalog and SQLite; it still runs real Spark and Iceberg. Initial installation downloads locked Python packages and checksum-verified JVM dependencies.

From a checkout of this repository:

```bash
cd LakeBridge
export JAVA_HOME=/path/to/jdk17
make install
make lint
make test
make integration

# Use a fresh root and namespace when repeating the full demo.
.venv/bin/lakebridge --root artifacts/demo --namespace demo demo --rows 200 --days 14
.venv/bin/lakebridge --root artifacts/demo --namespace demo query --limit 5
.venv/bin/lakebridge --root artifacts/demo --namespace demo status

mkdir -p artifacts/metrics
.venv/bin/lakebridge --root artifacts/demo --namespace demo \
  metrics --output artifacts/metrics/lakebridge.prom
```

The demo exercises migration, late-data catch-up, injected failure/recovery, and consumer rollback followed by forward correction. Reports and Spark event logs are written under the chosen root. Generated datasets, virtual environments and JARs are excluded from Git; a fresh checkout creates its own artifacts.

`make test` explicitly skips PostgreSQL cases unless the test DSN below is set. `make integration` enables real Spark tests. Python 3.12 is allowed by package metadata; archived qualification used Python 3.11.11.

## PostgreSQL and local S3 lab

```bash
docker compose up -d postgres minio minio-init
docker compose build spark
docker compose run --rm spark --root /workspace/artifacts/compose compatibility
docker compose run --rm spark --root /workspace/artifacts/compose demo --rows 200 --days 14
docker compose run --rm spark --root /workspace/artifacts/compose query --limit 5

# Tests create and remove isolated audit schemas in this local lab database.
LAKEBRIDGE_TEST_POSTGRES_DSN='postgresql://postgres:postgres_local@127.0.0.1:55439/postgres' \
  .venv/bin/pytest -m postgres -q

# Stop containers while preserving lab data volumes.
docker compose --profile airflow down
```

Compose binds exposed ports to localhost and contains explicit **local-only fixture credentials**. The catalog and audit store use separate schemas and roles. Each audit store binds one namespace and warehouse; changing only the output directory does not create an independent dataset. For qualification of another dataset, provision a separate database and warehouse prefix. See [infrastructure setup](infra/README.md) and [runtime qualification](scripts/verify_runtime.py).

The pinned MinIO image and optional Airflow 2.10.5 profile are historical lab dependencies: MinIO's repository is archived and Airflow 2 has reached end of life. They are not a current production deployment recommendation. See [design decisions](docs/design-decisions.md) and [versions](docs/versions.md).

## Recovery and monitoring

Replace `FAILED_RUN_ID` and `RELEASE_ID` with identifiers from your audit store.

```bash
.venv/bin/lakebridge --root artifacts/demo --namespace demo status
.venv/bin/lakebridge --root artifacts/demo --namespace demo recover FAILED_RUN_ID
.venv/bin/lakebridge --root artifacts/demo --namespace demo \
  rollback RELEASE_ID --reason 'reviewed consumer rollback'
```

Writer ownership does not expire automatically. A terminated PostgreSQL session releases its execution lock but leaves persistent ownership for deliberate recovery. Old executors must be stopped before recovery or a coordinated application upgrade; the protocol does not provide storage-level fencing for an independently running JVM or an external writer.

Monitoring uses a consistent, read-only audit snapshot. A missing database fails instead of being initialized as an empty healthy store. Metrics include collection freshness, release generation, writer ownership and fixed run-state counts. State age is not a heartbeat. See the [monitoring runbook](docs/observability.md) for textfile collection and failure handling.

## Verified results

These are archived local results from September 2026. Test counts are qualification results, not throughput or availability measurements.

| Evidence | Result | Scope |
|---|---|---|
| [Regression suite](docs/evidence/market-refresh/tests.xml) | **64 passed**, including 7 Spark and 6 PostgreSQL cases | Spark 3.5.9 / Iceberg 1.10.2 |
| [Pre-fix regressions](docs/evidence/market-refresh/baseline.xml) | 4 failed / 8 passed before fixes | Controlled PostgreSQL races and immutable release identity |
| [S3/JDBC qualification](docs/evidence/market-refresh/runtime-verification.json) | **18 compatibility checks**, four business demos, five releases | Local PostgreSQL + MinIO; small synthetic business dataset |
| [Operational checks](docs/evidence/market-refresh/checks.json) | Ruff, frozen dependencies, Compose and two promtool checks passed | Local checks; no long-running monitoring deployment |
| [Historical storage experiment](benchmarks/results/local-10g/report.json) | **19,773,338 rows / 10.157 GiB** of compressed source | Three workers on one workstation; Spark 3.5.6 / Iceberg 1.8.1 |
| Same-table compaction | **720 → 90** active files; full-scan median **9.715 → 8.407 s** | Three warm repetitions on the historical runtime |

The full-scan query reads and checksums the wide payload. Query order was fixed and OS/MinIO caches were not cleared: the approximately 13.5% lower median is a bounded local result, not a cloud performance prediction. Retained snapshots still occupy storage. The 10 GiB workload tests storage and queries, not the complete SaaS business pipeline at that scale.

The [benchmark methodology](docs/local-simulation.md) documents topology, inputs, SQL, validation and raw evidence. The [cost model](benchmarks/results/local-10g/cost-estimate.json) uses explicit assumptions; it is not an AWS bill or realized cost savings. The historical large-data benchmark has not been rerun on the upgraded runtime.

## Repository map

| Path | Purpose |
|---|---|
| [src/lakebridge](src/lakebridge) | CLI, contracts, transforms, release control, validation, monitoring and simulation |
| [tests](tests) | Unit, real PostgreSQL and real Spark regression tests |
| [contracts](contracts) / [examples](examples) | Manifest schema, data contracts and small synthetic inputs |
| [infra](infra) / [orchestration](orchestration) | Local topology, Terraform lab module and optional historical Airflow DAGs |
| [benchmarks/results](benchmarks/results) | Archived measurements, plans, event summaries and compressed raw evidence |
| [docs](docs) | Design, recovery, retention, compatibility and measurement records |
| [.github/workflows/ci.yml](.github/workflows/ci.yml) | Automated lint, PostgreSQL tests, Spark qualification and evidence upload |

## Remaining scope

No production deployment, real cloud bill, cross-host network-partition qualification, storage fencing, or 50–100 GiB result is claimed. Airflow 3 migration, a maintained replacement for the historical S3 simulator, and new-runtime large-scale benchmarks remain separate work.

Archived reports retain their original execution paths and timestamps. The pre-publication source manifest records the workspace at that time; later documentation and CI changes are identified by Git commits and the publication source manifest.

Private job-search notes named in the historical manifest are intentionally not distributed. The publication manifest covers the files included in this repository.
