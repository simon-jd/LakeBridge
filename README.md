# LakeBridge

**Auditable Parquet-to-Iceberg migration, recoverable release control, and measured local performance.**

[中文说明](README.zh-CN.md) · [Architecture](docs/migration.md) · [Verification](docs/market-refresh-validation.md) · [Benchmark](docs/performance-local-10g.md)

LakeBridge migrates immutable historical Parquet batches into Apache Iceberg, reconciles them against an independent DuckDB oracle, and publishes a release binding detail, plan, and daily aggregate tables to specific snapshots. Readers can switch back to a previous release while a later correction preserves legitimate data that arrived afterward.

This is a locally verified data-platform engineering project with synthetic data. It runs real Spark, Iceberg, PostgreSQL and Docker; MinIO provides a local S3-compatible endpoint.

## Cloud qualification results

These results follow the project owner’s separately run cloud qualification recorded in the experience bank. The workload, environment, denominators and limitations below belong to that round; local regression checks for this checkout are separate. [Result record](docs/experience-bank-results.json).

The current implementation and reproduction commands are documented in the
[numeric snapshot contract](docs/releasequery-numeric-contract.md), with
[local regression evidence](docs/github-alignment-validation.md) recorded separately.

### LakeBridge — Auditable Lakehouse and Versioned Consumer Refresh

- Built an immutable release manifest holding per-table Iceberg snapshot_ids behind a PostgreSQL active-release pointer, so a consumer reads one release_id and gets one consistent multi-table view; single-table snapshots commit atomically by themselves, while the multi-table view exists only through the manifest plus the control-plane pointer, and bypassing the release to query tables directly is not covered.

- Bound tenant refresh plans to their full content: old and new release, tenant, date range and diff hash are part of the plan, an independent approver signs plan_hash, the worker has no approval rights, and execution re-verifies plan and generation before switching the pointer.

- Published consumer results and completion status in one PostgreSQL transaction after temporary-table validation, with the dataset generation guarded by a row lock (SELECT FOR UPDATE, compare generation, switch pointer) under a dataset advisory lock; a report-generation failure cannot undo already-committed data.

- Matched 2,000/2,000 difference business keys (1,000 added, 200 deleted, 800 modified) per key against a SQL oracle with tenant and date impact sets exactly equal, across the 100-test new round: 75 lifecycle/recovery plus 25 consumer bridge tests, with the 19 runtime-compatibility scenarios a labeled subset of the 100 rather than extra cases.

- Injected 20 faults across 5 windows × 4 repetitions (after data write, after a single-table snapshot, after manifest write, after the PostgreSQL pointer commit, before report write): the first 12 left the old release active and the last 8 left the new release active, and retries returned the same release instead of republishing; orphaned files are left to the retention policy, and object storage is not claimed to roll back with the PostgreSQL transaction.

- Held 4 concurrent identical refresh requests to a single job and one consumer publication, and rejected all 8 attempts to reuse a valid approval token with a different tenant or target release.

- Exposed status through REPEATABLE READ snapshot reads and an atomically replaced Prometheus textfile, and explicitly exported a failure state when collection failed.

### LakeBridge Ops Agent — MCP Investigation and Approved Refresh

- Built a LangGraph companion agent with 7 typed MCP tools (list_releases, describe_release, preview_diff, create_plan, approval_status, submit_refresh, get_job), synchronous SQLite checkpoints and a cumulative call budget of at most 12 calls per run that persists across restarts instead of resetting.

- Kept model claims subordinate to complete tool facts: on the fixed old-R1/new-R2 change (tenant A affected 3 days, tenant B affected 2 days) the standard 9-call path produced a plan with exactly 5 tenant/date combinations, and when the scripted model claimed only 1 day was affected the final answer still cited all 5 combinations and marked the inconsistency.

- Bound approval to an independent human identity on the service side: the PostgreSQL approval is tied to plan_hash and expires after 15 minutes, and because the agent token cannot call approve, 2 self-approval attempts returned HTTP 403 while 2 explicit human rejections ended in DENIED with no submit.

- Recovered from ambiguous execution: a submit whose reply was lost reused the original idempotency key to return the same job and publish only once, two successful refreshes each created 1 independent job with no idempotency-key sharing between rounds, and after the checkpointer wrote to disk a killed process recovered with tool results still referencing the same release.

- Exercised a 12-scenario fault matrix (6 classes × 2) covering restart after plan, approval expiry, agent self-approval, plan replacement after approval, lost submit reply and budget exhaustion: restart retained the same plan, expiry and replacement each rejected submission twice, exhaustion stopped new tool calls twice, and retrying submit moved the flow from 9 to 10 calls, still inside the 12-call budget.

- Treated model text and external MCP descriptions as data only — an 'ignore approval' instruction changed nothing about service permissions — while limiting the claim to scripted prompt scenarios rather than all prompt injection or OAuth production systems.

### High-Performance Data Tooling — Rust Arrow/DataFusion Snapshot Diff

- Diffed two primary-key-ordered snapshots of 100M rows each with a streaming merge that never loads a full table, over five projected columns - int64 key, float64 value, timestamp64 and two int32, 32 bytes logical width - in a 22-second core-stage median and 1.1 GiB peak RSS across five warm-file-cache runs on Linux x86_64, 16 vCPU/64 GiB, single local NVMe; the Python baseline took a 540-second median at 6.0 GiB on the same projection.

- Reported throughput honestly as about 9.09 million input rows/s, not output diffs/s, at a fixed 0.1% difference rate, with core output written to a bounded local sink and persistent publication accounted separately; cold-cache results are listed separately and do not reuse the 22 seconds.

- Cut the end-to-end simulated pipeline from 52 to 14 minutes on one full run trajectory per side (baseline preparation 26 min + core 9 min + audit/publish 17 min; candidate preparation 8.5 min + core 22 s + audit/publish 308 s), and scoped it correctly: because preparation changed, this is a whole-pipeline rework, not a merge-algorithm result.

- Made numerical decisions tolerance-aware rather than hash-based: keys are compared exactly, float values use abs(a-b) <= 1e-8 + 1e-6*max(abs(a),abs(b)), NULL equals only NULL, NaN compares by an explicit policy, same-sign infinities and positive/negative zero are equal; xxHash is used only for locating and diagnostics and never bypasses the per-column comparison.

- Treated duplicate primary keys as input errors, used 64K-row RecordBatches with a bounded queue, and ran DataFusion scheduling and outer concurrency under one shared thread budget to avoid nested thread-pool oversubscription; a small-scale independent Python oracle with random differential plus NULL, NaN, infinity and tolerance-boundary cases reconciled added, deleted and changed key sets.

## Engineering problems it addresses

- **Incomplete multi-table publication:** validate isolated Iceberg branches, retain snapshots, and publish one audited snapshot set for readers to resolve.
- **Late corrections and repeatable backfills:** pin manifests and watermarks; reject conflicting source versions, unsafe scopes, and reused run IDs with different parameters.
- **Failure between data commit and audit publication:** preserve the original run's ownership and candidate snapshots, then resume the unfinished steps.
- **Concurrent rollback and writers:** serialize control decisions with PostgreSQL row locks, enforce release generations, and scope execution locks to each audit table.
- **Invisible failed jobs:** expose read-only JSON status and bounded-cardinality Prometheus metrics without starting Spark; report failed collection as `audit_up 0`.
- **Executor interruption at admission:** commit recovery inputs and writer ownership together; protect terminal states against stale aborts and diagnostic report failures. See [run lifecycle](docs/run-lifecycle.md).

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

## Historical local results

These are archived local results from September 2026. Test counts are qualification results, not throughput or availability measurements.

| Evidence | Result | Scope |
|---|---|---|
| [Lifecycle regression qualification](docs/evidence/run-lifecycle/verification.json) | **73 passed**, including 8 Spark and 8 PostgreSQL cases | September 7 local rerun; 9 new lifecycle tests; no new S3/large-data benchmark |
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

The [September 7 demand and ecosystem review](docs/reliability-refresh-2026-09-07.md)
explains the lifecycle work and retained runtime. Existing owners without run records
and interrupted maintenance require investigation; the new admission guarantee
applies to pipeline runs admitted by the updated code.

Archived reports retain their original execution paths and timestamps. The pre-publication source manifest records the workspace at that time; later documentation and CI changes are identified by Git commits and the publication source manifest.

Private job-search notes named in the historical manifest are intentionally not distributed. The publication manifest covers the files included in this repository.

## Consumer Bridge

The [Consumer Bridge](docs/consumer-bridge.md) adds fixed-release SQL comparison,
complete impact artifacts, human-approved downstream refresh jobs, and a separate
PostgreSQL/dbt `tenant_daily_usage` publication. The API and single worker run
independently of Spark export. Snapshot IDs and decimal values retain exact wire
representations; tenant/date scopes, plan hashes and generations bind approvals.

Run `scripts/consumer_demo.py` for the combined local Iceberg-to-PostgreSQL workflow.
[Ops Agent](ops-agent/README.md) adds seven typed MCP tools, durable LangGraph
investigation, cumulative budgets, independent approval and restart/reconciliation.
[ReleaseQuery](release-query/README.md) adds an exact Rust/Arrow/DataFusion merge
operator, CLI and fixed native Iceberg provider alongside the SQL backend.

The combined local demo (`scripts/ops_agent_demo.py --diff-backend rust
--native-sources ...`) uses actual stdio MCP, HTTP, native Iceberg, PostgreSQL and
dbt. The configured model adapter is tested against a deterministic HTTP fixture;
the full demo uses explicit IDs and scripted separate human approval. SQL remains
the default. The [implementation audit](docs/implementation-roadmap-2026-09-13.md)
links tests, source manifests, complete artifacts and backend measurements.
