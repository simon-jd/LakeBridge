# Release control and runtime maintenance decisions

This record describes the locally verified September 2026 implementation. The project is a
reference data platform with synthetic inputs, not an operated production service.

## Publish a snapshot set, not independent table heads

Iceberg provides atomic single-table commits. A migration can still fail between updates to
detail, plan and aggregate tables. LakeBridge validates audit branches and records the specific
snapshots that belong to a release. Consumers resolve that release instead of reading each main
branch independently. Protected tags keep published snapshots available for rollback.

This avoids mixed release reads through the supported reader protocol. It does not create a
distributed transaction spanning Iceberg, PostgreSQL and object storage.

## Separate short database decisions from long Spark execution

Writer admission, publication and consumer activation lock the same PostgreSQL control row
before checking generation and ownership. This closes a race hidden by SQLite's
`BEGIN IMMEDIATE` behavior. A session advisory lock protects the longer execution interval;
its key includes the audit table OID so independent datasets do not share a global lock.

Persistent writer ownership survives session loss. Recovery reuses the original run and
candidate. Release identity includes immutable content but excludes retry timing telemetry.
Queued duplicates reread state after acquiring the execution lock and return an already
completed release without rerunning Spark transformations.

The cost is explicit recovery instead of time-based takeover. An old JVM or external writer
is not fenced at the storage layer. The previous global advisory-lock protocol cannot safely
run alongside the new protocol: stop old jobs and switch all writers together.

Evidence: [controlled regressions](../tests/test_postgres_control.py),
[pre-fix results](evidence/market-refresh/baseline.xml), and
[postmortem](concurrency-postmortem.md). PostgreSQL semantics are documented in
[Explicit Locking](https://www.postgresql.org/docs/17/explicit-locking.html).

## Read-only monitoring instead of another application service

Audit status and metrics need neither a Spark JVM nor access to the data plane. PostgreSQL
sampling uses a read-only repeatable-read transaction. The collector verifies dataset binding,
does not create a missing store, and emits a fixed set of state labels without run IDs or paths.
Atomic textfile replacement prevents partial scrapes; collection failures publish `audit_up 0`.

This keeps deployment small and compatible with the
[node_exporter textfile collector](https://github.com/prometheus/node_exporter#textfile-collector).
Collection freshness must be checked because an output file can outlive its producer.
Writer state age is not a heartbeat, and audit health does not prove object-storage health.
No permanent collector, dashboard or production alerting deployment is claimed.

## Select maintenance releases with a concrete benefit

Official sources were checked on September 6, 2026.

| Decision | Benefit and compatibility cost |
|---|---|
| Spark 3.5.6 → **3.5.9** | The [July 16 maintenance release](https://spark.apache.org/releases/spark-release-3-5-9.html) includes security and correctness fixes. Retains the Spark 3.5 / Scala 2.12 / JDK 17 line; qualified with real integration tests. [Source tag](https://github.com/apache/spark/tree/v3.5.9). |
| Iceberg 1.8.1 → **1.10.2** | [May 18 release notes](https://iceberg.apache.org/releases/#1102-release) include fixes around accidental file cleanup after commits and failed CREATE transactions. Runtime and AWS bundle are pinned together and checksum-verified. These upstream bugs were not independently reproduced here. [GitHub release](https://github.com/apache/iceberg/releases/tag/apache-iceberg-1.10.2). |
| Retain the Spark 3.5 line | Spark 4 introduces migration and Scala ABI work; format-v2 release control does not need it. See the [SQL migration guide](https://spark.apache.org/docs/latest/sql-migration-guide.html). No blanket latest-version or full security-audit claim. |
| Keep optional historical lab components explicit | [Airflow 2 reached EOL on April 22, 2026](https://airflow.apache.org/docs/apache-airflow/stable/installation/supported-versions.html). [MinIO's repository was archived April 25, 2026](https://github.com/minio/minio). Their pinned local profiles remain for historical experiments; replacement requires a separate qualification. |

The new runtime passed 64 local tests, 18 S3/JDBC compatibility checks and four small business
demonstrations. The large 10 GiB experiment remains evidence for its original Spark 3.5.6 /
Iceberg 1.8.1 runtime. A dependency upgrade does not retroactively requalify a benchmark.

## Keep measured performance separate from modeled cost

Compaction is compared against the same partitioned table and identical full-payload SQL.
Results include all three warm repetitions, active file counts, exact content comparisons,
task events and S3 request observations. Shared hardware, fixed query order and cache effects
limit generalization. Historical tags also retain old storage after active files are compacted.

The cloud model applies explicit instance, storage, request, startup and retention assumptions.
Its savings are hypothetical; local CPU time does not establish EC2 performance or a cloud bill.
See the [measurement report](performance-local-10g.md) and
[qualification boundaries](market-refresh-validation.md).
