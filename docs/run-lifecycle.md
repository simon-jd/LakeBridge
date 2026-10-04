# Crash-consistent run admission and terminal states

Verified locally on 2026-09-07. This extends release control without changing audit
tables, release JSON, CLI arguments, runtime versions or lock keys.

## Recovery data and ownership share one commit

Previously, `Control.writer` committed `writer_run` before `Pipeline.execute`
saved its first run record. A Python process exiting between those calls left an
owner that `recover` could not find. Pipeline admission now supplies immutable
manifest/rule/scope/reason parameters to `writer(initial_run=...)`. Under the
control-row lock, a new run's `RUNNING` record, generation, start time and owner
commit together. A retry never replaces an existing recovery record. The pipeline
still rereads it and checks parameters after acquiring the execution lock.

```mermaid
flowchart TD
  A[Acquire execution lock] --> B[Transaction: owner + initial recovery record]
  B --> C[Read latest run and compare parameters]
  C --> D[Spark branches and exact reconciliation]
  D --> E[Protect snapshots and publish audited release]
  E --> F[Transaction: COMPLETED + clear owner]
  F --> G[Write diagnostic release.json]
  B -. executor stops .-> H[Recover same run after stopping old executor]
  G -. disk write fails .-> I[Keep COMPLETED; retry returns audit release]
```

The SQL transaction stays short and does not cover Spark execution. SQLite uses
`BEGIN IMMEDIATE`; PostgreSQL uses the existing row lock and session advisory lock.
A terminated session releases the advisory lock but preserves both committed rows.
No lease expiry, storage fencing or automatic takeover is introduced. Maintenance
callers do not supply pipeline recovery context and retain their prior ownership
protocol; interrupted maintenance is not automatically recoverable.

## Abort uses fresh state and one terminal transaction

`abort` rereads the run after acquiring the execution lock. A stale request cannot
replace a newly `COMPLETED` or `ABORTED` run. It retains the existing check that main
snapshots have not moved, then commits `ABORTED` and owner release together. If that
transaction fails, unfinished state and ownership remain available for retry.

## Diagnostic output does not define publication success

After the completed-state transaction, writing `runs/<run_id>/release.json` can
still raise an I/O error. That error reaches the caller, but does not write
`FAILED` over an audited `COMPLETED` run. Retrying the same immutable request or
`recover <run_id>` returns the release ledger entry without executing Spark again.
It does not repair the missing report. Use `releases` or the returned JSON to
retrieve the audit record after repairing local disk access.

This guarantee covers post-completion diagnostic failure, not every ambiguous
network failure during SQL COMMIT. Existing recovery and immutable release checks
still govern those cases. No transaction spans PostgreSQL and Iceberg.

## Operator procedure

1. Stop or positively identify the old Python/Spark executor before recovery;
   a released SQL session lock does not prove that a detached JVM has stopped.
2. Run `status`. For an admitted `RUNNING` task with intact immutable source paths,
   use `recover RUN_ID`. If no main snapshot moved and cancellation is intended,
   use `abort RUN_ID --reason 'reviewed cancellation'`.
3. After a report-write error, inspect audit state. For `COMPLETED`, retry the same
   run to obtain its release. Do not create another run to repair a report.
4. For a pre-upgrade owner without a run record, investigate using backups and
   original invocation evidence. This update does not synthesize missing inputs
   or clear historical owners. Monitoring still exposes `writer_state_missing`.

Upgrade stopped writers together so all new admissions use the stronger invariant.
No schema migration is required; older binaries can still leave the old crash
window. Existing snapshots, releases and immutable retry parameters remain valid.

## Evidence and reproduction

See [qualification](evidence/run-lifecycle/verification.json),
[SQLite/process regressions](../tests/test_run_lifecycle.py),
[PostgreSQL tests](../tests/test_postgres_control.py) and
[Spark scenario](../tests/test_integration.py).

From the repository root, with the locked environment and disposable local database:

```bash
LAKEBRIDGE_TEST_POSTGRES_DSN='postgresql://postgres:postgres_local@127.0.0.1:55439/postgres' \
  .venv/bin/pytest -m 'not integration' -q --junitxml=/tmp/lakebridge-unit.xml
JAVA_HOME=/path/to/jdk17 LAKEBRIDGE_INTEGRATION=1 \
  .venv/bin/pytest -m integration -q --basetemp=/tmp/lakebridge-spark \
  --junitxml=/tmp/lakebridge-spark.xml
make lint
```

The PostgreSQL fixture creates random schemas and drops only its own schemas.
The process test uses `os._exit(23)` to bypass Python cleanup; the PostgreSQL test
uses `pg_terminate_backend` on its own session. Spark runs on one host with Hadoop
Catalog/local files and SQLite audit. PostgreSQL concurrency is tested separately.
No combined S3/JDBC rerun, cloud deployment, benchmark or hosted CI result is
claimed for this change.
