# Consumer Bridge v1

Consumer Bridge compares explicitly pinned LakeBridge releases and publishes a
separate PostgreSQL `tenant_daily_usage` result after human approval. It uses the
existing event/tenant/usage domain. A registered consumer covers one tenant and
one immutable UTC date window. Changing that scope requires a new consumer.

`src/lakebridge/consumer` contains the protocol, release export registry, DuckDB
FULL OUTER JOIN reference backend, impact analysis, PostgreSQL ledger, plans,
approvals, workers, COPY/dbt sink, API and operator CLI. The API has no Spark
import or Spark lifecycle. Export runs explicitly in the upstream worker environment.

## Install and run

```bash
uv sync --frozen --extra dev --extra consumer --python 3.11
uv venv .venv-dbt --python .venv/bin/python
uv pip install --python .venv-dbt/bin/python -r consumer-dbt/requirements.lock
export LAKEBRIDGE_CONSUMER_DSN='postgresql://postgres:postgres_local@127.0.0.1:55439/postgres'
.venv/bin/lakebridge-consumer init
.venv/bin/lakebridge-consumer register daily_A saas_seed_7 t0000 2025-01-01 2025-01-08
.venv/bin/lakebridge-consumer grant agent daily_A read plan execute
.venv/bin/lakebridge-consumer grant human daily_A read approve
```

Use the actual dataset/release IDs from your ledger. Export each named source:

```bash
.venv/bin/lakebridge-consumer export DATASET RELEASE \
  --upstream-root artifacts/demo --upstream-namespace demo
```

Export scans the exact `detail` snapshot ID, attaches the known UTC meaning to
Spark microsecond timestamps, checks decimal types and writes an immutable
manifest with file checksums. It never lists Iceberg data directories to find
"latest" Parquet. Source kinds distinguish actual `iceberg_snapshot_export` from
`synthetic_fixture`. The original upstream source metadata stays in its snapshot.

Supply API configuration with trusted paths and environment-variable names:

```json
{
  "dsn_env": "LAKEBRIDGE_CONSUMER_DSN",
  "namespace": "consumer",
  "registry": "artifacts/consumer/exports",
  "artifacts": "artifacts/consumer/diffs",
  "identities": [
    {"token_env": "LAKEBRIDGE_AGENT_TOKEN", "principal_id": "agent", "entrypoint": "agent"},
    {"token_env": "LAKEBRIDGE_HUMAN_TOKEN", "principal_id": "human", "entrypoint": "human"}
  ],
  "runbooks": {"usage.v1": "docs/consumer-runbook.md"}
}
```

Set two distinct randomly generated tokens of at least 24 characters. They are
local trusted-operator credentials, not an OAuth implementation. Start the API
and a separate worker in two terminals:

```bash
.venv/bin/lakebridge-consumer serve --config artifacts/consumer/api.json
.venv/bin/lakebridge-consumer worker
```

The API binds to loopback. The model's configured token cannot use the human
approval route even if an operator accidentally gives that principal an approve
grant. Bodies reject extra fields such as `principal_id`, SQL, paths or `approval=true`.

## Workflow and protocol

1. `GET /releases/{dataset}/{release}`, `GET /runs/{dataset}/{run}` and `GET /runbooks`
   retrieve registered facts. Optional upstream run ledgers are deployment configuration.
2. `POST /comparisons` takes `consumer_id`, explicit `from_release` (null only for
   initial consumption) and `to_release`. It returns a persisted PENDING identity.
3. A worker produces the complete diff. `GET /comparisons/{id}` returns persisted
   status, counts, artifact identity and a separate bounded preview; it does not scan files.
4. `POST /refresh-plans` takes the completed comparison and consumer IDs. The
   service reads the entire artifact and computes all affected tenant/date pairs.
5. Review with `lakebridge-consumer review PLAN_ID`, then independently approve:
   `lakebridge-consumer approve PLAN_ID --plan-hash REVIEWED_HASH --decision approved`.
6. `POST /refresh-jobs` supplies plan ID, approval ID and an idempotency key. Query
   `/refresh-jobs/{id}` or `/refresh-jobs/by-key/{key}` after an uncertain response.
7. Read `/consumers/{id}/usage` after SUCCEEDED. Decimal API values are strings.

Pins, schemas, comparison columns and hash encoding are owned by `protocol.py`.
Canonical JSON uses sorted ASCII keys, UTF-8 values, no insignificant whitespace,
no floats, safe-range integers, decimal strings and int64 snapshot strings.
Dates are UTC `[start,end)`; this differs from the upstream inclusive backfill
end boundary. No downstream call forwards a tenant approval to upstream backfill.

SQL compares the complete registered scope using exact, null-safe field equality.
Tombstones are absent records. Reordering, compaction and source metadata changes
are business-equivalent. Before/after dates both contribute to impact. A generic
date-move fixture tests this behavior; the existing upstream still rejects event
timestamp changes. First consumption and explicit rebuilds cover the full window.
Changing comparison or transform versions requires a full rebuild; incompatible
schema requires an adapter. Deployments must bump the transform version when changing SQL semantics.

## Publication, recovery and retention

`consumer_control`, `consumer_work` and `consumer_serving` are isolated schemas
(a deployment can use another prefix). COPY loads only target-snapshot live events
for affected days. Two fixed dbt models create job-specific staging and daily
relations. The complete candidate reuses untouched days and replaces affected
days. Deleting the last event removes that day's old row. The final transaction
locks the consumer and permissions, rechecks approval/expiry/generation, inserts
the full consumer release, advances its source pointer and marks the job SUCCEEDED.

Spark export, files and dbt run outside that publication transaction. There is no
cross-system transaction claim. A session advisory lock enforces one live worker
per consumer store; a process crash releases the lock. `worker` finds PENDING or
interrupted RUNNING work. Explicit `compare-worker ID` and `refresh-worker ID`
recover FAILED work with the same identity. Confirm service state before recovery;
never create a fresh idempotency key because a response was lost.

Every admitted comparison, plan and job records source references. Unconfirmed
terminal jobs retain references. Confirmation releases only that job's reference;
v1 conservatively retains comparison/plan references and all upstream release tags.
It does not yet garbage-collect source exports or old consumer releases.

Configure `LAKEBRIDGE_CONSUMER_DSN` and, for a custom prefix,
`LAKEBRIDGE_CONSUMER_NAMESPACE` in the upstream maintenance environment. Maintenance
reads the in-flight reference ledger and verifies corresponding release tags;
unavailable configured ledgers or missing tags stop maintenance. Restoring the
consumer database does not reconstruct missing object storage dependencies.

## Reproduce the combined local demonstration

```bash
JAVA_HOME=/path/to/java17 .venv/bin/python scripts/consumer_demo.py \
  --root artifacts/consumer-demo --namespace consumer_demo
```

Choose a new root and namespace. The script builds two real Iceberg releases
from synthetic data, exports exact snapshots, publishes the first full consumer
and a changed version through separate scripted agent/human identities, retries
the same job, and compares the final PostgreSQL result to the target Iceberg
daily snapshot. It leaves the schemas and a structured report for inspection.
This demonstrates the service; a model-driven investigation is a separate Agent deliverable.

API tests use the real FastAPI application. Database tests use disposable PostgreSQL
schemas and real dbt execution. Semantic tests cover empty inputs, nulls, decimal
precision, tombstones, date moves, duplicates, truncated previews and incomplete
artifacts. Existing upstream tests remain part of the regression suite.

The implementation uses the [DuckDB FROM/JOIN semantics](https://duckdb.org/docs/stable/sql/query_syntax/from)
and [dbt PostgreSQL adapter](https://docs.getdbt.com/reference/resource-configs/postgres-configs).
All evidence is local and synthetic; no production throughput or availability claim is made.
