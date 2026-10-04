# Consumer Bridge, Ops Agent and ReleaseQuery implementation audit

Scope: the three 2026-09-13 plans in `jd-resume-tailor/docs`, including the
roadmap's linked ReleaseQuery R0–R4 plan. Pre-existing lifecycle edits are retained;
they are not reclassified as newly authored work. Changes remain local/uncommitted.

| Stage | Implemented behavior | Evidence |
| --- | --- | --- |
| M0 / B0 | Versioned release pins, positive int64 snapshot strings, canonical cross-language hashes, UTC half-open scope, exact decimals/nulls/tombstones, two tenants | `tests/test_consumer_diff.py`, `tests/test_releasequery.py`, checked-in pinned exports |
| B1 | Immutable consumer registration, initial full build, complete registered-scope comparison, before/after dates, generation/source baseline, full rebuild on semantics or transform change | `tests/test_consumer_service.py`, Consumer Bridge demo |
| B2 | Separate PostgreSQL control/work/serving schemas, COPY, fixed dbt staging/daily models, complete candidate including unaffected days, removal of empty days, atomic pointer/job completion | Real PostgreSQL/dbt tests and combined demo |
| B3 | Distinct identities, independent approval, plan hash/expiry/permission/dependency rechecks, asynchronous worker admission, source retention, Spark-free API reads | `tests/test_consumer_api.py`, retention/service tests, Agent token approval rejected HTTP 403 |
| A0–A2 / M1 | Independent Python companion, seven official MCP SDK tools, release/run/runbook adapters, bounded LangGraph graph, model HTTP configuration, synchronous SQLite checkpoints and cumulative budgets | `ops-agent/tests`, actual stdio MCP/HTTP demo; model adapter uses deterministic mocked endpoint |
| Agent behavior | Tool facts govern claims, full scope survives preview truncation, exact independent approval, original-key reconciliation after lost response/HTTP 502, missing IDs/docs request input, waiting expiry stops | `test_runtime.py`, `test_tools.py`, repeated CLI invocations through actual service |
| M3 / R0–R2 | Compiled locked Rust/Arrow/DataFusion, generic typed ReleaseDiffExec, ordering requirements, cross-batch/Pending/EOF semantics, inspect/diff/explain CLI, checksummed complete artifacts | Cargo tests, SQL/Rust full-row parity, late-failure staging test |
| M4 / R3 | Native fixed Iceberg provider, explicit field-ID/time adapter, conservative feature rejection, real global SortExec, drop/cancellation and memory reservations | Actual pinned native/export/SQL equality, resource tests, large sorted-file regression |
| M4 / R4 | Sorted/unsorted × 1%/50% requested changes × 16/512-byte strings, full backend/process timing, phase metrics and separate RSS accounting | 48 complete observations; SQL/Rust full-row equality; SQL remains default |
| Delivery | Bilingual entrypoints, separate module docs, source manifests, local-only experience/evidence records | Verified evidence manifests; bank synchronization recorded in the local receipt |

The continuous scenario is: explicit release question → asynchronous comparison
→ full service-derived plan → independent human review/approval → same-identity
submission → separate Rust/dbt workers → committed consumer release → Agent
Completed. Final serving rows are checked against both the earlier target Iceberg
aggregate and a separate full target-export aggregate. A running job never becomes
Completed because of model prose.

## Reproducibility and boundaries

- [Consumer Bridge](consumer-bridge.md), [runbook](consumer-runbook.md),
  [Ops Agent](../ops-agent/README.md), [ReleaseQuery](../release-query/README.md).
- `scripts/consumer_demo.py` creates real local synthetic Iceberg releases.
  `scripts/releasequery_demo.py` compares them through all three scan/backend paths.
  `scripts/ops_agent_demo.py --diff-backend rust --native-sources ...` executes the
  complete approved downstream refresh across separate processes.
- Consumer core evidence was saved at an earlier checkpoint under
  `docs/evidence/consumer-bridge`; its Agent/Rust pending notes describe that
  checkpoint only. Current integration evidence is saved under
  `docs/evidence/releasequery` and `docs/evidence/ops-agent`.
- Main Python, dbt and Agent dependencies use separate environments/locks. Rust
  dependencies are locked to the actually compiled compatible combination.
- Native v1 accepts local Iceberg format-v2 with known fields and one schema.
  Delete files, schema evolution/name mapping and missing field IDs are rejected.
  Rejection tests do not claim native delete execution. Export remains explicit.
- Core reservations cover retained ownership, not every library allocation or RSS.
  No custom spill, distributed execution or hard process-memory bound is claimed.
- The full demo uses explicit IDs and scripted human approval. Model intent/claim
  behavior is exercised with a mocked HTTP endpoint; no paid/live model is claimed.
- Static local tokens are not OAuth. Remote MCP/OAuth, cloud sinks, automatic schema
  migration, external connectors, frontends and external PRs remain outside scope.
- No production deployment, Git push or hosted CI run for these changes is claimed.

## Observed sorting correction

The initial benchmark exposed DataFusion splitting a larger sorted Parquet file
into parallel byte-range scans. Coalescing those partitions can reorder keys. The
strict raw-key check rejected the task and withheld the completion manifest.
`inputs_sorted=true` now uses one scan partition; unsorted inputs still use a real
global sort. A >10 MiB sorted-file regression and the full workload matrix verify
that adapter decision. This is a local plan-construction correction, not a claimed
upstream defect or open-source contribution.

## Final local evidence

[Verification](evidence/releasequery/verification.json) records 100 non-Spark root
cases, the earlier 9 real Spark/Iceberg cases, 10 Agent cases, 7 Rust core cases
and 14 cross-language cases (the latter are already included in the 100).
The [combined demo](evidence/releasequery/combined-demo.json) finishes on the target
consumer release after real native scanning and PostgreSQL/dbt publication.
The [benchmark](evidence/releasequery/benchmark.json) retains all 48 measurements.
At a 64 MiB pool budget the wide unsorted [resource probe](evidence/releasequery/resource-probe.json)
completed with six sort spills; 32/48 MiB failed without publishing a complete
artifact. This is not an RSS guarantee.

Experience-bank updates target the existing LakeBridge record plus independently
scoped Ops Agent and ReleaseQuery component entries in the two `jd-resume-tailor`
banks. Backups, exact staged JSON, validation and the write/readback receipt are
kept under local `artifacts/experience-bank-update`; private job-search records
are not part of the repository's distributed source evidence.
