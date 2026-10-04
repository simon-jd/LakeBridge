# Usage rules, source version usage.v1

The canonical business input is the `detail` snapshot in an explicitly named
LakeBridge release. Use tool-returned release IDs and snapshot IDs for facts.
This document explains semantics; it does not establish any event counts or totals.

Business keys are `(tenant_id,event_id)`. Tombstones (`is_deleted=true`) mean an
event is absent. Dates derive from UTC event timestamps; consumer windows are
`[start,end)`. The upstream backfill CLI instead uses inclusive end dates.

`units` and `billable_units` are exact decimals with scale 6. `billable_units` and
`plan_id` already reflect upstream LakeBridge rules. Rule v1 uses units directly;
rule v2 applies the existing 0.9 factor to pro-plan usage. The consumer does not
recompute historical plan associations.

Only event_time, units, plan_id, billable_units and rule_version determine updates.
File rewrites and ingestion/source metadata changes alone do not change usage.
The current upstream rejects changing an event's timestamp; the generic diff can
still represent date moves in standalone fixtures.

A complete comparison covers one consumer's full registered tenant/date scope.
Its preview may be truncated. Derive recomputation dates from the complete
artifact's before and after rows. Reaggregate each affected day from the target
snapshot; remove a day's summary when its last live event is deleted.

Human approval binds the reviewed plan hash. Changing versions, scope, transform
or consumer generation requires a new plan and approval. Agent tools cannot approve.
An accepted refresh may still be running; only a SUCCEEDED job establishes a
published consumer release. If a submission response is lost, query the original
principal/idempotency key before attempting the same submission identity again.
