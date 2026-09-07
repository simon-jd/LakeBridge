-- First resolve ONE release via `lakebridge releases` or audit.releases.
-- Substitute the exact snapshot IDs from that same release. Never resolve each table separately.

-- 1. Seven-day tenant usage
SELECT tenant_id, sum(billable_units) AS billable_units
FROM lake.saas.detail VERSION AS OF ${detail_snapshot}
WHERE tenant_id = 't0000' AND NOT is_deleted
  AND event_time >= TIMESTAMP '2025-03-25 00:00:00'
  AND event_time < TIMESTAMP '2025-04-01 00:00:00'
GROUP BY tenant_id;

-- 2. Ninety-day daily aggregates
SELECT business_date, sum(event_count) events, sum(billable_units) billable_units
FROM lake.saas.daily VERSION AS OF ${daily_snapshot}
WHERE business_date BETWEEN DATE '2025-01-01' AND DATE '2025-03-31'
GROUP BY business_date ORDER BY business_date;

-- 3. Temporal plan join (always bind the plan snapshot as well)
SELECT e.tenant_id, p.plan_id, sum(e.units)
FROM lake.saas.detail VERSION AS OF ${detail_snapshot} e
JOIN lake.saas.plans VERSION AS OF ${plans_snapshot} p
  ON e.tenant_id=p.tenant_id AND e.event_time>=p.effective_from
  AND (p.effective_to IS NULL OR e.event_time<p.effective_to)
WHERE NOT e.is_deleted
GROUP BY e.tenant_id,p.plan_id;

-- 4. Missing keys (run both directions; duplicate/row-value checks remain separate)
SELECT tenant_id,event_id FROM expected_detail
EXCEPT
SELECT tenant_id,event_id FROM lake.saas.detail VERSION AS OF ${detail_snapshot};

-- 5. Backfill impact, before writing
SELECT business_date,count(*) affected_keys
FROM lake.saas.detail VERSION AS OF ${detail_snapshot}
WHERE business_date BETWEEN DATE '2025-03-01' AND DATE '2025-03-31'
GROUP BY business_date;

-- Evidence. These metadata tables are for operators, not release-bound consumers.
SELECT * FROM lake.saas.detail.snapshots ORDER BY committed_at;
SELECT * FROM lake.saas.detail.refs;
SELECT content,count(*),sum(file_size_in_bytes) FROM lake.saas.detail.files GROUP BY content;

