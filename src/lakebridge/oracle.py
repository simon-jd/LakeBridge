"""Independent DuckDB SQL oracle. Never calls the production Spark transformation.

DuckDB can spill to disk; COPY streams reference files rather than collecting events
in the driver. The oracle deliberately reads the exact immutable manifest file set.
"""

from pathlib import Path

import duckdb

from lakebridge.common import LakeBridgeError, literal
from lakebridge.contracts import verify_manifest

SEMANTIC_COLUMNS = [
    "tenant_id",
    "event_id",
    "event_time",
    "ingested_at",
    "units",
    "source_version",
    "is_deleted",
    "plan_id",
    "business_date",
    "billable_units",
    "rule_version",
]
SUMMARY_COLUMNS = ["tenant_id", "business_date", "event_count", "total_units", "billable_units"]


def reference(
    manifest: Path,
    watermark: int,
    rule: str,
    output: Path,
    scope: tuple[str, str] | None = None,
    previous: Path | None = None,
) -> Path:
    if rule not in ("v1", "v2"):
        raise LakeBridgeError("Unknown rule version")
    inputs = verify_manifest(manifest, watermark)
    output.mkdir(parents=True, exist_ok=True)
    db = duckdb.connect()
    try:
        db.execute("SET TimeZone='UTC'")
        db.execute("SET memory_limit='512MB'")
        db.execute(f"SET temp_directory={literal(str(output / 'spill'))}")
        db.read_parquet(inputs.files("events"), hive_partitioning=False).create_view("raw")
        db.read_parquet(inputs.files("plans"), hive_partitioning=False).create_view("raw_plans")
        db.execute("CREATE TEMP VIEW plans AS SELECT DISTINCT * FROM raw_plans")
        # DISTINCT eliminates exact replays; row_number selects a source revision, not event time.
        db.execute("""CREATE TEMP TABLE latest AS
            SELECT * EXCLUDE (rn) FROM (
              SELECT *, row_number() OVER (
                PARTITION BY tenant_id,event_id ORDER BY source_version DESC) rn
              FROM (SELECT DISTINCT * FROM raw)) WHERE rn=1""")
        factor = "CASE WHEN p.plan_id='pro' THEN 0.9 ELSE 1 END" if rule == "v2" else "1"
        db.execute(f"""CREATE TEMP TABLE computed AS
            SELECT e.*, p.plan_id, CAST(e.event_time AS DATE) business_date,
                   CAST(e.units * ({factor}) AS DECIMAL(18,6)) billable_units,
                   {literal(rule)} rule_version
            FROM latest e LEFT JOIN plans p ON e.tenant_id=p.tenant_id
              AND e.event_time>=p.effective_from
              AND (p.effective_to IS NULL OR e.event_time<p.effective_to)""")
        if scope:
            if previous is None:
                raise LakeBridgeError("Scoped oracle requires previous release data")
            previous_glob = str(previous / "*.parquet") if previous.is_dir() else str(previous)
            db.read_parquet(previous_glob).create_view("previous")
            predicate = (
                f"business_date BETWEEN DATE {literal(scope[0])} AND DATE {literal(scope[1])}"
            )
            columns = ",".join(SEMANTIC_COLUMNS)
            db.execute(f"""CREATE TEMP VIEW expected AS
                SELECT {columns} FROM computed WHERE {predicate}
                UNION ALL SELECT {columns} FROM previous WHERE NOT ({predicate})""")
        else:
            db.execute("CREATE TEMP VIEW expected AS SELECT * FROM computed")
        detail = output / "detail.parquet"
        summary = output / "summary.parquet"
        db.execute(f"COPY expected TO {literal(str(detail))} (FORMAT PARQUET)")
        db.execute(f"""COPY (SELECT tenant_id,business_date,COUNT(*)::BIGINT event_count,
            CAST(SUM(units) AS DECIMAL(28,6)) total_units,
            CAST(SUM(billable_units) AS DECIMAL(28,6)) billable_units
            FROM expected WHERE NOT is_deleted GROUP BY tenant_id,business_date)
            TO {literal(str(summary))} (FORMAT PARQUET)""")
        return detail
    finally:
        db.close()
