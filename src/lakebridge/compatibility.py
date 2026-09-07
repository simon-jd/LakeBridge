import platform
import uuid

from lakebridge.common import json_write
from lakebridge.config import ICEBERG_VERSION
from lakebridge.maintenance import orphan_inventory


def compatibility(spark, settings):
    name = "compat_" + uuid.uuid4().hex[:12]
    table = settings.table(name)
    short = f"{settings.namespace}.{name}"
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS lake.{settings.namespace}")
    results = {
        "spark": spark.version,
        "iceberg": ICEBERG_VERSION,
        "python": platform.python_version(),
        "java": spark._jvm.java.lang.System.getProperty("java.version"),
        "scala": spark._jvm.scala.util.Properties.versionNumberString(),
        "warehouse": settings.warehouse,
        "checks": [],
    }
    spark.sql(
        f"CREATE TABLE {table} (id BIGINT, value STRING, ts TIMESTAMP) USING iceberg "
        "PARTITIONED BY (days(ts)) TBLPROPERTIES ('format-version'='2')"
    )
    spark.sql(f"INSERT INTO {table} VALUES (1,'before',TIMESTAMP '2025-01-01 00:00:00')")
    base = spark.sql(f"SELECT snapshot_id FROM {table}.refs WHERE name='main'").first()[0]
    spark.sql(f"ALTER TABLE {table} CREATE BRANCH audit AS OF VERSION {base}")
    spark.sql(
        f"MERGE INTO {table}.branch_audit t USING (SELECT 1L id, 'after' value) s "
        "ON t.id=s.id WHEN MATCHED THEN UPDATE SET value=s.value"
    )
    assert spark.table(table).first()["value"] == "before"
    assert spark.table(f"{table}.branch_audit").first()["value"] == "after"
    results["checks"].extend(["create", "insert", "merge", "branch_isolation"])
    spark.sql(f"ALTER TABLE {table} CREATE TAG protected AS OF VERSION {base}")
    spark.sql(f"CALL lake.system.fast_forward('{short}','main','audit')")
    assert spark.table(table).first()["value"] == "after"
    assert spark.read.option("snapshot-id", str(base)).table(table).first()["value"] == "before"
    results["checks"].extend(["fast_forward", "tag", "time_travel"])
    spark.sql(f"ALTER TABLE {table} ADD COLUMN annotation STRING")
    spark.sql(f"ALTER TABLE {table} ADD PARTITION FIELD bucket(4,id)")
    results["checks"].extend(["schema_evolution", "partition_evolution"])
    before_rollback = spark.sql(f"SELECT snapshot_id FROM {table}.refs WHERE name='main'").first()[
        0
    ]
    spark.sql(f"CALL lake.system.rollback_to_snapshot('{short}',{base})")
    assert spark.table(table).first()["value"] == "before"
    spark.sql(f"CALL lake.system.set_current_snapshot('{short}',{before_rollback})")
    results["checks"].append("rollback")
    spark.sql(f"UPDATE {table} SET annotation='expire_this_snapshot' WHERE id=1")
    expendable = spark.sql(f"SELECT snapshot_id FROM {table}.refs WHERE name='main'").first()[0]
    spark.sql(f"UPDATE {table} SET annotation=NULL WHERE id=1")
    spark.sql(
        f"CALL lake.system.rewrite_data_files(table=>'{short}',options=>map('rewrite-all','true'))"
    ).collect()
    # Aggressive future cutoff is used ONLY on this isolated compatibility table.
    spark.sql(
        f"CALL lake.system.expire_snapshots(table=>'{short}',"
        "older_than=>TIMESTAMP '2099-01-01 00:00:00',retain_last=>1)"
    ).collect()
    assert spark.table(f"{table}.snapshots").where(f"snapshot_id={expendable}").count() == 0
    results["checks"].append("unprotected_snapshot_actually_expired")
    assert spark.read.option("snapshot-id", str(base)).table(table).first()["value"] == "before"
    inventory_args = orphan_inventory(spark, settings, table)
    spark.sql(
        f"CALL lake.system.remove_orphan_files(table=>'{short}',dry_run=>true{inventory_args})"
    ).collect()
    results["checks"].extend(["compaction", "expiration", "protected_snapshot", "orphan_dry_run"])
    # Expiring intermediate history can break ancestor traversal. A retained snapshot
    # remains readable, but rollback_to_snapshot requires an intact ancestor chain.
    spark.sql(f"CALL lake.system.set_current_snapshot('{short}',{base})")
    assert spark.table(table).first()["value"] == "before"
    results["checks"].append("retained_snapshot_reselected_after_expiration")
    # Real Iceberg divergence: audit and main both commit after their common ancestor.
    spark.sql(f"ALTER TABLE {table} CREATE BRANCH conflict AS OF VERSION {base}")
    spark.sql(f"UPDATE {table}.branch_conflict SET value='branch' WHERE id=1")
    spark.sql(f"UPDATE {table} SET value='legitimate_main' WHERE id=1")
    from py4j.protocol import Py4JJavaError
    from pyspark.errors import PySparkException

    try:
        spark.sql(f"CALL lake.system.fast_forward('{short}','main','conflict')").collect()
    except (PySparkException, Py4JJavaError):
        results["checks"].append("divergence_rejected")
    else:
        raise AssertionError("A divergent branch was incorrectly fast-forwarded")
    assert spark.table(table).first()["value"] == "legitimate_main"
    spark.sql(f"ALTER TABLE {table} DROP BRANCH conflict")
    head = spark.sql(f"SELECT snapshot_id FROM {table}.refs WHERE name='main'").first()[0]
    spark.sql(f"ALTER TABLE {table} CREATE BRANCH conflict AS OF VERSION {head}")
    spark.sql(f"UPDATE {table}.branch_conflict SET annotation='reviewed' WHERE id=1")
    spark.sql(f"CALL lake.system.fast_forward('{short}','main','conflict')")
    assert spark.table(table).first()["value"] == "legitimate_main"
    results["checks"].append("divergence_rebuilt_preserving_main")
    results["passed"] = True
    json_write(settings.root / "compatibility.json", results)
    # Keep isolated compatibility data as evidence; no destructive cleanup by default.
    return results
