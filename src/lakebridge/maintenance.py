import uuid
from datetime import UTC, datetime, timedelta

import pyarrow as pa
import pyarrow.parquet as pq

from lakebridge.common import LakeBridgeError, json_write, literal


def orphan_inventory(spark, settings, table):
    """Supply S3FileIO inventory instead of adding a second Hadoop S3 client stack."""
    if not settings.warehouse.startswith("s3://"):
        return ""
    java_table = spark._jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(
        spark._jsparkSession, table
    )
    iterator = java_table.io().listPrefix(java_table.location() + "/").iterator()
    inventory = settings.root / "maintenance" / "inventories" / uuid.uuid4().hex
    inventory.mkdir(parents=True)
    schema = pa.schema(
        [("file_path", pa.string()), ("last_modified", pa.timestamp("us", tz="UTC"))]
    )
    buffer, index = [], 0
    while iterator.hasNext():
        item = iterator.next()
        buffer.append(
            {
                "file_path": item.location(),
                "last_modified": datetime.fromtimestamp(item.createdAtMillis() / 1000, UTC),
            }
        )
        if len(buffer) == 1000:
            pq.write_table(
                pa.Table.from_pylist(buffer, schema=schema), inventory / f"{index}.parquet"
            )
            index += 1
            buffer = []
    if buffer or index == 0:
        pq.write_table(pa.Table.from_pylist(buffer, schema=schema), inventory / f"{index}.parquet")
    spark.read.parquet(str(inventory)).createOrReplaceTempView("orphan_inventory")
    return ",file_list_view=>'orphan_inventory'"


def maintain(
    pipeline, action, name="detail", execute=False, retention_days=7, longest_job_hours=24
):
    if retention_days < 1 or longest_job_hours < 1:
        raise LakeBridgeError("Retention and maximum job duration must be positive")
    spark, settings = pipeline.spark, pipeline.settings
    table = pipeline.table(name)
    short = f"{settings.namespace}.{name}"
    if action == "inspect":
        statistics = spark.sql(f"""SELECT count(*) files, sum(file_size_in_bytes) bytes,
            min(file_size_in_bytes) min_bytes, max(file_size_in_bytes) max_bytes,
            percentile_approx(file_size_in_bytes,0.5) median_bytes FROM {table}.files""").first()
        return {
            "table": table,
            "files": statistics.asDict(),
            "snapshots": spark.table(f"{table}.snapshots").count(),
            "references": spark.table(f"{table}.refs").count(),
        }
    # Three days also respects Iceberg's default orphan-cleanup safety floor.
    safe_hours = max(72, longest_job_hours * 2 + 24)
    age = timedelta(hours=safe_hours) if action == "orphans" else timedelta(days=retention_days)
    cutoff = (datetime.now(UTC) - age).strftime("%Y-%m-%d %H:%M:%S")
    report = {
        "table": table,
        "action": action,
        "execute": execute,
        "older_than": cutoff,
        "orphan_minimum_age_hours": safe_hours,
    }
    run_id = "maintenance"
    with pipeline.control.writer(run_id):
        try:
            # Every published snapshot must still be pinned. Do not repair silently.
            for release in pipeline.control.releases():
                pointer = release["tables"][name]
                if pipeline.snapshot(name, release["release_id"]) != pointer["snapshot_id"]:
                    raise LakeBridgeError(f"Missing release retention tag: {release['release_id']}")
            if action == "orphans":
                args = f"table=>'{short}',older_than=>TIMESTAMP {literal(cutoff)}"
                args += orphan_inventory(spark, settings, table)
                candidates = spark.sql(
                    f"CALL lake.system.remove_orphan_files({args},dry_run=>true)"
                )
                evidence = (
                    settings.root
                    / "maintenance"
                    / name
                    / datetime.now(UTC).strftime("%Y%m%d%H%M%S%f")
                )
                candidates.write.mode("overwrite").json(str(evidence / "orphan_candidates"))
                report["candidates"] = candidates.count()
                report["evidence"] = str(evidence)
                json_write(evidence / "plan.json", report)
                if execute:
                    report["deleted"] = spark.sql(
                        f"CALL lake.system.remove_orphan_files({args},dry_run=>false)"
                    ).count()
            elif action == "compact" and execute:
                report["result"] = [
                    r.asDict()
                    for r in spark.sql(
                        f"CALL lake.system.rewrite_data_files(table=>'{short}',"
                        "options=>map('target-file-size-bytes','134217728','min-input-files','2'))"
                    ).collect()
                ]
            elif action == "expire" and execute:
                report["result"] = [
                    r.asDict()
                    for r in spark.sql(
                        f"CALL lake.system.expire_snapshots(table=>'{short}',"
                        f"older_than=>TIMESTAMP {literal(cutoff)},retain_last=>5)"
                    ).collect()
                ]
            elif action not in ("compact", "expire"):
                raise LakeBridgeError("Unknown maintenance action")
            json_write(settings.root / "maintenance" / f"{name}_{action}.json", report)
        finally:
            pipeline.control.finish_writer(run_id)
    return report
