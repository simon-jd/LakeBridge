from pathlib import Path

from pyspark.sql import functions as F

from lakebridge.common import json_write
from lakebridge.oracle import SEMANTIC_COLUMNS, SUMMARY_COLUMNS


def compare(actual, expected, columns, directory: Path, name: str) -> dict:
    left = actual.select(*columns)
    right = expected.select(*columns)
    missing = right.exceptAll(left).persist()
    unexpected = left.exceptAll(right).persist()
    try:
        missing_count, unexpected_count = missing.count(), unexpected.count()
        if missing_count:
            missing.write.mode("overwrite").parquet(str(directory / f"{name}_missing"))
        if unexpected_count:
            unexpected.write.mode("overwrite").parquet(str(directory / f"{name}_unexpected"))
        return {
            "missing": missing_count,
            "unexpected": unexpected_count,
            "passed": missing_count == unexpected_count == 0,
        }
    finally:
        missing.unpersist()
        unexpected.unpersist()


def validate(
    spark, detail, summary, expected_dir: Path, output: Path, previous=None, scope=None
) -> dict:
    output.mkdir(parents=True, exist_ok=True)
    expected = spark.read.parquet(str(expected_dir / "detail.parquet"))
    expected_summary = spark.read.parquet(str(expected_dir / "summary.parquet"))
    keys = ["tenant_id", "event_id"]
    report = {
        "keys": compare(detail, expected, keys, output, "keys"),
        "detail": compare(detail, expected, SEMANTIC_COLUMNS, output, "detail"),
        "daily": compare(summary, expected_summary, SUMMARY_COLUMNS, output, "daily"),
        "duplicate_keys": detail.groupBy(*keys).count().where("count>1").count(),
        "null_keys": detail.where("tenant_id IS NULL OR event_id IS NULL").count(),
        "tombstones": detail.where("is_deleted").count(),
        "detail_rows": detail.count(),
        "live_rows": detail.where("NOT is_deleted").count(),
    }
    if previous is not None and scope:
        outside = ~F.col("business_date").between(*scope)
        report["outside_range"] = compare(
            detail.where(outside), previous.where(outside), detail.columns, output, "outside_range"
        )
    report["passed"] = (
        all(v["passed"] for v in report.values() if isinstance(v, dict))
        and report["duplicate_keys"] == report["null_keys"] == 0
    )
    json_write(output / "report.json", report)
    return report
