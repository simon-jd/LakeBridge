from __future__ import annotations

from pyspark.sql import Window
from pyspark.sql import functions as F

from lakebridge.common import LakeBridgeError
from lakebridge.contracts import InputSet


def reject_if_any(frame, reason, directory):
    if frame.limit(1).count():
        frame.write.mode("overwrite").parquet(str(directory / reason))
        raise LakeBridgeError(f"Contract rejected: {reason}; see {directory / reason}")


def prepare(spark, inputs: InputSet, quarantine, rule: str):
    if rule not in ("v1", "v2"):
        raise LakeBridgeError("Unknown rule version")
    raw = spark.read.parquet(*inputs.files("events")).withColumn("source_file", F.input_file_name())
    plans = spark.read.parquet(*inputs.files("plans")).dropDuplicates()
    invalid = raw.where("""tenant_id IS NULL OR trim(tenant_id)='' OR event_id IS NULL
        OR trim(event_id)='' OR event_time IS NULL OR ingested_at IS NULL OR units IS NULL
        OR units<0 OR source_version IS NULL OR source_version<1 OR is_deleted IS NULL""")
    reject_if_any(invalid, "invalid_events", quarantine)
    payload = ["event_time", "ingested_at", "units", "is_deleted"]
    conflicts = (
        raw.groupBy("tenant_id", "event_id", "source_version")
        .agg(F.countDistinct(F.struct(*payload)).alias("variants"))
        .where("variants>1")
    )
    reject_if_any(
        raw.join(conflicts, ["tenant_id", "event_id", "source_version"]),
        "same_version_conflict",
        quarantine,
    )
    moved = (
        raw.groupBy("tenant_id", "event_id")
        .agg(F.countDistinct("event_time").alias("dates"))
        .where("dates>1")
    )
    reject_if_any(raw.join(moved, ["tenant_id", "event_id"]), "event_time_mutation", quarantine)
    invalid_plans = plans.where("""tenant_id IS NULL OR plan_id IS NULL
        OR plan_id NOT IN ('basic','pro') OR effective_from IS NULL OR attributes IS NULL
        OR (effective_to IS NOT NULL AND effective_to<=effective_from)""")
    reject_if_any(invalid_plans, "invalid_plans", quarantine)
    # Running MAX catches nested overlaps, including a prior open-ended interval.
    history = (
        Window.partitionBy("tenant_id")
        .orderBy("effective_from")
        .rowsBetween(Window.unboundedPreceding, -1)
    )
    overlaps = plans.withColumn(
        "previous_end",
        F.max(F.coalesce(F.col("effective_to"), F.lit("9999-12-31").cast("timestamp"))).over(
            history
        ),
    ).where("previous_end>effective_from")
    reject_if_any(overlaps, "overlapping_plans", quarantine)
    order = Window.partitionBy("tenant_id", "event_id").orderBy(
        F.col("source_version").desc(), F.col("ingested_at").desc(), F.col("source_file")
    )
    latest = raw.withColumn("rn", F.row_number().over(order)).where("rn=1").drop("rn")
    events, dims = latest.alias("e"), plans.alias("p")
    joined = events.join(
        dims,
        (F.col("e.tenant_id") == F.col("p.tenant_id"))
        & (F.col("e.event_time") >= F.col("p.effective_from"))
        & (F.col("p.effective_to").isNull() | (F.col("e.event_time") < F.col("p.effective_to"))),
        "left",
    ).select("e.*", "p.plan_id")
    reject_if_any(joined.where("plan_id IS NULL"), "unmatched_plan", quarantine)
    factor = (
        F.when(F.col("plan_id") == "pro", F.lit("0.9").cast("decimal(2,1)")).otherwise(F.lit(1))
        if rule == "v2"
        else F.lit(1)
    )
    result = (
        joined.withColumn("business_date", F.to_date("event_time"))
        .withColumn("billable_units", (F.col("units") * factor).cast("decimal(18,6)"))
        .withColumn("rule_version", F.lit(rule))
    )
    return result, plans


def aggregate(detail):
    return (
        detail.where("NOT is_deleted")
        .groupBy("tenant_id", "business_date")
        .agg(
            F.count("*").alias("event_count"),
            F.sum("units").cast("decimal(28,6)").alias("total_units"),
            F.sum("billable_units").cast("decimal(28,6)").alias("billable_units"),
        )
    )
