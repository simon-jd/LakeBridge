from __future__ import annotations

import json
import platform
import statistics
import time
import uuid

from pyspark.sql import functions as F

from lakebridge.common import LakeBridgeError, json_write


def benchmark(spark, settings, rows=10000, repetitions=3, file_mib=128):
    if rows < 100 or repetitions < 3 or file_mib not in (128, 256):
        raise LakeBridgeError("Benchmarks need rows>=100, repetitions>=3, file_mib=128 or 256")
    batch = "bench_" + uuid.uuid4().hex[:10]
    output = settings.root / "benchmarks" / batch
    output.mkdir(parents=True)
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS lake.{settings.namespace}")
    # Deterministic values, distinct event IDs, temporal distribution and tenant skew.
    data = spark.range(rows).selectExpr(
        "id event_id",
        "CASE WHEN id%10<6 THEN 0 ELSE id%100 END tenant_id",
        "timestampadd(DAY, cast(id%14 as int), TIMESTAMP '2025-01-01 00:00:00') event_time",
        "cast((id%997)/1000 AS DECIMAL(18,6)) units",
        "sha2(cast(id AS STRING),256) payload",
    )
    source_path = output / "legacy_parquet"
    data.repartition(4, "event_id").write.option("compression", "zstd").parquet(str(source_path))
    source = spark.read.parquet(str(source_path))
    source.createOrReplaceTempView("benchmark_legacy")
    tables = {}
    for variant, partitioned in (
        ("unpartitioned", False),
        ("partitioned", True),
        ("small_files", True),
    ):
        table = settings.table(f"{batch}_{variant}")
        tables[variant] = table
        frame = source.repartition(16 if variant == "small_files" else 4, "event_id")
        writer = (
            frame.writeTo(table)
            .using("iceberg")
            .tableProperty("format-version", "2")
            .tableProperty("write.target-file-size-bytes", str(file_mib * 1024 * 1024))
            .tableProperty("write.distribution-mode", "none")
            .tableProperty("write.parquet.compression-codec", "zstd")
            .tableProperty("write.spark.fanout.enabled", "true")
        )
        if partitioned:
            writer = writer.partitionedBy(F.days("event_time"))
        writer.create()

    def files(table):
        row = spark.sql(f"""SELECT count(*) file_count,sum(file_size_in_bytes) compressed_bytes,
            min(file_size_in_bytes) min_bytes,max(file_size_in_bytes) max_bytes,
            percentile_approx(file_size_in_bytes,0.5) median_bytes FROM {table}.files""").first()
        return row.asDict()

    def measured(label, table, query):
        statement = query.format(table=table)
        planning = time.perf_counter()
        explained = spark.sql("EXPLAIN FORMATTED " + statement).first()[0]
        explain_seconds = time.perf_counter() - planning
        (output / f"{label}.plan.txt").write_text(explained)
        spark.sql(statement).collect()  # Fixed one-result warmup, outside measured repetitions.
        durations, fingerprints, internal_metrics = [], [], []
        for _ in range(repetitions):
            spark.catalog.clearCache()
            start = time.perf_counter()
            frame = spark.sql(statement)
            result = [r.asDict() for r in frame.collect()]  # Query output is one aggregate row.
            durations.append(time.perf_counter() - start)
            fingerprints.append(json.dumps(result, sort_keys=True, default=str))
            internal_metrics.append(plan_metrics(frame))
        if len(set(fingerprints)) != 1:
            raise LakeBridgeError("Benchmark result changed across repetitions")
        return {
            "table": table,
            "query": statement,
            "seconds": durations,
            "median_seconds": statistics.median(durations),
            "min_seconds": min(durations),
            "max_seconds": max(durations),
            "stdev_seconds": statistics.stdev(durations),
            "explain_seconds": explain_seconds,
            "result": json.loads(fingerprints[0]),
            "files": files(table) if table != "benchmark_legacy" else None,
            "plan_metrics": internal_metrics,
        }

    filtered = """SELECT count(*) events, sum(units) units FROM {table}
        WHERE event_time>=TIMESTAMP '2025-01-07 00:00:00'
          AND event_time<TIMESTAMP '2025-01-08 00:00:00'"""
    scan = "SELECT count(*) events,sum(units) units,sum(length(payload)) payload_bytes FROM {table}"
    results = {
        "legacy": measured("legacy", "benchmark_legacy", filtered),
        "unpartitioned": measured("unpartitioned", tables["unpartitioned"], filtered),
        "partitioned": measured("partitioned", tables["partitioned"], filtered),
        "before_compaction": measured("before_compaction", tables["small_files"], scan),
    }
    begin = time.perf_counter()
    compaction = spark.sql(
        f"CALL lake.system.rewrite_data_files("
        f"table=>'{settings.namespace}.{batch}_small_files',"
        f"options=>map('rewrite-all','true','target-file-size-bytes','{file_mib * 1024 * 1024}'))"
    )
    compaction_result = [r.asDict() for r in compaction.collect()]
    compaction_seconds = time.perf_counter() - begin
    results["after_compaction"] = measured("after_compaction", tables["small_files"], scan)
    if not (
        results["legacy"]["result"]
        == results["unpartitioned"]["result"]
        == results["partitioned"]["result"]
    ):
        raise LakeBridgeError("Partition experiment changed query semantics")
    if results["before_compaction"]["result"] != results["after_compaction"]["result"]:
        raise LakeBridgeError("Compaction changed query semantics")
    report = {
        "benchmark_id": batch,
        "scope": "local synthetic experiment",
        "rows": rows,
        "repetitions": repetitions,
        "spark": spark.version,
        "python": platform.python_version(),
        "master": spark.sparkContext.master,
        "application_id": spark.sparkContext.applicationId,
        "shuffle_partitions": spark.conf.get("spark.sql.shuffle.partitions"),
        "adaptive": spark.conf.get("spark.sql.adaptive.enabled"),
        "driver_memory": spark.sparkContext.getConf().get("spark.driver.memory"),
        "cache": "Warm JVM and OS cache; Spark cached tables cleared each repetition. Not cold.",
        "compression": "zstd in both legacy Parquet and all Iceberg tables",
        "legacy_files": 4,
        "variant_order": "legacy, unpartitioned, partitioned, before, after; order bias possible",
        "target_file_mib": file_mib,
        "results": results,
        "compaction_seconds": compaction_seconds,
        "compaction_result": compaction_result,
        "passed": True,
        "evidence": str(output),
    }
    json_write(output / "report.json", report)
    lines = [
        "# 本地性能实验实测",
        "",
        f"数据量：{rows:,} 条；每项重复 {repetitions} 次。",
        "预热 JVM/OS 缓存，清空 Spark cache；单机结果不能外推至 10 GB 或云端。",
        "",
        "| 场景 | 中位数（秒） | 最小–最大（秒） | 文件数 |",
        "|---|---:|---:|---:|",
    ]
    for label, result in results.items():
        lines.append(
            f"| {label} | {result['median_seconds']:.4f} | "
            f"{result['min_seconds']:.4f}–{result['max_seconds']:.4f} | "
            f"{result['files']['file_count'] if result['files'] else 'Parquet source'} |"
        )
    for title, before, after in [
        ("日期分区", "unpartitioned", "partitioned"),
        ("文件合并", "before_compaction", "after_compaction"),
    ]:
        ratio = results[before]["median_seconds"] / results[after]["median_seconds"]
        lines.extend(
            [
                "",
                f"## {title}",
                "",
                f"前后查询结果完全一致。基线耗时 / 改动后耗时 = {ratio:.3f}。",
                "此数值小于 1 时表示变慢；小样本调度和缓存开销可能大于扫描收益。",
                f"执行计划见 `{before}.plan.txt` 和 `{after}.plan.txt`。",
            ]
        )
    lines.extend(
        [
            "",
            f"合并作业额外耗时 {compaction_seconds:.4f} 秒，必须计入总成本。",
            "未采集云账单，不推导成本节省；详细 SQL 指标与样本见 report.json，"
            "task 指标见 event logs。",
        ]
    )
    (output / "report.md").write_text("\n".join(lines) + "\n")
    return report


def plan_metrics(frame):
    """Read actual executed SQL metrics; missing engine metrics remain absent, never invented."""
    result = []

    def visit(node):
        if node.nodeName() == "AdaptiveSparkPlan":
            visit(node.executedPlan())
            return
        metrics = node.metrics().iterator()
        values = {}
        while metrics.hasNext():
            entry = metrics.next()
            metric = entry._2()
            values[str(entry._1())] = {"value": metric.value(), "type": metric.metricType()}
        if values:
            result.append({"node": node.nodeName(), "metrics": values})
        if node.nodeName() in ("ShuffleQueryStage", "BroadcastQueryStage"):
            visit(node.plan())
        children = node.children().iterator()
        while children.hasNext():
            visit(children.next())

    visit(frame._jdf.queryExecution().executedPlan())
    return result
