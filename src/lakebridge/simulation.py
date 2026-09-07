"""A size-calibrated, multi-executor S3/Parquet/Iceberg experiment.

This is a synthetic storage/performance experiment, not a run of the SaaS migration
oracle. Every source event has a unique ID and independently generated trace data.
"""

from __future__ import annotations

import argparse
import json
import math
import statistics
import time
from contextlib import contextmanager
from pathlib import Path

from pyspark.sql import functions as F

from lakebridge.benchmark import plan_metrics
from lakebridge.common import LakeBridgeError, identifier, json_write, now
from lakebridge.config import Settings, spark_session
from lakebridge.simulation_requests import RequestCounter

GIB = 1024**3


class Experiment:
    def __init__(self, spark, settings, run_id):
        self.spark, self.settings = spark, settings
        self.run_id = identifier(run_id)
        self.output = settings.root / "runs" / run_id
        self.output.mkdir(parents=True, exist_ok=True)
        self.stages = []
        self.observation_errors = []
        self.request_counter = RequestCounter(settings.root / "s3-access.jsonl")

    def requests(self):
        try:
            return self.request_counter.read()
        except (OSError, ValueError) as exc:
            self.observation_errors.append(str(exc))
            return None

    @contextmanager
    def stage(self, name):
        self.spark.sparkContext.setJobGroup(name, name)
        before = self.requests()
        begin = time.perf_counter()
        entry = {"name": name, "started_at": now(), "status": "running"}
        self.stages.append(entry)
        json_write(self.settings.root / "progress.json", {"run_id": self.run_id, **entry})
        print(json.dumps({"stage": name, "event": "start", "time": now()}), flush=True)
        try:
            yield entry
            entry["status"] = "passed"
        except BaseException as exc:
            entry["status"] = "failed"
            entry["error"] = str(exc)
            raise
        finally:
            entry["seconds"] = time.perf_counter() - begin
            entry["finished_at"] = now()
            after = self.requests()
            entry["s3_requests"] = (
                {k: v - before.get(k, 0) for k, v in after.items()}
                if before is not None and after is not None
                else None
            )
            json_write(self.output / "stages.json", self.stages)
            json_write(self.settings.root / "progress.json", {"run_id": self.run_id, **entry})
            print(
                json.dumps(
                    {
                        "stage": name,
                        "event": "end",
                        "status": entry["status"],
                        "seconds": entry["seconds"],
                    }
                ),
                flush=True,
            )

    def data(self, start, count, partitions):
        payload = (
            "concat("
            + ",".join(
                f"sha2(concat(cast(id as string),':trace_field_{i}'),256)" for i in range(16)
            )
            + ")"
        )
        return (
            self.spark.range(start, start + count, numPartitions=partitions)
            .selectExpr(
                "id event_id",
                "CASE WHEN pmod(xxhash64(id),100)<60 THEN 0 "
                "ELSE pmod(xxhash64(id,17),999)+1 END tenant_id",
                "timestampadd(SECOND,cast(pmod(xxhash64(id,29),2592000) as int),"
                "TIMESTAMP '2025-01-01 00:00:00') event_time",
                "cast(pmod(xxhash64(id,31),1000000)/1000 as DECIMAL(18,6)) units",
                payload + " trace_payload",
            )
            .withColumn("business_date", F.to_date("event_time"))
        )

    def source_stats(self, uri):
        path = self.spark._jvm.org.apache.hadoop.fs.Path(uri)
        filesystem = path.getFileSystem(self.spark._jsc.hadoopConfiguration())
        iterator = filesystem.listFiles(path, True)
        count, size = 0, 0
        manifest = self.output / ("pilot-files.jsonl" if "pilot" in uri else "source-files.jsonl")
        with manifest.open("w") as out:
            while iterator.hasNext():
                item = iterator.next()
                if item.getPath().getName().endswith(".parquet"):
                    count += 1
                    size += item.getLen()
                    out.write(
                        json.dumps({"path": str(item.getPath()), "bytes": item.getLen()}) + "\n"
                    )
        return {"files": count, "compressed_bytes": size, "compressed_gib": size / GIB}

    def table_stats(self, table):
        return (
            self.spark.sql(f"""SELECT count(*) files, sum(file_size_in_bytes) compressed_bytes,
            min(file_size_in_bytes) min_file_bytes, max(file_size_in_bytes) max_file_bytes,
            percentile_approx(file_size_in_bytes,0.5) median_file_bytes FROM {table}.files""")
            .first()
            .asDict()
        )

    def fingerprint(self, frame):
        # Hash every semantic column, not metadata-only count or payload length.
        return (
            frame.selectExpr(
                "count(*) rows",
                "min(event_id) min_id",
                "max(event_id) max_id",
                "sum(units) units",
                "sum(cast(xxhash64(event_id,tenant_id,event_time,units,trace_payload)"
                " as decimal(38,0))) row_hash_sum",
            )
            .first()
            .asDict()
        )

    def measure(self, label, table, query, repetitions):
        statement = query.format(table=table)
        explain = self.spark.sql("EXPLAIN FORMATTED " + statement).first()[0]
        (self.output / f"{label}.plan.txt").write_text(explain)
        with self.stage(label + "_warmup"):
            self.spark.sql(statement).collect()
        samples, results, metrics, request_samples = [], [], [], []
        for attempt in range(repetitions):
            self.spark.catalog.clearCache()
            with self.stage(f"{label}_repeat_{attempt + 1}") as entry:
                start = time.perf_counter()
                frame = self.spark.sql(statement)
                rows = [row.asDict() for row in frame.collect()]
                samples.append(time.perf_counter() - start)
                results.append(json.dumps(rows, sort_keys=True, default=str))
                metrics.append(plan_metrics(frame))
            request_samples.append(entry["s3_requests"])
        if len(set(results)) != 1:
            raise LakeBridgeError("Unstable query result")
        result = {
            "table": table,
            "sql": statement,
            "seconds": samples,
            "median_seconds": statistics.median(samples),
            "min_seconds": min(samples),
            "max_seconds": max(samples),
            "stdev_seconds": statistics.stdev(samples),
            "result": json.loads(results[0]),
            "plan_metrics": metrics,
            "request_samples": request_samples,
        }
        json_write(self.output / f"{label}.json", result)
        return result

    def execute(self, target_gib, repetitions, write_partitions):
        total_started = time.perf_counter()
        spark = self.spark
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS lake.{self.settings.namespace}")
        if not spark.sparkContext.master.startswith("spark://"):
            raise LakeBridgeError("Simulation requires a real Spark standalone master")
        pilot_uri = f"s3a://simulation-raw/{self.run_id}/pilot"
        source_uri = f"s3a://simulation-raw/{self.run_id}/source"
        with self.stage("calibrate") as entry:
            pilot_rows = 120000
            self.data(0, pilot_rows, 12).write.option("compression", "zstd").parquet(pilot_uri)
            pilot = self.source_stats(pilot_uri)
            bytes_per_row = pilot["compressed_bytes"] / pilot_rows
            rows = max(pilot_rows, math.ceil(target_gib * GIB / bytes_per_row * 1.02))
            entry.update(pilot_rows=pilot_rows, bytes_per_row=bytes_per_row, planned_rows=rows)
        with self.stage("generate_source") as entry:
            self.data(0, rows, write_partitions).write.option("compression", "zstd").partitionBy(
                "business_date"
            ).parquet(source_uri)
            source_stats = self.source_stats(source_uri)
            # Calibration is approximate. Add new unique event IDs if compression is better.
            for _ in range(3):
                if source_stats["compressed_bytes"] >= target_gib * GIB:
                    break
                missing = target_gib * GIB - source_stats["compressed_bytes"]
                extra = math.ceil(missing / (source_stats["compressed_bytes"] / rows) * 1.05)
                self.data(rows, extra, 12).write.mode("append").option(
                    "compression", "zstd"
                ).partitionBy("business_date").parquet(source_uri)
                rows += extra
                source_stats = self.source_stats(source_uri)
            if source_stats["compressed_bytes"] < target_gib * GIB:
                raise LakeBridgeError("Source did not meet requested compressed byte target")
            entry.update(source_stats)
            entry["rows"] = rows
        source = spark.read.parquet(source_uri)
        source.createOrReplaceTempView("simulation_legacy")
        with self.stage("source_fingerprint") as entry:
            expected = self.fingerprint(source)
            entry["fingerprint"] = expected
            if (
                expected["rows"] != rows
                or expected["min_id"] != 0
                or expected["max_id"] != rows - 1
            ):
                raise LakeBridgeError("Source row count/range differs from generated contract")
        tables, stats, validation = {}, {}, {}
        for variant in ("unpartitioned", "partitioned"):
            table = self.settings.table(self.run_id + "_" + variant)
            tables[variant] = table
            with self.stage("write_" + variant):
                writer = (
                    source.repartition(write_partitions, "event_id")
                    .writeTo(table)
                    .using("iceberg")
                    .tableProperty("format-version", "2")
                    .tableProperty("write.parquet.compression-codec", "zstd")
                    .tableProperty("write.distribution-mode", "none")
                    .tableProperty("write.spark.fanout.enabled", "true")
                    .tableProperty("write.target-file-size-bytes", str(128 * 1024**2))
                )
                if variant == "partitioned":
                    writer = writer.partitionedBy(F.days("event_time"))
                writer.create()
            with self.stage("validate_" + variant):
                actual = self.fingerprint(spark.table(table))
                if actual != expected:
                    raise LakeBridgeError(f"Fingerprint mismatch: {variant}")
                # Exact distributed multiset comparison; excludes the hash-collision shortcut.
                columns = source.columns
                target = spark.table(table).select(*columns)
                if (
                    source.exceptAll(target).limit(1).count()
                    or target.exceptAll(source).limit(1).count()
                ):
                    raise LakeBridgeError(f"Exact row mismatch: {variant}")
                validation[variant] = {"passed": True, "fingerprint": actual, "except_all": True}
            stats[variant] = self.table_stats(table)
        filtered = """SELECT count(*) events,sum(units) units FROM {table}
            WHERE event_time>=TIMESTAMP '2025-01-15 00:00:00'
              AND event_time<TIMESTAMP '2025-01-16 00:00:00'"""
        legacy_filtered = filtered + " AND business_date=DATE '2025-01-15'"
        full_scan = """SELECT count(*) events,sum(units) units,
            sum(cast(xxhash64(trace_payload) as decimal(38,0))) payload_checksum FROM {table}"""
        queries = {
            "legacy_date": self.measure(
                "legacy_date", "simulation_legacy", legacy_filtered, repetitions
            ),
            "unpartitioned_date": self.measure(
                "unpartitioned_date", tables["unpartitioned"], filtered, repetitions
            ),
            "partitioned_date": self.measure(
                "partitioned_date", tables["partitioned"], filtered, repetitions
            ),
            "before_compaction": self.measure(
                "before_compaction", tables["partitioned"], full_scan, repetitions
            ),
        }
        table = tables["partitioned"]
        before_snapshot = spark.sql(
            f"SELECT snapshot_id FROM {table}.refs WHERE name='main'"
        ).first()[0]
        spark.sql(
            f"ALTER TABLE {table} CREATE TAG before_compaction AS OF VERSION {before_snapshot}"
        )
        with self.stage("compaction") as compaction:
            short = table.removeprefix("lake.")
            result = spark.sql(
                f"CALL lake.system.rewrite_data_files(table=>'{short}',"
                "options=>map('rewrite-all','true','target-file-size-bytes','134217728',"
                "'max-concurrent-file-group-rewrites','3'))"
            ).collect()
            compaction["result"] = [row.asDict() for row in result]
        stats["compacted"] = self.table_stats(table)
        with self.stage("validate_compaction"):
            after = spark.table(table)
            before = spark.read.option("snapshot-id", str(before_snapshot)).table(table)
            if self.fingerprint(after) != expected:
                raise LakeBridgeError("Compaction fingerprint mismatch")
            if after.exceptAll(before).limit(1).count() or before.exceptAll(after).limit(1).count():
                raise LakeBridgeError("Compaction changed exact data")
            validation["compaction"] = {
                "passed": True,
                "except_all": True,
                "retained_snapshot": before_snapshot,
            }
        queries["after_compaction"] = self.measure(
            "after_compaction", table, full_scan, repetitions
        )
        if not (
            queries["legacy_date"]["result"]
            == queries["unpartitioned_date"]["result"]
            == queries["partitioned_date"]["result"]
        ):
            raise LakeBridgeError("Date query results differ")
        if queries["before_compaction"]["result"] != queries["after_compaction"]["result"]:
            raise LakeBridgeError("Full scan results differ")
        report = {
            "run_id": self.run_id,
            "passed": True,
            "scope": "single-host multi-container simulation",
            "target_compressed_gib": target_gib,
            "source": {**source_stats, "rows": rows, "uri": source_uri},
            "schema": source.schema.jsonValue(),
            "generator": "16 independent SHA-256 trace fields per unique ID; not duplicated rows",
            "payload_hex_characters": 1024,
            "pilot": pilot,
            "tables": tables,
            "file_statistics": stats,
            "validation": validation,
            "queries": queries,
            "stages": self.stages,
            "repetitions": repetitions,
            "cache": (
                "one warmup per query; clear Spark cache each repeat; "
                "shared-host OS/S3 cache uncontrolled"
            ),
            "engine": {
                "spark": spark.version,
                "master": spark.sparkContext.master,
                "application_id": spark.sparkContext.applicationId,
                "shuffle_partitions": spark.conf.get("spark.sql.shuffle.partitions"),
                "executor_cores": spark.sparkContext.getConf().get("spark.executor.cores"),
                "executor_memory": spark.sparkContext.getConf().get("spark.executor.memory"),
            },
            "total_application_seconds": time.perf_counter() - total_started,
            "observation_errors": self.observation_errors,
            "retention": "before_compaction tag retained; physical old files still occupy storage",
        }
        json_write(self.output / "report.json", report)
        print(
            json.dumps(
                {
                    "passed": True,
                    "report": str(self.output / "report.json"),
                    "rows": rows,
                    "source_gib": source_stats["compressed_gib"],
                }
            ),
            flush=True,
        )
        return report


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--target-gib", type=float, default=10)
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--write-partitions", type=int, default=24)
    args = parser.parse_args()
    if not 0.01 <= args.target_gib <= 100 or args.repetitions < 3 or args.write_partitions < 12:
        parser.error("target-gib must be 0.01..100, repetitions>=3, write-partitions>=12")
    identifier(args.run_id)
    settings = Settings(Path("/workspace/artifacts/simulation"), "simulation")
    spark = spark_session(settings)
    try:
        Experiment(spark, settings, args.run_id).execute(
            args.target_gib, args.repetitions, args.write_partitions
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
