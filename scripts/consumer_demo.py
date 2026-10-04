"""Reproducible local Iceberg -> SQL diff -> approved PostgreSQL/dbt publication.

This is a scripted demo with separate configured human/agent identities. It does
not give model tools approval authority. Output retains the actual source pins,
plans, approvals and job results; the isolated PostgreSQL schemas remain available.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
from pathlib import Path

from lakebridge.common import digest, json_write
from lakebridge.config import Settings, spark_session
from lakebridge.consumer.diff_backend import wire_row
from lakebridge.consumer.jobs import JobService, run_comparison
from lakebridge.consumer.plans import PlanService
from lakebridge.consumer.protocol import Scope
from lakebridge.consumer.releases import ReleaseRegistry, export_snapshot
from lakebridge.consumer.sink_postgres import PostgresSink
from lakebridge.consumer.store import Store
from lakebridge.generate import generate
from lakebridge.pipeline import Pipeline


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--namespace", required=True)
    args = parser.parse_args()
    root = args.root.resolve()
    if root.exists():
        parser.error("Use a new output root; prior evidence is immutable")
    root.mkdir(parents=True)
    settings = Settings(root / "lake", args.namespace)
    spark = spark_session(settings)
    pipeline = Pipeline(spark, settings)
    store = Store(os.environ["LAKEBRIDGE_CONSUMER_DSN"], args.namespace)
    trace = []
    try:
        store.initialize()
        manifest = generate(root / "source", rows=30, tenants=2, days=7)
        dataset = json.loads(manifest.read_text())["dataset_id"]
        upstream = [
            pipeline.execute(
                manifest, watermark, reason="consumer demo", run_id=f"demo_{watermark}"
            )
            for watermark in (1, 2)
        ]
        registry = ReleaseRegistry(root / "exports")
        for release in upstream:
            export_snapshot(
                spark,
                pipeline.control,
                registry,
                dataset,
                release["release_id"],
                root / "export-work",
            )
        store.register("daily_t0000", dataset, Scope("t0000", "2025-01-01", "2025-01-08"))
        store.grant("agent", "daily_t0000", ["read", "plan", "execute"])
        store.grant("human", "daily_t0000", ["read", "approve"])
        plans = PlanService(store, registry, root / "diffs")
        jobs = JobService(plans)
        sink = PostgresSink(
            store,
            registry,
            dbt_executable=Path(".venv-dbt/bin/dbt"),
            project=Path("consumer-dbt"),
            artifacts=root / "jobs",
        )
        source = None
        for index, release in enumerate(upstream):
            comparison = plans.compare("agent", "daily_t0000", source, release["release_id"])
            run_comparison(plans, comparison["comparison_id"])
            comparison = plans.comparison("agent", comparison["comparison_id"])
            plan = plans.create("agent", comparison["comparison_id"], "daily_t0000")
            approval = plans.approve("human", plan["plan_id"], plan["plan_hash"], "approved")
            job = jobs.submit("agent", plan["plan_id"], approval["approval_id"], f"demo-{index}")
            result = jobs.run(job["job_id"], sink)
            retry = jobs.submit("agent", plan["plan_id"], approval["approval_id"], f"demo-{index}")
            assert retry["job_id"] == result["job_id"] and result["state"] == "SUCCEEDED"
            trace.append(
                {
                    "comparison": comparison,
                    "plan": plan,
                    "approval": approval,
                    "job": result,
                    "retry_job_id": retry["job_id"],
                }
            )
            source = release["release_id"]
        rows = store.execute("""SELECT tenant_id,business_date,event_count,
            total_units,billable_units
            FROM {serving}.tenant_daily_usage ORDER BY tenant_id,business_date""").fetchall()
        expected = (
            pipeline.read_snapshot("daily", upstream[-1]["tables"]["daily"]["snapshot_id"])
            .where(
                "tenant_id='t0000' AND business_date >= DATE '2025-01-01'"
                " AND business_date < DATE '2025-01-08'"
            )
            .orderBy("tenant_id", "business_date")
            .collect()
        )
        expected = [wire_row(r.asDict()) for r in expected]
        actual = [wire_row(r) for r in rows]
        assert actual == expected, (actual, expected)
        report = {
            "status": "passed",
            "scope": "local synthetic data, real Spark/Iceberg/PostgreSQL/dbt",
            "rows_match_target_full_aggregate": True,
            "trace": trace,
            "usage": actual,
            "source_manifest_sha256": digest(manifest),
            "schemas": store.schemas,
            "python": platform.python_version(),
            "consumer": store.consumer("daily_t0000"),
            "approval_mode": "scripted human identity; no model approval tool",
        }
        json_write(root / "report.json", report)
        print(
            json.dumps(
                {
                    "status": "passed",
                    "report": str(root / "report.json"),
                    "consumer_release_id": report["consumer"]["consumer_release_id"],
                }
            )
        )
    finally:
        store.close()
        pipeline.close()
        spark.stop()


if __name__ == "__main__":
    main()
