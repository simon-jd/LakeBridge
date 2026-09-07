"""Qualify a runtime using isolated tables and a fresh audit store; no benchmark claims."""

import argparse
from pathlib import Path

from lakebridge.common import LakeBridgeError, json_write
from lakebridge.compatibility import compatibility
from lakebridge.config import Settings, spark_session
from lakebridge.control import Control
from lakebridge.demo import demo
from lakebridge.maintenance import maintain
from lakebridge.observability import inspect_audit, prometheus, write_textfile
from lakebridge.pipeline import Pipeline


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--namespace", required=True)
    args = parser.parse_args()
    settings = Settings(args.root, args.namespace)
    control = Control(settings.root, settings.audit_dsn)
    try:
        control.bind(settings.namespace, settings.warehouse)
        if control.releases() or control.status()["writer_run"]:
            raise LakeBridgeError("Runtime qualification requires a fresh, isolated audit store")
    finally:
        control.close()
    spark = spark_session(settings)
    pipeline = None
    try:
        results = {"compatibility": compatibility(spark, settings)}
        pipeline = Pipeline(spark, settings)
        results["demo"] = demo(pipeline, rows=40, days=7)
        results["compaction"] = maintain(pipeline, "compact", execute=True)
        results["orphan_plan"] = maintain(pipeline, "orphans")
        results["audit"] = inspect_audit(settings)
        results["passed"] = bool(results["compatibility"]["passed"] and results["demo"]["passed"])
        json_write(settings.root / "runtime-verification.json", results)
        write_textfile(settings.root / "audit.prom", prometheus(results["audit"]))
    finally:
        if pipeline:
            pipeline.close()
        spark.stop()


if __name__ == "__main__":
    main()
