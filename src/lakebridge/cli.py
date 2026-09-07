from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from lakebridge.common import LakeBridgeError


def parser():
    p = argparse.ArgumentParser(prog="lakebridge", description="Auditable Iceberg migration lab")
    p.add_argument("--root", type=Path, default=Path("artifacts/local"))
    p.add_argument("--namespace", default="saas")
    sub = p.add_subparsers(dest="command", required=True)
    gen = sub.add_parser("generate", help="Generate immutable SaaS Parquet, manifests and oracle")
    gen.add_argument("--output", type=Path, default=Path("artifacts/source"))
    gen.add_argument("--rows", type=int, default=3000)
    gen.add_argument("--tenants", type=int, default=30)
    gen.add_argument("--days", type=int, default=90)
    gen.add_argument("--seed", type=int, default=42)
    gen.add_argument("--file-rows", type=int, default=5000)
    gen.add_argument("--hot-share", type=float, default=0.55)
    check = sub.add_parser("verify-manifest")
    check.add_argument("manifest", type=Path)
    check.add_argument("--watermark", type=int)
    stage = sub.add_parser("stage-s3", help="Stage a hash-pinned S3 input manifest and exact files")
    stage.add_argument("uri")
    stage.add_argument("--sha256", required=True)
    stage.add_argument("--output", type=Path, required=True)
    for command in ("migrate", "backfill", "dry-run"):
        run = sub.add_parser(command)
        run.add_argument("--manifest", type=Path, required=True)
        run.add_argument("--watermark", type=int)
        run.add_argument("--rule", choices=["v1", "v2"], default="v1")
        run.add_argument("--start")
        run.add_argument("--end")
        if command != "dry-run":
            run.add_argument("--reason", required=True)
            run.add_argument("--run-id")
            run.add_argument("--lock-timeout", type=float, default=0)
            run.add_argument(
                "--fail-at",
                choices=[
                    "after_detail_commit",
                    "after_fast_forward",
                    "before_audit",
                    "after_audit",
                ],
            )
    recover = sub.add_parser("recover")
    recover.add_argument("run_id")
    abort = sub.add_parser("abort")
    abort.add_argument("run_id")
    abort.add_argument("--reason", required=True)
    sub.add_parser("status")
    metrics = sub.add_parser("metrics", help="Read-only Prometheus audit metrics; no Spark JVM")
    metrics.add_argument("--output", type=Path, help="Atomically replace a textfile collector file")
    sub.add_parser("releases")
    query = sub.add_parser("query")
    query.add_argument("--release")
    query.add_argument("--limit", type=int, default=20)
    rollback = sub.add_parser("rollback")
    rollback.add_argument("release_id")
    rollback.add_argument("--reason", required=True)
    bench = sub.add_parser("benchmark")
    bench.add_argument("--rows", type=int, default=10000)
    bench.add_argument("--repetitions", type=int, default=3)
    bench.add_argument("--file-mib", type=int, choices=[128, 256], default=128)
    maint = sub.add_parser("maintain")
    maint.add_argument("action", choices=["inspect", "compact", "expire", "orphans"])
    maint.add_argument("--table", choices=["detail", "plans", "daily"], default="detail")
    maint.add_argument("--execute", action="store_true")
    maint.add_argument("--retention-days", type=int, default=7)
    maint.add_argument("--longest-job-hours", type=int, default=24)
    sub.add_parser("compatibility")
    demo = sub.add_parser("demo")
    demo.add_argument("--rows", type=int, default=200)
    demo.add_argument("--days", type=int, default=14)
    return p


def dispatch(args):
    from lakebridge.config import Settings, spark_session
    from lakebridge.control import Control

    settings = Settings(args.root, args.namespace)
    if args.command == "status":
        from lakebridge.observability import inspect_audit

        return inspect_audit(settings)
    if args.command == "releases":
        from lakebridge.observability import inspect_audit

        inspect_audit(settings)
        control = Control(settings.root, settings.audit_dsn, read_only=True)
        try:
            return control.releases()
        finally:
            control.close()
    from lakebridge.contracts import verify_manifest
    from lakebridge.generate import generate

    if args.command == "stage-s3":
        from lakebridge.adapters import stage_s3

        return {"manifest": stage_s3(args.uri, args.sha256, args.output)}
    if args.command == "generate":
        return {
            "manifest": generate(
                args.output,
                args.rows,
                args.tenants,
                args.days,
                args.seed,
                args.file_rows,
                args.hot_share,
            )
        }
    if args.command == "verify-manifest":
        inputs = verify_manifest(args.manifest, args.watermark)
        return {
            "passed": True,
            "high_watermark": inputs.watermark,
            "files": len(inputs.entries),
            "sha256": inputs.fingerprint,
        }
    from lakebridge.pipeline import Pipeline

    spark = spark_session(settings)
    pipeline = Pipeline(spark, settings)
    try:
        if args.command in ("migrate", "backfill"):
            if args.command == "backfill" and not (args.start and args.end):
                raise LakeBridgeError("Backfill requires an explicit date range")
            return pipeline.execute(
                args.manifest,
                args.watermark,
                args.rule,
                args.start,
                args.end,
                args.reason,
                args.run_id,
                args.fail_at,
                args.lock_timeout,
            )
        if args.command == "dry-run":
            return pipeline.dry_run(args.manifest, args.watermark, args.rule, args.start, args.end)
        if args.command == "recover":
            return pipeline.recover(args.run_id)
        if args.command == "abort":
            return pipeline.abort(args.run_id, args.reason)
        if args.command == "rollback":
            return pipeline.activate(args.release_id, args.reason)
        if args.command == "query":
            return pipeline.query(args.release, args.limit)
        if args.command == "maintain":
            from lakebridge.maintenance import maintain

            return maintain(
                pipeline,
                args.action,
                args.table,
                args.execute,
                args.retention_days,
                args.longest_job_hours,
            )
        if args.command == "benchmark":
            from lakebridge.benchmark import benchmark

            return benchmark(spark, settings, args.rows, args.repetitions, args.file_mib)
        if args.command == "compatibility":
            from lakebridge.compatibility import compatibility

            return compatibility(spark, settings)
        if args.command == "demo":
            from lakebridge.demo import demo

            return demo(pipeline, args.rows, args.days)
        raise LakeBridgeError("Unknown command")
    finally:
        pipeline.close()
        spark.stop()


def main():
    try:
        args = parser().parse_args()
        if args.command == "metrics":
            from lakebridge.config import Settings
            from lakebridge.observability import (
                inspect_audit,
                prometheus,
                unavailable,
                write_textfile,
            )

            try:
                snapshot = inspect_audit(Settings(args.root, args.namespace))
            except LakeBridgeError as exc:
                snapshot = unavailable()
                print(json.dumps({"error": str(exc)}), file=sys.stderr)
            output = prometheus(snapshot)
            if args.output:
                try:
                    write_textfile(args.output, output)
                except OSError as exc:
                    raise LakeBridgeError("Cannot atomically write metrics output") from exc
            else:
                print(output, end="")
            if not snapshot["audit_available"]:
                sys.exit(2)
            return
        result = dispatch(args)
        print(json.dumps(result, indent=2, ensure_ascii=False, default=str))
    except LakeBridgeError as exc:
        print(json.dumps({"error": str(exc)}, ensure_ascii=False), file=sys.stderr)
        sys.exit(2)


if __name__ == "__main__":
    main()
