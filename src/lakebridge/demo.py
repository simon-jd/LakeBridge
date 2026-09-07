from datetime import date, timedelta
from time import perf_counter

from lakebridge.common import ConflictError, LakeBridgeError, json_write
from lakebridge.control import Control
from lakebridge.generate import generate


def demo(pipeline, rows=200, days=14):
    root = pipeline.settings.root
    if pipeline.control.releases():
        raise LakeBridgeError("Demo requires a fresh root and catalog namespace")
    manifest = generate(root / "source", rows=rows, tenants=5, days=days)
    report = {"scope": "local functional demonstration", "rows": rows, "scenarios": []}
    started = perf_counter()
    first = pipeline.execute(manifest, 1, reason="H0 full migration", run_id="demo_initial")
    replay = pipeline.execute(manifest, 1, reason="H0 full migration", run_id="demo_initial")
    assert replay["tables"] == first["tables"]
    report["scenarios"].append(
        {
            "name": "full_migration_and_idempotent_retry",
            "passed": True,
            "release": first["release_id"],
        }
    )
    last = (date(2025, 1, 1) + timedelta(days=days - 1)).isoformat()
    plan = pipeline.dry_run(manifest, 2, "v1", "2025-01-01", last)
    try:
        pipeline.execute(
            manifest,
            2,
            "v1",
            "2025-01-01",
            last,
            reason="late source revisions",
            run_id="demo_late",
            fail_at="after_detail_commit",
        )
    except LakeBridgeError as exc:
        if "Injected failure" not in str(exc):
            raise
    else:
        raise AssertionError("Fault injection did not fire")
    assert pipeline.control.release()["release_id"] == first["release_id"]
    recovery_start = perf_counter()
    second = pipeline.recover("demo_late")
    report["scenarios"].append(
        {
            "name": "late_revision_backfill_and_commit_recovery",
            "passed": True,
            "dry_run": plan,
            "release": second["release_id"],
            "recovery_seconds": perf_counter() - recovery_start,
            "tombstones": second["validation"]["tombstones"],
        }
    )

    competitor = Control(root, pipeline.settings.audit_dsn)
    try:
        with pipeline.control.writer("conflict_probe"):
            try:
                with competitor.writer("daily_writer"):
                    raise AssertionError("Second writer entered the critical section")
            except ConflictError:
                pass
            finally:
                pipeline.control.finish_writer("conflict_probe")
        with competitor.writer("daily_writer"):
            competitor.finish_writer("daily_writer")
    finally:
        competitor.close()
    report["scenarios"].append({"name": "concurrent_writer_blocked_then_resumed", "passed": True})

    # Simulate a rule considered incorrect by the business after technical acceptance.
    start_rule = (date(2025, 1, 1) + timedelta(days=days - 3)).isoformat()
    try:
        pipeline.execute(
            manifest,
            2,
            "v2",
            start_rule,
            last,
            reason="rule rollout simulation",
            run_id="demo_rule",
            fail_at="before_audit",
        )
    except LakeBridgeError as exc:
        if "Injected failure" not in str(exc):
            raise
    else:
        raise AssertionError("Fault injection did not fire")
    assert pipeline.control.release()["release_id"] == second["release_id"]
    bad = pipeline.recover("demo_rule")
    assert bad["validation"]["outside_range"]["passed"]
    legal = pipeline.execute(
        manifest, 3, "v2", reason="legitimate post-release arrivals", run_id="demo_legal"
    )
    before_heads = {n: pipeline.snapshot(n) for n in legal["tables"]}
    rollback_start = perf_counter()
    pipeline.activate(second["release_id"], "business rule rollback drill")
    assert pipeline.query(limit=1)["release_id"] == second["release_id"]
    assert before_heads == {n: pipeline.snapshot(n) for n in legal["tables"]}
    rollback_seconds = perf_counter() - rollback_start
    corrected = pipeline.execute(
        manifest,
        3,
        "v1",
        reason="forward correction preserving legal data",
        run_id="demo_corrected",
    )
    assert corrected["validation"]["live_rows"] == legal["validation"]["live_rows"]
    assert corrected["input_high_watermark"] == 3
    report["scenarios"].append(
        {
            "name": "consumer_rollback_and_legal_data_forward_correction",
            "passed": True,
            "bad_release": bad["release_id"],
            "rolled_back_to": second["release_id"],
            "corrected_release": corrected["release_id"],
            "consumer_recovery_seconds": rollback_seconds,
            "legal_new_events_preserved": max(2, rows // 100),
        }
    )
    report["passed"] = all(item["passed"] for item in report["scenarios"])
    report["total_seconds"] = perf_counter() - started
    report["final_release"] = corrected["release_id"]
    json_write(root / "demo-report.json", report)
    return report
