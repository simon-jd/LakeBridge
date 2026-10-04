import json
import os
from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from lakebridge.common import LakeBridgeError
from lakebridge.config import Settings, spark_session
from lakebridge.contracts import EVENT_SCHEMA, PLAN_SCHEMA, file_entry, verify_manifest
from lakebridge.demo import demo
from lakebridge.generate import generate
from lakebridge.pipeline import Pipeline
from lakebridge.transform import prepare

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.environ.get("LAKEBRIDGE_INTEGRATION") != "1", reason="Set LAKEBRIDGE_INTEGRATION=1"
    ),
]


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    settings = Settings(tmp_path_factory.mktemp("spark"), "integration")
    session = spark_session(settings)
    yield session
    session.stop()


def test_end_to_end_four_scenarios(spark, tmp_path):
    pipeline = Pipeline(spark, Settings(tmp_path, "integration_demo"))
    try:
        result = demo(pipeline, rows=40, days=7)
        assert result["passed"]
        assert len(result["scenarios"]) == 4
        assert len(pipeline.control.releases()) == 5
    finally:
        pipeline.close()


@pytest.mark.parametrize(
    "fault", ["conflicting_version", "null_key", "negative_units", "time_move"]
)
def test_invalid_source_is_quarantined(spark, tmp_path, fault):
    manifest = generate(tmp_path / "source", rows=25, tenants=3, days=7)
    payload = json.loads(manifest.read_text())
    entry = next(e for e in payload["files"] if e["kind"] == "events" and e["batch"] == 1)
    path = manifest.parent / entry["path"]
    rows = pq.ParquetFile(path).read().to_pylist()
    changed = dict(rows[0])
    if fault == "conflicting_version":
        changed["units"] += Decimal("1")
    elif fault == "null_key":
        changed["tenant_id"] = None
    elif fault == "negative_units":
        changed["units"] = Decimal("-1")
    else:
        from datetime import timedelta

        changed["source_version"] = 2
        changed["event_time"] += timedelta(days=1)
    rows.append(changed)
    pq.write_table(pa.Table.from_pylist(rows, schema=EVENT_SCHEMA), path)
    entry.update(file_entry(manifest.parent, path, "events", 1))
    manifest.write_text(json.dumps(payload))
    quarantine = tmp_path / "quarantine"
    with pytest.raises(LakeBridgeError, match="Contract rejected"):
        prepare(spark, verify_manifest(manifest, 1), quarantine, "v1")
    assert list(quarantine.glob("*/*.parquet"))


def test_nested_plan_overlap_is_rejected(spark, tmp_path):
    from datetime import datetime

    manifest = generate(tmp_path / "source", rows=25, tenants=3, days=7)
    payload = json.loads(manifest.read_text())
    entry = next(e for e in payload["files"] if e["kind"] == "plans")
    path = manifest.parent / entry["path"]
    rows = pq.ParquetFile(path).read().to_pylist()
    rows.append(
        dict(rows[0], effective_from=datetime(2025, 1, 2), effective_to=datetime(2025, 1, 3))
    )
    pq.write_table(pa.Table.from_pylist(rows, schema=PLAN_SCHEMA), path)
    entry.update(file_entry(manifest.parent, path, "plans", 1))
    manifest.write_text(json.dumps(payload))
    with pytest.raises(LakeBridgeError, match="overlapping_plans"):
        prepare(spark, verify_manifest(manifest, 1), tmp_path / "quarantine", "v1")


def test_scope_cannot_advance_past_outside_revisions(spark, tmp_path):
    from pyspark.sql import functions as F

    from lakebridge.common import ConflictError
    from lakebridge.validation import validate

    manifest = generate(tmp_path / "source", rows=25, tenants=3, days=7)
    pipeline = Pipeline(spark, Settings(tmp_path / "lake", "scope_guard"))
    try:
        first = pipeline.execute(manifest, 1, reason="initial", run_id="initial")
        with pytest.raises(LakeBridgeError, match="outside range"):
            pipeline.execute(
                manifest, 2, "v1", "2025-01-01", "2025-01-01", reason="too narrow", run_id="narrow"
            )
        assert pipeline.control.release()["release_id"] == first["release_id"]
        pipeline.abort("narrow", "expand scope")
        report_dir = tmp_path / "invalid_summary"
        detail = pipeline.read_snapshot("detail", first["tables"]["detail"]["snapshot_id"])
        daily = pipeline.read_snapshot("daily", first["tables"]["daily"]["snapshot_id"])
        broken = daily.withColumn("event_count", F.col("event_count") + 1)
        result = validate(spark, detail, broken, manifest.parent / "expected/h1_v1", report_dir)
        assert not result["passed"] and not result["daily"]["passed"]
        assert list(report_dir.glob("daily_unexpected/*.parquet"))
        # A new writer cannot bypass an unfinished run after a failure.
        with pipeline.control.writer("unfinished"):
            pass
        with pytest.raises(ConflictError):
            with pipeline.control.writer("bypass"):
                pass
        pipeline.control.finish_writer("unfinished")
    finally:
        pipeline.close()


def test_admitted_run_recovers_and_report_failure_preserves_release(spark, tmp_path, monkeypatch):
    import contextlib

    import lakebridge.pipeline as module

    manifest = generate(tmp_path / "source", rows=20, tenants=2, days=7)
    pipeline = Pipeline(spark, Settings(tmp_path / "lake", "lifecycle_recovery"))
    original_writer = pipeline.control.writer
    original_json_write = module.json_write

    @contextlib.contextmanager
    def interrupted(*args, **kwargs):
        with original_writer(*args, **kwargs):
            raise RuntimeError("executor stopped after admission")
            yield  # pragma: no cover - contextmanager deliberately never yields

    def report_disk_full(path, payload):
        if path.name == "release.json":
            raise OSError("report disk full")
        return original_json_write(path, payload)

    try:
        with monkeypatch.context() as patch:
            patch.setattr(pipeline.control, "writer", interrupted)
            with pytest.raises(RuntimeError, match="after admission"):
                pipeline.execute(manifest, 1, reason="initial", run_id="admitted")
        assert pipeline.control.run("admitted")["state"] == "RUNNING"
        with monkeypatch.context() as patch:
            patch.setattr(module, "json_write", report_disk_full)
            with pytest.raises(OSError, match="report disk full"):
                pipeline.recover("admitted")
        assert pipeline.control.run("admitted")["state"] == "COMPLETED"
        assert pipeline.control.status()["writer_run"] is None
        audited = pipeline.control.release()
        assert audited["validation"]["passed"]
        snapshots = {name: pipeline.snapshot(name) for name in audited["tables"]}
        # A repeated recovery must not call Spark or advance any snapshot.
        with monkeypatch.context() as patch:
            patch.setattr(pipeline, "spark", None)
            assert pipeline.recover("admitted") == audited
        assert pipeline.control.status()["generation"] == 1
        assert len(pipeline.control.releases()) == 1
        assert {name: pipeline.snapshot(name) for name in snapshots} == snapshots
        assert pipeline.query()["rows"]
    finally:
        pipeline.close()
