"""Real immutable Iceberg releases feed the SQL consumer diff adapter."""

import json
import os

import pytest

from lakebridge.config import Settings, spark_session
from lakebridge.consumer.diff_backend import DiffArtifact, SqlDiffBackend
from lakebridge.consumer.protocol import DiffRequest, Scope
from lakebridge.consumer.releases import ReleaseRegistry, export_snapshot
from lakebridge.generate import generate
from lakebridge.pipeline import Pipeline

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.environ.get("LAKEBRIDGE_INTEGRATION") != "1", reason="Set LAKEBRIDGE_INTEGRATION=1"
    ),
]


def test_actual_release_snapshots_export_and_do_not_follow_current(tmp_path):
    settings = Settings(tmp_path / "lake", "consumer_pins")
    spark = spark_session(settings)
    pipeline = Pipeline(spark, settings)
    try:
        manifest = generate(tmp_path / "source", rows=30, tenants=2, days=7)
        dataset = json.loads(manifest.read_text())["dataset_id"]
        base = pipeline.execute(manifest, 1, reason="consumer base", run_id="cb_base")
        target = pipeline.execute(manifest, 2, reason="consumer target", run_id="cb_target")
        registry = ReleaseRegistry(tmp_path / "exports")
        for release in (base, target):
            export_snapshot(
                spark,
                pipeline.control,
                registry,
                dataset,
                release["release_id"],
                tmp_path / "export-work",
            )
        left, lm = registry.get(dataset, base["release_id"])
        right, rm = registry.get(dataset, target["release_id"])
        assert lm["source_kind"] == rm["source_kind"] == "iceberg_snapshot_export"
        request = DiffRequest(left, right, Scope("t0000", "2025-01-01", "2025-01-08"))
        SqlDiffBackend().run(request, registry, tmp_path / "diff")
        artifact = DiffArtifact(tmp_path / "diff", request)
        assert artifact.manifest["counts"]["UPDATE"] > 0
        # Frozen base still resolves and scans after the active release has advanced.
        assert pipeline.control.release()["release_id"] == target["release_id"]
        assert all(isinstance(t.snapshot_id, str) for t in left.tables)
        assert all(r["key"]["tenant_id"] == "t0000" for r in artifact.rows())
    finally:
        pipeline.close()
        spark.stop()
