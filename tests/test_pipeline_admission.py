"""Exercise the queue boundary without executing Spark transformations."""

import contextlib

import pytest

from lakebridge.common import LakeBridgeError
from lakebridge.config import Settings
from lakebridge.contracts import verify_manifest
from lakebridge.control import Control
from lakebridge.generate import generate
from lakebridge.pipeline import Pipeline


@pytest.mark.parametrize("changed", [False, True])
def test_queued_duplicate_reloads_completed_run_before_spark(tmp_path, monkeypatch, changed):
    manifest = generate(tmp_path / "source", rows=20, tenants=2, days=7)
    inputs = verify_manifest(manifest, 1)
    control = Control(tmp_path / "audit")
    pipeline = object.__new__(Pipeline)
    pipeline.control = control
    pipeline.settings = Settings(tmp_path / "audit")
    pipeline.spark = None  # Any attempt to replay transformations must fail this test.
    original = control.writer

    @contextlib.contextmanager
    def finish_while_queued(run_id, timeout=0, **kwargs):
        with original(run_id, timeout, **kwargs):
            release = {"run_id": run_id, "release_id": "rel_same", "validation": {"passed": True}}
            control.publish(release, 0)
            control.save_run(
                run_id,
                "COMPLETED",
                {
                    "manifest": str(inputs.path),
                    "manifest_sha256": inputs.fingerprint,
                    "input_high_watermark": 1,
                    "rule_version": "v1",
                    "scope": None,
                    "reason": "different" if changed else "initial",
                    "candidate": release,
                },
            )
            yield 0.1

    monkeypatch.setattr(control, "writer", finish_while_queued)
    try:
        if changed:
            with pytest.raises(LakeBridgeError, match="immutable"):
                pipeline.execute(manifest, 1, reason="initial", run_id="same")
        else:
            assert (
                pipeline.execute(manifest, 1, reason="initial", run_id="same")["release_id"]
                == "rel_same"
            )
        assert control.status()["writer_run"] is None
        assert control.status()["generation"] == 1
        assert control.run("same")["state"] == "COMPLETED"
    finally:
        control.close()
