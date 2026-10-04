"""Failure boundaries using real SQLite; Spark work is absent or already validated."""

import contextlib
import subprocess
import sys
from types import SimpleNamespace

import pytest

from lakebridge.common import LakeBridgeError
from lakebridge.config import Settings
from lakebridge.contracts import verify_manifest
from lakebridge.generate import generate
from lakebridge.pipeline import Pipeline


@pytest.fixture
def job(tmp_path):
    manifest = generate(tmp_path / "source", rows=20, tenants=2, days=7)
    inputs = verify_manifest(manifest, 1)
    spark = SimpleNamespace(sparkContext=SimpleNamespace(applicationId="fixture"))
    pipeline = Pipeline(spark, Settings(tmp_path / "audit"))
    context = {
        "manifest": str(inputs.path),
        "manifest_sha256": inputs.fingerprint,
        "input_high_watermark": 1,
        "rule_version": "v1",
        "scope": None,
        "reason": "initial",
    }
    yield pipeline, context
    pipeline.close()


def test_process_exit_after_admission_keeps_recoverable_context(job):
    pipeline, context = job
    # os._exit bypasses finally, just as a terminated Python executor would.
    script = """
import contextlib, os, sys
from pathlib import Path
from lakebridge.config import Settings
from lakebridge.pipeline import Pipeline
p = Pipeline(None, Settings(Path(sys.argv[1])))
original = p.control.writer
@contextlib.contextmanager
def die_after_admission(*args, **kwargs):
    with original(*args, **kwargs):
        os._exit(23)
p.control.writer = die_after_admission
p.execute(sys.argv[2], 1, reason='initial', run_id='crashed')
"""
    result = subprocess.run(
        [sys.executable, "-c", script, str(pipeline.settings.root), context["manifest"]],
        timeout=30,
        capture_output=True,
    )
    assert result.returncode == 23, result.stderr.decode()
    assert pipeline.control.status()["writer_run"] == "crashed"
    run = pipeline.control.run("crashed")
    assert run is not None, "committed ownership must have durable recovery inputs"
    assert all(run[k] == v for k, v in context.items())
    assert run["state"] == "RUNNING" and run["generation"] == 0
    # No data-plane operation began, so an explicit abort is safe.
    assert pipeline.abort("crashed", "executor stopped")["state"] == "ABORTED"
    assert pipeline.control.status()["writer_run"] is None


def test_admission_context_and_owner_roll_back_together(job, monkeypatch):
    pipeline, context = job
    original = pipeline.control.save_run

    def interrupt(*args):
        original(*args)
        raise RuntimeError("admission interrupted")

    monkeypatch.setattr(pipeline.control, "save_run", interrupt)
    with pytest.raises(RuntimeError, match="admission interrupted"):
        pipeline.execute(context["manifest"], 1, reason="initial", run_id="new")
    assert pipeline.control.status()["writer_run"] is None
    assert pipeline.control.run("new") is None


def test_abort_reloads_state_after_execution_lock(job, monkeypatch):
    pipeline, context = job
    pipeline.control.save_run("same", "RUNNING", context)
    original = pipeline.control.writer

    @contextlib.contextmanager
    def completed_before_lock(*args, **kwargs):
        pipeline.control.save_run("same", "COMPLETED", context)
        with original(*args, **kwargs) as waited:
            yield waited

    monkeypatch.setattr(pipeline.control, "writer", completed_before_lock)
    with pytest.raises(LakeBridgeError, match="unfinished"):
        pipeline.abort("same", "stale abort")
    assert pipeline.control.run("same")["state"] == "COMPLETED"
    assert pipeline.control.status()["writer_run"] is None


def test_abort_state_and_owner_roll_back_together(job, monkeypatch):
    pipeline, context = job
    pipeline.control.save_run("same", "FAILED", context)
    with pipeline.control.writer("same"):
        pass
    original = pipeline.control.finish_writer

    def interrupt(run_id):
        original(run_id)
        raise RuntimeError("abort interrupted")

    monkeypatch.setattr(pipeline.control, "finish_writer", interrupt)
    with pytest.raises(RuntimeError, match="abort interrupted"):
        pipeline.abort("same", "cancel")
    assert pipeline.control.run("same")["state"] == "FAILED"
    assert pipeline.control.status()["writer_run"] == "same"


def test_report_failure_cannot_demote_completed_publication(job, monkeypatch):
    pipeline, context = job
    candidate = {
        "run_id": "same",
        "release_id": "rel_same",
        "tables": {},
        "validation": {"passed": True},
    }
    context.update(candidate=candidate, generation=0)
    pipeline.control.save_run("same", "VALIDATED", context)
    pipeline.spark = SimpleNamespace(sparkContext=SimpleNamespace(applicationId="fixture"))

    def disk_full(path, payload):
        raise OSError("disk full")

    monkeypatch.setattr("lakebridge.pipeline.json_write", disk_full)
    with pytest.raises(OSError, match="disk full"):
        pipeline.recover("same")
    assert pipeline.control.run("same")["state"] == "COMPLETED"
    assert pipeline.control.status()["writer_run"] is None
    assert len(pipeline.control.releases()) == 1
    assert pipeline.recover("same") == pipeline.control.release("rel_same")


def test_retry_preserves_initial_recovery_context(job):
    pipeline, context = job
    pipeline.control.save_run("same", "FAILED", {**context, "generation": 0})
    with pipeline.control.writer("same", initial_run={"manifest": "changed"}):
        assert pipeline.control.run("same")["manifest"] == context["manifest"]
        pipeline.control.finish_writer("same")
