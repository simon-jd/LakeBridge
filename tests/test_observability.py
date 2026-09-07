import json
import os
import subprocess
import sys

import pytest

from lakebridge.common import LakeBridgeError
from lakebridge.config import Settings
from lakebridge.control import Control
from lakebridge.observability import inspect_audit, prometheus, write_textfile


@pytest.fixture
def settings(tmp_path, monkeypatch):
    monkeypatch.delenv("LAKEBRIDGE_AUDIT_DSN", raising=False)
    monkeypatch.delenv("LAKEBRIDGE_WAREHOUSE", raising=False)
    settings = Settings(tmp_path / "audit")
    store = Control(settings.root)
    store.bind(settings.namespace, settings.warehouse)
    yield settings, store
    store.close()


def test_snapshot_reports_interrupted_writer_and_bounded_labels(settings):
    config, store = settings
    with store.writer("private_run_id"):
        store.save_run("private_run_id", "FAILED", {"last_error": "private error"})
    store.save_run("future_run", "ARBITRARY_FUTURE_STATE", {})
    snapshot = inspect_audit(config)
    assert snapshot["writer_state"] == "FAILED"
    assert snapshot["writer_state_age_seconds"] >= 0
    text = prometheus(snapshot)
    assert 'lakebridge_runs{state="FAILED"} 1' in text
    assert 'lakebridge_runs{state="OTHER"} 1' in text
    assert "private" not in text and "ARBITRARY" not in text
    assert "lakebridge_writer_owned 1" in text
    assert snapshot["generation"] == 0


def test_status_does_not_create_an_absent_database(tmp_path):
    missing = tmp_path / "absent"
    with pytest.raises(LakeBridgeError, match="inspection failed"):
        inspect_audit(Settings(missing))
    assert not missing.exists()


def test_read_only_connection_cannot_write_and_binding_must_match(settings):
    config, _ = settings
    import sqlite3

    reader = Control(config.root, read_only=True)
    try:
        with pytest.raises(sqlite3.OperationalError, match="readonly"):
            reader.finish_writer("anything")
    finally:
        reader.close()
    with pytest.raises(LakeBridgeError, match="binding"):
        inspect_audit(Settings(config.root, "wrong_dataset"))


def test_metrics_failure_replaces_stale_success_file_and_exits_nonzero(tmp_path):
    target = tmp_path / "audit.prom"
    write_textfile(target, "lakebridge_audit_up 1\n")
    env = {k: v for k, v in os.environ.items() if not k.startswith("LAKEBRIDGE_")}
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "lakebridge.cli",
            "--root",
            str(tmp_path / "missing"),
            "metrics",
            "--output",
            str(target),
        ],
        env=env,
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert result.returncode == 2
    assert "lakebridge_audit_up 0\n" in target.read_text()
    assert json.loads(result.stderr)["error"]
    assert not list(tmp_path.glob(".lakebridge-*.tmp"))


def test_status_cli_never_imports_spark_or_creates_a_jvm(settings):
    config, _ = settings
    code = (
        "import sys; from lakebridge.cli import parser, dispatch; "
        "dispatch(parser().parse_args(sys.argv[1:])); "
        "assert not any(m == 'pyspark' or m.startswith('pyspark.') for m in sys.modules)"
    )
    subprocess.run(
        [sys.executable, "-c", code, "--root", str(config.root), "status"],
        check=True,
        timeout=10,
        capture_output=True,
        text=True,
    )
