"""Real PostgreSQL tests; each test owns disposable, isolated schemas."""

import os
import threading
from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

import pytest

from lakebridge.common import ConflictError
from lakebridge.control import Control

pytestmark = pytest.mark.postgres


@pytest.fixture
def stores(tmp_path):
    dsn = os.environ.get("LAKEBRIDGE_TEST_POSTGRES_DSN")
    if not dsn:
        pytest.skip("set LAKEBRIDGE_TEST_POSTGRES_DSN to a disposable PostgreSQL database")
    import psycopg
    from psycopg import sql
    from psycopg.conninfo import make_conninfo

    admin = psycopg.connect(dsn, autocommit=True)
    schemas, opened = [], []

    def create(schema=None):
        if schema is None:
            schema = "lb_test_" + uuid4().hex
            admin.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
            schemas.append(schema)
        control = Control(tmp_path / schema, make_conninfo(dsn, options=f"-csearch_path={schema}"))
        opened.append(control)
        return control, schema

    yield create
    for control in opened:
        control.close()
    for schema in schemas:
        admin.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))
    admin.close()


def seed(control):
    with control.writer("seed"):
        control.publish(
            {"run_id": "seed", "release_id": "rel_seed", "validation": {"passed": True}}, 0
        )
        control.finish_writer("seed")


def test_activation_serializes_with_writer_admission(stores, monkeypatch):
    activator, schema = stores()
    writer, _ = stores(schema)
    seed(activator)
    read, proceed, entered = threading.Event(), threading.Event(), threading.Event()
    original = activator.status

    def pause_after_status(*args, **kwargs):
        status = original(*args, **kwargs)
        read.set()
        assert proceed.wait(5), "test barrier timed out"
        return status

    monkeypatch.setattr(activator, "status", pause_after_status)

    def write():
        with writer.writer("new"):
            entered.set()
            writer.finish_writer("new")

    with ThreadPoolExecutor(2) as pool:
        activation = pool.submit(activator.activate, "rel_seed", "rollback", 1)
        assert read.wait(5)
        execution = pool.submit(write)
        try:
            assert not entered.wait(0.4), "writer entered during an uncommitted activation"
        finally:
            proceed.set()
            activation.result(timeout=5)
            execution.result(timeout=5)
    assert writer.status()["generation"] == 2


def test_independent_audit_schemas_have_independent_execution_locks(stores):
    first, _ = stores()
    second, _ = stores()
    with first.writer("first"), second.writer("second"):
        first.finish_writer("first")
        second.finish_writer("second")


def test_same_dataset_still_serializes_and_session_loss_retains_owner(stores):
    first, schema = stores()
    second, _ = stores(schema)
    with first.writer("interrupted"):
        with pytest.raises(ConflictError, match="executing"):
            with second.writer("interrupted"):
                pytest.fail("same run executed concurrently")
    # Closing the original session cannot erase its committed ownership row.
    first.close()
    with pytest.raises(ConflictError, match="Unfinished"):
        with second.writer("new"):
            pytest.fail("new run stole persistent ownership")
    with second.writer("interrupted"):
        second.finish_writer("interrupted")


def test_concurrent_activations_require_fresh_generation(stores):
    first, schema = stores()
    second, _ = stores(schema)
    seed(first)
    barrier = threading.Barrier(2)

    def activate(control):
        barrier.wait(timeout=5)
        try:
            return control.activate("rel_seed", "concurrent rollback", 1)
        except ConflictError:
            return "conflict"

    with ThreadPoolExecutor(2) as pool:
        futures = [pool.submit(activate, control) for control in (first, second)]
        assert sorted(str(f.result(timeout=5)) for f in futures) == ["2", "conflict"]
    assert first.execute("SELECT count(*) AS n FROM activations").fetchone()["n"] == 2


def test_forced_backend_termination_releases_lock_but_not_ownership(stores):
    import psycopg

    victim, schema = stores()
    recovery, _ = stores(schema)
    with victim.writer("killed"):
        victim.save_run("killed", "WRITING", {"input": "immutable"})
        pid = victim.db.info.backend_pid
        recovery.execute("SELECT pg_terminate_backend(?)", (pid,))
        with pytest.raises(psycopg.OperationalError):
            victim.status()
    with pytest.raises(ConflictError, match="Unfinished"):
        with recovery.writer("replacement", timeout=2):
            pytest.fail("interrupted writer was silently replaced")
    with recovery.writer("killed", timeout=2):
        assert recovery.run("killed")["input"] == "immutable"
        recovery.finish_writer("killed")


def test_postgres_monitoring_uses_a_coherent_read_only_snapshot(stores, monkeypatch):
    import psycopg
    from psycopg.conninfo import make_conninfo

    from lakebridge.config import Settings
    from lakebridge.observability import inspect_audit

    writer, schema = stores()
    config = Settings(writer.root)
    writer.bind(config.namespace, config.warehouse)
    dsn = make_conninfo(
        os.environ["LAKEBRIDGE_TEST_POSTGRES_DSN"], options=f"-csearch_path={schema}"
    )
    monkeypatch.setenv("LAKEBRIDGE_AUDIT_DSN", dsn)
    reader = Control(config.root, dsn, read_only=True)
    try:
        with pytest.raises(psycopg.errors.ReadOnlySqlTransaction):
            reader.finish_writer("anything")
    finally:
        reader.close()
    read, proceed = threading.Event(), threading.Event()
    original = Control.status

    def pause_reader(self, **kwargs):
        status = original(self, **kwargs)
        if self is not writer:
            read.set()
            assert proceed.wait(5)
        return status

    monkeypatch.setattr(Control, "status", pause_reader)
    with ThreadPoolExecutor(1) as pool:
        observation = pool.submit(inspect_audit, config)
        assert read.wait(5)
        try:
            seed(writer)
        finally:
            proceed.set()
        snapshot = observation.result(timeout=5)
    assert snapshot["generation"] == 0
    assert snapshot["release_count"] == 0
    assert snapshot["active_release"] is None


def test_atomic_admission_survives_backend_termination(stores):
    import psycopg

    victim, schema = stores()
    recovery, _ = stores(schema)
    context = {"manifest": "/local/immutable/manifest.json", "input_high_watermark": 1}
    with victim.writer("admitted", initial_run=context):
        assert recovery.run("admitted")["manifest"] == context["manifest"]
        recovery.execute("SELECT pg_terminate_backend(?)", (victim.db.info.backend_pid,))
        with pytest.raises(psycopg.OperationalError):
            victim.status()
    assert recovery.status()["writer_run"] == "admitted"
    with recovery.writer("admitted", timeout=2, initial_run={"manifest": "wrong"}):
        run = recovery.run("admitted")
        assert run["state"] == "RUNNING" and run["generation"] == 0
        assert run["manifest"] == context["manifest"]
        recovery.finish_writer("admitted")


def test_admission_failure_rolls_back_both_rows_in_postgres(stores, monkeypatch):
    writer, schema = stores()
    observer, _ = stores(schema)
    original = writer.save_run

    def interrupt(*args):
        original(*args)
        # A second connection must see neither uncommitted row change.
        assert observer.status()["writer_run"] is None
        assert observer.run("interrupted") is None
        raise RuntimeError("admission interrupted")

    monkeypatch.setattr(writer, "save_run", interrupt)
    with pytest.raises(RuntimeError, match="admission interrupted"):
        with writer.writer("interrupted", initial_run={"manifest": "fixed"}):
            pytest.fail("admission should fail before execution")
    assert observer.status()["writer_run"] is None
    assert observer.run("interrupted") is None
    # The failed transaction must release its session advisory lock too.
    with observer.writer("next", initial_run={"manifest": "fixed"}):
        observer.finish_writer("next")
