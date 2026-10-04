"""Real PostgreSQL state transitions and dbt publication; no SQLite substitute."""

import os
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import pytest
from psycopg import sql
from psycopg.types.json import Jsonb
from test_consumer_diff import event, register

from lakebridge.consumer.jobs import JobService, run_comparison
from lakebridge.consumer.plans import PlanService
from lakebridge.consumer.protocol import BridgeError, Scope
from lakebridge.consumer.releases import ReleaseRegistry
from lakebridge.consumer.sink_postgres import PostgresSink
from lakebridge.consumer.store import Store

pytestmark = pytest.mark.postgres


@pytest.fixture
def service(tmp_path):
    dsn = os.environ.get("LAKEBRIDGE_TEST_POSTGRES_DSN")
    if not dsn:
        pytest.skip("set LAKEBRIDGE_TEST_POSTGRES_DSN for isolated-schema PostgreSQL tests")
    store = Store(dsn, "cb_" + uuid4().hex[:16])
    store.initialize()
    store.register("daily_A", "saas", Scope("A", "2025-01-01", "2025-01-05"))
    store.grant("agent", "daily_A", ["read", "plan", "execute"])
    store.grant("human", "daily_A", ["read", "approve"])
    registry = ReleaseRegistry(tmp_path / "exports")
    register(
        registry,
        "base",
        [
            event("update"),
            event("keep", day=2),
            event("delete", day=3),
            event("other", tenant="B", units="999"),
        ],
    )
    register(
        registry,
        "target",
        [
            event("update", units="2.5"),
            event("new", day=1, units="4"),
            event("keep", day=2),
            event("delete", day=3, is_deleted=True),
            event("other", tenant="B", units="9999"),
        ],
    )
    plans = PlanService(store, registry, tmp_path / "diffs")
    yield plans
    for schema in reversed(list(store.schemas.values())):
        store.db.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))
    store.close()


def draft(plans, target="base", baseline=None, **kwargs):
    comparison = plans.compare("agent", "daily_A", baseline, target)
    run_comparison(plans, comparison["comparison_id"])
    return plans.create("agent", comparison["comparison_id"], "daily_A", **kwargs)


def admit(plans, plan, key):
    approval = plans.approve("human", plan["plan_id"], plan["plan_hash"], "approved")
    return JobService(plans).submit("agent", plan["plan_id"], approval["approval_id"], key)


def sink(plans, tmp_path):
    executable = Path(os.environ.get("LAKEBRIDGE_TEST_DBT", ".venv-dbt/bin/dbt")).resolve()
    if not executable.is_file():
        pytest.skip("Install consumer-dbt requirements and set LAKEBRIDGE_TEST_DBT")
    return PostgresSink(
        plans.store,
        plans.registry,
        dbt_executable=executable,
        project=Path("consumer-dbt"),
        artifacts=tmp_path / "dbt",
    )


def serving(plans):
    return plans.store.execute("""SELECT tenant_id,business_date::text,event_count,
        total_units,billable_units FROM {serving}.tenant_daily_usage
        WHERE consumer_id='daily_A' ORDER BY business_date""").fetchall()


def test_real_copy_dbt_publish_updates_deletes_equivalence_and_retry(service, tmp_path):
    plans, jobs = service, JobService(service)
    target_sink = sink(plans, tmp_path)
    initial = draft(plans)
    assert initial["full_rebuild"] and initial["partition_count"] == 4
    assert plans.store.consumer("daily_A")["generation"] == 0  # Draft has no serving effect.
    first = admit(plans, initial, "first")
    assert len(plans.store.protected_pins()) == 1
    result = jobs.run(first["job_id"], target_sink)
    assert result["state"] == "SUCCEEDED"
    baseline_rows = serving(plans)
    assert len(baseline_rows) == 3 and {r["tenant_id"] for r in baseline_rows} == {"A"}
    update = draft(plans, "target", "base")
    assert not update["full_rebuild"]
    assert [s["business_date"] for s in update["affected_scope"]] == ["2025-01-01", "2025-01-03"]
    stale = draft(plans, "target", "base")
    second = admit(plans, update, "update")
    result = jobs.run(second["job_id"], target_sink)
    assert result["state"] == "SUCCEEDED"
    rows = serving(plans)
    assert rows == [
        {
            "tenant_id": "A",
            "business_date": "2025-01-01",
            "event_count": 2,
            "total_units": Decimal("6.500000"),
            "billable_units": Decimal("6.500000"),
        },
        baseline_rows[1],
    ]
    # Response loss: resubmitting the exact identity returns the same committed job,
    # despite generation having advanced. Running that job again does no dbt work.
    retry = jobs.submit("agent", second["plan_id"], second["approval_id"], "update")
    assert retry["job_id"] == second["job_id"]
    assert (
        jobs.run(second["job_id"], target_sink)["output_consumer_release"]
        == result["output_consumer_release"]
    )
    assert (
        plans.store.execute("SELECT count(*) n FROM {control}.consumer_releases").fetchone()["n"]
        == 2
    )
    with pytest.raises(BridgeError, match="baseline changed"):
        admit(plans, stale, "stale")
    register(
        plans.registry,
        "rewrite",
        [
            event("other", tenant="B", units="9999"),
            event("delete", day=3, is_deleted=True),
            event("keep", day=2),
            event("new", day=1, units="4"),
            event("update", units="2.5"),
        ],
    )
    equivalent = draft(plans, "rewrite", "target")
    assert equivalent["affected_scope"] == []
    third = admit(plans, equivalent, "rewrite")
    jobs.run(third["job_id"], target_sink)
    assert serving(plans) == rows
    assert plans.store.consumer("daily_A")["source_pin"]["release_id"] == "rewrite"


def test_independent_approval_expiry_permissions_and_plan_tampering(service):
    plans, jobs = service, JobService(service)
    plan = draft(plans)
    with pytest.raises(BridgeError, match="authorized"):
        plans.approve("agent", plan["plan_id"], plan["plan_hash"], "approved")
    with pytest.raises(BridgeError, match="reviewed|Reviewed"):
        plans.approve("human", plan["plan_id"], "different", "approved")
    rejected = plans.approve("human", plan["plan_id"], plan["plan_hash"], "rejected")
    with pytest.raises(BridgeError, match="approval"):
        jobs.submit("agent", plan["plan_id"], rejected["approval_id"], "rejected")
    approved = plans.approve("human", plan["plan_id"], plan["plan_hash"], "approved")
    plans.store.grant("agent", "daily_A", ["read", "plan"])
    with pytest.raises(BridgeError, match="authorized"):
        jobs.submit("agent", plan["plan_id"], approved["approval_id"], "revoked")
    plans.store.grant("agent", "daily_A", ["read", "plan", "execute"])
    plans.clock = lambda: datetime.now(UTC) + timedelta(days=1)
    with pytest.raises(BridgeError, match="expired"):
        jobs.submit("agent", plan["plan_id"], approved["approval_id"], "expired")
    plans.clock = lambda: datetime.now(UTC)
    changed = {k: v for k, v in plan.items() if k != "plan_hash"}
    changed["registered_scope"]["tenant_id"] = "B"
    plans.store.execute(
        "UPDATE {control}.plans SET payload=%s WHERE plan_id=%s", (Jsonb(changed), plan["plan_id"])
    )
    with pytest.raises(BridgeError, match="content changed"):
        jobs.submit("agent", plan["plan_id"], approved["approval_id"], "tampered")


def test_missing_incomplete_or_wrong_baseline_cannot_plan(service):
    with pytest.raises(BridgeError, match="not registered"):
        service.compare("agent", "daily_A", None, "missing")
    with pytest.raises(BridgeError, match="from_release"):
        service.compare("agent", "daily_A", "target", "base")
    comparison = service.compare("agent", "daily_A", None, "base")
    with pytest.raises(BridgeError, match="complete"):
        service.create("agent", comparison["comparison_id"], "daily_A")
    with pytest.raises(BridgeError, match="scope is immutable"):
        service.store.register("daily_A", "saas", Scope("B", "2025-01-01", "2025-01-05"))


def test_failure_before_commit_is_invisible_and_same_job_recovers(service, tmp_path):
    plans, jobs = service, JobService(service)
    target_sink = sink(plans, tmp_path)
    plan = draft(plans)
    job = admit(plans, plan, "recover")
    original = target_sink.publish

    def fail(job_id, plan):
        raise RuntimeError("lost worker after dbt; before publish transaction")

    target_sink.publish = fail
    with pytest.raises(RuntimeError, match="lost worker"):
        jobs.run(job["job_id"], target_sink)
    assert serving(plans) == []
    assert plans.store.consumer("daily_A")["generation"] == 0
    assert jobs.get("agent", job["job_id"])["state"] == "FAILED"
    assert plans.store.protected_pins()  # Unconfirmed failure retains input dependencies.
    target_sink.publish = original
    jobs.run(job["job_id"], target_sink)
    assert len(serving(plans)) == 3
    assert jobs.get("agent", job["job_id"])["state"] == "SUCCEEDED"


def test_transaction_rolls_back_pointer_and_complete_candidate_together(
    service, tmp_path, monkeypatch
):
    jobs = JobService(service)
    job = admit(service, draft(service), "atomic")
    original = service.store.execute

    def fail_success(statement, params=()):
        if "SET state='SUCCEEDED'" in statement:
            raise RuntimeError("database failure after pointer update, before success")
        return original(statement, params)

    monkeypatch.setattr(service.store, "execute", fail_success)
    with pytest.raises(RuntimeError, match="after pointer update"):
        jobs.run(job["job_id"], sink(service, tmp_path))
    assert serving(service) == []
    assert service.store.consumer("daily_A")["generation"] == 0
    assert (
        service.store.execute("SELECT count(*) n FROM {control}.consumer_releases").fetchone()["n"]
        == 0
    )


def test_concurrent_admission_returns_one_job(service):
    from concurrent.futures import ThreadPoolExecutor

    plan = draft(service)
    approval = service.approve("human", plan["plan_id"], plan["plan_hash"], "approved")
    namespace = service.store.schemas["control"].removesuffix("_control")

    def submit():
        store = Store(service.store.dsn, namespace)
        try:
            plans = PlanService(store, service.registry, service.artifacts)
            return JobService(plans).submit(
                "agent", plan["plan_id"], approval["approval_id"], "same"
            )
        finally:
            store.close()

    with ThreadPoolExecutor(2) as pool:
        first, second = list(pool.map(lambda _: submit(), range(2)))
    assert first["job_id"] == second["job_id"]
    assert service.store.execute("SELECT count(*) n FROM {control}.jobs").fetchone()["n"] == 1


def test_transform_change_requires_explicit_rebuild_and_invalidates_old_plan(service):
    old_plan = draft(service)
    service.store.execute("UPDATE {control}.consumers SET transform_version='older.v0'")
    comparison = service.compare("agent", "daily_A", None, "base")
    with pytest.raises(BridgeError, match="full rebuild"):
        service.create("agent", comparison["comparison_id"], "daily_A")
    replacement = service.create("agent", comparison["comparison_id"], "daily_A", rebuild=True)
    assert replacement["full_rebuild"] and replacement["partition_count"] == 4
    with pytest.raises(BridgeError, match="baseline changed"):
        admit(service, old_plan, "old-transform")


def test_retention_requires_original_tags_for_unconfirmed_dependencies(service):
    from lakebridge.common import LakeBridgeError
    from lakebridge.consumer.retention import verify_references

    admit(service, draft(service), "protected")

    class Upstream:
        available = True

        def table(self, name):
            return "lake.saas." + name

        def snapshot(self, name, release):
            return 9007199254740993 if self.available else None

    upstream = Upstream()
    pins = verify_references(upstream, "detail", service.store)
    assert pins == [
        {"release_id": "base", "table": "lake.saas.detail", "snapshot_id": "9007199254740993"}
    ]
    upstream.available = False
    with pytest.raises(LakeBridgeError, match="dependency lost"):
        verify_references(upstream, "detail", service.store)
