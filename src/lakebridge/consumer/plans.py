"""Service-computed plans and independently authenticated human approvals."""

from __future__ import annotations

from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from uuid import uuid4

from psycopg.types.json import Jsonb

from lakebridge.consumer.diff_backend import DiffArtifact
from lakebridge.consumer.impact import affected_scope, full_scope
from lakebridge.consumer.protocol import (
    COMPARISON_VERSION,
    PROTOCOL_VERSION,
    TRANSFORM_VERSION,
    DiffRequest,
    ErrorCode,
    ReleasePin,
    Scope,
    canonical,
    content_hash,
    logical_id,
    require,
    utc_timestamp,
)


class PlanService:
    def __init__(self, store, registry, artifacts, *, clock=None):
        self.store, self.registry, self.artifacts = store, registry, artifacts
        self.clock = clock or (lambda: datetime.now(UTC))

    def comparison(self, principal_id, comparison_id):
        row = self.store.execute(
            "SELECT * FROM {control}.comparisons WHERE comparison_id=%s",
            (logical_id(comparison_id),),
        ).fetchone()
        require(row is not None, ErrorCode.NOT_FOUND, "Unknown comparison")
        self.store.authorize(principal_id, row["consumer_id"], "read")
        return row

    def compare(self, principal_id, consumer_id, from_release, to_release):
        """Admit metadata only. A separate worker scans/compares the pinned exports."""
        self.store.authorize(principal_id, consumer_id, "plan")
        with self.store.db.transaction():
            consumer = self.store.consumer(consumer_id, lock=True)
            registration = consumer["registration"]
            dataset = registration["dataset_id"]
            target, _ = self.registry.get(dataset, logical_id(to_release))
            baseline = consumer["source_pin"]
            require(
                (from_release is None and baseline is None)
                or (baseline is not None and from_release == baseline["release_id"]),
                ErrorCode.STALE_PLAN,
                "from_release must equal this consumer's current source",
            )
            source = ReleasePin.from_dict(baseline) if baseline else None
            if source:
                actual, _ = self.registry.get(dataset, source.release_id)
                require(actual == source, ErrorCode.INTEGRITY_ERROR, "Baseline pin changed")
            request = DiffRequest(source, target, Scope(**registration["scope"]))
            comparison_id = (
                "cmp_"
                + content_hash(
                    {
                        "request": asdict(request),
                        "consumer_id": consumer_id,
                        "generation": consumer["generation"],
                        "principal_id": principal_id,
                    }
                )[:40]
            )
            self.store.execute(
                """INSERT INTO {control}.comparisons
                (comparison_id,consumer_id,expected_generation,requested_by,request,state)
                VALUES (%s,%s,%s,%s,%s,'PENDING') ON CONFLICT DO NOTHING""",
                (
                    comparison_id,
                    consumer_id,
                    consumer["generation"],
                    principal_id,
                    Jsonb(asdict(request)),
                ),
            )
            self.store.reference(
                "comparison", comparison_id, [asdict(source) if source else None, asdict(target)]
            )
        return self.comparison(principal_id, comparison_id)

    def artifact(self, comparison):
        require(
            comparison["state"] == "COMPLETE",
            ErrorCode.INCOMPLETE_DIFF,
            "Wait for the complete comparison artifact",
        )
        request = DiffRequest.from_dict(comparison["request"])
        artifact = DiffArtifact(self.artifacts / comparison["comparison_id"], request)
        require(
            artifact.manifest["checksum"] == comparison["artifact_hash"],
            ErrorCode.INTEGRITY_ERROR,
            "Artifact differs from the completed comparison",
        )
        return artifact

    def create(self, principal_id, comparison_id, consumer_id, *, rebuild=False, ttl_seconds=3600):
        self.store.authorize(principal_id, consumer_id, "plan")
        comparison = self.comparison(principal_id, comparison_id)
        require(
            comparison["consumer_id"] == consumer_id,
            ErrorCode.INVALID_REQUEST,
            "Comparison belongs to another consumer",
        )
        artifact = self.artifact(comparison)
        scope = affected_scope(artifact)  # Always consume and validate the whole artifact.
        request = artifact.request
        require(1 <= ttl_seconds <= 86400, ErrorCode.INVALID_REQUEST, "Plan TTL must be 1..86400s")
        with self.store.db.transaction():
            consumer = self.store.consumer(consumer_id, lock=True)
            require(
                consumer["generation"] == comparison["expected_generation"]
                and canonical(consumer["source_pin"])
                == canonical(asdict(request.from_pin) if request.from_pin else None),
                ErrorCode.STALE_PLAN,
                "Consumer advanced; compare again",
            )
            versions_changed = (
                consumer["transform_version"] != TRANSFORM_VERSION
                or consumer["comparison_version"] != COMPARISON_VERSION
            )
            require(
                not versions_changed or rebuild,
                ErrorCode.STALE_PLAN,
                "Transform/comparison changed; explicitly request a full rebuild",
            )
            full = rebuild or consumer["source_pin"] is None
            if full:
                scope = full_scope(request.scope)
            payload = {
                "protocol_version": PROTOCOL_VERSION,
                "plan_id": "plan_" + uuid4().hex,
                "source_pins": [
                    asdict(request.from_pin) if request.from_pin else None,
                    asdict(request.to_pin),
                ],
                "comparison_id": comparison_id,
                "diff_hash": artifact.manifest["checksum"],
                "comparison_version": COMPARISON_VERSION,
                "consumer_id": consumer_id,
                "expected_generation": consumer["generation"],
                "expected_transform_version": consumer["transform_version"],
                "expected_comparison_version": consumer["comparison_version"],
                "base_consumer_release": consumer["consumer_release_id"],
                "registered_scope": asdict(request.scope),
                "affected_scope": scope,
                "partition_count": len(scope),
                "full_rebuild": full,
                "transform_version": TRANSFORM_VERSION,
                "expires_at": utc_timestamp(self.clock() + timedelta(seconds=ttl_seconds)),
                "steps": [
                    "export pinned target scope",
                    "COPY job staging",
                    "dbt daily aggregate",
                    "build complete consumer release",
                    "atomically publish and record success",
                ],
            }
            plan_hash = content_hash(payload)
            self.store.execute(
                "INSERT INTO {control}.plans VALUES (%s,%s,%s,%s)",
                (payload["plan_id"], consumer_id, plan_hash, Jsonb(payload)),
            )
            self.store.reference("plan", payload["plan_id"], payload["source_pins"])
        return {**payload, "plan_hash": plan_hash}

    def get(self, principal_id, plan_id):
        row = self.store.execute(
            "SELECT * FROM {control}.plans WHERE plan_id=%s", (logical_id(plan_id),)
        ).fetchone()
        require(row is not None, ErrorCode.NOT_FOUND, "Unknown plan")
        self.store.authorize(principal_id, row["consumer_id"], "read")
        require(
            content_hash(row["payload"]) == row["plan_hash"],
            ErrorCode.INTEGRITY_ERROR,
            "Plan content changed after creation",
        )
        return {**row["payload"], "plan_hash": row["plan_hash"]}

    def validate(self, principal_id, plan, *, check_expiry=True):
        self.store.authorize(principal_id, plan["consumer_id"], "execute")
        require(
            plan["transform_version"] == TRANSFORM_VERSION
            and plan["comparison_version"] == COMPARISON_VERSION,
            ErrorCode.STALE_PLAN,
            "Service transformation changed; create a new plan",
        )
        if check_expiry:
            require(
                datetime.fromisoformat(plan["expires_at"]) > self.clock(),
                ErrorCode.EXPIRED,
                "Plan expired",
            )
        consumer = self.store.consumer(plan["consumer_id"])
        require(
            consumer["generation"] == plan["expected_generation"]
            and consumer["transform_version"] == plan["expected_transform_version"]
            and consumer["comparison_version"] == plan["expected_comparison_version"]
            and consumer["consumer_release_id"] == plan["base_consumer_release"]
            and canonical(consumer["source_pin"]) == canonical(plan["source_pins"][0])
            and consumer["registration"]["scope"] == plan["registered_scope"],
            ErrorCode.STALE_PLAN,
            "Consumer baseline changed; generate a new plan",
        )
        artifact = self.artifact(self.comparison(principal_id, plan["comparison_id"]))
        require(
            artifact.manifest["checksum"] == plan["diff_hash"],
            ErrorCode.INTEGRITY_ERROR,
            "Plan diff identity changed",
        )
        actual_scope = affected_scope(artifact)
        if plan["full_rebuild"]:
            actual_scope = full_scope(artifact.request.scope)
        require(
            actual_scope == plan["affected_scope"],
            ErrorCode.INTEGRITY_ERROR,
            "Plan scope differs from complete artifact",
        )
        for pin in plan["source_pins"]:
            if pin:
                self.registry.files(ReleasePin.from_dict(pin))

    def approve(self, principal_id, plan_id, plan_hash, decision):
        """Independent human entrypoint requires the exact reviewed plan hash."""
        plan = self.get(principal_id, plan_id)
        self.store.authorize(principal_id, plan["consumer_id"], "approve")
        require(decision in ("approved", "rejected"), ErrorCode.INVALID_REQUEST, "Invalid decision")
        require(plan["plan_hash"] == plan_hash, ErrorCode.STALE_PLAN, "Reviewed plan hash changed")
        require(
            datetime.fromisoformat(plan["expires_at"]) > self.clock(),
            ErrorCode.EXPIRED,
            "Plan expired",
        )
        approval = {
            "approval_id": "approval_" + uuid4().hex,
            "plan_id": plan_id,
            "plan_hash": plan_hash,
            "principal_id": principal_id,
            "decision": decision,
            "expires_at": plan["expires_at"],
        }
        self.store.execute(
            "INSERT INTO {control}.approvals VALUES (%s,%s,%s,%s,%s,%s)", tuple(approval.values())
        )
        return approval
