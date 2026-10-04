"""Durable comparison/refresh identities; single-worker execution and recovery."""

from __future__ import annotations

from contextlib import contextmanager

from psycopg.types.json import Jsonb

from lakebridge.consumer.diff_backend import DiffArtifact, SqlDiffBackend
from lakebridge.consumer.protocol import (
    BridgeError,
    DiffRequest,
    ErrorCode,
    content_hash,
    logical_id,
    require,
)


@contextmanager
def worker_lock(store):
    # A session lock, not a time-based lease. A crashed process releases it; a
    # slow/lost response never causes another live worker to execute concurrently.
    key = int(content_hash(store.schemas)[0:15], 16)
    locked = store.db.execute("SELECT pg_try_advisory_lock(%s) AS locked", (key,)).fetchone()[
        "locked"
    ]
    require(locked, ErrorCode.CONFLICT, "Consumer worker is already executing")
    try:
        yield
    finally:
        store.db.execute("SELECT pg_advisory_unlock(%s)", (key,))


def failure(exc):
    return {
        "code": exc.code if isinstance(exc, BridgeError) else "WORKER_FAILED",
        "message": str(exc)[:2000],
    }


def run_comparison(service, comparison_id, backend=None):
    store = service.store
    with worker_lock(store):
        row = store.execute(
            "SELECT * FROM {control}.comparisons WHERE comparison_id=%s",
            (logical_id(comparison_id),),
        ).fetchone()
        require(row is not None, ErrorCode.NOT_FOUND, "Unknown comparison")
        request = DiffRequest.from_dict(row["request"])
        destination = service.artifacts / comparison_id
        if row["state"] == "COMPLETE":
            return service.artifact(row).manifest
        store.execute(
            "UPDATE {control}.comparisons SET state='RUNNING',error=NULL WHERE comparison_id=%s",
            (comparison_id,),
        )
        try:
            if destination.exists():
                # Files committed before a worker crash: validate and record completion.
                manifest = DiffArtifact(destination, request).manifest
            else:
                manifest = (backend or SqlDiffBackend()).run(request, service.registry, destination)
            list_count = sum(1 for _ in DiffArtifact(destination, request).rows())
            require(
                list_count == sum(manifest["counts"].values()),
                ErrorCode.INTEGRITY_ERROR,
                "Diff count validation failed",
            )
            preview = DiffArtifact(destination, request).preview(20)
            result = {
                "counts": manifest["counts"],
                "preview": preview,
                "artifact_id": comparison_id,
                "checksum": manifest["checksum"],
                "backend": manifest["backend"],
            }
            store.execute(
                "UPDATE {control}.comparisons SET state='COMPLETE',artifact_hash=%s,result=%s"
                " WHERE comparison_id=%s",
                (manifest["checksum"], Jsonb(result), comparison_id),
            )
            return manifest
        except Exception as exc:
            store.execute(
                "UPDATE {control}.comparisons SET state='FAILED',error=%s WHERE comparison_id=%s",
                (Jsonb(failure(exc)), comparison_id),
            )
            raise


class JobService:
    def __init__(self, plans):
        self.plans, self.store = plans, plans.store

    def work_once(self, sink, backend=None):
        """Poll persisted work, including work interrupted by a dead worker process."""
        comparison = self.store.execute("""SELECT comparison_id FROM {control}.comparisons
            WHERE state IN ('PENDING','RUNNING') ORDER BY comparison_id LIMIT 1""").fetchone()
        if comparison:
            run_comparison(self.plans, comparison["comparison_id"], backend)
            return {"comparison_id": comparison["comparison_id"]}
        job = self.store.execute("""SELECT job_id FROM {control}.jobs
            WHERE state IN ('PENDING','RUNNING') ORDER BY job_id LIMIT 1""").fetchone()
        if job:
            return self.run(job["job_id"], sink)
        return None

    def get(self, principal_id, job_id):
        row = self.store.execute(
            """SELECT j.*,p.consumer_id FROM {control}.jobs j
            JOIN {control}.plans p USING(plan_id) WHERE job_id=%s""",
            (logical_id(job_id),),
        ).fetchone()
        require(row is not None, ErrorCode.NOT_FOUND, "Unknown refresh job")
        self.store.authorize(principal_id, row["consumer_id"], "read")
        return row

    def find(self, principal_id, idempotency_key):
        row = self.store.execute(
            "SELECT job_id FROM {control}.jobs WHERE principal_id=%s AND idempotency_key=%s",
            (principal_id, idempotency_key),
        ).fetchone()
        return self.get(principal_id, row["job_id"]) if row else None

    def _approval(self, plan, approval_id):
        approval = self.store.execute(
            "SELECT * FROM {control}.approvals WHERE approval_id=%s", (logical_id(approval_id),)
        ).fetchone()
        require(
            approval is not None
            and approval["decision"] == "approved"
            and approval["plan_hash"] == plan["plan_hash"]
            and approval["plan_id"] == plan["plan_id"],
            ErrorCode.APPROVAL_REQUIRED,
            "A matching independent approval is required",
        )
        require(approval["expires_at"] > self.plans.clock(), ErrorCode.EXPIRED, "Approval expired")
        self.store.authorize(approval["principal_id"], plan["consumer_id"], "approve")
        return approval

    def submit(self, principal_id, plan_id, approval_id, idempotency_key):
        logical_id(idempotency_key)
        plan = self.plans.get(principal_id, plan_id)
        self.store.authorize(principal_id, plan["consumer_id"], "execute")
        job_id = (
            "job_"
            + content_hash({"principal_id": principal_id, "idempotency_key": idempotency_key})[:40]
        )
        with self.store.db.transaction():
            self.store.consumer(plan["consumer_id"], lock=True)
            existing = self.find(principal_id, idempotency_key)
            if existing:
                require(
                    existing["plan_id"] == plan_id and existing["approval_id"] == approval_id,
                    ErrorCode.CONFLICT,
                    "Idempotency key is already bound to another request",
                )
                return (
                    existing  # Includes success after a lost response, even when baseline advanced.
                )
            self.plans.validate(principal_id, plan)
            self._approval(plan, approval_id)
            # A per-identity advisory xact lock also serializes admission across consumers.
            self.store.db.execute(
                "SELECT pg_advisory_xact_lock(%s)",
                (int(content_hash([principal_id, idempotency_key])[:15], 16),),
            )
            existing = self.find(principal_id, idempotency_key)
            require(existing is None, ErrorCode.CONFLICT, "Concurrent idempotency key collision")
            self.store.execute(
                """INSERT INTO {control}.jobs
                (job_id,plan_id,approval_id,principal_id,idempotency_key,state)
                VALUES (%s,%s,%s,%s,%s,'PENDING')""",
                (job_id, plan_id, approval_id, principal_id, idempotency_key),
            )
            self.store.reference("job", job_id, plan["source_pins"])
        return self.get(principal_id, job_id)

    def run(self, job_id, sink):
        """Explicit worker/recovery invocation always retains the original job ID."""
        with worker_lock(self.store):
            row = self.store.execute(
                "SELECT * FROM {control}.jobs WHERE job_id=%s", (logical_id(job_id),)
            ).fetchone()
            require(row is not None, ErrorCode.NOT_FOUND, "Unknown refresh job")
            if row["state"] == "SUCCEEDED":
                return self.get(row["principal_id"], job_id)
            self.store.execute(
                "UPDATE {control}.jobs SET state='RUNNING',error=NULL WHERE job_id=%s", (job_id,)
            )
            try:
                plan = self.plans.get(row["principal_id"], row["plan_id"])
                self.plans.validate(row["principal_id"], plan)
                self._approval(plan, row["approval_id"])
                sink.build(job_id, plan)
                # Hold consumer + grant locks until pointer and success commit together.
                with self.store.db.transaction():
                    self.store.consumer(plan["consumer_id"], lock=True)
                    self.plans.validate(row["principal_id"], plan)
                    self._approval(plan, row["approval_id"])
                    sink.publish(job_id, plan)
            except Exception as exc:
                # Never overwrite a committed success when a post-commit response fails.
                self.store.execute(
                    "UPDATE {control}.jobs SET state='FAILED',error=%s"
                    " WHERE job_id=%s AND state <> 'SUCCEEDED'",
                    (Jsonb(failure(exc)), job_id),
                )
                raise
            return self.get(row["principal_id"], job_id)

    def confirm(self, principal_id, job_id):
        row = self.get(principal_id, job_id)
        require(
            row["principal_id"] == principal_id and row["state"] in ("SUCCEEDED", "FAILED"),
            ErrorCode.CONFLICT,
            "Only the submitting principal can confirm a terminal result",
        )
        with self.store.db.transaction():
            self.store.execute(
                "UPDATE {control}.jobs SET confirmed=true WHERE job_id=%s", (job_id,)
            )
            self.store.execute(
                "UPDATE {control}.source_references SET released=true"
                " WHERE owner_kind='job' AND owner_id=%s",
                (job_id,),
            )
        return self.get(principal_id, job_id)
