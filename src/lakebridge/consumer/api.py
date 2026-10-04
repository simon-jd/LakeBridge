"""Local trusted-operator API. Static tokens are explicitly not OAuth.

Configuration supplies DSNs, registered files and identities. Request bodies
contain logical IDs only. No route starts Spark or executes a comparison/job.
"""

from __future__ import annotations

import hashlib
import hmac
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Literal

from fastapi import Depends, FastAPI, Header
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field

from lakebridge.common import digest
from lakebridge.consumer.jobs import JobService
from lakebridge.consumer.plans import PlanService
from lakebridge.consumer.protocol import BridgeError, ErrorCode, logical_id, require
from lakebridge.consumer.releases import ReleaseRegistry
from lakebridge.consumer.store import Store


@dataclass(frozen=True)
class Identity:
    principal_id: str
    entrypoint: Literal["agent", "human"]


@dataclass(frozen=True)
class APIConfig:
    dsn: str
    registry: Path
    artifacts: Path
    tokens: dict[str, Identity]  # SHA256(token) -> configured identity; never serialized.
    namespace: str = "consumer"
    runbooks: dict[str, Path] = field(default_factory=dict)
    upstream: dict[str, tuple[Path, str]] = field(default_factory=dict)


class Body(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)


class CompareBody(Body):
    consumer_id: str
    from_release: str | None
    to_release: str


class PlanBody(Body):
    comparison_id: str
    consumer_id: str
    rebuild: bool = False
    ttl_seconds: int = Field(default=3600, ge=1, le=86400)


class ApprovalBody(Body):
    plan_hash: str
    decision: Literal["approved", "rejected"]


class JobBody(Body):
    plan_id: str
    approval_id: str
    idempotency_key: str


def create_app(config: APIConfig):
    app = FastAPI(title="LakeBridge Consumer Bridge", version="1.0.0")

    @app.exception_handler(BridgeError)
    async def error_handler(request, exc):
        status = {
            ErrorCode.NOT_FOUND: 404,
            ErrorCode.FORBIDDEN: 403,
            ErrorCode.INVALID_REQUEST: 422,
        }.get(exc.code, 409)
        return JSONResponse(
            status_code=status, content={"error": {"code": exc.code, "message": str(exc)}}
        )

    def identity(authorization: str = Header(default="")):
        require(authorization.startswith("Bearer "), ErrorCode.FORBIDDEN, "Bearer token required")
        hashed = hashlib.sha256(authorization[7:].encode()).hexdigest()
        for token_hash, principal in config.tokens.items():
            if hmac.compare_digest(token_hash, hashed):
                return principal
        raise BridgeError(ErrorCode.FORBIDDEN, "Unknown trusted-operator token")

    def services():
        store = Store(config.dsn, config.namespace)
        try:
            yield PlanService(store, ReleaseRegistry(config.registry), config.artifacts)
        finally:
            store.close()

    def authorize_dataset(service, who, dataset_id):
        logical_id(dataset_id)
        match = service.store.execute(
            """SELECT 1 FROM {control}.grants g
            JOIN {control}.consumers c USING(consumer_id) WHERE g.principal_id=%s
            AND 'read'=ANY(g.actions) AND c.registration->>'dataset_id'=%s LIMIT 1""",
            (who.principal_id, dataset_id),
        ).fetchone()
        require(match is not None, ErrorCode.FORBIDDEN, "Dataset is outside principal scope")

    @app.get("/consumers/{consumer_id}")
    def consumer(consumer_id: str, who: Identity = Depends(identity), service=Depends(services)):
        service.store.authorize(who.principal_id, consumer_id, "read")
        return service.store.consumer(consumer_id)

    @app.get("/releases/{dataset_id}/{release_id}")
    def release(
        dataset_id: str,
        release_id: str,
        who: Identity = Depends(identity),
        service=Depends(services),
    ):
        authorize_dataset(service, who, dataset_id)
        pin, manifest = service.registry.get(dataset_id, release_id)
        return {
            "pin": asdict(pin),
            "source_kind": manifest["source_kind"],
            "rows": manifest["rows"],
        }

    @app.get("/runs/{dataset_id}/{run_id}")
    def run(
        dataset_id: str, run_id: str, who: Identity = Depends(identity), service=Depends(services)
    ):
        from lakebridge.control import Control

        authorize_dataset(service, who, dataset_id)
        logical_id(run_id)
        require(dataset_id in config.upstream, ErrorCode.NOT_FOUND, "No upstream ledger registered")
        root, dsn = config.upstream[dataset_id]
        ledger = Control(root, dsn, read_only=True)
        try:
            row = ledger.run(run_id)
            require(row is not None, ErrorCode.NOT_FOUND, "Run is not recorded")
            # Only facts useful to investigation, not filesystem paths or input-file inventories.
            return {
                key: row[key]
                for key in (
                    "run_id",
                    "state",
                    "release_id",
                    "rule_version",
                    "input_high_watermark",
                    "reason",
                    "validation",
                    "error",
                )
                if key in row
            }
        finally:
            ledger.close()

    @app.get("/runbooks")
    def runbook(query: str, source_version: str, who: Identity = Depends(identity)):
        require(
            source_version in config.runbooks, ErrorCode.NOT_FOUND, "Runbook version is missing"
        )
        path = config.runbooks[source_version]
        words = query.casefold().split()
        require(0 < len(query) <= 500, ErrorCode.INVALID_REQUEST, "Query must be 1..500 characters")
        paragraphs = path.read_text().split("\n\n")
        matches = [p[:2000] for p in paragraphs if any(w in p.casefold() for w in words)][:5]
        return {
            "source_version": source_version,
            "source_id": path.name,
            "sha256": digest(path),
            "excerpts": matches,
            "authority": "explanation_only; pinned tool facts take precedence",
        }

    @app.post("/comparisons", status_code=202)
    def compare(body: CompareBody, who: Identity = Depends(identity), service=Depends(services)):
        return service.compare(who.principal_id, **body.model_dump())

    @app.get("/comparisons/{comparison_id}")
    def comparison(
        comparison_id: str, who: Identity = Depends(identity), service=Depends(services)
    ):
        return service.comparison(who.principal_id, comparison_id)

    @app.post("/refresh-plans", status_code=201)
    def plan(body: PlanBody, who: Identity = Depends(identity), service=Depends(services)):
        return service.create(who.principal_id, **body.model_dump())

    @app.get("/refresh-plans/{plan_id}")
    def get_plan(plan_id: str, who: Identity = Depends(identity), service=Depends(services)):
        return service.get(who.principal_id, plan_id)

    @app.post("/refresh-plans/{plan_id}/approvals", status_code=201)
    def approve(
        plan_id: str,
        body: ApprovalBody,
        who: Identity = Depends(identity),
        service=Depends(services),
    ):
        require(
            who.entrypoint == "human",
            ErrorCode.FORBIDDEN,
            "Approval requires the independent human entrypoint",
        )
        return service.approve(who.principal_id, plan_id, **body.model_dump())

    @app.post("/refresh-jobs", status_code=202)
    def submit(body: JobBody, who: Identity = Depends(identity), service=Depends(services)):
        return JobService(service).submit(who.principal_id, **body.model_dump())

    @app.get("/refresh-jobs/by-key/{idempotency_key}")
    def find_job(
        idempotency_key: str, who: Identity = Depends(identity), service=Depends(services)
    ):
        return {"job": JobService(service).find(who.principal_id, idempotency_key)}

    @app.get("/refresh-jobs/{job_id}")
    def get_job(job_id: str, who: Identity = Depends(identity), service=Depends(services)):
        return JobService(service).get(who.principal_id, job_id)

    @app.post("/refresh-jobs/{job_id}/confirm")
    def confirm(job_id: str, who: Identity = Depends(identity), service=Depends(services)):
        return JobService(service).confirm(who.principal_id, job_id)

    @app.get("/consumers/{consumer_id}/usage")
    def usage(consumer_id: str, who: Identity = Depends(identity), service=Depends(services)):
        service.store.authorize(who.principal_id, consumer_id, "read")
        # Decimal strings preserve precision across API/JavaScript clients.
        return service.store.execute(
            """SELECT consumer_release_id,tenant_id,
            business_date::text,event_count,total_units::text,billable_units::text
            FROM {serving}.tenant_daily_usage WHERE consumer_id=%s ORDER BY business_date""",
            (consumer_id,),
        ).fetchall()

    return app
