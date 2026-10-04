import hashlib
import subprocess
import sys
from pathlib import Path

from fastapi.testclient import TestClient
from test_consumer_service import service  # noqa: F401 - shared isolated PostgreSQL fixture

from lakebridge.consumer.api import APIConfig, Identity, create_app
from lakebridge.consumer.jobs import JobService


def test_api_auth_async_admission_and_independent_approval(service):  # noqa: F811
    store = service.store
    config = APIConfig(
        store.dsn,
        service.registry.root,
        service.artifacts,
        {
            hashlib.sha256(token.encode()).hexdigest(): Identity(principal, mode)
            for token, principal, mode in (
                ("agent-token", "agent", "agent"),
                ("human-token", "human", "human"),
            )
        },
        namespace=store.schemas["control"].removesuffix("_control"),
        runbooks={"usage.v1": Path("docs/consumer-runbook.md")},
    )
    client = TestClient(create_app(config))
    assert client.get("/consumers/daily_A").status_code == 403
    client.headers["Authorization"] = "Bearer agent-token"
    assert client.get("/consumers/daily_A").status_code == 200
    assert client.get("/releases/saas/missing").status_code == 404
    assert client.get("/releases/other/base").status_code == 403
    response = client.post(
        "/comparisons",
        json={
            "consumer_id": "daily_A",
            "from_release": None,
            "to_release": "base",
            "principal_id": "human",
        },
    )
    assert response.status_code == 422
    response = client.post(
        "/comparisons", json={"consumer_id": "daily_A", "from_release": None, "to_release": "base"}
    )
    assert response.status_code == 202, response.text
    comparison = response.json()
    assert comparison["state"] == "PENDING"
    assert not service.artifacts.exists()  # HTTP admission did not scan or start Spark.
    assert JobService(service).work_once(None) == {"comparison_id": comparison["comparison_id"]}
    assert JobService(service).work_once(None) is None
    status = client.get("/comparisons/" + comparison["comparison_id"]).json()
    assert status["state"] == "COMPLETE"
    assert status["result"]["counts"]["INSERT"] == 3
    response = client.post(
        "/refresh-plans",
        json={"consumer_id": "daily_A", "comparison_id": comparison["comparison_id"]},
    )
    assert response.status_code == 201, response.text
    plan = response.json()
    route = "/refresh-plans/" + plan["plan_id"] + "/approvals"
    approval_body = {"plan_hash": plan["plan_hash"], "decision": "approved"}
    assert client.post(route, json=approval_body).status_code == 403
    client.headers["Authorization"] = "Bearer human-token"
    approved = client.post(route, json=approval_body)
    assert approved.status_code == 201, approved.text
    client.headers["Authorization"] = "Bearer agent-token"
    response = client.post(
        "/refresh-jobs",
        json={
            "plan_id": plan["plan_id"],
            "approval_id": approved.json()["approval_id"],
            "idempotency_key": "api-first",
        },
    )
    assert response.status_code == 202, response.text
    job = response.json()
    assert job["state"] == "PENDING"
    assert client.get("/refresh-jobs/by-key/api-first").json()["job"]["job_id"] == job["job_id"]
    assert client.get("/runbooks", params={"query": "units", "source_version": "usage.v1"}).json()[
        "excerpts"
    ]
    assert (
        client.get("/runbooks", params={"query": "units", "source_version": "missing"}).status_code
        == 404
    )


def test_api_import_does_not_load_spark():
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import sys; import lakebridge.consumer.api; "
            "assert not any(k.startswith('pyspark') for k in sys.modules)",
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
