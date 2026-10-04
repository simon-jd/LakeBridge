"""Real stdio MCP -> HTTP -> PostgreSQL/dbt demonstration with durable CLI restarts.

Uses the two actual fixed Iceberg exports from consumer_demo.py. Human approval
is a separate CLI process/credential. No paid model is needed for this factual
mode demonstration; the configurable model adapter is verified separately.
"""

from __future__ import annotations

import argparse
import json
import os
import secrets
import socket
import subprocess
import time
from pathlib import Path

import duckdb
import httpx

from lakebridge.common import json_write
from lakebridge.consumer.diff_backend import wire_row
from lakebridge.consumer.jobs import JobService, run_comparison
from lakebridge.consumer.plans import PlanService
from lakebridge.consumer.protocol import ReleasePin, Scope
from lakebridge.consumer.releases import ReleaseRegistry
from lakebridge.consumer.sink_postgres import PostgresSink
from lakebridge.consumer.store import Store


def run_json(command, env, output):
    with output.with_suffix(".stderr.log").open("w") as log:
        result = subprocess.run(
            command,
            env=env,
            capture_output=False,
            stdout=subprocess.PIPE,
            stderr=log,
            text=True,
            timeout=90,
            check=False,
        )
    output.write_text(result.stdout)
    if result.returncode:
        raise RuntimeError(f"Command failed with {result.returncode}; inspect {output}")
    return json.loads(result.stdout)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", required=True, type=Path)
    parser.add_argument("--namespace", required=True)
    parser.add_argument(
        "--source-root", type=Path, default=Path("artifacts/consumer-demo-20260913")
    )
    parser.add_argument("--diff-backend", choices=("sql", "rust"), default="sql")
    parser.add_argument(
        "--release-query-bin", type=Path, default=Path("release-query/target/release/release-query")
    )
    parser.add_argument("--native-sources", type=Path)
    args = parser.parse_args()
    root, repo = args.root.resolve(), Path(__file__).resolve().parents[1]
    if root.exists():
        parser.error("Use a new output root; prior evidence is immutable")
    source = json.loads((args.source_root / "report.json").read_text())
    plan = source["trace"][1]["plan"]
    baseline, target = [ReleasePin.from_dict(pin) for pin in plan["source_pins"]]
    registry = ReleaseRegistry(args.source_root / "exports")
    registry.files(baseline)
    registry.files(target)
    root.mkdir(parents=True)
    store = Store(os.environ["LAKEBRIDGE_CONSUMER_DSN"], args.namespace)
    server = None
    server_log = None
    try:
        exists = store.db.execute(
            "SELECT 1 FROM information_schema.schemata WHERE schema_name=%s",
            (store.schemas["control"],),
        ).fetchone()
        if exists:
            raise ValueError("Use a new namespace to avoid changing existing consumers")
        store.initialize()
        consumer = plan["consumer_id"]
        store.register(consumer, target.dataset_id, Scope(**plan["registered_scope"]))
        store.grant("agent", consumer, ["read", "plan", "execute"])
        store.grant("human", consumer, ["read", "approve"])
        plans = PlanService(store, registry, root / "diffs")
        cmp = plans.compare("agent", consumer, None, baseline.release_id)
        backend = None
        if args.diff_backend == "rust":
            from lakebridge.consumer.rust_backend import RustDiffBackend

            backend = RustDiffBackend(
                args.release_query_bin,
                native_sources=json.loads(args.native_sources.read_text())
                if args.native_sources
                else None,
            )
        run_comparison(plans, cmp["comparison_id"], backend)
        initial = plans.create("agent", cmp["comparison_id"], consumer)
        initial_approval = plans.approve(
            "human", initial["plan_id"], initial["plan_hash"], "approved"
        )
        jobs = JobService(plans)
        initial_job = jobs.submit(
            "agent", initial["plan_id"], initial_approval["approval_id"], "bootstrap"
        )
        sink = PostgresSink(
            store,
            registry,
            dbt_executable=repo / ".venv-dbt/bin/dbt",
            project=repo / "consumer-dbt",
            artifacts=root / "jobs",
        )
        jobs.run(initial_job["job_id"], sink)
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]
        url = f"http://127.0.0.1:{port}"
        env = {
            **os.environ,
            "LB_BRIDGE_URL": url,
            "LB_DATASET_ID": target.dataset_id,
            "LB_AGENT_TOKEN": secrets.token_urlsafe(32),
            "LB_HUMAN_TOKEN": secrets.token_urlsafe(32),
        }
        # Deterministic factual-mode demo never inherits an unrelated paid-model configuration.
        for name in ("LB_MODEL_ENDPOINT", "LB_MODEL_NAME", "LB_MODEL_API_KEY"):
            env.pop(name, None)
        config = {
            "dsn_env": "LAKEBRIDGE_CONSUMER_DSN",
            "namespace": args.namespace,
            "registry": str(registry.root),
            "artifacts": str(root / "diffs"),
            "identities": [
                {"token_env": "LB_AGENT_TOKEN", "principal_id": "agent", "entrypoint": "agent"},
                {"token_env": "LB_HUMAN_TOKEN", "principal_id": "human", "entrypoint": "human"},
            ],
            "runbooks": {"usage.v1": str(repo / "docs/consumer-runbook.md")},
            "upstream": {target.dataset_id: {"root": str((args.source_root / "lake").resolve())}},
        }
        json_write(root / "api.json", config)
        python = str(repo / ".venv/bin/python")
        consumer_cli = [python, "-m", "lakebridge.consumer.cli"]
        server_log = (root / "api.log").open("w")
        server = subprocess.Popen(
            [*consumer_cli, "serve", "--config", str(root / "api.json"), "--port", str(port)],
            env=env,
            stdout=server_log,
            stderr=server_log,
        )
        with httpx.Client(
            base_url=url, headers={"Authorization": "Bearer " + env["LB_AGENT_TOKEN"]}
        ) as client:
            deadline = time.monotonic() + 20
            while time.monotonic() < deadline:
                if server.poll() is not None:
                    raise RuntimeError("API process exited; inspect api.log")
                try:
                    if client.get("/consumers/" + consumer).status_code == 200:
                        break
                except httpx.TransportError:
                    time.sleep(0.1)
            else:
                raise RuntimeError("API did not become ready")
            agent_cli = [
                str(repo / "ops-agent/.venv/bin/python"),
                "-m",
                "lakebridge_ops.cli",
                "--state-dir",
                str(root / "agent"),
            ]
            question = (
                f"Compare {baseline.release_id} to {target.release_id} for tenant "
                f"{plan['registered_scope']['tenant_id']}; "
                "explain usage changes and propose a refresh."
            )
            investigation = run_json(
                [
                    *agent_cli,
                    "investigate",
                    "usage-investigation",
                    "--question",
                    question,
                    "--consumer",
                    consumer,
                    "--from-release",
                    baseline.release_id,
                    "--to-release",
                    target.release_id,
                    "--tenant",
                    plan["registered_scope"]["tenant_id"],
                    "--run-id",
                    "demo_2",
                ],
                env,
                root / "01-investigation.json",
            )
            assert investigation["state"]["phase"] == "Investigating"
            comparison_id = investigation["state"]["comparison"]["comparison_id"]
            worker = [
                *consumer_cli,
                "--namespace",
                args.namespace,
                "--registry",
                str(registry.root),
                "--artifacts",
                str(root / "diffs"),
            ]
            worker += [
                "--diff-backend",
                args.diff_backend,
                "--release-query-bin",
                str(args.release_query_bin.resolve()),
            ]
            if args.native_sources:
                worker += ["--native-sources", str(args.native_sources.resolve())]
            run_json([*worker, "compare-worker", comparison_id], env, root / "02-comparison.json")
            proposed = run_json(
                [*agent_cli, "resume", "usage-investigation"], env, root / "03-proposed.json"
            )
            assert proposed["waiting"] and proposed["state"]["phase"] == "WaitingApproval"
            planned = proposed["state"]["plan"]
            denied = client.post(
                "/refresh-plans/" + planned["plan_id"] + "/approvals",
                json={"plan_hash": planned["plan_hash"], "decision": "approved"},
            )
            assert denied.status_code == 403
            reviewed = run_json(
                [
                    *consumer_cli,
                    "review",
                    planned["plan_id"],
                    "--url",
                    url,
                    "--token-env",
                    "LB_HUMAN_TOKEN",
                ],
                env,
                root / "04-human-review.json",
            )
            assert reviewed["plan_hash"] == planned["plan_hash"]
            approval = run_json(
                [
                    *consumer_cli,
                    "approve",
                    planned["plan_id"],
                    "--url",
                    url,
                    "--token-env",
                    "LB_HUMAN_TOKEN",
                    "--plan-hash",
                    reviewed["plan_hash"],
                    "--decision",
                    "approved",
                ],
                env,
                root / "05-human-approval.json",
            )
            submitting = run_json(
                [
                    *agent_cli,
                    "resume",
                    "usage-investigation",
                    "--approval-id",
                    approval["approval_id"],
                    "--plan-hash",
                    reviewed["plan_hash"],
                ],
                env,
                root / "06-submission.json",
            )
            assert submitting["state"]["phase"] == "Tracking"
            job_id = submitting["state"]["job"]["job_id"]
            pending = run_json(
                [*agent_cli, "resume", "usage-investigation"], env, root / "07-tracking.json"
            )
            assert pending["state"]["phase"] == "Tracking"
            run_json(
                [*worker, "refresh-worker", job_id, "--evidence", str(root / "jobs")],
                env,
                root / "08-worker.json",
            )
            completed = run_json(
                [*agent_cli, "resume", "usage-investigation"], env, root / "09-completed.json"
            )
            assert completed["state"]["phase"] == "Completed"
            usage = client.get("/consumers/" + consumer + "/usage").json()
            actual = [{k: v for k, v in row.items() if k != "consumer_release_id"} for row in usage]
            assert (
                actual == source["usage"]
            )  # Source demo compared this to the target Iceberg daily snapshot.
            db = duckdb.connect()
            try:
                db.execute("SET TimeZone='UTC'")
                db.from_parquet(registry.files(target)).create_view("target")
                reader = db.execute(
                    """SELECT tenant_id,CAST(event_time AS DATE) business_date,
                    count(*) event_count,sum(units)::DECIMAL(28,6) total_units,
                    sum(billable_units)::DECIMAL(28,6) billable_units FROM target
                    WHERE NOT is_deleted AND tenant_id=? AND event_time>=?::DATE
                    AND event_time<?::DATE GROUP BY 1,2 ORDER BY 1,2""",
                    [plan["registered_scope"][k] for k in ("tenant_id", "start", "end")],
                ).fetch_record_batch()
                expected = [wire_row(row) for batch in reader for row in batch.to_pylist()]
            finally:
                db.close()
            assert actual == expected
            report = {
                "status": "passed",
                "source_root": str(args.source_root),
                "transport": "real stdio MCP child -> real HTTP FastAPI "
                "-> separate workers -> PostgreSQL/dbt",
                "diff_backend": args.diff_backend,
                "native_source_descriptors": bool(args.native_sources),
                "model_mode": "factual_only; no paid/live model call",
                "question": question,
                "phases": [
                    x["state"]["phase"]
                    for x in (investigation, proposed, submitting, pending, completed)
                ],
                "agent_approval_http_status": denied.status_code,
                "budget": completed["budget"],
                "pins": planned["source_pins"],
                "plan_hash": planned["plan_hash"],
                "job_id": job_id,
                "consumer_release_id": completed["state"]["job"]["output_consumer_release"],
                "target_full_aggregate_matches": True,
                "usage": usage,
                "schemas": store.schemas,
            }
            json_write(root / "report.json", report)
            print(json.dumps({"status": "passed", "report": str(root / "report.json")}))
    finally:
        if server and server.poll() is None:
            server.terminate()
            try:
                server.wait(timeout=10)
            except subprocess.TimeoutExpired:
                server.kill()
                server.wait(timeout=10)
        if server_log:
            server_log.close()
        store.close()


if __name__ == "__main__":
    main()
