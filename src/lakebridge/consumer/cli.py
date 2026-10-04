"""Operator setup, standalone workers and independent human approval CLI."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import time
from pathlib import Path

from lakebridge.consumer.jobs import JobService, run_comparison
from lakebridge.consumer.plans import PlanService
from lakebridge.consumer.protocol import Scope
from lakebridge.consumer.releases import ReleaseRegistry
from lakebridge.consumer.store import Store


def load_config(path):
    from lakebridge.consumer.api import APIConfig, Identity

    value = json.loads(Path(path).read_text())
    identities = {}
    for entry in value["identities"]:
        token = os.environ[entry["token_env"]]
        if len(token) < 24:
            raise ValueError(
                "Use distinct randomly generated operator/agent tokens (24+ characters)"
            )
        key = hashlib.sha256(token.encode()).hexdigest()
        if key in identities:
            raise ValueError("Agent and human entrypoints must use distinct tokens")
        identities[key] = Identity(entry["principal_id"], entry["entrypoint"])
    return APIConfig(
        dsn=os.environ[value["dsn_env"]],
        registry=Path(value["registry"]),
        artifacts=Path(value["artifacts"]),
        namespace=value.get("namespace", "consumer"),
        tokens=identities,
        runbooks={k: Path(v) for k, v in value.get("runbooks", {}).items()},
        upstream={
            key: (Path(entry["root"]), os.environ.get(entry.get("dsn_env", ""), ""))
            for key, entry in value.get("upstream", {}).items()
        },
    )


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dsn-env", default="LAKEBRIDGE_CONSUMER_DSN")
    parser.add_argument("--namespace", default="consumer")
    parser.add_argument("--registry", type=Path, default=Path("artifacts/consumer/exports"))
    parser.add_argument("--artifacts", type=Path, default=Path("artifacts/consumer/diffs"))
    parser.add_argument("--diff-backend", choices=("sql", "rust"), default="sql")
    parser.add_argument(
        "--release-query-bin", type=Path, default=Path("release-query/target/release/release-query")
    )
    parser.add_argument(
        "--native-sources",
        type=Path,
        help="Operator-owned release ID to fixed native descriptor JSON",
    )
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("init")
    register = commands.add_parser("register")
    for field in ("consumer", "dataset", "tenant", "start", "end"):
        register.add_argument(field)
    grant = commands.add_parser("grant")
    grant.add_argument("principal")
    grant.add_argument("consumer")
    grant.add_argument("actions", nargs="+", choices=("read", "plan", "approve", "execute"))
    compare = commands.add_parser("compare-worker")
    compare.add_argument("comparison_id")
    for worker_name in ("refresh-worker", "worker"):
        refresh = commands.add_parser(worker_name)
        if worker_name == "refresh-worker":
            refresh.add_argument("job_id")
        else:
            refresh.add_argument("--once", action="store_true")
        refresh.add_argument("--dbt", type=Path, default=Path(".venv-dbt/bin/dbt"))
        refresh.add_argument("--project", type=Path, default=Path("consumer-dbt"))
        refresh.add_argument("--evidence", type=Path, default=Path("artifacts/consumer/jobs"))
    export = commands.add_parser("export")
    export.add_argument("dataset")
    export.add_argument("release")
    export.add_argument("--upstream-root", required=True, type=Path)
    export.add_argument("--upstream-namespace", required=True)
    export.add_argument("--scratch", type=Path, default=Path("artifacts/consumer/export-work"))
    serve = commands.add_parser("serve")
    serve.add_argument("--config", required=True, type=Path)
    serve.add_argument("--port", type=int, default=8765)
    for name in ("review", "approve", "job"):
        cmd = commands.add_parser(name)
        cmd.add_argument("id")
        cmd.add_argument("--url", default="http://127.0.0.1:8765")
        cmd.add_argument("--token-env", default="LAKEBRIDGE_HUMAN_TOKEN")
        if name == "approve":
            cmd.add_argument("--plan-hash", required=True)
            cmd.add_argument("--decision", choices=("approved", "rejected"), required=True)
    args = parser.parse_args(argv)
    if args.command == "serve":
        import uvicorn

        from lakebridge.consumer.api import create_app

        uvicorn.run(create_app(load_config(args.config)), host="127.0.0.1", port=args.port)
        return
    if args.command in ("review", "approve", "job"):
        import httpx

        route = "/refresh-jobs/" if args.command == "job" else "/refresh-plans/"
        with httpx.Client(
            base_url=args.url,
            headers={"Authorization": "Bearer " + os.environ[args.token_env]},
            timeout=30,
        ) as client:
            if args.command == "approve":
                response = client.post(
                    route + args.id + "/approvals",
                    json={"plan_hash": args.plan_hash, "decision": args.decision},
                )
            else:
                response = client.get(route + args.id)
            response.raise_for_status()
            print(json.dumps(response.json(), ensure_ascii=False, indent=2))
        return
    if args.command == "export":
        from lakebridge.config import Settings, spark_session
        from lakebridge.consumer.releases import export_snapshot
        from lakebridge.control import Control

        settings = Settings(args.upstream_root, args.upstream_namespace)
        spark = spark_session(settings)
        ledger = Control(settings.root, settings.audit_dsn, read_only=True)
        try:
            result = export_snapshot(
                spark,
                ledger,
                ReleaseRegistry(args.registry),
                args.dataset,
                args.release,
                args.scratch,
            )
            print(json.dumps(result, indent=2))
        finally:
            ledger.close()
            spark.stop()
        return
    store = Store(os.environ[args.dsn_env], args.namespace)
    try:
        if args.command == "init":
            store.initialize()
            result = {"schemas": store.schemas}
        elif args.command == "register":
            result = store.register(
                args.consumer, args.dataset, Scope(args.tenant, args.start, args.end)
            )
        elif args.command == "grant":
            store.grant(args.principal, args.consumer, args.actions)
            result = {
                "principal": args.principal,
                "consumer": args.consumer,
                "actions": args.actions,
            }
        else:
            plans = PlanService(store, ReleaseRegistry(args.registry), args.artifacts)
            backend = None
            if args.diff_backend == "rust":
                from lakebridge.consumer.rust_backend import RustDiffBackend

                backend = RustDiffBackend(
                    args.release_query_bin,
                    native_sources=json.loads(args.native_sources.read_text())
                    if args.native_sources
                    else None,
                )
            if args.command == "compare-worker":
                result = run_comparison(plans, args.comparison_id, backend)
            else:
                from lakebridge.consumer.sink_postgres import PostgresSink

                sink = PostgresSink(
                    store,
                    plans.registry,
                    dbt_executable=args.dbt,
                    project=args.project,
                    artifacts=args.evidence,
                )
                jobs = JobService(plans)
                if args.command == "worker":
                    while True:
                        result = jobs.work_once(sink, backend)
                        if args.once:
                            break
                        if result:
                            print(json.dumps(result, default=str), flush=True)
                        else:
                            time.sleep(1)
                else:
                    result = jobs.run(args.job_id, sink)
        print(json.dumps(result, default=str, ensure_ascii=False, indent=2))
    finally:
        store.close()


if __name__ == "__main__":
    main()
