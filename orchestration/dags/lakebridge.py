"""Trigger with dag_run.conf. All values are argv elements, never interpolated shell code."""

from __future__ import annotations

import os
import subprocess
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param

ROOT = os.environ.get("LAKEBRIDGE_ROOT", "/workspace/artifacts/airflow")

with DAG(
    dag_id="lakebridge_history",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(seconds=30)},
    params={
        "manifest": Param("/workspace/artifacts/source/manifest.json", type="string"),
        "watermark": Param(1, type="integer", minimum=1),
        "rule": Param("v1", enum=["v1", "v2"]),
        "start": Param("", type="string"),
        "end": Param("", type="string"),
        "reason": Param("scheduled history processing", type="string", minLength=1),
    },
    tags=["lakebridge", "iceberg"],
) as dag:

    @task
    def verify(params=None):
        subprocess.run(
            [
                "lakebridge",
                "verify-manifest",
                params["manifest"],
                "--watermark",
                str(params["watermark"]),
            ],
            check=True,
        )

    @task
    def dry_run(params=None):
        argv = [
            "lakebridge",
            "--root",
            ROOT,
            "dry-run",
            "--manifest",
            params["manifest"],
            "--watermark",
            str(params["watermark"]),
            "--rule",
            params["rule"],
        ]
        if params["start"] or params["end"]:
            argv += ["--start", params["start"], "--end", params["end"]]
        subprocess.run(argv, check=True)

    @task(execution_timeout=timedelta(hours=12))
    def write_validate_publish(params=None, run_id=None):
        import hashlib

        stable_id = "airflow_" + hashlib.sha256(run_id.encode()).hexdigest()[:32]
        argv = [
            "lakebridge",
            "--root",
            ROOT,
            "migrate",
            "--manifest",
            params["manifest"],
            "--watermark",
            str(params["watermark"]),
            "--rule",
            params["rule"],
            "--reason",
            params["reason"],
            "--run-id",
            stable_id,
            "--lock-timeout",
            "60",
        ]
        if params["start"] or params["end"]:
            argv += ["--start", params["start"], "--end", params["end"]]
        # The critical section spans branch creation, validation and publication in one process.
        subprocess.run(argv, check=True)

    @task
    def verify_consumer():
        subprocess.run(["lakebridge", "--root", ROOT, "query", "--limit", "5"], check=True)

    verify() >> dry_run() >> write_validate_publish() >> verify_consumer()

with DAG(
    dag_id="lakebridge_maintenance",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    params={
        "action": Param("inspect", enum=["inspect", "compact", "expire", "orphans"]),
        "execute": Param(False, type="boolean"),
    },
    tags=["lakebridge", "maintenance"],
) as maintenance_dag:

    @task
    def maintenance(params=None):
        argv = ["lakebridge", "--root", ROOT, "maintain", params["action"]]
        if params["execute"]:
            argv.append("--execute")
        subprocess.run(argv, check=True)

    maintenance()
