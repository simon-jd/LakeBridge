"""COPY -> fixed dbt model -> complete candidate -> atomic PostgreSQL publication."""

from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

import duckdb
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict
from psycopg.types.json import Jsonb

from lakebridge.consumer.protocol import ErrorCode, ReleasePin, require


class PostgresSink:
    def __init__(self, store, registry, *, dbt_executable: Path, project: Path, artifacts: Path):
        # Paths are trusted deployment configuration, never model-supplied arguments.
        self.store, self.registry = store, registry
        self.dbt_executable, self.project, self.artifacts = dbt_executable, project, artifacts

    @staticmethod
    def relation_name(job_id):
        require(
            job_id.startswith("job_")
            and len(job_id) == 44
            and all(c in "0123456789abcdef" for c in job_id[4:]),
            ErrorCode.INVALID_REQUEST,
            "Only service-generated job IDs are executable",
        )
        return "daily_" + job_id[4:]

    def build(self, job_id, plan):
        relation = self.relation_name(job_id)
        source = ReleasePin.from_dict(plan["source_pins"][1])
        db = duckdb.connect()
        try:
            db.execute("SET TimeZone='UTC'")
            db.execute("SET threads=2")
            db.from_parquet(self.registry.files(source)).create_view("target")
            # Select the target state, not arithmetic on diff rows. Empty affected
            # days produce no staging records and are removed from the candidate.
            days = [s["business_date"] for s in plan["affected_scope"]]
            query = db.execute(
                """SELECT tenant_id,event_id,event_time,units,plan_id,
                billable_units,rule_version FROM target WHERE NOT is_deleted AND tenant_id=?
                AND CAST(event_time AS DATE)::VARCHAR IN (SELECT unnest(?))""",
                [plan["registered_scope"]["tenant_id"], days],
            )
            with self.store.db.transaction():
                self.store.execute("DELETE FROM {work}.raw_events WHERE job_id=%s", (job_id,))
                copy_sql = sql.SQL("COPY {}.raw_events FROM STDIN").format(
                    sql.Identifier(self.store.schemas["work"])
                )
                with self.store.db.cursor().copy(copy_sql) as copy:
                    for batch in query.fetch_record_batch(8192):
                        for values in batch.to_pylist():
                            copy.write_row((job_id, *values.values()))
        finally:
            db.close()
        directory = self.artifacts / job_id
        directory.mkdir(parents=True, exist_ok=True)
        config = conninfo_to_dict(self.store.dsn)
        env = {
            **os.environ,
            "DBT_SEND_ANONYMOUS_USAGE_STATS": "false",
            "LB_DBT_HOST": config.get("host", "127.0.0.1"),
            "LB_DBT_PORT": config.get("port", "5432"),
            "LB_DBT_USER": config.get("user", "postgres"),
            "LB_DBT_PASSWORD": config.get("password", ""),
            "LB_DBT_DATABASE": config.get("dbname", "postgres"),
            "LB_DBT_SCHEMA": self.store.schemas["work"],
        }
        variables = {
            "job_id": job_id,
            "result_alias": relation,
            "staging_alias": "stg_" + job_id[4:],
            "work_schema": self.store.schemas["work"],
        }
        command = [
            str(self.dbt_executable.resolve()),
            "build",
            "--project-dir",
            str(self.project.resolve()),
            "--profiles-dir",
            str(self.project.resolve()),
            "--target-path",
            str((directory / "target").resolve()),
            "--log-path",
            str((directory / "logs").resolve()),
            "--vars",
            json.dumps(variables),
            "--no-use-colors",
        ]
        with (directory / "dbt.log").open("w") as log:
            result = subprocess.run(
                command, env=env, stdout=log, stderr=subprocess.STDOUT, timeout=300, check=False
            )
        require(
            result.returncode == 0,
            ErrorCode.DEPENDENCY_UNAVAILABLE,
            f"dbt build failed; inspect worker evidence for {job_id}",
        )

    def publish(self, job_id, plan):
        relation = self.relation_name(job_id)
        release_id = "cr_" + job_id[4:]
        scope = plan["affected_scope"]
        days = [d["business_date"] for d in scope]
        payload = {
            "consumer_release_id": release_id,
            "job_id": job_id,
            "consumer_id": plan["consumer_id"],
            "base_consumer_release": plan["base_consumer_release"],
            "source_pin": plan["source_pins"][1],
            "recomputed_scope": scope,
            "transform_version": plan["transform_version"],
            "plan_hash": plan["plan_hash"],
        }
        with self.store.db.transaction():
            consumer = self.store.consumer(plan["consumer_id"], lock=True)
            job = self.store.execute(
                "SELECT * FROM {control}.jobs WHERE job_id=%s FOR UPDATE", (job_id,)
            ).fetchone()
            if job["state"] == "SUCCEEDED":
                return job["output_consumer_release"]
            require(
                job["state"] == "RUNNING" and job["plan_id"] == plan["plan_id"],
                ErrorCode.CONFLICT,
                "Job is not admitted for this plan",
            )
            require(
                consumer["generation"] == plan["expected_generation"]
                and consumer["consumer_release_id"] == plan["base_consumer_release"],
                ErrorCode.STALE_PLAN,
                "Consumer advanced during build; candidate is not published",
            )
            self.store.execute(
                "INSERT INTO {control}.consumer_releases VALUES (%s,%s,%s,%s)",
                (release_id, plan["consumer_id"], job_id, Jsonb(payload)),
            )
            if plan["base_consumer_release"]:
                self.store.execute(
                    """INSERT INTO {serving}.release_rows
                    SELECT %s,tenant_id,business_date,event_count,total_units,billable_units
                    FROM {serving}.release_rows WHERE consumer_release_id=%s
                    AND NOT (business_date = ANY(%s::date[]))""",
                    (release_id, plan["base_consumer_release"], days),
                )
            result_query = sql.SQL("""INSERT INTO {}.release_rows
                SELECT %s,tenant_id,business_date,event_count,total_units,billable_units
                FROM {}.{}""").format(
                sql.Identifier(self.store.schemas["serving"]),
                sql.Identifier(self.store.schemas["work"]),
                sql.Identifier(relation),
            )
            self.store.db.execute(result_query, (release_id,))
            escaped = self.store.execute(
                """SELECT 1 FROM {serving}.release_rows
                WHERE consumer_release_id=%s AND (tenant_id<>%s OR business_date<%s::date
                OR business_date>=%s::date) LIMIT 1""",
                (
                    release_id,
                    plan["registered_scope"]["tenant_id"],
                    plan["registered_scope"]["start"],
                    plan["registered_scope"]["end"],
                ),
            ).fetchone()
            require(not escaped, ErrorCode.INTEGRITY_ERROR, "Candidate escaped consumer scope")
            self.store.execute(
                """UPDATE {control}.consumers SET generation=generation+1,
                consumer_release_id=%s,source_pin=%s,transform_version=%s,comparison_version=%s
                WHERE consumer_id=%s""",
                (
                    release_id,
                    Jsonb(plan["source_pins"][1]),
                    plan["transform_version"],
                    plan["comparison_version"],
                    plan["consumer_id"],
                ),
            )
            self.store.execute(
                "UPDATE {control}.jobs SET state='SUCCEEDED',output_consumer_release=%s"
                " WHERE job_id=%s",
                (release_id, job_id),
            )
        return release_id
