"""PostgreSQL control ledger, separate from upstream LakeBridge publication."""

from __future__ import annotations

from dataclasses import asdict

import psycopg
from psycopg import sql
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from lakebridge.common import identifier
from lakebridge.consumer.protocol import (
    COMPARISON_VERSION,
    TRANSFORM_VERSION,
    ErrorCode,
    Scope,
    logical_id,
    require,
)

DDL = [
    """CREATE TABLE IF NOT EXISTS {control}.consumers (
        consumer_id text PRIMARY KEY, registration jsonb NOT NULL,
        generation bigint NOT NULL DEFAULT 0, source_pin jsonb, consumer_release_id text,
        transform_version text NOT NULL, comparison_version text NOT NULL)""",
    """CREATE TABLE IF NOT EXISTS {control}.grants (
        principal_id text NOT NULL, consumer_id text NOT NULL REFERENCES {control}.consumers,
        actions text[] NOT NULL, PRIMARY KEY(principal_id, consumer_id))""",
    """CREATE TABLE IF NOT EXISTS {control}.comparisons (
        comparison_id text PRIMARY KEY, consumer_id text NOT NULL REFERENCES {control}.consumers,
        expected_generation bigint NOT NULL, requested_by text NOT NULL,
        request jsonb NOT NULL, state text NOT NULL, artifact_hash text, error jsonb,
        result jsonb)""",
    "ALTER TABLE {control}.comparisons ADD COLUMN IF NOT EXISTS result jsonb",
    """CREATE TABLE IF NOT EXISTS {control}.plans (
        plan_id text PRIMARY KEY, consumer_id text NOT NULL REFERENCES {control}.consumers,
        plan_hash text NOT NULL, payload jsonb NOT NULL)""",
    """CREATE TABLE IF NOT EXISTS {control}.approvals (
        approval_id text PRIMARY KEY, plan_id text NOT NULL REFERENCES {control}.plans,
        plan_hash text NOT NULL, principal_id text NOT NULL, decision text NOT NULL,
        expires_at timestamptz NOT NULL)""",
    """CREATE TABLE IF NOT EXISTS {control}.jobs (
        job_id text PRIMARY KEY, plan_id text NOT NULL REFERENCES {control}.plans,
        approval_id text NOT NULL REFERENCES {control}.approvals,
        principal_id text NOT NULL, idempotency_key text NOT NULL, state text NOT NULL,
        output_consumer_release text, error jsonb, confirmed boolean NOT NULL DEFAULT false,
        UNIQUE(principal_id,idempotency_key))""",
    """CREATE TABLE IF NOT EXISTS {control}.source_references (
        owner_kind text NOT NULL, owner_id text NOT NULL, pin jsonb NOT NULL,
        released boolean NOT NULL DEFAULT false, PRIMARY KEY(owner_kind,owner_id,pin))""",
    """CREATE TABLE IF NOT EXISTS {control}.consumer_releases (
        consumer_release_id text PRIMARY KEY, consumer_id text NOT NULL,
        job_id text NOT NULL UNIQUE REFERENCES {control}.jobs, payload jsonb NOT NULL)""",
    """CREATE TABLE IF NOT EXISTS {work}.raw_events (
        job_id text NOT NULL, tenant_id text NOT NULL, event_id text NOT NULL,
        event_time timestamptz NOT NULL, units numeric(18,6), plan_id text,
        billable_units numeric(18,6), rule_version text,
        PRIMARY KEY(job_id,tenant_id,event_id))""",
    """CREATE TABLE IF NOT EXISTS {serving}.release_rows (
        consumer_release_id text NOT NULL REFERENCES {control}.consumer_releases,
        tenant_id text NOT NULL, business_date date NOT NULL, event_count bigint NOT NULL,
        total_units numeric(28,6), billable_units numeric(28,6),
        PRIMARY KEY(consumer_release_id,tenant_id,business_date))""",
    """CREATE OR REPLACE VIEW {serving}.tenant_daily_usage AS
        SELECT c.consumer_id,r.* FROM {control}.consumers c JOIN {serving}.release_rows r
        ON c.consumer_release_id=r.consumer_release_id""",
]


class Store:
    def __init__(self, dsn: str, namespace="consumer"):
        identifier(namespace)
        require(len(namespace) <= 40, ErrorCode.INVALID_REQUEST, "Namespace is too long")
        self.dsn = dsn
        self.schemas = {kind: f"{namespace}_{kind}" for kind in ("control", "work", "serving")}
        self.db = psycopg.connect(dsn, autocommit=True, row_factory=dict_row, connect_timeout=5)
        self.db.execute("SET TIME ZONE 'UTC'")

    def close(self):
        self.db.close()

    def execute(self, statement, params=()):
        query = sql.SQL(statement).format(**{k: sql.Identifier(v) for k, v in self.schemas.items()})
        return self.db.execute(query, params)

    def initialize(self):
        with self.db.transaction():
            for schema in self.schemas.values():
                self.db.execute(
                    sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema))
                )
            for statement in DDL:
                self.execute(statement)

    def consumer(self, consumer_id, *, lock=False):
        row = self.execute(
            "SELECT * FROM {control}.consumers WHERE consumer_id=%s"
            + (" FOR UPDATE" if lock else ""),
            (logical_id(consumer_id),),
        ).fetchone()
        require(row is not None, ErrorCode.NOT_FOUND, "Unknown consumer")
        return row

    def register(self, consumer_id, dataset_id, scope: Scope):
        registration = {
            "dataset_id": logical_id(dataset_id),
            "scope": asdict(scope),
            "model": "tenant_daily_usage",
        }
        with self.db.transaction():
            self.execute(
                """INSERT INTO {control}.consumers
                (consumer_id,registration,transform_version,comparison_version) VALUES (%s,%s,%s,%s)
                ON CONFLICT DO NOTHING""",
                (
                    logical_id(consumer_id),
                    Jsonb(registration),
                    TRANSFORM_VERSION,
                    COMPARISON_VERSION,
                ),
            )
            require(
                self.consumer(consumer_id)["registration"] == registration,
                ErrorCode.CONFLICT,
                "Consumer scope is immutable; register a separate consumer",
            )
        return self.consumer(consumer_id)

    def grant(self, principal_id, consumer_id, actions):
        # Operator configuration only; never an Agent tool or JSON self-asserted identity.
        require(
            set(actions) <= {"read", "plan", "approve", "execute"},
            ErrorCode.INVALID_REQUEST,
            "Unknown grant",
        )
        self.execute(
            """INSERT INTO {control}.grants VALUES (%s,%s,%s)
            ON CONFLICT(principal_id,consumer_id) DO UPDATE SET actions=excluded.actions""",
            (logical_id(principal_id), logical_id(consumer_id), list(actions)),
        )

    def authorize(self, principal_id, consumer_id, action):
        row = self.execute(
            "SELECT actions FROM {control}.grants WHERE principal_id=%s"
            " AND consumer_id=%s FOR SHARE",
            (principal_id, consumer_id),
        ).fetchone()
        require(
            row is not None and action in row["actions"],
            ErrorCode.FORBIDDEN,
            "Principal is not authorized for this consumer/action",
        )

    def reference(self, kind, owner_id, pins):
        for pin in pins:
            if pin is not None:
                self.execute(
                    """INSERT INTO {control}.source_references(owner_kind,owner_id,pin)
                    VALUES (%s,%s,%s) ON CONFLICT DO NOTHING""",
                    (kind, owner_id, Jsonb(pin)),
                )

    def protected_pins(self):
        return [
            r["pin"]
            for r in self.execute(
                "SELECT DISTINCT pin FROM {control}.source_references WHERE NOT released"
            ).fetchall()
        ]
