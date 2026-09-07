from __future__ import annotations

import contextlib
import fcntl
import json
import sqlite3
import time
from pathlib import Path

from lakebridge.common import ConflictError, LakeBridgeError, now

DDL = [
    """CREATE TABLE IF NOT EXISTS dataset_binding (
        id INTEGER PRIMARY KEY, namespace TEXT NOT NULL, warehouse TEXT NOT NULL)""",
    """CREATE TABLE IF NOT EXISTS runs (
        run_id TEXT PRIMARY KEY, state TEXT NOT NULL, payload TEXT NOT NULL,
        updated_at TEXT NOT NULL)""",
    """CREATE TABLE IF NOT EXISTS releases (
        release_id TEXT PRIMARY KEY, run_id TEXT NOT NULL UNIQUE,
        payload TEXT NOT NULL, created_at TEXT NOT NULL)""",
    """CREATE TABLE IF NOT EXISTS control (
        id INTEGER PRIMARY KEY, generation INTEGER NOT NULL,
        active_release TEXT, writer_run TEXT)""",
    """CREATE TABLE IF NOT EXISTS activations (
        generation INTEGER PRIMARY KEY, release_id TEXT NOT NULL,
        reason TEXT NOT NULL, activated_at TEXT NOT NULL)""",
]


class Control:
    """One control instance belongs to one dataset/namespace.

    Persistent writer ownership never expires automatically. A separate execution
    lock prevents simultaneous retries with the same run_id. PostgreSQL is the
    Compose backend; SQLite plus flock supports single-host labs only.
    """

    def __init__(self, root: Path, dsn: str = "", *, read_only=False):
        if not read_only:
            root.mkdir(parents=True, exist_ok=True)
        self.root = root
        self.pg = bool(dsn)
        self._executing = False
        if self.pg:
            import psycopg
            from psycopg.rows import dict_row

            self.db = psycopg.connect(dsn, autocommit=True, row_factory=dict_row, connect_timeout=5)
            if read_only:
                self.db.execute("SET default_transaction_read_only = on")
                self.db.execute("SET statement_timeout = '5s'")
        else:
            path = root / "audit.sqlite"
            self.db = sqlite3.connect(
                path.resolve().as_uri() + "?mode=ro" if read_only else path,
                uri=read_only,
                isolation_level=None,
                timeout=5 if read_only else 30,
            )
            self.db.row_factory = sqlite3.Row
            if not read_only:
                self.db.execute("PRAGMA journal_mode=WAL")
        if read_only:
            return
        for statement in DDL:
            self.execute(statement)
        self.execute("INSERT INTO control (id,generation) VALUES (1,0) ON CONFLICT (id) DO NOTHING")

    def close(self):
        self.db.close()

    def bind(self, namespace, warehouse):
        self.execute(
            "INSERT INTO dataset_binding VALUES (1,?,?) ON CONFLICT (id) DO NOTHING",
            (namespace, warehouse),
        )
        row = self.execute("SELECT * FROM dataset_binding WHERE id=1").fetchone()
        if row["namespace"] != namespace or row["warehouse"] != warehouse:
            raise LakeBridgeError("Audit store belongs to another namespace or warehouse")

    def execute(self, statement, parameters=()):
        return self.db.execute(statement.replace("?", "%s") if self.pg else statement, parameters)

    @contextlib.contextmanager
    def transaction(self):
        self.execute("BEGIN" if self.pg else "BEGIN IMMEDIATE")
        try:
            yield
            self.execute("COMMIT")
        except BaseException:
            self.execute("ROLLBACK")
            raise

    def status(self, *, for_update=False):
        suffix = " FOR UPDATE" if self.pg and for_update else ""
        row = self.execute("SELECT * FROM control WHERE id=1" + suffix).fetchone()
        if row is None:
            raise LakeBridgeError("Audit control row is missing")
        return dict(row)

    def run(self, run_id):
        row = self.execute("SELECT * FROM runs WHERE run_id=?", (run_id,)).fetchone()
        if not row:
            return None
        return {**json.loads(row["payload"]), "state": row["state"], "run_id": row["run_id"]}

    def save_run(self, run_id, state, payload):
        self.execute(
            """INSERT INTO runs VALUES (?,?,?,?) ON CONFLICT (run_id)
            DO UPDATE SET state=excluded.state,payload=excluded.payload,
            updated_at=excluded.updated_at""",
            (run_id, state, json.dumps(payload, default=str), now()),
        )

    def release(self, release_id=None):
        release_id = release_id or self.status()["active_release"]
        if not release_id:
            return None
        row = self.execute(
            "SELECT payload FROM releases WHERE release_id=?", (release_id,)
        ).fetchone()
        if not row:
            raise LakeBridgeError(f"Unknown release: {release_id}")
        return json.loads(row["payload"])

    def releases(self):
        return [
            json.loads(r["payload"])
            for r in self.execute("SELECT payload FROM releases ORDER BY created_at").fetchall()
        ]

    @contextlib.contextmanager
    def writer(self, run_id, timeout=0):
        if self._executing:
            raise ConflictError("Nested writer execution on one connection is not supported")
        started = time.monotonic()
        handle = None
        acquired = False
        key = None
        try:
            if self.pg:
                # Advisory locks are database-local. The table OID scopes this key
                # to the actual audit store, even when search_path uses aliases.
                key = self.execute(
                    "SELECT (78431069::bigint << 32) | 'control'::regclass::oid::bigint AS key"
                ).fetchone()["key"]
            else:
                handle = (self.root / "writer.lock").open("a")
            while not acquired:
                if self.pg:
                    acquired = self.execute(
                        "SELECT pg_try_advisory_lock(?) AS locked", (key,)
                    ).fetchone()["locked"]
                else:
                    try:
                        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
                        acquired = True
                    except BlockingIOError:
                        pass
                if not acquired:
                    if time.monotonic() - started >= timeout:
                        raise ConflictError("Another writer is executing; queue or retry later")
                    time.sleep(0.1)
            with self.transaction():
                status = self.status(for_update=True)
                if status["writer_run"] not in (None, run_id):
                    raise ConflictError(
                        f"Unfinished writer {status['writer_run']}; recover it first"
                    )
                self.execute("UPDATE control SET writer_run=? WHERE id=1", (run_id,))
            self._executing = True
            yield time.monotonic() - started
        finally:
            self._executing = False
            if acquired and self.pg and not self.db.closed:
                self.execute("SELECT pg_advisory_unlock(?)", (key,))
            if handle:
                handle.close()

    def finish_writer(self, run_id):
        self.execute("UPDATE control SET writer_run=NULL WHERE id=1 AND writer_run=?", (run_id,))

    def publish(self, release: dict, expected_generation: int):
        if not release.get("validation", {}).get("passed"):
            raise LakeBridgeError("Unvalidated data cannot be published")
        with self.transaction():
            status = self.status(for_update=True)
            existing = self.execute(
                "SELECT payload FROM releases WHERE release_id=?", (release["release_id"],)
            ).fetchone()
            if existing:
                if self._release_identity(json.loads(existing["payload"])) != (
                    self._release_identity(release)
                ):
                    raise ConflictError("release_id is bound to different immutable content")
                return  # Exactly-once audit recovery, never reactivates a rolled-back release.
            if status["generation"] != expected_generation:
                raise ConflictError("Release generation changed; reconcile before publishing")
            if status["writer_run"] != release["run_id"]:
                raise ConflictError("Writer ownership lost")
            generation = expected_generation + 1
            self.execute(
                "INSERT INTO releases VALUES (?,?,?,?)",
                (release["release_id"], release["run_id"], json.dumps(release, default=str), now()),
            )
            self.execute(
                "UPDATE control SET generation=?,active_release=? WHERE id=1",
                (generation, release["release_id"]),
            )
            self.execute(
                "INSERT INTO activations VALUES (?,?,?,?)",
                (generation, release["release_id"], "publish", now()),
            )

    @staticmethod
    def _release_identity(release):
        return json.dumps(
            {k: v for k, v in release.items() if k not in ("execution_seconds", "queue_seconds")},
            sort_keys=True,
            default=str,
        )

    def activate(self, release_id, reason, expected_generation):
        self.release(release_id)
        if not reason.strip():
            raise LakeBridgeError("Rollback/activation requires a reason")
        with self.transaction():
            status = self.status(for_update=True)
            if status["generation"] != expected_generation or status["writer_run"]:
                raise ConflictError("Writer in progress or activation generation changed")
            generation = expected_generation + 1
            self.execute(
                "UPDATE control SET generation=?,active_release=? WHERE id=1",
                (generation, release_id),
            )
            self.execute(
                "INSERT INTO activations VALUES (?,?,?,?)", (generation, release_id, reason, now())
            )
        return generation
