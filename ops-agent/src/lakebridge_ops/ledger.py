"""Durable task budgets and call receipts, independent of LangGraph replay.

Reserve a bounded call's time and count before invoking it. A process that dies
inside the call keeps that reservation charged; replay cannot reset its budget.
Waiting for external input/approval never reserves execution time.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from pathlib import Path
from time import monotonic


class BudgetExceeded(RuntimeError):
    pass


def encoded(value):
    return json.dumps(
        value, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False
    )


def fingerprint(value):
    return hashlib.sha256(encoded(value).encode()).hexdigest()


class Ledger:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path, isolation_level=None)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS tasks (
                task_id TEXT PRIMARY KEY, config TEXT NOT NULL,
                model_calls INTEGER NOT NULL DEFAULT 0, tool_calls INTEGER NOT NULL DEFAULT 0,
                active_ms INTEGER NOT NULL DEFAULT 0);
            CREATE TABLE IF NOT EXISTS calls (
                call_id INTEGER PRIMARY KEY, task_id TEXT NOT NULL, kind TEXT NOT NULL,
                operation TEXT NOT NULL, request TEXT NOT NULL, cache_key TEXT NOT NULL,
                reserved_ms INTEGER NOT NULL, state TEXT NOT NULL, result TEXT);
        """)

    def close(self):
        self.db.close()

    def create(self, task_id, config):
        self.db.execute(
            "INSERT OR IGNORE INTO tasks(task_id,config) VALUES (?,?)", (task_id, encoded(config))
        )
        if self.task(task_id)["config"] != config:
            raise ValueError("Task configuration/identity/budgets changed; use a new task ID")

    def task(self, task_id):
        row = self.db.execute("SELECT * FROM tasks WHERE task_id=?", (task_id,)).fetchone()
        if row is None:
            raise ValueError("Unknown task")
        return {**dict(row), "config": json.loads(row["config"])}

    def cached(self, task_id, key):
        row = self.db.execute(
            """SELECT result FROM calls WHERE task_id=? AND cache_key=?
            AND state='COMPLETE' ORDER BY call_id DESC LIMIT 1""",
            (task_id, key),
        ).fetchone()
        return json.loads(row["result"]) if row else None

    def reserve(self, task_id, kind, operation, request, key):
        if kind not in ("model", "tool"):
            raise ValueError("Unknown budget kind")
        self.db.execute("BEGIN IMMEDIATE")
        try:
            task = self.task(task_id)
            cfg = task["config"]["budget"]
            reserved = min(cfg["call_timeout_ms"], cfg["active_ms"] - task["active_ms"])
            if task[kind + "_calls"] >= cfg[kind + "_calls"] or reserved <= 0:
                raise BudgetExceeded(
                    "Cumulative task budget exhausted; waiting/resume cannot reset it"
                )
            self.db.execute(
                f"UPDATE tasks SET {kind}_calls={kind}_calls+1,active_ms=active_ms+?"
                " WHERE task_id=?",
                (reserved, task_id),
            )
            call = self.db.execute(
                """INSERT INTO calls
                (task_id,kind,operation,request,cache_key,reserved_ms,state)
                VALUES (?,?,?,?,?,?,'RUNNING')""",
                (task_id, kind, operation, encoded(request), key, reserved),
            )
            self.db.execute("COMMIT")
            return call.lastrowid, reserved, monotonic()
        except BaseException:
            self.db.execute("ROLLBACK")
            raise

    def finish(self, call_id, started, result, cacheable):
        self.db.execute("BEGIN IMMEDIATE")
        try:
            call = self.db.execute("SELECT * FROM calls WHERE call_id=?", (call_id,)).fetchone()
            if call["state"] != "RUNNING":
                raise ValueError("Call receipt is already terminal")
            elapsed = max(1, int((monotonic() - started) * 1000))
            # Include actual overrun (e.g. cancellation cleanup); do not hide it.
            self.db.execute(
                "UPDATE tasks SET active_ms=active_ms+? WHERE task_id=?",
                (elapsed - call["reserved_ms"], call["task_id"]),
            )
            self.db.execute(
                "UPDATE calls SET state=?,result=? WHERE call_id=?",
                ("COMPLETE" if cacheable else "OBSERVED", encoded(result), call_id),
            )
            self.db.execute("COMMIT")
        except BaseException:
            self.db.execute("ROLLBACK")
            raise

    def receipts(self, task_id):
        return [
            dict(row)
            for row in self.db.execute(
                """SELECT call_id,kind,operation,state,
            reserved_ms FROM calls WHERE task_id=? ORDER BY call_id""",
                (task_id,),
            )
        ]

    def overhead(self, task_id, elapsed_ms, active_before):
        """Charge graph/checkpoint work in addition to already charged external calls."""
        charged = self.task(task_id)["active_ms"] - active_before
        self.db.execute(
            "UPDATE tasks SET active_ms=active_ms+? WHERE task_id=?",
            (max(0, elapsed_ms - charged), task_id),
        )
