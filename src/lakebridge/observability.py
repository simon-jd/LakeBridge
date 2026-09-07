"""Read-only audit snapshots and bounded-cardinality Prometheus textfile output."""

from __future__ import annotations

import os
import sqlite3
import tempfile
from datetime import UTC, datetime
from pathlib import Path

from lakebridge.common import LakeBridgeError, now
from lakebridge.control import Control

STATES = ("RUNNING", "WRITING", "VALIDATED", "COMMITTED", "COMPLETED", "FAILED", "ABORTED")


def inspect_audit(settings):
    import psycopg

    control = None
    try:
        control = Control(settings.root, settings.audit_dsn, read_only=True)
        control.execute(
            "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY" if control.pg else "BEGIN"
        )
        binding = control.execute("SELECT * FROM dataset_binding WHERE id=1").fetchone()
        if not binding or (binding["namespace"], binding["warehouse"]) != (
            settings.namespace,
            settings.warehouse,
        ):
            raise LakeBridgeError("Audit binding is missing or differs from configured dataset")
        status = control.status()
        counts = {state: 0 for state in (*STATES, "OTHER")}
        for row in control.execute("SELECT state, count(*) AS n FROM runs GROUP BY state"):
            counts[row["state"] if row["state"] in STATES else "OTHER"] += row["n"]
        releases = control.execute(
            "SELECT count(*) AS n, max(created_at) AS latest FROM releases"
        ).fetchone()
        active = control.release(status["active_release"]) if status["active_release"] else None
        writer = control.execute(
            "SELECT state, updated_at FROM runs WHERE run_id=?", (status["writer_run"],)
        ).fetchone()
        control.execute("COMMIT")
        observed = datetime.now(UTC)
        # This is time since a state update, NOT a heartbeat or an expiring lease.
        age = (
            max(0, (observed - datetime.fromisoformat(writer["updated_at"])).total_seconds())
            if writer
            else None
        )
        return {
            **status,
            "audit_available": True,
            "observed_at": observed.isoformat(),
            "observed_timestamp_seconds": observed.timestamp(),
            "backend": "postgresql" if control.pg else "sqlite",
            "active_release_validated": bool(active and active.get("validation", {}).get("passed")),
            "release_count": releases["n"],
            "last_publish_at": releases["latest"],
            "last_publish_timestamp_seconds": (
                datetime.fromisoformat(releases["latest"]).timestamp() if releases["latest"] else 0
            ),
            "run_counts": counts,
            "writer_state": writer["state"] if writer else None,
            "writer_state_age_seconds": age,
            "writer_state_missing": bool(status["writer_run"] and not writer),
        }
    except (OSError, sqlite3.Error, psycopg.Error, ValueError, TypeError) as exc:
        # DSNs, credentials and server messages must not enter monitoring output.
        raise LakeBridgeError(
            "Audit inspection failed; check database access and audit schema"
        ) from exc
    finally:
        if control:
            control.close()


def prometheus(snapshot):
    lines = []

    def gauge(name, help_text, value):
        name = "lakebridge_" + name
        lines.extend([f"# HELP {name} {help_text}", f"# TYPE {name} gauge", f"{name} {value}"])

    gauge(
        "audit_up",
        "Whether the configured audit store was read successfully.",
        int(snapshot["audit_available"]),
    )
    gauge(
        "audit_observed_timestamp_seconds",
        "Unix time of this collection attempt.",
        snapshot["observed_timestamp_seconds"],
    )
    if snapshot["audit_available"]:
        gauge("release_generation", "Current consumer release generation.", snapshot["generation"])
        gauge("releases", "Number of immutable audited releases.", snapshot["release_count"])
        gauge(
            "active_release_validated",
            "Active release carries a passing validation result.",
            int(snapshot["active_release_validated"]),
        )
        gauge(
            "last_publish_timestamp_seconds",
            "Latest audit publication time; zero if none.",
            snapshot["last_publish_timestamp_seconds"],
        )
        gauge(
            "writer_owned",
            "Persistent writer ownership; not proof of a live executor.",
            int(bool(snapshot["writer_run"])),
        )
        gauge(
            "writer_state_missing",
            "Writer ownership has no matching audit run.",
            int(snapshot["writer_state_missing"]),
        )
        if snapshot["writer_state_age_seconds"] is not None:
            gauge(
                "writer_state_age_seconds",
                "Seconds since writer state update; not a heartbeat.",
                snapshot["writer_state_age_seconds"],
            )
        lines.extend(
            [
                "# HELP lakebridge_runs Audited runs by bounded state.",
                "# TYPE lakebridge_runs gauge",
            ]
        )
        for state in (*STATES, "OTHER"):
            lines.append(f'lakebridge_runs{{state="{state}"}} {snapshot["run_counts"][state]}')
    return "\n".join(lines) + "\n"


def unavailable():
    return {
        "audit_available": False,
        "observed_at": now(),
        "observed_timestamp_seconds": datetime.now(UTC).timestamp(),
    }


def write_textfile(path: Path, text: str):
    """Rename in the destination directory so a collector never reads partial text."""
    path = path.resolve()
    with tempfile.NamedTemporaryFile(
        mode="w", dir=path.parent, prefix=".lakebridge-", suffix=".tmp", delete=False
    ) as stream:
        temporary = Path(stream.name)
        try:
            stream.write(text)
            stream.flush()
            os.fsync(stream.fileno())
            os.chmod(temporary, 0o644)
            temporary.replace(path)
        finally:
            temporary.unlink(missing_ok=True)
