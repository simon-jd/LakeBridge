"""SQL reference implementation and complete Parquet artifact contract."""

from __future__ import annotations

import json
import os
import tempfile
from collections import Counter
from dataclasses import asdict
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from time import monotonic
from typing import Protocol

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from lakebridge.common import digest
from lakebridge.consumer.protocol import (
    COMPARE_COLUMNS,
    KEY_COLUMNS,
    DiffRequest,
    ErrorCode,
    canonical,
    content_hash,
    require,
    utc_timestamp,
)
from lakebridge.consumer.releases import ReleaseRegistry, detail_schema, live_schema


def diff_schema():
    # Outer-join structs have nullable physical children; rows() enforces non-null
    # keys on present records. A null parent represents an absent before/after.
    children = [pa.field(f.name, f.type) for f in live_schema()]
    return pa.schema(
        [
            pa.field("change_type", pa.string(), nullable=False),
            pa.field("key", pa.struct(children[:2]), nullable=False),
            pa.field("before", pa.struct(children)),
            pa.field("after", pa.struct(children)),
        ]
    )


def wire_row(value):
    if isinstance(value, Decimal):
        return format(value, ".6f")
    if isinstance(value, datetime):
        return utc_timestamp(value)
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, dict):
        return {k: wire_row(v) for k, v in value.items()}
    return value


class DiffBackend(Protocol):
    def run(self, request: DiffRequest, registry: ReleaseRegistry, output: Path) -> dict: ...


def reference_sql():
    same_key = " AND ".join(f"l.{key}=r.{key}" for key in KEY_COLUMNS)
    different = " OR ".join(f"l.{col} IS DISTINCT FROM r.{col}" for col in COMPARE_COLUMNS)

    def packed(side):
        return (
            "struct_pack(" + ",".join(f"{col}:={side}.{col}" for col in live_schema().names) + ")"
        )

    return f"""
    SELECT CASE WHEN l.tenant_id IS NULL THEN 'INSERT'
                WHEN r.tenant_id IS NULL THEN 'DELETE' ELSE 'UPDATE' END AS change_type,
           struct_pack(tenant_id:=coalesce(l.tenant_id,r.tenant_id),
                       event_id:=coalesce(l.event_id,r.event_id)) AS key,
           CASE WHEN l.tenant_id IS NULL THEN NULL ELSE {packed("l")} END AS before,
           CASE WHEN r.tenant_id IS NULL THEN NULL ELSE {packed("r")} END AS after
    FROM left_live l FULL OUTER JOIN right_live r ON {same_key}
    WHERE l.tenant_id IS NULL OR r.tenant_id IS NULL OR ({different})
    ORDER BY coalesce(l.tenant_id,r.tenant_id),coalesce(l.event_id,r.event_id)
    """


class SqlDiffBackend:
    def __init__(self, *, memory_limit="512MB", batch_size=8192):
        require(1 <= batch_size <= 65536, ErrorCode.INVALID_REQUEST, "Invalid output batch size")
        self.memory_limit, self.batch_size = memory_limit, batch_size

    def run(self, request: DiffRequest, registry: ReleaseRegistry, output: Path) -> dict:
        require(not output.exists(), ErrorCode.CONFLICT, "Artifact destination already exists")
        output.parent.mkdir(parents=True, exist_ok=True)
        stage = Path(tempfile.mkdtemp(prefix=".diff-", dir=output.parent))
        started = monotonic()
        db = duckdb.connect()
        try:
            db.execute("SET TimeZone='UTC'")
            db.execute("SET memory_limit = ?", [self.memory_limit])
            db.execute("SET temp_directory = ?", [str(stage / "spill")])
            db.execute("SET threads=2")
            for side, pin in (("left", request.from_pin), ("right", request.to_pin)):
                if pin is None:
                    db.register(f"{side}_raw", pa.Table.from_batches([], schema=detail_schema()))
                else:
                    db.from_parquet(registry.files(pin)).create_view(f"{side}_raw")
                invalid = db.execute(f"""SELECT 1 FROM {side}_raw
                    WHERE tenant_id IS NULL OR tenant_id='' OR event_id IS NULL OR event_id=''
                          OR event_time IS NULL OR is_deleted IS NULL LIMIT 1""").fetchone()
                require(
                    not invalid, ErrorCode.INVALID_KEYS, "Null/empty key or missing date/tombstone"
                )
                duplicate = db.execute(f"""SELECT 1 FROM {side}_raw GROUP BY tenant_id,event_id
                                           HAVING count(*)>1 LIMIT 1""").fetchone()
                require(not duplicate, ErrorCode.INVALID_KEYS, "Duplicate source key")
                db.sql(
                    f"""SELECT * EXCLUDE(is_deleted) FROM {side}_raw
                    WHERE NOT is_deleted AND tenant_id=$tenant
                    AND event_time >= $start::DATE::TIMESTAMPTZ
                    AND event_time < $end::DATE::TIMESTAMPTZ""",
                    params={
                        "tenant": request.scope.tenant_id,
                        "start": request.scope.start,
                        "end": request.scope.end,
                    },
                ).create_view(f"{side}_live")
            reader = db.execute(reference_sql()).fetch_record_batch(self.batch_size)
            manifest = publish_artifact(
                request,
                reader,
                stage,
                backend="duckdb-sql",
                batch_size=self.batch_size,
                started=started,
            )
            os.rename(stage, output)
            return manifest
        finally:
            db.close()


def publish_artifact(request, batches, stage, *, backend, batch_size, started):
    counts = Counter({"INSERT": 0, "DELETE": 0, "UPDATE": 0})
    path = stage / "changes.parquet"
    with pq.ParquetWriter(path, diff_schema(), compression="zstd") as writer:
        for batch in batches:
            batch = pa.Table.from_batches([batch]).cast(diff_schema(), safe=True).to_batches()[0]
            counts.update(batch.column("change_type").to_pylist())
            writer.write_batch(batch)
    manifest = {
        "status": "complete",
        "request": asdict(request),
        "request_hash": request.request_hash,
        "counts": dict(counts),
        "files": [{"path": path.name, "sha256": digest(path), "rows": sum(counts.values())}],
        "backend": backend,
        "output_batch_rows": batch_size,
        "metrics": {"total_ms": int((monotonic() - started) * 1000)},
    }
    manifest["checksum"] = content_hash(manifest)
    (stage / "manifest.json").write_bytes(canonical(manifest))
    return manifest


class DiffArtifact:
    def __init__(self, directory: Path, expected_request: DiffRequest):
        self.directory = directory
        path = directory / "manifest.json"
        require(path.is_file(), ErrorCode.INCOMPLETE_DIFF, "Diff has no complete manifest")
        self.manifest = json.loads(path.read_text())
        manifest = self.manifest
        require(
            manifest.get("status") == "complete",
            ErrorCode.INCOMPLETE_DIFF,
            "Partial or preview-only diffs cannot generate a plan",
        )
        require(
            manifest.get("checksum")
            == content_hash({k: v for k, v in manifest.items() if k != "checksum"}),
            ErrorCode.INTEGRITY_ERROR,
            "Diff manifest checksum mismatch",
        )
        require(
            manifest["request_hash"] == expected_request.request_hash
            and canonical(manifest["request"]) == canonical(expected_request),
            ErrorCode.INTEGRITY_ERROR,
            "Diff is for a different pinned request",
        )
        require(manifest["files"], ErrorCode.INCOMPLETE_DIFF, "Diff files are missing")
        self.request = expected_request
        self.paths = []
        for entry in manifest["files"]:
            require(
                Path(entry["path"]).name == entry["path"],
                ErrorCode.INTEGRITY_ERROR,
                "Invalid artifact file name",
            )
            file = directory / entry["path"]
            require(
                file.is_file() and not file.is_symlink() and digest(file) == entry["sha256"],
                ErrorCode.INTEGRITY_ERROR,
                "Diff file is missing or changed",
            )
            require(
                pq.read_schema(file).equals(diff_schema()),
                ErrorCode.INCOMPATIBLE_SCHEMA,
                "Unexpected diff schema",
            )
            require(
                pq.ParquetFile(file).metadata.num_rows == entry["rows"],
                ErrorCode.INTEGRITY_ERROR,
                "Diff row count mismatch",
            )
            self.paths.append(file)
        require(
            len(set(self.paths)) == len(self.paths),
            ErrorCode.INTEGRITY_ERROR,
            "Duplicate diff file entry",
        )

    def rows(self):
        counts = Counter({"INSERT": 0, "DELETE": 0, "UPDATE": 0})
        previous = None
        for path in self.paths:
            for batch in pq.ParquetFile(path).iter_batches(batch_size=8192):
                for row in batch.to_pylist():
                    kind = row["change_type"]
                    before, after = row["before"], row["after"]
                    require(
                        kind in counts
                        and (
                            (kind == "INSERT" and before is None and after is not None)
                            or (kind == "DELETE" and before is not None and after is None)
                            or (kind == "UPDATE" and before is not None and after is not None)
                        ),
                        ErrorCode.INTEGRITY_ERROR,
                        "Invalid before/after diff record",
                    )
                    key = tuple(row["key"][k] for k in KEY_COLUMNS)
                    require(
                        all(isinstance(k, str) and k for k in key)
                        and (previous is None or key > previous),
                        ErrorCode.INVALID_KEYS,
                        "Diff keys must be nonempty, unique and globally ordered",
                    )
                    for value in (before, after):
                        if value is not None:
                            require(
                                tuple(value[k] for k in KEY_COLUMNS) == key
                                and value["tenant_id"] == self.request.scope.tenant_id
                                and self.request.scope.start
                                <= value["event_time"].date().isoformat()
                                < self.request.scope.end,
                                ErrorCode.INTEGRITY_ERROR,
                                "Diff row escaped the registered scope",
                            )
                    previous = key
                    counts[kind] += 1
                    yield row
        require(
            dict(counts) == self.manifest["counts"],
            ErrorCode.INTEGRITY_ERROR,
            "Complete artifact counts do not match records",
        )

    def preview(self, limit=20):
        require(0 <= limit <= 100, ErrorCode.INVALID_REQUEST, "Preview limit must be 0..100")
        # Validate the complete artifact even when only a bounded preview is requested.
        sample = []
        total = 0
        for row in self.rows():
            if total < limit:
                sample.append(wire_row(row))
            total += 1
        return {"rows": sample, "truncated": total > limit, "total_rows": total}
