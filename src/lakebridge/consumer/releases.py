"""Registered immutable exports of release-pinned detail snapshots.

Only the worker export adapter touches Spark. API lookups read this registry and
the upstream ledger, never resolve a missing release to the active pointer.
"""

from __future__ import annotations

import json
import os
import tempfile
from dataclasses import asdict
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from lakebridge.common import digest
from lakebridge.consumer.protocol import (
    KEY_COLUMNS,
    ErrorCode,
    ReleasePin,
    TablePin,
    canonical,
    content_hash,
    logical_id,
    require,
)


def detail_schema():
    return pa.schema(
        [
            pa.field("tenant_id", pa.string(), nullable=False),
            pa.field("event_id", pa.string(), nullable=False),
            pa.field("event_time", pa.timestamp("us", tz="UTC"), nullable=False),
            pa.field("units", pa.decimal128(18, 6)),
            pa.field("plan_id", pa.string()),
            pa.field("billable_units", pa.decimal128(18, 6)),
            pa.field("rule_version", pa.string()),
            pa.field("is_deleted", pa.bool_(), nullable=False),
        ]
    )


def live_schema():
    return pa.schema(list(detail_schema())[:-1])


def pin_release(ledger, dataset_id: str, release_id: str) -> ReleasePin:
    logical_id(dataset_id)
    logical_id(release_id)
    # Control.release has a current fallback; never invoke it without an explicit ID.
    release = ledger.release(release_id)
    require(
        release is not None and release.get("dataset_id") == dataset_id,
        ErrorCode.NOT_FOUND,
        "No registered release for this dataset",
    )
    return ReleasePin(
        dataset_id,
        release_id,
        tuple(
            TablePin(name, entry["table"], str(entry["snapshot_id"]))
            for name, entry in sorted(release["tables"].items())
        ),
    )


class ReleaseRegistry:
    def __init__(self, root: Path):
        self.root = root.resolve()

    def directory(self, pin):
        return self.root / pin.dataset_id / pin.release_id

    def get(self, dataset_id: str, release_id: str):
        path = self.root / logical_id(dataset_id) / logical_id(release_id) / "manifest.json"
        require(path.is_file(), ErrorCode.NOT_FOUND, "Release export is not registered")
        manifest = json.loads(path.read_text())
        pin = ReleasePin.from_dict(manifest["pin"])
        require(
            pin.dataset_id == dataset_id and pin.release_id == release_id,
            ErrorCode.INTEGRITY_ERROR,
            "Registry identity mismatch",
        )
        return pin, manifest

    def files(self, pin: ReleasePin) -> list[str]:
        actual, manifest = self.get(pin.dataset_id, pin.release_id)
        require(actual == pin, ErrorCode.INTEGRITY_ERROR, "Frozen release pin changed")
        expected_hash = manifest.get("manifest_hash")
        require(
            expected_hash
            == content_hash({k: v for k, v in manifest.items() if k != "manifest_hash"}),
            ErrorCode.INTEGRITY_ERROR,
            "Export manifest changed",
        )
        require(
            manifest["status"] == "complete" and manifest["files"],
            ErrorCode.DEPENDENCY_UNAVAILABLE,
            "Export is incomplete",
        )
        files = []
        for entry in manifest["files"]:
            require(
                Path(entry["path"]).name == entry["path"],
                ErrorCode.INTEGRITY_ERROR,
                "Export files must be direct children of the registered directory",
            )
            path = self.directory(pin) / entry["path"]
            require(
                path.is_file() and not path.is_symlink(),
                ErrorCode.DEPENDENCY_UNAVAILABLE,
                "Export dependency is missing",
            )
            require(
                digest(path) == entry["sha256"],
                ErrorCode.INTEGRITY_ERROR,
                "Export dependency checksum changed",
            )
            require(
                pq.read_schema(path).equals(detail_schema()),
                ErrorCode.INCOMPATIBLE_SCHEMA,
                "Export schema requires an explicit adapter",
            )
            files.append(str(path))
        return files

    def register(self, pin: ReleasePin, batches, *, source_kind: str, source: dict):
        """Worker/admin-only registration, after a pinned scan or synthetic fixture build."""
        require(
            source_kind in ("iceberg_snapshot_export", "synthetic_fixture"),
            ErrorCode.INVALID_REQUEST,
            "Unknown export adapter",
        )
        destination = self.directory(pin)
        destination.parent.mkdir(parents=True, exist_ok=True)
        require(not destination.exists(), ErrorCode.CONFLICT, "Release export already exists")
        stage = Path(tempfile.mkdtemp(prefix=".export-", dir=destination.parent))
        path = stage / "detail.parquet"
        rows = 0
        # No completion manifest on exceptions; partial files stay in diagnostic staging.
        with pq.ParquetWriter(path, detail_schema(), compression="zstd") as writer:
            for batch in batches:
                require(
                    batch.schema.equals(detail_schema()),
                    ErrorCode.INCOMPATIBLE_SCHEMA,
                    "Normalized batch schema mismatch",
                )
                for field in detail_schema():
                    if not field.nullable:
                        require(
                            batch.column(field.name).null_count == 0,
                            ErrorCode.INVALID_KEYS,
                            f"Null {field.name} in normalized source",
                        )
                writer.write_batch(batch)
                rows += batch.num_rows
        manifest = {
            "status": "complete",
            "pin": asdict(pin),
            "source_kind": source_kind,
            "source": source,
            "schema": "detail.utc.v1",
            "rows": rows,
            "files": [{"path": path.name, "rows": rows, "sha256": digest(path)}],
        }
        manifest["manifest_hash"] = content_hash(manifest)
        (stage / "manifest.json").write_bytes(canonical(manifest))
        os.rename(stage, destination)
        return manifest


def export_snapshot(
    spark, ledger, registry: ReleaseRegistry, dataset_id, release_id, scratch: Path
):
    """Explicit Spark adapter: scan snapshot ID, project, globally sort, then export.

    Source Spark TIMESTAMP has UTC session semantics and no Arrow timezone. The
    only type adaptation attaches that known UTC meaning at microsecond precision.
    Values/decimal types are never inferred from JSON or coerced from float.
    """

    require(
        spark.conf.get("spark.sql.session.timeZone") == "UTC",
        ErrorCode.INCOMPATIBLE_SCHEMA,
        "Snapshot export requires Spark UTC session",
    )
    pin = pin_release(ledger, dataset_id, release_id)
    detail = next(t for t in pin.tables if t.name == "detail")
    frame = spark.read.option("snapshot-id", detail.snapshot_id).table(detail.table)
    frame = frame.select(*detail_schema().names)
    export_dir = scratch / content_hash(pin)
    frame.orderBy(*KEY_COLUMNS).coalesce(1).write.mode("error").parquet(str(export_dir))
    # Directory enumeration is limited to output of this just-completed pinned scan;
    # it is never used to discover underlying Iceberg data files.
    paths = sorted(export_dir.glob("part-*.parquet"))
    require(bool(paths), ErrorCode.DEPENDENCY_UNAVAILABLE, "Spark export returned no data files")

    def batches():
        for path in paths:
            for batch in pq.ParquetFile(path).iter_batches(batch_size=8192):
                fields = list(detail_schema())
                expected = pa.schema(
                    [
                        pa.field(f.name, pa.timestamp("us") if f.name == "event_time" else f.type)
                        for f in fields
                    ]
                )
                require(
                    all(batch.schema.field(f.name).type == f.type for f in expected),
                    ErrorCode.INCOMPATIBLE_SCHEMA,
                    "Upstream schema requires an adapter",
                )
                yield (
                    pa.Table.from_batches([batch]).cast(detail_schema(), safe=True).to_batches()[0]
                )

    # Resolve and scan only the pinned detail; plans/daily remain recorded provenance.
    return registry.register(
        pin,
        batches(),
        source_kind="iceberg_snapshot_export",
        source={
            "table": detail.table,
            "snapshot_id": detail.snapshot_id,
            "timestamp_adapter": "spark-utc-us-to-arrow-utc-us",
        },
    )
