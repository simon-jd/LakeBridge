from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from lakebridge.common import LakeBridgeError, digest

EVENT_SCHEMA = pa.schema(
    [
        ("tenant_id", pa.string()),
        ("event_id", pa.string()),
        ("event_time", pa.timestamp("us")),
        ("ingested_at", pa.timestamp("us")),
        ("units", pa.decimal128(18, 6)),
        ("source_version", pa.int64()),
        ("is_deleted", pa.bool_()),
    ]
)
PLAN_SCHEMA = pa.schema(
    [
        ("tenant_id", pa.string()),
        ("plan_id", pa.string()),
        ("effective_from", pa.timestamp("us")),
        ("effective_to", pa.timestamp("us")),
        ("attributes", pa.string()),
    ]
)
SCHEMAS = {"events": EVENT_SCHEMA, "plans": PLAN_SCHEMA}


def schema_description(schema: pa.Schema) -> list[dict]:
    return [{"name": f.name, "type": str(f.type)} for f in schema]


def file_entry(root: Path, path: Path, kind: str, batch: int) -> dict:
    metadata = pq.ParquetFile(path)
    return {
        "path": path.relative_to(root).as_posix(),
        "kind": kind,
        "batch": batch,
        "sha256": digest(path),
        "bytes": path.stat().st_size,
        "rows": metadata.metadata.num_rows,
        "schema": schema_description(metadata.schema_arrow),
    }


@dataclass
class InputSet:
    path: Path
    watermark: int
    fingerprint: str
    entries: list[dict]
    dataset_id: str

    def files(self, kind: str) -> list[str]:
        return [str(self.path.parent / e["path"]) for e in self.entries if e["kind"] == kind]


def verify_manifest(path: Path, watermark: int | None = None) -> InputSet:
    path = path.resolve()
    try:
        manifest = json.loads(path.read_text())
        if type(manifest["contract_version"]) is not int or manifest["contract_version"] != 1:
            raise LakeBridgeError("Unsupported manifest contract version")
        if manifest.get("timezone") != "UTC":
            raise LakeBridgeError("Input timestamps must use UTC")
        if not isinstance(manifest.get("dataset_id"), str) or not manifest["dataset_id"].strip():
            raise LakeBridgeError("A nonempty dataset_id is required")
        maximum = manifest["high_watermark"]
        if type(maximum) is not int or maximum < 1:
            raise LakeBridgeError("Invalid high watermark")
        watermark = maximum if watermark is None else watermark
        if type(watermark) is not int or not 1 <= watermark <= maximum:
            raise LakeBridgeError("Watermark must be an existing positive arrival batch")
        entries = manifest["files"]
        paths: set[str] = set()
        batches: set[int] = set()
        selected = []
        for entry in entries:
            if (
                type(entry["rows"]) is not int
                or entry["rows"] < 0
                or type(entry["bytes"]) is not int
                or entry["bytes"] < 1
            ):
                raise LakeBridgeError("Invalid file row count or byte size")
            batch = entry["batch"]
            if type(batch) is not int or not 1 <= batch <= maximum:
                raise LakeBridgeError("Invalid arrival batch")
            batches.add(batch)
            if entry["path"] in paths:
                raise LakeBridgeError("Duplicate file in manifest")
            paths.add(entry["path"])
            relative = Path(entry["path"])
            file = (path.parent / relative).resolve()
            if relative.is_absolute() or not file.is_relative_to(path.parent):
                raise LakeBridgeError("Manifest path escapes immutable input root")
            if batch > watermark:
                continue
            if not file.is_file() or file.stat().st_size != entry["bytes"]:
                raise LakeBridgeError(f"Missing file or size mismatch: {relative}")
            if digest(file) != entry["sha256"]:
                raise LakeBridgeError(f"Content digest mismatch: {relative}")
            parquet = pq.ParquetFile(file)
            expected_schema = SCHEMAS[entry["kind"]]
            if not parquet.schema_arrow.equals(expected_schema, check_metadata=False):
                raise LakeBridgeError(f"Schema drift: {relative}")
            if schema_description(expected_schema) != entry["schema"]:
                raise LakeBridgeError(f"Manifest schema mismatch: {relative}")
            if parquet.metadata.num_rows != entry["rows"]:
                raise LakeBridgeError(f"Row count mismatch: {relative}")
            selected.append(entry)
        if batches != set(range(1, maximum + 1)):
            raise LakeBridgeError("Arrival batches are not contiguous")
        if not all(any(e["kind"] == kind for e in selected) for kind in SCHEMAS):
            raise LakeBridgeError("Events and plan history are both required")
        return InputSet(path, watermark, digest(path), selected, manifest["dataset_id"])
    except (KeyError, ValueError, TypeError, OSError, pa.ArrowException) as exc:
        raise LakeBridgeError(f"Invalid manifest: {exc}") from exc
