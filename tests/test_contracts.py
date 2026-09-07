import json
from datetime import datetime
from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from lakebridge.common import LakeBridgeError, identifier
from lakebridge.contracts import file_entry, verify_manifest
from lakebridge.generate import generate
from lakebridge.pipeline import date_scope


@pytest.fixture
def source(tmp_path):
    return generate(tmp_path / "source", rows=30, tenants=3, days=7)


def test_generator_is_deterministic(tmp_path, source):
    other = generate(tmp_path / "copy", rows=30, tenants=3, days=7)
    assert json.loads(source.read_text()) == json.loads(other.read_text())
    one, three = verify_manifest(source, 1), verify_manifest(source, 3)
    assert len(one.entries) < len(three.entries)
    assert (source.parent / "expected/h3_v1/detail.parquet").is_file()
    data = pq.read_table(source.parent / "expected/h3_v1/detail.parquet").to_pylist()
    assert any(r["is_deleted"] and r["source_version"] == 3 for r in data)
    assert all(isinstance(r["units"], Decimal) for r in data)


@pytest.mark.parametrize(
    "fault", ["missing", "content", "rows", "schema", "path", "duplicate", "gap"]
)
def test_manifest_rejects_tampering(source, fault):
    manifest = json.loads(source.read_text())
    entry = next(e for e in manifest["files"] if e["kind"] == "events")
    file = source.parent / entry["path"]
    if fault == "missing":
        file.unlink()
    elif fault == "content":
        content = bytearray(file.read_bytes())
        content[20] ^= 1
        file.write_bytes(content)
    elif fault == "rows":
        entry["rows"] += 1
    elif fault == "schema":
        pq.write_table(pa.table({"units": ["oops"]}), file)
        entry.update(file_entry(source.parent, file, "events", 1))
    elif fault == "path":
        entry["path"] = "../../outside.parquet"
    elif fault == "duplicate":
        manifest["files"].append(entry.copy())
    else:
        manifest["high_watermark"] = 4
    source.write_text(json.dumps(manifest))
    with pytest.raises(LakeBridgeError):
        verify_manifest(source)


@pytest.mark.parametrize("watermark", [0, -1, 4, True, "1"])
def test_invalid_watermark(source, watermark):
    with pytest.raises(LakeBridgeError):
        verify_manifest(source, watermark)


def test_no_overwrite(source):
    with pytest.raises(LakeBridgeError, match="empty"):
        generate(source.parent)


@pytest.mark.parametrize("value", ["x; DROP TABLE x", "../oops", "x.y", "a'", ""])
def test_identifiers_fail_closed(value):
    with pytest.raises(LakeBridgeError):
        identifier(value)


def test_scope():
    assert date_scope("2025-01-01", "2025-01-31") == ("2025-01-01", "2025-01-31")
    for start, end in [("2025-01-02", "2025-01-01"), ("oops", "2025-01-01"), ("2025-01-01", None)]:
        with pytest.raises(LakeBridgeError):
            date_scope(start, end)


def test_late_batch_has_old_event_dates(source):
    data = verify_manifest(source, 2)
    late = [e for e in data.entries if e["batch"] == 2 and e["kind"] == "events"]
    first = pq.ParquetFile(source.parent / late[0]["path"]).read().to_pylist()[0]
    assert first["event_time"] < datetime(2025, 1, 8)
    assert first["ingested_at"] >= datetime(2025, 1, 8)


def test_published_json_schema_matches_generator(source):
    from pathlib import Path

    import jsonschema

    schema = json.loads(Path("contracts/manifest.schema.json").read_text())
    jsonschema.validate(json.loads(source.read_text()), schema)


@pytest.mark.parametrize(
    "field,value",
    [("timezone", "America/Los_Angeles"), ("dataset_id", ""), ("high_watermark", True)],
)
def test_manifest_metadata_is_validated(source, field, value):
    payload = json.loads(source.read_text())
    payload[field] = value
    source.write_text(json.dumps(payload))
    with pytest.raises(LakeBridgeError):
        verify_manifest(source)
