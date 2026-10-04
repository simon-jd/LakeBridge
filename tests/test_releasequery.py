"""Cross-language contract and CLI tests; build the Rust binary before running."""

import copy
import json
import os
import subprocess
from dataclasses import asdict
from pathlib import Path

import pytest
from test_consumer_diff import event, register

from lakebridge.common import digest
from lakebridge.consumer.diff_backend import DiffArtifact, SqlDiffBackend
from lakebridge.consumer.protocol import BridgeError, DiffRequest, Scope, canonical, content_hash
from lakebridge.consumer.releases import ReleaseRegistry
from lakebridge.consumer.rust_backend import RustDiffBackend


@pytest.fixture
def executable():
    path = Path(
        os.environ.get("LAKEBRIDGE_RELEASE_QUERY", "release-query/target/debug/release-query")
    )
    if not path.is_file():
        pytest.skip("Build release-query or set LAKEBRIDGE_RELEASE_QUERY")
    return path.resolve()


@pytest.mark.parametrize(
    "left,right",
    [
        ([], []),
        ([], [event("new")]),
        ([event("gone")], []),
        ([event("same", plan_id=None, units=None)], [event("same", plan_id=None, units=None)]),
        ([event("a"), event("b")], [event("b"), event("a")]),
        (
            [
                event("decimal", units="999999999999.999998"),
                event("gone"),
                event("move"),
                event("null", plan_id=None),
                event("other", tenant="B"),
            ],
            [
                event("decimal", units="999999999999.999999"),
                event("gone", is_deleted=True),
                event("move", day=4),
                event("null", plan_id="pro"),
                event("new", day=3),
                event("boundary", day=5),
                event("other", tenant="B", units="99"),
            ],
        ),
    ],
)
def test_sql_rust_full_artifact_parity(tmp_path, executable, left, right):
    registry = ReleaseRegistry(tmp_path / "exports")
    a, b = register(registry, "a", left), register(registry, "b", right)
    request = DiffRequest(a, b, Scope("A", "2025-01-01", "2025-01-05"))
    SqlDiffBackend(batch_size=2).run(request, registry, tmp_path / "sql")
    RustDiffBackend(
        executable,
        options={
            "limits": {
                "input_batch_rows": 2,
                "output_batch_rows": 1,
                "max_value_bytes": 1000,
                "output_buffer_bytes": 32768,
            }
        },
    ).run(request, registry, tmp_path / "rust")
    rust, sql = [DiffArtifact(tmp_path / name, request) for name in ("rust", "sql")]
    assert list(rust.rows()) == list(sql.rows())
    assert rust.manifest["request_hash"] == content_hash(asdict(request))
    assert rust.manifest["counts"] == sql.manifest["counts"]
    assert "SortExec" in rust.manifest["physical_plan"]
    if sum(rust.manifest["counts"].values()) > 1:
        assert rust.preview(1)["truncated"]


@pytest.mark.parametrize(
    "rows,options",
    [
        ([event("dup"), event("dup", is_deleted=True)], {}),
        ([event("dup", tenant="B"), event("dup", tenant="B")], {}),
        ([event("b"), event("a")], {"inputs_sorted": True}),
        (
            [event("wide", plan_id="X" * 2000)],
            {
                "limits": {
                    "input_batch_rows": 8192,
                    "output_batch_rows": 2,
                    "max_value_bytes": 1000,
                    "output_buffer_bytes": 32768,
                }
            },
        ),
    ],
)
def test_invalid_input_never_publishes_complete_artifact(tmp_path, executable, rows, options):
    registry = ReleaseRegistry(tmp_path / "exports")
    pin = register(registry, "a", rows)
    request = DiffRequest(None, pin, Scope("A", "2025-01-01", "2025-01-05"))
    with pytest.raises(BridgeError):
        RustDiffBackend(executable, options=options).run(request, registry, tmp_path / "diff")
    assert not (tmp_path / "diff").exists()
    failures = list(tmp_path.glob(".release-query-*/failure.json"))
    assert failures
    assert not list(tmp_path.glob(".release-query-*/manifest.json"))


def test_cli_inspect_explain_canonical_and_checksum_rejection(tmp_path, executable):
    registry = ReleaseRegistry(tmp_path / "exports")
    pin = register(registry, "a", [event("a")])
    request = DiffRequest(None, pin, Scope("A", "2025-01-01", "2025-01-05"))
    _, manifest = registry.get(pin.dataset_id, pin.release_id)
    value = {
        "request": asdict(request),
        "left_source": None,
        "right_source": {
            "kind": "parquet_export",
            "manifest_path": str(registry.directory(pin) / "manifest.json"),
            "manifest_hash": manifest["manifest_hash"],
        },
    }
    path = tmp_path / "request.json"
    path.write_bytes(canonical(value))
    for command in ["inspect", "explain"]:
        result = subprocess.run(
            [str(executable), command, str(path)], capture_output=True, text=True, check=True
        )
        response = json.loads(result.stdout)
        assert response["request_hash"] == request.request_hash
        assert response["scanned_complete"] is False
        if command == "explain":
            assert "ReleaseDiffExec" in response["physical_plan"]
    value["right_source"]["manifest_hash"] = "0" * 64
    path.write_bytes(canonical(value))
    result = subprocess.run(
        [str(executable), "diff", str(path), "--output", str(tmp_path / "bad")],
        capture_output=True,
        text=True,
    )
    assert result.returncode != 0 and "INTEGRITY_ERROR" in result.stderr
    assert not (tmp_path / "bad").exists()


def test_real_native_snapshot_matches_exports_and_rejects_features(tmp_path, executable):
    fixture = Path(
        os.environ.get("LAKEBRIDGE_NATIVE_REQUEST", "artifacts/release-query/native-request.json")
    )
    if not fixture.is_file():
        pytest.skip("Generate the real pinned native request using the ReleaseQuery demo")
    value = json.loads(fixture.read_text())
    request = DiffRequest.from_dict(value["request"])
    registry = ReleaseRegistry(Path("examples/consumer/pinned-exports"))
    native = {v["pin"]["release_id"]: v for v in (value["left_source"], value["right_source"])}
    RustDiffBackend(executable, native_sources=native).run(request, registry, tmp_path / "native")
    SqlDiffBackend().run(request, registry, tmp_path / "sql")
    assert list(DiffArtifact(tmp_path / "native", request).rows()) == list(
        DiffArtifact(tmp_path / "sql", request).rows()
    )
    # These corrupted/unsupported metadata descriptors are rejection fixtures, not
    # claims that actual Iceberg delete files were read by this v1 adapter.
    for mode in ("version", "schema", "deletes", "field_id", "missing_snapshot"):
        bad = copy.deepcopy(value)
        descriptor = bad["right_source"]
        metadata = json.loads(Path(descriptor["metadata_path"]).read_text())
        if mode == "version":
            metadata["format-version"] = 3
        elif mode == "schema":
            metadata["schemas"].append(copy.deepcopy(metadata["schemas"][0]))
        elif mode == "field_id":
            metadata["schemas"][0]["fields"][0]["id"] = 101
        elif mode == "missing_snapshot":
            metadata["snapshots"] = []
        else:
            for snapshot in metadata["snapshots"]:
                snapshot["summary"]["total-delete-files"] = "1"
        meta_path = tmp_path / f"{mode}.metadata.json"
        meta_path.write_text(json.dumps(metadata))
        descriptor.update(metadata_path=str(meta_path), metadata_sha256=digest(meta_path))
        request_path = tmp_path / f"{mode}.json"
        request_path.write_bytes(canonical(bad))
        destination = tmp_path / mode
        result = subprocess.run(
            [str(executable), "diff", str(request_path), "--output", str(destination)],
            capture_output=True,
            text=True,
        )
        assert result.returncode != 0
        assert not destination.exists()


def test_late_duplicate_after_partial_output_keeps_only_failed_staging(tmp_path, executable):
    registry = ReleaseRegistry(tmp_path / "exports")
    pin = register(registry, "late", [event("a"), event("b"), event("b", is_deleted=True)])
    request = DiffRequest(None, pin, Scope("A", "2025-01-01", "2025-01-05"))
    limits = {
        "input_batch_rows": 1,
        "output_batch_rows": 1,
        "max_value_bytes": 1000,
        "output_buffer_bytes": 32768,
    }
    with pytest.raises(BridgeError, match="INVALID_KEYS"):
        RustDiffBackend(executable, options={"inputs_sorted": True, "limits": limits}).run(
            request, registry, tmp_path / "late"
        )
    assert not (tmp_path / "late").exists()
    staged = list(tmp_path.glob(".release-query-*"))
    assert len(staged) == 1
    assert (staged[0] / "changes.parquet").stat().st_size > 4
    assert (staged[0] / "failure.json").is_file()
    assert not (staged[0] / "manifest.json").exists()


def test_sorted_large_parquet_uses_one_scan_partition(tmp_path, executable):
    import hashlib

    import pyarrow as pa

    from lakebridge.consumer.protocol import ReleasePin, TablePin
    from lakebridge.consumer.releases import detail_schema

    registry = ReleaseRegistry(tmp_path / "exports")
    pin = ReleasePin("saas", "large", (TablePin("detail", "lake.saas.detail", "9007199254740993"),))
    rows = [
        event(f"e{i:06d}", plan_id=hashlib.shake_256(str(i).encode()).hexdigest(256))
        for i in range(50000)
    ]
    registry.register(
        pin,
        pa.Table.from_pylist(rows, schema=detail_schema()).to_batches(max_chunksize=8192),
        source_kind="synthetic_fixture",
        source={"test": "parallel byte-range scan ordering"},
    )
    assert (registry.directory(pin) / "detail.parquet").stat().st_size > 10 * 1024 * 1024
    request = DiffRequest(pin, pin, Scope("A", "2025-01-01", "2025-01-05"))
    result = RustDiffBackend(executable, options={"inputs_sorted": True}).run(
        request, registry, tmp_path / "sorted"
    )
    assert sum(result["counts"].values()) == 0
    assert "SortExec" not in result["physical_plan"]
    assert "file_groups={1 group" in result["physical_plan"]
