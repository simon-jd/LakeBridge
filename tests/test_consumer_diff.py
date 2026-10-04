from dataclasses import asdict
from datetime import UTC, datetime
from decimal import Decimal

import pyarrow as pa
import pytest

from lakebridge.consumer.diff_backend import DiffArtifact, SqlDiffBackend
from lakebridge.consumer.impact import affected_scope
from lakebridge.consumer.protocol import (
    BridgeError,
    DiffRequest,
    ReleasePin,
    Scope,
    TablePin,
    canonical,
    content_hash,
)
from lakebridge.consumer.releases import ReleaseRegistry, detail_schema


def event(key, day=1, units="1.000000", tenant="A", **changes):
    return {
        "tenant_id": tenant,
        "event_id": key,
        "event_time": datetime(2025, 1, day, tzinfo=UTC),
        "units": Decimal(units) if units is not None else None,
        "plan_id": "basic",
        "billable_units": Decimal(units) if units is not None else None,
        "rule_version": "v1",
        "is_deleted": False,
        **changes,
    }


def register(registry, release_id, rows):
    pin = ReleasePin(
        "saas", release_id, (TablePin("detail", "lake.saas.detail", "9007199254740993"),)
    )
    registry.register(
        pin,
        pa.Table.from_pylist(rows, schema=detail_schema()).to_batches(),
        source_kind="synthetic_fixture",
        source={"fixture": release_id},
    )
    return pin


@pytest.fixture
def compared(tmp_path):
    def compare(left, right, *, scope=None, batch_size=2):
        registry = ReleaseRegistry(tmp_path / "registry")
        a = register(registry, "a", left)
        b = register(registry, "b", right)
        request = DiffRequest(a, b, scope or Scope("A", "2025-01-01", "2025-01-05"))
        output = tmp_path / "diff"
        SqlDiffBackend(batch_size=batch_size).run(request, registry, output)
        return DiffArtifact(output, request), registry

    return compare


def test_decimal_tombstone_null_and_full_impact_despite_preview(compared):
    left = [
        event("update", units="999999999999.999998"),
        event("gone", day=3),
        event("null", day=2, plan_id=None),
        event("other", tenant="B"),
    ]
    right = [
        event("update", units="999999999999.999999"),
        event("gone", day=3, is_deleted=True),
        event("new", day=4),
        event("null", day=2, plan_id="pro"),
        event("other", tenant="B", units="99"),
    ]
    artifact, _ = compared(left, right)
    assert artifact.manifest["counts"] == {"INSERT": 1, "UPDATE": 2, "DELETE": 1}
    assert artifact.preview(1)["truncated"]
    assert len(artifact.preview(1)["rows"]) == 1
    assert affected_scope(artifact) == [
        {"tenant_id": "A", "business_date": f"2025-01-0{i}"} for i in range(1, 5)
    ]
    update = next(row for row in artifact.rows() if row["key"]["event_id"] == "update")
    assert update["after"]["units"] - update["before"]["units"] == Decimal(".000001")


def test_file_order_and_null_null_are_business_equivalent(compared):
    rows = [event("one", plan_id=None, units=None), event("two"), event("deleted", is_deleted=True)]
    artifact, _ = compared(rows, rows[::-1])
    assert list(artifact.rows()) == []
    assert affected_scope(artifact) == []
    assert artifact.preview(0) == {"rows": [], "truncated": False, "total_rows": 0}


def test_generic_date_move_recomputes_before_and_after(compared):
    artifact, _ = compared([event("move", day=1)], [event("move", day=4)])
    assert artifact.manifest["counts"]["UPDATE"] == 1
    assert [d["business_date"] for d in affected_scope(artifact)] == ["2025-01-01", "2025-01-04"]


def test_half_open_scope_boundary(compared):
    artifact, _ = compared([], [event("included", day=1), event("excluded", day=5)])
    assert [r["key"]["event_id"] for r in artifact.rows()] == ["included"]


@pytest.mark.parametrize(
    "left,right,counts",
    [
        ([], [], {"INSERT": 0, "UPDATE": 0, "DELETE": 0}),
        ([], [event("new")], {"INSERT": 1, "UPDATE": 0, "DELETE": 0}),
        ([event("gone")], [], {"INSERT": 0, "UPDATE": 0, "DELETE": 1}),
    ],
)
def test_empty_sides(compared, left, right, counts):
    artifact, _ = compared(left, right)
    assert artifact.manifest["counts"] == counts
    assert len(list(artifact.rows())) == sum(counts.values())


def test_duplicate_source_keys_fail_without_publishing(compared, tmp_path):
    with pytest.raises(BridgeError, match="Duplicate"):
        compared([], [event("dup"), event("dup", is_deleted=True)])
    assert not (tmp_path / "diff").exists()


def test_initial_request_and_frozen_pin_integrity(tmp_path):
    registry = ReleaseRegistry(tmp_path / "registry")
    target = register(registry, "first", [event("first")])
    request = DiffRequest(None, target, Scope("A", "2025-01-01", "2025-01-05"))
    SqlDiffBackend().run(request, registry, tmp_path / "diff")
    artifact = DiffArtifact(tmp_path / "diff", request)
    assert artifact.manifest["counts"]["INSERT"] == 1
    assert DiffRequest.from_dict(asdict(request)) == request
    file = registry.directory(target) / "detail.parquet"
    file.write_bytes(file.read_bytes() + b"modified")
    with pytest.raises(BridgeError, match="checksum"):
        registry.files(target)


def test_partial_or_changed_artifact_is_rejected(compared):
    artifact, _ = compared([], [event("new")])
    manifest = artifact.directory / "manifest.json"
    manifest.unlink()
    with pytest.raises(BridgeError, match="complete manifest"):
        DiffArtifact(artifact.directory, artifact.request)
    manifest.write_bytes(canonical(artifact.manifest))
    file = artifact.paths[0]
    file.write_bytes(file.read_bytes() + b"truncated-or-modified")
    with pytest.raises(BridgeError, match="missing or changed"):
        DiffArtifact(artifact.directory, artifact.request)


def test_wire_contract_is_stable_and_rejects_unsafe_numbers():
    assert content_hash({"b": "雪", "a": 1}) == content_hash({"a": 1, "b": "雪"})
    assert canonical({"b": "雪", "a": 1}) == '{"a":1,"b":"雪"}'.encode()
    for value in [1.5, 2**53, Decimal("1"), {"é": 1}]:
        with pytest.raises(BridgeError):
            canonical(value)
    for snapshot in [9007199254740993, "01", "9223372036854775808"]:
        with pytest.raises(BridgeError):
            TablePin("detail", "lake.saas.detail", snapshot)
    with pytest.raises(BridgeError):
        Scope("A", "2025-01-01", "2025-01-01")
