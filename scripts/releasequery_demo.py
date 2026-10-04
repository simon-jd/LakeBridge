"""Compare a real LakeBridge release pair through SQL, exported Parquet and native Iceberg."""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from pathlib import Path

from lakebridge.common import digest
from lakebridge.consumer.diff_backend import DiffArtifact, SqlDiffBackend
from lakebridge.consumer.protocol import DiffRequest, ReleasePin, Scope, canonical
from lakebridge.consumer.releases import ReleaseRegistry
from lakebridge.consumer.rust_backend import RustDiffBackend


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument(
        "--source-root", type=Path, default=Path("artifacts/consumer-demo-20260913")
    )
    parser.add_argument(
        "--executable", type=Path, default=Path("release-query/target/release/release-query")
    )
    args = parser.parse_args()
    if args.root.exists():
        parser.error("Use a fresh output root")
    source = json.loads((args.source_root / "report.json").read_text())
    plan = source["trace"][1]["plan"]
    pins = [ReleasePin.from_dict(p) for p in plan["source_pins"]]
    request = DiffRequest(*pins, Scope(**plan["registered_scope"]))
    registry = ReleaseRegistry(args.source_root / "exports")
    sources = {}
    for pin in pins:
        table = next(t for t in pin.tables if t.name == "detail")
        _, namespace, name = table.table.split(".")
        directory = args.source_root / "lake" / "warehouse" / namespace / name / "metadata"
        # Resolve once, then persist this exact immutable metadata file and snapshot ID.
        metadata = max(
            directory.glob("v*.metadata.json"), key=lambda p: int(p.name.split(".")[0][1:])
        ).resolve()
        value = json.loads(metadata.read_text())
        assert any(str(s["snapshot-id"]) == table.snapshot_id for s in value["snapshots"])
        sources[pin.release_id] = {
            "kind": "iceberg_native",
            "pin": asdict(pin),
            "metadata_path": str(metadata),
            "metadata_sha256": digest(metadata),
            "adapter": "lakebridge.spark-utc-us.v1",
        }
    args.root.mkdir(parents=True)
    (args.root / "native-sources.json").write_bytes(canonical(sources))
    wrapper = {
        "request": asdict(request),
        "left_source": sources[pins[0].release_id],
        "right_source": sources[pins[1].release_id],
    }
    (args.root / "native-request.json").write_bytes(canonical(wrapper))
    backends = {
        "sql": SqlDiffBackend(),
        "rust_export": RustDiffBackend(args.executable),
        "rust_native": RustDiffBackend(args.executable, native_sources=sources),
    }
    expected = None
    evidence = {}
    for name, backend in backends.items():
        manifest = backend.run(request, registry, args.root / name)
        artifact = DiffArtifact(args.root / name, request)
        rows = list(artifact.rows())
        if expected is None:
            expected = rows
        assert rows == expected, name
        evidence[name] = {
            "manifest": str(args.root / name / "manifest.json"),
            "counts": manifest["counts"],
            "metrics": manifest["metrics"],
        }
    report = {
        "status": "passed",
        "source_root": str(args.source_root),
        "request_hash": request.request_hash,
        "pins": [asdict(p) for p in pins],
        "rows_equal": True,
        "native_provider": "IcebergStaticTableProvider::try_new_from_table_snapshot",
        "backends": evidence,
        "binary_sha256": digest(args.executable),
    }
    (args.root / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({"status": "passed", "report": str(args.root / "report.json")}))


if __name__ == "__main__":
    main()
