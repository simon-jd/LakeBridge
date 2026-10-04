"""Probe bounded-memory failures and DataFusion sort spill on a benchmark fixture."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

from lakebridge.consumer.diff_backend import DiffArtifact, wire_row
from lakebridge.consumer.protocol import BridgeError, DiffRequest, canonical
from lakebridge.consumer.releases import ReleaseRegistry
from lakebridge.consumer.rust_backend import RustDiffBackend


def rows_hash(artifact):
    digest = hashlib.sha256()
    for row in artifact.rows():
        digest.update(canonical(wire_row(row)))
        digest.update(b"\n")
    return digest.hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--benchmark-root", type=Path, required=True)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument(
        "--executable", type=Path, default=Path("release-query/target/release/release-query")
    )
    args = parser.parse_args()
    if args.root.exists():
        parser.error("Use a fresh probe root")
    case = args.benchmark_root / "unsorted-p50-w512"
    config = json.loads((case / "rust-0.request.json").read_text())
    request = DiffRequest.from_dict(config["request"])
    registry = ReleaseRegistry(Path(config["registry"]))
    expected = rows_hash(DiffArtifact(case / "sql-0", request))
    args.root.mkdir(parents=True)
    results = []
    for memory in (32, 48, 64):
        destination = args.root / f"memory-{memory}m"
        backend = RustDiffBackend(
            args.executable,
            options={"memory_bytes": memory << 20, "sort_spill_reservation_bytes": 1 << 20},
        )
        try:
            manifest = backend.run(request, registry, destination)
            assert rows_hash(DiffArtifact(destination, request)) == expected
            spills = sum(
                m["spill_count"]
                for m in manifest["metrics"]["physical_operators"]
                if m["operator"] == "SortExec"
            )
            results.append(
                {
                    "memory_mib": memory,
                    "status": "passed",
                    "sort_spill_count": spills,
                    "full_sql_parity": True,
                    "manifest": str(destination / "manifest.json"),
                }
            )
        except BridgeError as exc:
            assert not destination.exists()
            assert not list(args.root.glob(".release-query-*/manifest.json"))
            results.append(
                {
                    "memory_mib": memory,
                    "status": "resource_failure",
                    "code": exc.code,
                    "error": str(exc),
                    "complete_output_absent": True,
                }
            )
    assert results[-1]["status"] == "passed" and results[-1]["sort_spill_count"] > 0
    report = {
        "status": "passed",
        "request_hash": request.request_hash,
        "binary_sha256": hashlib.sha256(args.executable.read_bytes()).hexdigest(),
        "results": results,
        "scope": (
            "Single local synthetic 50,000-row-per-side wide unsorted case; "
            "pool budgets are not process RSS bounds"
        ),
    }
    (args.root / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({"status": "passed", "report": str(args.root / "report.json")}))


if __name__ == "__main__":
    main()
