"""Reproducible 2x2x2 whole-backend benchmark; fixture generation is untimed."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import random
import statistics
import subprocess
import sys
import time
from dataclasses import asdict
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path


def write(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n")


def worker(config):
    from lakebridge.consumer.diff_backend import DiffArtifact, SqlDiffBackend, wire_row
    from lakebridge.consumer.protocol import DiffRequest, canonical
    from lakebridge.consumer.releases import ReleaseRegistry
    from lakebridge.consumer.rust_backend import RustDiffBackend

    request = DiffRequest.from_dict(config["request"])
    registry = ReleaseRegistry(Path(config["registry"]))
    output = Path(config["output"])
    backend = (
        SqlDiffBackend(memory_limit="512MB", batch_size=4096)
        if config["backend"] == "sql"
        else RustDiffBackend(
            Path(config["executable"]), options={"inputs_sorted": config["sorted"]}
        )
    )
    start = time.perf_counter_ns()
    manifest = backend.run(request, registry, output)
    backend_ns = time.perf_counter_ns() - start
    start = time.perf_counter_ns()
    digest = hashlib.sha256()
    for row in DiffArtifact(output, request).rows():
        digest.update(canonical(wire_row(row)))
        digest.update(b"\n")
    print(
        json.dumps(
            {
                "backend_ns": backend_ns,
                "validation_ns": time.perf_counter_ns() - start,
                "rows_hash": digest.hexdigest(),
                "counts": manifest["counts"],
                "metrics": manifest["metrics"],
            }
        )
    )


def tree_rss(pid):
    """Linux procfs sampled sum of resident memory across this worker and children."""
    try:
        status = Path(f"/proc/{pid}/status").read_text()
        own = next(
            (int(line.split()[1]) for line in status.splitlines() if line.startswith("VmRSS:")), 0
        )
        children = Path(f"/proc/{pid}/task/{pid}/children").read_text().split()
        return own + sum(tree_rss(int(child)) for child in children)
    except (OSError, ProcessLookupError):
        return 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path)
    parser.add_argument("--rows", type=int, default=50000)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument(
        "--executable", type=Path, default=Path("release-query/target/release/release-query")
    )
    parser.add_argument("--worker", type=Path)
    args = parser.parse_args()
    if args.worker:
        worker(json.loads(args.worker.read_text()))
        return
    if not args.root or args.root.exists() or not args.executable.is_file():
        parser.error("Use a fresh --root and an existing optimized --executable")
    if args.rows < 100 or not 1 <= args.repeats <= 10:
        parser.error("rows >= 100 and repeats 1..10 required")
    import pyarrow as pa

    from lakebridge.consumer.protocol import DiffRequest, ReleasePin, Scope, TablePin
    from lakebridge.consumer.releases import ReleaseRegistry, detail_schema

    args.root.mkdir(parents=True)
    all_results = []
    for ordered in (True, False):
        for proportion in (0.01, 0.50):
            for width in (16, 512):
                label = f"{'sorted' if ordered else 'unsorted'}-p{int(proportion * 100)}-w{width}"
                case = args.root / label
                registry = ReleaseRegistry(case / "exports")
                rows = []
                for i in range(args.rows):
                    text = "".join(
                        hashlib.sha256(f"{i}:{j}".encode()).hexdigest()
                        for j in range((width + 63) // 64)
                    )[:width]
                    rows.append(
                        {
                            "tenant_id": "A" if i % 11 else "B",
                            "event_id": f"e{i:09d}",
                            "event_time": datetime(2025, 1, 1 + i % 7, tzinfo=UTC),
                            "units": Decimal(i % 1000).quantize(Decimal(".000001")),
                            "plan_id": text,
                            "billable_units": Decimal(i % 1000).quantize(Decimal(".000001")),
                            "rule_version": "v1",
                            "is_deleted": False,
                        }
                    )
                target = [
                    dict(row, units=row["units"] + 1, plan_id=row["plan_id"][:-2] + "v2")
                    if i % 100 < int(proportion * 100)
                    else row.copy()
                    for i, row in enumerate(rows)
                ]
                if ordered:
                    for values in (rows, target):
                        values.sort(key=lambda row: (row["tenant_id"], row["event_id"]))
                else:
                    random.Random(42).shuffle(rows)
                    random.Random(43).shuffle(target)
                pins = []
                for name, values in (("before", rows), ("after", target)):
                    pin = ReleasePin(
                        "bench",
                        name,
                        (
                            TablePin(
                                "detail",
                                "lake.bench.detail",
                                "9007199254740993" if name == "before" else "9007199254740995",
                            ),
                        ),
                    )
                    registry.register(
                        pin,
                        pa.Table.from_pylist(values, schema=detail_schema()).to_batches(
                            max_chunksize=8192
                        ),
                        source_kind="synthetic_fixture",
                        source={"seed": 42, "sorted": ordered, "width": width},
                    )
                    pins.append(pin)
                request = DiffRequest(*pins, Scope("A", "2025-01-01", "2025-01-08"))
                del rows, target
                expected_hash = None
                for repeat in range(args.repeats):
                    for backend in ("sql", "rust") if repeat % 2 == 0 else ("rust", "sql"):
                        config = {
                            "request": asdict(request),
                            "registry": str(registry.root),
                            "output": str((case / f"{backend}-{repeat}").resolve()),
                            "backend": backend,
                            "sorted": ordered,
                            "executable": str(args.executable.resolve()),
                        }
                        config_path = case / f"{backend}-{repeat}.request.json"
                        write(config_path, config)
                        out_path = case / f"{backend}-{repeat}.result.json"
                        log_path = case / f"{backend}-{repeat}.log"
                        started = time.perf_counter_ns()
                        with out_path.open("w") as out, log_path.open("w") as log:
                            process = subprocess.Popen(
                                [
                                    sys.executable,
                                    str(Path(__file__).resolve()),
                                    "--worker",
                                    str(config_path),
                                ],
                                stdout=out,
                                stderr=log,
                            )
                            peak, samples = 0, 0
                            while process.poll() is None:
                                peak = max(peak, tree_rss(process.pid))
                                samples += 1
                                time.sleep(0.005)
                        if process.returncode:
                            raise RuntimeError(f"Benchmark failed; inspect {log_path}")
                        elapsed = time.perf_counter_ns() - started
                        result = json.loads(out_path.read_text())
                        if expected_hash is None:
                            expected_hash = result["rows_hash"]
                        assert result["rows_hash"] == expected_hash, label
                        result.update(
                            case=label,
                            sorted=ordered,
                            requested_change_fraction=proportion,
                            width=width,
                            input_rows_per_side=args.rows,
                            repeat=repeat,
                            backend=backend,
                            process_wall_ns=elapsed,
                            sampled_tree_peak_rss_kib=peak,
                            rss_samples=samples,
                        )
                        all_results.append(result)
                        write(args.root / "raw-results.json", all_results)
                print(f"verified {label}", flush=True)
    medians = []
    for case in sorted({r["case"] for r in all_results}):
        row = {"case": case}
        for backend in ("sql", "rust"):
            values = [r for r in all_results if r["case"] == case and r["backend"] == backend]
            row[backend] = {
                k: statistics.median(v[k] for v in values)
                for k in ("backend_ns", "process_wall_ns", "sampled_tree_peak_rss_kib")
            }
        row["rust_to_sql_backend_ratio"] = row["rust"]["backend_ns"] / row["sql"]["backend_ns"]
        medians.append(row)
    write(
        args.root / "report.json",
        {
            "status": "passed",
            "rows_per_side": args.rows,
            "repeats": args.repeats,
            "cases": len(medians),
            "all_results_equal": True,
            "environment": {
                "platform": sys.platform,
                "python": sys.version,
                "cpu_count": os.cpu_count(),
                "rust_binary_sha256": hashlib.sha256(args.executable.read_bytes()).hexdigest(),
            },
            "method": (
                "Fresh worker processes; warm local filesystem caches not flushed; "
                "alternating backend order; generation untimed. backend_ns includes "
                "configured backend and Rust bridge validation; process_wall_ns includes "
                "Python imports and shared parity validation. RSS is 5ms-sampled "
                "process-tree sum, not a hard bound. Core/scan/sort/output metrics "
                "are retained separately in raw-results.json."
            ),
            "medians": medians,
        },
    )


if __name__ == "__main__":
    main()
