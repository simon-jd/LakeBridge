"""Inventory this experiment's retained S3 objects and archive verifiable evidence."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import shutil
import subprocess
from pathlib import Path

from lakebridge.common import identifier, json_write


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("run_directory", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    report = json.loads((args.run_directory / "report.json").read_text())
    run_id = identifier(report["run_id"])
    if not report["passed"]:
        raise RuntimeError("Cannot archive an incomplete run as acceptance evidence")
    if args.output.exists():
        raise RuntimeError("Choose a fresh archive directory to preserve existing evidence")
    script = (
        "mc alias set local http://minio:9000 simulation_admin simulation_local_secret >/dev/null; "
        "mc ls --recursive --json local/simulation-raw; "
        "mc ls --recursive --json local/simulation-warehouse"
    )
    command = [
        "docker",
        "compose",
        "-f",
        "compose.simulation.yaml",
        "run",
        "--rm",
        "--no-deps",
        "--entrypoint",
        "/bin/sh",
        "minio-init",
        "-ec",
        script,
    ]
    objects = []
    # Metadata only; no Parquet contents are copied through the host process.
    completed = subprocess.run(command, check=True, capture_output=True, text=True)
    for line in completed.stdout.splitlines():
        record = json.loads(line)
        if record.get("status") != "success":
            raise RuntimeError(f"Inventory failed: {record}")
        if record.get("type") == "file" and any(
            part == run_id or part in {run_id + "_partitioned", run_id + "_unpartitioned"}
            for part in record["key"].split("/")
        ):
            objects.append(record)
    if not objects:
        raise RuntimeError("Empty run inventory")
    json_write(args.run_directory / "object-inventory.json", objects)
    actual_bytes = sum(item["size"] for item in objects)
    rates = json.loads((args.run_directory / "cost-estimate.json").read_text())["assumptions"]
    json_write(
        args.run_directory / "retained-storage.json",
        {
            "objects": len(objects),
            "bytes": actual_bytes,
            "gib": actual_bytes / 1024**3,
            "monthly_s3_reference_usd": actual_bytes / 1024**3 * rates["s3_usd_per_gib_month"],
            "scope": (
                "this run's raw source, pilot, both Iceberg tables, old tagged files and metadata"
            ),
            "excludes": (
                "other smoke runs, local shuffle/event logs, MinIO internal filesystem overhead"
            ),
            "deletion_executed": False,
        },
    )
    args.output.mkdir(parents=True)
    for source in args.run_directory.iterdir():
        if not source.is_file():
            continue
        if source.suffix in {".jsonl", ".log"}:
            with (
                source.open("rb") as incoming,
                gzip.open(args.output / (source.name + ".gz"), "wb") as outgoing,
            ):
                shutil.copyfileobj(incoming, outgoing)
        else:
            shutil.copy2(source, args.output / source.name)
    event_log = args.run_directory.parent.parent / "event-logs" / report["engine"]["application_id"]
    with (
        event_log.open("rb") as incoming,
        gzip.open(args.output / "spark-event-log.json.gz", "wb") as outgoing,
    ):
        shutil.copyfileobj(incoming, outgoing)
    checksums = {}
    for path in sorted(args.output.iterdir()):
        with path.open("rb") as stream:
            checksums[path.name] = hashlib.file_digest(stream, "sha256").hexdigest()
    json_write(args.output / "sha256.json", checksums)
    print(json.dumps({"archive": str(args.output), "retained_gib": actual_bytes / 1024**3}))


if __name__ == "__main__":
    main()
