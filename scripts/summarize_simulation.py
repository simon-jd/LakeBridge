"""Summarize retained resource counters, task events and completed S3 requests."""

from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path

from lakebridge.common import json_write
from lakebridge.simulation_requests import request_class


def resource_summary(path):
    containers = {}
    errors = Counter()
    peak_working, peak_charged, samples = 0, 0, 0
    with path.open() as stream:
        for line in stream:
            record = json.loads(line)
            samples += 1
            errors.update(record["errors"])
            peak_working = max(
                peak_working, sum(c["working_set_bytes"] for c in record["containers"])
            )
            peak_charged = max(
                peak_charged, sum(c["memory_bytes"] or 0 for c in record["containers"])
            )
            for sample in record["containers"]:
                entry = containers.setdefault(
                    sample["container_id"],
                    {
                        "service": sample["service"],
                        "first": sample,
                        "last": sample,
                        "peak_working_set_bytes": 0,
                        "peak_memory_bytes": 0,
                    },
                )
                entry["last"] = sample
                entry["peak_working_set_bytes"] = max(
                    entry["peak_working_set_bytes"], sample["working_set_bytes"]
                )
                entry["peak_memory_bytes"] = max(
                    entry["peak_memory_bytes"], sample["memory_bytes"] or 0
                )
    result = []
    for entry in containers.values():
        first, last = entry.pop("first"), entry.pop("last")
        for metric in (
            "cpu_nanoseconds",
            "network_rx_bytes",
            "network_tx_bytes",
            "block_read_bytes",
            "block_write_bytes",
        ):
            entry[metric + "_observed_delta"] = (
                last[metric] - first[metric]
                if first[metric] is not None and last[metric] is not None
                else None
            )
        result.append(entry)
    return {
        "samples": samples,
        "sample_interval_seconds": 2,
        "peak_simultaneous_working_set_bytes": peak_working,
        "peak_simultaneous_memory_bytes_including_file_cache": peak_charged,
        "containers": result,
        "sampling_errors": dict(errors),
        "scope": (
            "first-to-last observed counters; startup/exit tails may be missed; "
            "peaks sampled, not exact"
        ),
        "network_note": "virtual interfaces; summing services double-counts internal traffic",
        "memory_note": (
            "working_set = cgroup usage minus inactive_file; other file cache may remain"
        ),
    }


def events_summary(path):
    from event_metrics import summarize

    totals = summarize(path)
    executors, groups, metrics = {}, {}, defaultdict(Counter)
    with path.open() as stream:
        for line in stream:
            event = json.loads(line)
            if event["Event"] == "SparkListenerExecutorAdded":
                executors[event["Executor ID"]] = event["Executor Info"]
            if event["Event"] == "SparkListenerJobStart":
                group = (event.get("Properties") or {}).get("spark.jobGroup.id", "ungrouped")
                for stage in event["Stage IDs"]:
                    groups[stage] = group
            if event["Event"] != "SparkListenerTaskEnd":
                continue
            info, task = event["Task Info"], event.get("Task Metrics", {})
            counter = metrics[groups.get(event["Stage ID"], "ungrouped")]
            counter["task_attempts"] += 1
            counter["failed_tasks"] += bool(info.get("Failed", False))
            counter["executor_run_ms"] += task.get("Executor Run Time", 0)
            counter["executor_cpu_ns"] += task.get("Executor CPU Time", 0)
            counter["input_bytes"] += task.get("Input Metrics", {}).get("Bytes Read", 0)
            counter["disk_spill_bytes"] += task.get("Disk Bytes Spilled", 0)
            counter["gc_ms"] += task.get("JVM GC Time", 0)
            read = task.get("Shuffle Read Metrics", {})
            counter["shuffle_read_bytes"] += read.get("Remote Bytes Read", 0) + read.get(
                "Local Bytes Read", 0
            )
            counter["shuffle_write_bytes"] += task.get("Shuffle Write Metrics", {}).get(
                "Shuffle Bytes Written", 0
            )
    return {**totals, "executors": executors, "by_job_group": dict(metrics)}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("run_directory", type=Path)
    args = parser.parse_args()
    output = args.run_directory
    root = output.parent.parent
    report = json.loads((output / "report.json").read_text())
    json_write(output / "resource-summary.json", resource_summary(output / "resources.jsonl"))
    event_log = root / "event-logs" / report["engine"]["application_id"]
    json_write(output / "event-summary.json", events_summary(event_log))
    first = datetime.fromisoformat(report["stages"][0]["started_at"]).timestamp()
    last = datetime.fromisoformat(report["stages"][-1]["finished_at"]).timestamp()
    counts, statuses, body_bytes = Counter(), Counter(), 0
    with (
        (root / "s3-access.jsonl").open() as stream,
        (output / "s3-requests.jsonl").open("w") as dest,
    ):
        for line in stream:
            record = json.loads(line)
            if first <= record["time"] <= last:
                dest.write(line)
                counts[request_class(record)] += 1
                statuses[record["status"]] += 1
                body_bytes += record["body_sent_bytes"]
    json_write(
        output / "s3-summary.json",
        {
            "requests": dict(counts),
            "http_statuses": dict(statuses),
            "response_body_bytes": body_bytes,
            "scope": (
                "completed proxy requests between calibration start and final query end; "
                "includes inter-stage calls"
            ),
            "note": (
                "HEAD 404 existence probes are expected; "
                "response bytes include repeated reads and S3 copy responses"
            ),
        },
    )
    print(output)


if __name__ == "__main__":
    main()
