"""Summarize uncompressed Spark JSON event logs without loading a log into RAM."""

import argparse
import json
import statistics
from pathlib import Path

from lakebridge.common import json_write


def summarize(path):
    durations, input_bytes, shuffle_read, shuffle_write, spill, gc_ms = [], 0, 0, 0, 0, 0
    failed, application_id = 0, None
    with path.open() as stream:
        for line in stream:
            event = json.loads(line)
            if event["Event"] == "SparkListenerApplicationStart":
                application_id = event.get("App ID")
            if event["Event"] != "SparkListenerTaskEnd":
                continue
            info, metrics = event["Task Info"], event.get("Task Metrics", {})
            durations.append(info["Finish Time"] - info["Launch Time"])
            failed += bool(info.get("Failed", False))
            input_bytes += metrics.get("Input Metrics", {}).get("Bytes Read", 0)
            read = metrics.get("Shuffle Read Metrics", {})
            shuffle_read += read.get("Remote Bytes Read", 0) + read.get("Local Bytes Read", 0)
            shuffle_write += metrics.get("Shuffle Write Metrics", {}).get(
                "Shuffle Bytes Written", 0
            )
            spill += metrics.get("Disk Bytes Spilled", 0)
            gc_ms += metrics.get("JVM GC Time", 0)
    return {
        "application_id": application_id,
        "tasks": len(durations),
        "failed_tasks": failed,
        "task_median_ms": statistics.median(durations) if durations else None,
        "task_max_ms": max(durations) if durations else None,
        "input_bytes": input_bytes,
        "shuffle_read_bytes": shuffle_read,
        "shuffle_write_bytes": shuffle_write,
        "disk_spill_bytes": spill,
        "gc_ms": gc_ms,
        "scope": "all task attempts in the application, including retries and warmups",
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("event_log", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    json_write(args.output, summarize(args.event_log))
