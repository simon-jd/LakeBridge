"""Explicit cloud scenarios calculated from local measurements, never a cloud bill."""

from __future__ import annotations

import argparse
import json
import math
import statistics
from collections import Counter
from pathlib import Path

from lakebridge.common import json_write

GIB = 1024**3


def request_cost(counts, rates):
    if counts is None:
        raise ValueError("Missing request measurements cannot be priced as zero")
    unknown = set(counts) - {"GET", "HEAD", "PUT", "POST", "LIST", "DELETE"}
    unknown = {key for key in unknown if not key.startswith("HTTP_") and counts[key]}
    if unknown or any(value < 0 for value in counts.values()):
        raise ValueError(f"Invalid request measurements: {counts}")
    return (
        sum(counts.get(key, 0) for key in ("GET", "HEAD")) * rates["s3_read_usd_per_1000"] / 1000
        + sum(counts.get(key, 0) for key in ("PUT", "POST", "LIST"))
        * rates["s3_write_list_usd_per_1000"]
        / 1000
    )


def compute_cost(seconds, rates, starts=0):
    # The entire allocated cluster is billed, including idle cores and the head node.
    billed_seconds = max(seconds, starts * 60)
    return billed_seconds / 3600 * rates["nodes"] * rates["node_usd_per_hour"]


def storage_cost(size_bytes, days, rates):
    return size_bytes / GIB * days / rates["month_days"] * rates["s3_usd_per_gib_month"]


def estimate(report, runner, rates):
    if not report.get("passed") or runner.get("exit_code") != 0:
        raise ValueError("Only a completed and validated experiment can be priced")
    if report.get("observation_errors"):
        raise ValueError("Resolve missing request observations before pricing")
    stages = {stage["name"]: stage for stage in report["stages"]}
    queries = report["queries"]
    total_requests = Counter()
    for stage in stages.values():
        if stage["status"] != "passed" or stage["s3_requests"] is None:
            raise ValueError("Incomplete stage evidence")
        total_requests.update(stage["s3_requests"])
    query_request_usd = {
        name: statistics.median(request_cost(sample, rates) for sample in q["request_samples"])
        for name, q in queries.items()
    }
    days = rates["month_days"]
    queries_month = rates["queries_per_day"] * days
    batches = rates["batches_per_day"] * days
    startup = batches * rates["startup_seconds_per_batch"]
    ebs_month = rates["nodes"] * rates["ebs_gib_per_node"] * rates["ebs_usd_per_gib_month"]
    load_stages = [stages["write_partitioned"], stages["validate_partitioned"]]
    maintenance = [stages["compaction"], stages["validate_compaction"]]
    load_seconds = sum(s["seconds"] for s in load_stages)
    maintenance_seconds = sum(s["seconds"] for s in maintenance)
    load_requests = sum(request_cost(s["s3_requests"], rates) for s in load_stages)
    maintenance_requests = sum(request_cost(s["s3_requests"], rates) for s in maintenance)
    source_bytes = report["source"]["compressed_bytes"]
    old_bytes = report["file_statistics"]["partitioned"]["compressed_bytes"]
    new_bytes = report["file_statistics"]["compacted"]["compressed_bytes"]
    scenarios = []
    for multiplier in rates["runtime_multipliers"]:
        if not math.isfinite(multiplier) or multiplier <= 0:
            raise ValueError("Runtime multiplier must be positive and finite")
        comparisons = {}
        for variant, label in (
            ("baseline", "before_compaction"),
            ("optimized", "after_compaction"),
        ):
            optimized = variant == "optimized"
            warmup = stages[label + "_warmup"]
            active_seconds = startup + multiplier * (
                load_seconds
                + queries_month * queries[label]["median_seconds"]
                + batches * warmup["seconds"]
                + (maintenance_seconds if optimized else 0)
            )
            if active_seconds > days * 86400:
                raise ValueError("Sequential batch scenario exceeds available monthly hours")
            request_usd = (
                load_requests
                + queries_month * query_request_usd[label]
                + batches * request_cost(warmup["s3_requests"], rates)
                + (maintenance_requests if optimized else 0)
            )
            storage_usd = storage_cost(
                source_bytes + (new_bytes if optimized else old_bytes), days, rates
            )
            if optimized:
                storage_usd += storage_cost(old_bytes, rates["retained_old_snapshot_days"], rates)
            components = {
                "compute_usd": compute_cost(active_seconds, rates, starts=batches),
                "ebs_usd": ebs_month,
                "s3_data_storage_usd": storage_usd,
                "s3_requests_usd": request_usd,
            }
            comparisons[variant] = {
                "allocated_cluster_hours": active_seconds / 3600,
                **components,
                "subtotal_usd": sum(components.values()),
                "always_on_subtotal_usd": (
                    compute_cost(days * 86400, rates) + ebs_month + storage_usd + request_usd
                ),
            }
        before, after = comparisons["baseline"], comparisons["optimized"]
        per_query = {
            label: {
                "compute_usd": compute_cost(q["median_seconds"] * multiplier, rates),
                "s3_requests_usd": query_request_usd[label],
                "incremental_usd": (
                    compute_cost(q["median_seconds"] * multiplier, rates) + query_request_usd[label]
                ),
            }
            for label, q in queries.items()
        }
        oneoff_seconds = (
            rates["startup_seconds_per_batch"] + runner["end_to_end_seconds"] * multiplier
        )
        scenarios.append(
            {
                "cloud_runtime_over_local": multiplier,
                "per_query_excludes_startup_and_storage": per_query,
                "complete_experiment_compute_usd": compute_cost(oneoff_seconds, rates, starts=1),
                "complete_experiment_observed_stage_requests_usd": request_cost(
                    total_requests, rates
                ),
                "monthly_full_scan": comparisons,
                "monthly_net_savings_usd": before["subtotal_usd"] - after["subtotal_usd"],
                "monthly_net_savings_percent": (
                    (before["subtotal_usd"] - after["subtotal_usd"]) / before["subtotal_usd"] * 100
                ),
            }
        )
    return {
        "run_id": report["run_id"],
        "is_cloud_bill": False,
        "assumptions": rates,
        "measured_end_to_end_seconds": runner["end_to_end_seconds"],
        "observed_stage_requests": dict(total_requests),
        "query_requests_usd": query_request_usd,
        "scenarios": scenarios,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("run_directory", type=Path)
    parser.add_argument(
        "--rates", type=Path, default=Path("benchmarks/cloud-cost-assumptions.json")
    )
    args = parser.parse_args()
    read = lambda path: json.loads(path.read_text())  # noqa: E731
    result = estimate(
        read(args.run_directory / "report.json"),
        read(args.run_directory / "runner.json"),
        read(args.rates),
    )
    json_write(args.run_directory / "cost-estimate.json", result)
    print(json.dumps(result["scenarios"], indent=2))


if __name__ == "__main__":
    main()
