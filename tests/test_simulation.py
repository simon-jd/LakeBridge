import json
from pathlib import Path

import pytest

from lakebridge.simulation_cost import compute_cost, estimate, request_cost, storage_cost
from lakebridge.simulation_requests import RequestCounter, request_class


@pytest.fixture
def rates():
    return json.loads(Path("benchmarks/cloud-cost-assumptions.json").read_text())


@pytest.mark.parametrize(
    "method,args,expected",
    [
        ("GET", "prefix=data%2F&list-type=2", "LIST"),
        ("GET", "uploads", "LIST"),
        ("GET", "", "GET"),
        ("HEAD", "", "HEAD"),
        ("POST", "delete", "DELETE"),
        ("POST", "uploadId=x", "POST"),
        ("PUT", "partNumber=1&uploadId=x", "PUT"),
    ],
)
def test_request_classification(method, args, expected):
    assert request_class({"method": method, "args": args}) == expected


def test_log_counter_handles_partial_lines_without_double_counting(tmp_path):
    path = tmp_path / "requests.jsonl"
    line = json.dumps({"method": "HEAD", "args": "", "status": 404}).encode()
    path.write_bytes(line[:20])
    counter = RequestCounter(path)
    assert counter.read() == {}
    with path.open("ab") as stream:
        stream.write(line[20:] + b"\n")
    assert counter.read() == {"HEAD": 1, "HTTP_404": 1}
    assert counter.read() == {"HEAD": 1, "HTTP_404": 1}
    path.write_bytes(b"")
    with pytest.raises(ValueError, match="truncated"):
        counter.read()


def test_cost_uses_allocated_nodes_not_task_cpu(rates):
    assert compute_cost(3600, rates) == pytest.approx(0.768)
    assert compute_cost(0.1, rates, starts=1) == pytest.approx(0.768 / 60)
    assert storage_cost(1024**3, 15, rates) == pytest.approx(0.0115)


def test_request_pricing_does_not_double_count_errors_or_charge_deletes(rates):
    assert request_cost(
        {"GET": 700, "HEAD": 300, "PUT": 1000, "LIST": 1000, "DELETE": 9000, "HTTP_404": 100},
        rates,
    ) == pytest.approx(0.0104)
    with pytest.raises(ValueError, match="Missing"):
        request_cost(None, rates)
    with pytest.raises(ValueError, match="Invalid"):
        request_cost({"UNKNOWN": 1}, rates)


def test_cloud_sensitivity_changes_compute_only_and_retains_storage(rates):
    labels = ["before_compaction", "after_compaction"]
    stages = [
        {"name": name, "seconds": 10, "status": "passed", "s3_requests": {"GET": 100}}
        for name in [
            "write_partitioned",
            "validate_partitioned",
            "compaction",
            "validate_compaction",
            "before_compaction_warmup",
            "after_compaction_warmup",
        ]
    ]
    report = {
        "run_id": "test",
        "passed": True,
        "observation_errors": [],
        "stages": stages,
        "source": {"compressed_bytes": 10 * 1024**3},
        "file_statistics": {
            "partitioned": {"compressed_bytes": 10 * 1024**3},
            "compacted": {"compressed_bytes": 9 * 1024**3},
        },
        "queries": {
            label: {"median_seconds": 10, "request_samples": [{"GET": 100}] * 3} for label in labels
        },
    }
    scenarios = estimate(report, {"exit_code": 0, "end_to_end_seconds": 60}, rates)["scenarios"]
    low, _, high = [s["monthly_full_scan"] for s in scenarios]
    assert low["baseline"]["compute_usd"] < high["baseline"]["compute_usd"]
    assert low["baseline"]["s3_requests_usd"] == high["baseline"]["s3_requests_usd"]
    assert low["baseline"]["ebs_usd"] == 32
    assert low["optimized"]["s3_data_storage_usd"] == pytest.approx(29 * 0.023)
    assert low["baseline"]["always_on_subtotal_usd"] == high["baseline"]["always_on_subtotal_usd"]
    # Equal query runtimes do not erase the cost of maintenance and retained old files.
    assert scenarios[1]["monthly_net_savings_usd"] < 0
