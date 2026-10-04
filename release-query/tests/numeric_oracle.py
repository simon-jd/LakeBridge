"""Independent Python set oracle against the compiled numeric-diff CLI.

Requires PyArrow only for file interchange; no candidate comparator is imported.
This is small-scale correctness evidence, not the 100M-row cloud benchmark.
"""

import argparse
import hashlib
import json
import math
import random
import subprocess
import tempfile
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

SCHEMA = pa.schema(
    [
        pa.field("key", pa.int64(), nullable=False),
        pa.field("value", pa.float64()),
        pa.field("timestamp", pa.timestamp("ns")),
        pa.field("flag_a", pa.int32()),
        pa.field("flag_b", pa.int32()),
    ]
)


def equal_float(left, right, nan_equal):
    if left is None or right is None:
        return left is None and right is None
    if math.isnan(left) or math.isnan(right):
        return nan_equal and math.isnan(left) and math.isnan(right)
    if left == right:
        return True
    if math.isinf(left) or math.isinf(right):
        return False
    return abs(left - right) <= 1e-8 + 1e-6 * max(abs(left), abs(right))


def oracle(left, right, nan_equal):
    before = {row[0]: row for row in left}
    after = {row[0]: row for row in right}
    assert len(before) == len(left) and len(after) == len(right)
    changes = []
    for key in sorted(before.keys() | after.keys()):
        if key not in before:
            changes.append(("INSERT", key))
        elif key not in after:
            changes.append(("DELETE", key))
        else:
            a, b = before[key], after[key]
            if not equal_float(a[1], b[1], nan_equal) or a[2:] != b[2:]:
                changes.append(("UPDATE", key))
    return changes


def write(path, rows):
    arrays = [
        pa.array([row[index] for row in rows], type=field.type)
        for index, field in enumerate(SCHEMA)
    ]
    pq.write_table(pa.Table.from_arrays(arrays, schema=SCHEMA), path, row_group_size=5)


def invoke(binary, work, left, right, nan_equal=True, batch_rows=7):
    work.mkdir()
    write(work / "left.parquet", left)
    write(work / "right.parquet", right)
    command = [
        str(binary),
        "--threads",
        "4",
        "numeric-diff",
        "--left",
        str(work / "left.parquet"),
        "--right",
        str(work / "right.parquet"),
        "--output",
        str(work / "changes.parquet"),
        "--batch-rows",
        str(batch_rows),
        "--nan-policy",
        "equal" if nan_equal else "unequal",
    ]
    result = subprocess.run(command, capture_output=True, text=True, timeout=20)
    return result, command


def run(binary, rounds):
    outcomes = []
    with tempfile.TemporaryDirectory(prefix="releasequery-numeric-oracle-") as temp:
        root = Path(temp)
        for seed in range(rounds):
            rng = random.Random(seed)
            left, right = [], []
            for key in range(-120, 120):
                value = rng.choice(
                    [
                        None,
                        float("nan"),
                        float("inf"),
                        float("-inf"),
                        -0.0,
                        0.0,
                        rng.uniform(-1e4, 1e4),
                    ]
                )
                row = (
                    key,
                    value,
                    rng.choice([None, -1, 0, 1, 10**12]),
                    rng.choice([None, 0, 7]),
                    1,
                )
                if rng.random() < 0.9:
                    left.append(row)
                if rng.random() < 0.9:
                    candidate = list(row)
                    if rng.random() < 0.2:
                        candidate[3] = 8
                    if value is not None and math.isfinite(value) and rng.random() < 0.5:
                        candidate[1] = value + rng.choice([1e-9, 1e-4, 2.0])
                    right.append(tuple(candidate))
            for nan_equal in [True, False]:
                name = f"random-{seed}-nan-{nan_equal}"
                result, command = invoke(binary, root / name, left, right, nan_equal)
                assert result.returncode == 0, (command, result.stderr)
                actual = [
                    (row["change_type"], row["key"]["key"])
                    for row in pq.read_table(
                        root / name / "changes.parquet", columns=["change_type", "key"]
                    ).to_pylist()
                ]
                assert actual == oracle(left, right, nan_equal), name
                summary = json.loads(result.stdout)
                assert summary["input_rows"] == len(left) + len(right)
                assert summary["operator"]["output_rows"] == len(actual)
                assert summary["async_executor_threads"] + summary["maximum_blocking_threads"] == 4
                outcomes.append(
                    {"case": name, "input_rows": len(left) + len(right), "changes": len(actual)}
                )
        edge = 1e-8 / (1 - 1e-6)
        left = [(i, 0.0, 0, 0, 0) for i in range(3)]
        right = [
            (i, value, 0, 0, 0)
            for i, value in enumerate(
                [math.nextafter(edge, 0), edge, math.nextafter(edge, math.inf)]
            )
        ]
        result, command = invoke(binary, root / "tolerance-edge", left, right)
        assert result.returncode == 0, result.stderr
        actual = [
            (row["change_type"], row["key"]["key"])
            for row in pq.read_table(
                root / "tolerance-edge/changes.parquet", columns=["change_type", "key"]
            ).to_pylist()
        ]
        assert actual == oracle(left, right, True)
        original = (root / "tolerance-edge/changes.parquet").read_bytes()
        assert subprocess.run(command, capture_output=True).returncode != 0
        assert (root / "tolerance-edge/changes.parquet").read_bytes() == original
        outcomes.append({"case": "tolerance-boundary-and-no-overwrite", "changes": len(actual)})
        for name, keys in [("duplicate-cross-batch", [1, 1]), ("descending", [2, 1])]:
            result, _ = invoke(
                binary, root / name, [(key, None, 0, 0, 0) for key in keys], [], batch_rows=1
            )
            assert result.returncode != 0 and "INVALID_KEYS" in result.stderr, result.stderr
            assert not (root / name / "changes.parquet").exists()
            outcomes.append({"case": name, "rejected": True})
    return {
        "passed": True,
        "cases": len(outcomes),
        "random_rounds": rounds,
        "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
        "outcomes": outcomes,
        "scope": "small-scale independent Python oracle, no cloud throughput/RSS rerun",
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--rounds", type=int, default=20)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    result = run(args.binary.resolve(), args.rounds)
    text = json.dumps(result, indent=2) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(text)
    print(text)
