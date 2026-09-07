"""Pause only LakeBridge's own Compose catalog, test failure, restore, then verify a release."""

import subprocess
import time
from pathlib import Path

from lakebridge.common import json_write


def run(*args):
    return subprocess.run(["docker", "compose", *args], capture_output=True, text=True)


def main():
    start = time.perf_counter()
    stopped = run("stop", "postgres")
    stopped.check_returncode()
    try:
        unavailable = run(
            "run", "--rm", "--no-deps", "spark", "--root", "/workspace/artifacts/compose", "status"
        )
        if unavailable.returncode == 0:
            raise AssertionError("Catalog/control outage was not detected")
    finally:
        run("start", "postgres").check_returncode()
        for _ in range(30):
            ready = run("exec", "-T", "postgres", "pg_isready", "-U", "postgres")
            if ready.returncode == 0:
                break
            time.sleep(1)
        else:
            raise RuntimeError("Catalog did not recover; inspect docker compose logs postgres")
    restored = run(
        "run",
        "--rm",
        "--no-deps",
        "spark",
        "--root",
        "/workspace/artifacts/compose",
        "query",
        "--limit",
        "1",
    )
    restored.check_returncode()
    evidence = Path("artifacts/catalog-drill")
    evidence.mkdir(parents=True, exist_ok=True)
    (evidence / "unavailable.log").write_text(unavailable.stdout + unavailable.stderr)
    (evidence / "restored.log").write_text(restored.stdout + restored.stderr)
    report = {
        "passed": True,
        "scope": "same-host Compose service restart; no cross-node DR claim",
        "outage_detected_exit_code": unavailable.returncode,
        "trusted_data_readable_seconds": time.perf_counter() - start,
    }
    json_write(evidence / "report.json", report)
    print(report)


if __name__ == "__main__":
    main()
