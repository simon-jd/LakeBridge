"""Run an isolated simulation and sample precise Docker counters every two seconds."""

from __future__ import annotations

import argparse
import concurrent.futures
import http.client
import json
import socket
import subprocess
import threading
import time
import urllib.parse
from pathlib import Path

from lakebridge.common import identifier, json_write, now


class DockerConnection(http.client.HTTPConnection):
    def __init__(self):
        super().__init__("localhost", timeout=5)

    def connect(self):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.settimeout(self.timeout)
        self.sock.connect("/var/run/docker.sock")


def docker_get(path):
    connection = DockerConnection()
    try:
        connection.request("GET", path)
        response = connection.getresponse()
        if response.status != 200:
            raise RuntimeError(f"Docker GET {path}: HTTP {response.status}")
        return json.loads(response.read())
    finally:
        connection.close()


def container_sample(container):
    raw = docker_get(f"/containers/{container['Id']}/stats?stream=false&one-shot=true")
    memory = raw.get("memory_stats", {})
    memory_stat = memory.get("stats", {})
    cpu = raw.get("cpu_stats", {})
    network = raw.get("networks", {})
    block = raw.get("blkio_stats", {}).get("io_service_bytes_recursive")
    return {
        "service": container["Labels"].get("com.docker.compose.service"),
        "container_id": container["Id"],
        "cpu_nanoseconds": cpu.get("cpu_usage", {}).get("total_usage"),
        "throttling": cpu.get("throttling_data"),
        "memory_bytes": memory.get("usage"),
        "working_set_bytes": max(0, memory.get("usage", 0) - memory_stat.get("inactive_file", 0)),
        "anon_bytes": memory_stat.get("anon"),
        "memory_limit_bytes": memory.get("limit"),
        "network_rx_bytes": sum(n.get("rx_bytes", 0) for n in network.values()),
        "network_tx_bytes": sum(n.get("tx_bytes", 0) for n in network.values()),
        "block_read_bytes": sum(x["value"] for x in block if x["op"].lower() == "read")
        if block is not None
        else None,
        "block_write_bytes": sum(x["value"] for x in block if x["op"].lower() == "write")
        if block is not None
        else None,
    }


def monitor(root, output, stopped):
    filters = urllib.parse.quote(
        json.dumps({"label": ["com.docker.compose.project=lakebridge-simulation"]})
    )
    with (output / "resources.jsonl").open("w") as stream:
        while not stopped.is_set():
            try:
                containers = docker_get("/containers/json?filters=" + filters)
                with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
                    futures = [pool.submit(container_sample, c) for c in containers]
                    samples, errors = [], []
                    for future in futures:
                        try:
                            samples.append(future.result())
                        except Exception as exc:
                            errors.append(str(exc))
                progress = root / "progress.json"
                phase = json.loads(progress.read_text()) if progress.exists() else {}
                record = {"time": now(), "phase": phase, "containers": samples, "errors": errors}
            except Exception as exc:
                record = {"time": now(), "containers": [], "errors": [str(exc)]}
            stream.write(json.dumps(record) + "\n")
            stream.flush()
            stopped.wait(2)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--target-gib", type=float, default=10)
    parser.add_argument("--write-partitions", type=int, default=24)
    args = parser.parse_args()
    identifier(args.run_id)
    root = Path("artifacts/simulation")
    output = root / "runs" / args.run_id
    if output.exists():
        raise RuntimeError("Use a new run-id; evidence must not be overwritten")
    output.mkdir(parents=True)
    stopped = threading.Event()
    sampler = threading.Thread(target=monitor, args=(root, output, stopped), daemon=True)
    sampler.start()
    command = [
        "docker",
        "compose",
        "-f",
        "compose.simulation.yaml",
        "run",
        "--rm",
        "--use-aliases",
        "--name",
        "lakebridge-simulation-driver",
        "driver",
        "--run-id",
        args.run_id,
        "--target-gib",
        str(args.target_gib),
        "--write-partitions",
        str(args.write_partitions),
    ]
    started = time.perf_counter()
    try:
        with (output / "driver.log").open("w") as log:
            result = subprocess.run(command, stdout=log, stderr=subprocess.STDOUT)
        elapsed = time.perf_counter() - started
        # Spark's root-owned event log defaults to group-only readability in the image.
        # Expose this run's generated evidence to the workspace user after Spark closes it.
        if result.returncode == 0:
            report = json.loads((output / "report.json").read_text())
            app_id = report["engine"]["application_id"]
            if Path(app_id).name != app_id:
                raise RuntimeError("Invalid Spark application ID")
            subprocess.run(
                [
                    "docker",
                    "compose",
                    "-f",
                    "compose.simulation.yaml",
                    "exec",
                    "-T",
                    "master",
                    "chmod",
                    "a+r",
                    f"/workspace/artifacts/simulation/event-logs/{app_id}",
                ],
                check=True,
            )
        json_write(
            output / "runner.json",
            {
                "command": command,
                "exit_code": result.returncode,
                "end_to_end_seconds": elapsed,
                "includes": (
                    "container scheduling, dependency readiness, JVM start, calibration, "
                    "data generation, all validation, warmups, queries, maintenance, JVM stop"
                ),
            },
        )
    finally:
        stopped.set()
        sampler.join(timeout=10)
    print(
        json.dumps({"run_id": args.run_id, "exit_code": result.returncode, "evidence": str(output)})
    )
    raise SystemExit(result.returncode)


if __name__ == "__main__":
    main()
