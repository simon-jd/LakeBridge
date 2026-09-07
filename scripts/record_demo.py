"""Record a real command as an asciicast v2 terminal recording (no shell interpolation)."""

import argparse
import json
import shlex
import subprocess
import sys
import time
from pathlib import Path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    if not command:
        parser.error("provide a command after --")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    start = time.perf_counter()
    with args.output.open("w") as recording:
        header = {
            "version": 2,
            "width": 120,
            "height": 35,
            "timestamp": int(time.time()),
            "title": "LakeBridge four end-to-end scenarios",
            "command": shlex.join(command),
        }
        recording.write(json.dumps(header) + "\n")
        with subprocess.Popen(
            command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1
        ) as process:
            for line in process.stdout:
                recording.write(
                    json.dumps([time.perf_counter() - start, "o", line.replace("\n", "\r\n")])
                    + "\n"
                )
                recording.flush()
                print(line, end="", flush=True)
            code = process.wait()
    sys.exit(code)


if __name__ == "__main__":
    main()
