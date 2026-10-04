"""Operator-configured Rust CLI adapter; approval and publishing stay in Python."""

from __future__ import annotations

import subprocess
import tempfile
from dataclasses import asdict
from pathlib import Path

from lakebridge.consumer.diff_backend import DiffArtifact
from lakebridge.consumer.protocol import BridgeError, ErrorCode, canonical, require


class RustDiffBackend:
    def __init__(self, executable: Path, *, options=None, native_sources=None, timeout=600):
        self.executable = executable.resolve()
        require(self.executable.is_file(), ErrorCode.DEPENDENCY_UNAVAILABLE, "Rust CLI missing")
        self.options = options or {}
        # Optional service-owned mapping by release ID; never part of Agent/API inputs.
        self.native_sources = native_sources or {}
        self.timeout = timeout

    def run(self, request, registry, output: Path):
        require(not output.exists(), ErrorCode.CONFLICT, "Artifact destination already exists")
        output.parent.mkdir(parents=True, exist_ok=True)

        def source(pin):
            if pin is None:
                return None
            if descriptor := self.native_sources.get(pin.release_id):
                require(
                    descriptor.get("kind") == "iceberg_native"
                    and canonical(descriptor.get("pin")) == canonical(pin),
                    ErrorCode.INTEGRITY_ERROR,
                    "Native descriptor does not match frozen release",
                )
                return descriptor
            registry.files(pin)
            _, manifest = registry.get(pin.dataset_id, pin.release_id)
            return {
                "kind": "parquet_export",
                "manifest_path": str(registry.directory(pin) / "manifest.json"),
                "manifest_hash": manifest["manifest_hash"],
            }

        wrapper = {
            "request": asdict(request),
            "left_source": source(request.from_pin),
            "right_source": source(request.to_pin),
            "options": self.options,
        }
        with tempfile.TemporaryDirectory(prefix=".rust-request-", dir=output.parent) as temp:
            request_file = Path(temp) / "request.json"
            request_file.write_bytes(canonical(wrapper))
            # Fixed executable/arguments; no shell, arbitrary SQL, DSN or Agent paths.
            with (Path(temp) / "stdout.json").open("w") as stdout:
                try:
                    result = subprocess.run(
                        [str(self.executable), "diff", str(request_file), "--output", str(output)],
                        stdout=stdout,
                        stderr=subprocess.PIPE,
                        text=True,
                        timeout=self.timeout,
                        check=False,
                    )
                except subprocess.TimeoutExpired as exc:
                    raise BridgeError(ErrorCode.RESOURCE_EXHAUSTED, "Rust diff timed out") from exc
            if result.returncode:
                message = result.stderr[-4000:]
                code = next(
                    (code for code in ErrorCode if code.value in message),
                    ErrorCode.RESOURCE_EXHAUSTED
                    if "memory" in message.lower() or "resource" in message.lower()
                    else ErrorCode.INCOMPLETE_DIFF,
                )
                raise BridgeError(code, "Rust diff failed: " + message)
        artifact = DiffArtifact(output, request)
        # Verify all keys, types, checksums, counts and scope, not stdout's summary.
        for _ in artifact.rows():
            pass
        return artifact.manifest
