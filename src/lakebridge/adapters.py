"""Stage a hash-pinned S3 manifest into an isolated, verified local input snapshot."""

from __future__ import annotations

import json
import os
import shutil
from pathlib import Path, PurePosixPath
from urllib.parse import urlsplit

import pyarrow.fs as fs

from lakebridge.common import LakeBridgeError, digest
from lakebridge.contracts import verify_manifest


def stage_s3(uri: str, expected_sha256: str, output: Path) -> Path:
    location = urlsplit(uri)
    if location.scheme != "s3" or not location.netloc or not location.path.endswith(".json"):
        raise LakeBridgeError("Expected s3://bucket/prefix/manifest.json")
    if len(expected_sha256) != 64 or any(c not in "0123456789abcdef" for c in expected_sha256):
        raise LakeBridgeError("A pinned lowercase manifest SHA-256 is required")
    if output.exists() and any(output.iterdir()):
        raise LakeBridgeError("Staging output must be empty")
    output.mkdir(parents=True, exist_ok=True)
    endpoint = os.environ.get("LAKEBRIDGE_S3_ENDPOINT")
    options = {"region": os.environ.get("AWS_REGION", "us-east-1")}
    if endpoint:
        parsed = urlsplit(endpoint)
        options.update(endpoint_override=parsed.netloc, scheme=parsed.scheme)
    filesystem = fs.S3FileSystem(**options)
    key = location.netloc + location.path

    def download(remote, local):
        local.parent.mkdir(parents=True, exist_ok=True)
        temporary = local.with_suffix(local.suffix + ".partial")
        with filesystem.open_input_stream(remote) as source, temporary.open("wb") as target:
            shutil.copyfileobj(source, target, length=1024 * 1024)
        temporary.replace(local)

    manifest = output / "manifest.json"
    download(key, manifest)
    if digest(manifest) != expected_sha256:
        raise LakeBridgeError("Remote manifest digest does not match the pinned input version")
    data = json.loads(manifest.read_text())
    prefix = str(PurePosixPath(key).parent)
    for entry in data["files"]:
        relative = PurePosixPath(entry["path"])
        if relative.is_absolute() or ".." in relative.parts or not relative.parts:
            raise LakeBridgeError("Remote input path escapes manifest prefix")
        download(prefix + "/" + str(relative), output / str(relative))
    verify_manifest(manifest)
    return manifest
