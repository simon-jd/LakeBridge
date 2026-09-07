"""Download pinned JVM dependencies; verify repository-frozen SHA-256 digests."""

from __future__ import annotations

import hashlib
import os
import urllib.request
from pathlib import Path

from lakebridge.config import ARTIFACT_SHA256, ARTIFACTS


def main():
    target = Path(os.environ.get("LAKEBRIDGE_JARS", ".runtime/jars"))
    target.mkdir(parents=True, exist_ok=True)
    for name, location in ARTIFACTS.items():
        url = f"https://repo.maven.apache.org/maven2/{location}/{name}"
        expected = ARTIFACT_SHA256[name]
        path = target / name
        if path.exists():
            with path.open("rb") as stream:
                if hashlib.file_digest(stream, "sha256").hexdigest() == expected:
                    print(f"Verified {name}")
                    continue
        temporary = path.with_suffix(".download")
        with urllib.request.urlopen(url, timeout=120) as source, temporary.open("wb") as output:
            for block in iter(lambda: source.read(1024 * 1024), b""):
                output.write(block)
        with temporary.open("rb") as stream:
            actual = hashlib.file_digest(stream, "sha256").hexdigest()
        if actual != expected:
            temporary.unlink()
            raise RuntimeError(f"Checksum mismatch: {name}")
        temporary.replace(path)
        print(f"Downloaded and verified {name}")


if __name__ == "__main__":
    main()
