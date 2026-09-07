"""Extra S3A jars pinned to Hadoop 3.3.4 in the qualified Spark 3.5 line."""

import hashlib
import json
import urllib.request
from pathlib import Path

ARTIFACTS = {
    "hadoop-aws-3.3.4.jar": "org/apache/hadoop/hadoop-aws/3.3.4",
    "aws-java-sdk-bundle-1.12.262.jar": "com/amazonaws/aws-java-sdk-bundle/1.12.262",
}


def main():
    output = Path(".runtime/jars")
    output.mkdir(parents=True, exist_ok=True)
    manifest = Path("infra/docker/simulation-jars.json")
    pinned = json.loads(manifest.read_text()) if manifest.exists() else {}
    for name, folder in ARTIFACTS.items():
        path = output / name
        if path.exists() and name in pinned:
            with path.open("rb") as stream:
                if hashlib.file_digest(stream, "sha256").hexdigest() == pinned[name]:
                    print("Verified", name)
                    continue
        url = f"https://repo.maven.apache.org/maven2/{folder}/{name}"
        temporary = path.with_suffix(".download")
        with urllib.request.urlopen(url, timeout=120) as source, temporary.open("wb") as target:
            for block in iter(lambda: source.read(1024 * 1024), b""):
                target.write(block)
        with temporary.open("rb") as stream:
            checksum = hashlib.file_digest(stream, "sha256").hexdigest()
        if name in pinned:
            if checksum != pinned[name]:
                raise RuntimeError(f"SHA-256 mismatch: {name}")
        else:
            # First freeze verifies Maven's published digest; future runs use checked-in SHA-256.
            expected = urllib.request.urlopen(url + ".sha1", timeout=60).read().decode().split()[0]
            with temporary.open("rb") as stream:
                if hashlib.file_digest(stream, "sha1").hexdigest() != expected:
                    raise RuntimeError(f"Maven digest mismatch: {name}")
        temporary.replace(path)
        pinned[name] = checksum
        print("Downloaded and verified", name)
    manifest.write_text(json.dumps(pinned, indent=2) + "\n")


if __name__ == "__main__":
    main()
