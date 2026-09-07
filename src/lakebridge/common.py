from __future__ import annotations

import hashlib
import json
import re
from datetime import UTC, datetime
from pathlib import Path


class LakeBridgeError(RuntimeError):
    """An actionable contract, validation or state failure."""


class ConflictError(LakeBridgeError):
    pass


def now() -> str:
    return datetime.now(UTC).isoformat()


def identifier(value: str) -> str:
    if not re.fullmatch(r"[a-z][a-z0-9_]{0,62}", value):
        raise LakeBridgeError(f"Unsafe identifier: {value!r}")
    return value


def literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def digest(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def json_write(path: Path, data: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, indent=2, ensure_ascii=False, default=str) + "\n")
    tmp.replace(path)
