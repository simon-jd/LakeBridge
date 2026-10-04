"""The v1 wire contract shared by the service, workers, Agent and Rust CLI.

Canonical JSON: sorted ASCII keys, UTF-8 values, no whitespace, no floats,
integers restricted to the interoperable 53-bit range. Decimal and snapshot
values are strings. Dates and timestamps have one canonical spelling.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import asdict, dataclass, is_dataclass
from datetime import UTC, date, datetime
from enum import StrEnum
from typing import Any

PROTOCOL_VERSION = "lakebridge.consumer.v1"
SEMANTIC_VERSION = "detail.utc.v1"
COMPARISON_VERSION = "usage.exact.v1"
TRANSFORM_VERSION = "tenant_daily_usage.v1"
KEY_COLUMNS = ("tenant_id", "event_id")
COMPARE_COLUMNS = ("event_time", "units", "plan_id", "billable_units", "rule_version")
# This schema descriptor is language independent; never hash Arrow's display string.
SCHEMA = [
    {"name": "tenant_id", "type": "utf8", "nullable": False},
    {"name": "event_id", "type": "utf8", "nullable": False},
    {"name": "event_time", "type": "timestamp", "unit": "us", "timezone": "UTC", "nullable": False},
    {"name": "units", "type": "decimal128", "precision": 18, "scale": 6, "nullable": True},
    {"name": "plan_id", "type": "utf8", "nullable": True},
    {"name": "billable_units", "type": "decimal128", "precision": 18, "scale": 6, "nullable": True},
    {"name": "rule_version", "type": "utf8", "nullable": True},
    {"name": "is_deleted", "type": "boolean", "nullable": False},
]


class ErrorCode(StrEnum):
    INVALID_REQUEST = "INVALID_REQUEST"
    NOT_FOUND = "NOT_FOUND"
    FORBIDDEN = "FORBIDDEN"
    INCOMPATIBLE_SCHEMA = "INCOMPATIBLE_SCHEMA"
    INVALID_KEYS = "INVALID_KEYS"
    INCOMPLETE_DIFF = "INCOMPLETE_DIFF"
    INTEGRITY_ERROR = "INTEGRITY_ERROR"
    STALE_PLAN = "STALE_PLAN"
    EXPIRED = "EXPIRED"
    APPROVAL_REQUIRED = "APPROVAL_REQUIRED"
    CONFLICT = "CONFLICT"
    DEPENDENCY_UNAVAILABLE = "DEPENDENCY_UNAVAILABLE"
    RESOURCE_EXHAUSTED = "RESOURCE_EXHAUSTED"


class BridgeError(RuntimeError):
    def __init__(self, code: ErrorCode, message: str):
        super().__init__(message)
        self.code = code


def require(condition: bool, code: ErrorCode, message: str):
    if not condition:
        raise BridgeError(code, message)


def canonical(value: Any) -> bytes:
    if is_dataclass(value) and not isinstance(value, type):
        value = asdict(value)

    def check(item):
        if item is None or isinstance(item, str | bool):
            return
        if type(item) is int and abs(item) <= 2**53 - 1:
            return
        if isinstance(item, list | tuple):
            for part in item:
                check(part)
            return
        if isinstance(item, dict) and all(isinstance(k, str) and k.isascii() for k in item):
            for part in item.values():
                check(part)
            return
        raise BridgeError(ErrorCode.INVALID_REQUEST, "Non-canonical wire value")

    check(value)
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()


def content_hash(value: Any) -> str:
    return hashlib.sha256(canonical(value)).hexdigest()


SCHEMA_FINGERPRINT = content_hash(SCHEMA)


def logical_id(value: str) -> str:
    require(
        isinstance(value, str) and bool(re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}", value)),
        ErrorCode.INVALID_REQUEST,
        "Expected a registered logical ID",
    )
    return value


def utc_timestamp(value: datetime) -> str:
    require(value.tzinfo is not None, ErrorCode.INVALID_REQUEST, "Timestamp timezone is required")
    return value.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


@dataclass(frozen=True)
class Scope:
    tenant_id: str
    start: str
    end: str

    def __post_init__(self):
        logical_id(self.tenant_id)
        try:
            first, last = date.fromisoformat(self.start), date.fromisoformat(self.end)
        except (ValueError, TypeError) as exc:
            raise BridgeError(ErrorCode.INVALID_REQUEST, "Expected UTC YYYY-MM-DD dates") from exc
        require(
            first.isoformat() == self.start and last.isoformat() == self.end and first < last,
            ErrorCode.INVALID_REQUEST,
            "Scope is a nonempty UTC [start,end) date window",
        )


@dataclass(frozen=True)
class TablePin:
    name: str
    table: str
    snapshot_id: str

    def __post_init__(self):
        logical_id(self.name)
        require(
            isinstance(self.table, str)
            and bool(
                re.fullmatch(r"[a-z][a-z0-9_]*\.[a-z][a-z0-9_]*\.[a-z][a-z0-9_]*", self.table)
            ),
            ErrorCode.INVALID_REQUEST,
            "Invalid registered table identifier",
        )
        require(
            isinstance(self.snapshot_id, str)
            and bool(re.fullmatch(r"[1-9][0-9]*", self.snapshot_id))
            and int(self.snapshot_id) <= 2**63 - 1,
            ErrorCode.INVALID_REQUEST,
            "snapshot_id must be a positive int64 decimal string",
        )


@dataclass(frozen=True)
class ReleasePin:
    dataset_id: str
    release_id: str
    tables: tuple[TablePin, ...]
    schema_fingerprint: str = SCHEMA_FINGERPRINT
    semantic_view_version: str = SEMANTIC_VERSION
    protocol_version: str = PROTOCOL_VERSION

    def __post_init__(self):
        logical_id(self.dataset_id)
        logical_id(self.release_id)
        require(
            isinstance(self.tables, tuple) and all(isinstance(t, TablePin) for t in self.tables),
            ErrorCode.INVALID_REQUEST,
            "Tables must be immutable TablePins",
        )
        names = [t.name for t in self.tables]
        require(
            "detail" in names and names == sorted(set(names)),
            ErrorCode.INVALID_REQUEST,
            "Table pins must be unique and sorted by name",
        )
        require(
            self.protocol_version == PROTOCOL_VERSION,
            ErrorCode.INVALID_REQUEST,
            "Unsupported protocol version",
        )
        require(
            self.schema_fingerprint == SCHEMA_FINGERPRINT
            and self.semantic_view_version == SEMANTIC_VERSION,
            ErrorCode.INCOMPATIBLE_SCHEMA,
            "Explicit schema/view adapter required",
        )

    @classmethod
    def from_dict(cls, value):
        value = dict(value)
        value["tables"] = tuple(TablePin(**t) for t in value["tables"])
        return cls(**value)


@dataclass(frozen=True)
class DiffRequest:
    from_pin: ReleasePin | None
    to_pin: ReleasePin
    scope: Scope
    key_columns: tuple[str, ...] = KEY_COLUMNS
    compare_columns: tuple[str, ...] = COMPARE_COLUMNS
    comparison_version: str = COMPARISON_VERSION
    protocol_version: str = PROTOCOL_VERSION

    def __post_init__(self):
        require(
            self.key_columns == KEY_COLUMNS
            and self.compare_columns == COMPARE_COLUMNS
            and self.comparison_version == COMPARISON_VERSION
            and self.protocol_version == PROTOCOL_VERSION,
            ErrorCode.INVALID_REQUEST,
            "Unsupported comparison contract",
        )
        require(
            self.from_pin is None or self.from_pin.dataset_id == self.to_pin.dataset_id,
            ErrorCode.INVALID_REQUEST,
            "Cannot compare different datasets",
        )

    @property
    def request_hash(self):
        return content_hash(self)

    @classmethod
    def from_dict(cls, value):
        value = dict(value)
        value["from_pin"] = ReleasePin.from_dict(value["from_pin"]) if value["from_pin"] else None
        value["to_pin"] = ReleasePin.from_dict(value["to_pin"])
        value["scope"] = Scope(**value["scope"])
        for field in ("key_columns", "compare_columns"):
            if field in value:
                value[field] = tuple(value[field])
        return cls(**value)
