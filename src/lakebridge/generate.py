from __future__ import annotations

import math
import random
from collections import defaultdict
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from lakebridge.common import LakeBridgeError, json_write
from lakebridge.contracts import EVENT_SCHEMA, PLAN_SCHEMA, file_entry


def generate(
    root: Path,
    rows: int = 3000,
    tenants: int = 30,
    days: int = 90,
    seed: int = 42,
    file_rows: int = 5000,
    hot_share: float = 0.55,
) -> Path:
    if rows < 20 or tenants < 2 or days < 7 or file_rows < 1 or not 0 <= hot_share < 1:
        raise LakeBridgeError("Need rows>=20, tenants>=2, days>=7, file_rows>=1, 0<=hot_share<1")
    root = root.resolve()
    if root.exists() and any(root.iterdir()):
        raise LakeBridgeError(f"Input directory must be empty: {root}")
    root.mkdir(parents=True, exist_ok=True)
    rng = random.Random(seed)
    start = datetime(2025, 1, 1)
    entries = []
    plans = []
    for tenant in range(tenants):
        boundary = start + timedelta(days=days // 2)
        for plan, left, right in [("basic", start, boundary), ("pro", boundary, None)]:
            plans.append(
                {
                    "tenant_id": f"t{tenant:04d}",
                    "plan_id": plan,
                    "effective_from": left,
                    "effective_to": right,
                    "attributes": '{"currency":"USD"}',
                }
            )
    dim_path = root / "plans" / "history.parquet"
    dim_path.parent.mkdir(parents=True)
    pq.write_table(pa.Table.from_pylist(plans, schema=PLAN_SCHEMA), dim_path)
    entries.append(file_entry(root, dim_path, "plans", 1))
    counters = defaultdict(int)

    def flush(records, batch):
        grouped = defaultdict(list)
        for record in records:
            grouped[record["event_time"].date().isoformat()].append(record)
        for day, group in grouped.items():
            path = root / "events" / f"batch={batch:04d}" / f"date={day}"
            path.mkdir(parents=True, exist_ok=True)
            index = counters[(batch, day)]
            file = path / f"part-{index:06d}.parquet"
            counters[(batch, day)] += 1
            pq.write_table(
                pa.Table.from_pylist(group, schema=EVENT_SCHEMA), file, compression="zstd"
            )
            entries.append(file_entry(root, file, "events", batch))

    buffer = []
    revisions = []
    revision_limit = min(max(5, rows // 20), 10000)
    for i in range(rows):
        tenant = 0 if rng.random() < hot_share else rng.randrange(1, tenants)
        day = rng.randrange(days)
        seasonal = Decimal(str(round(1 + 0.4 * math.sin(day * math.pi / 30), 6)))
        units = (Decimal(rng.randrange(1, 100000)) / 1000 * seasonal).quantize(Decimal(".000001"))
        event_time = start + timedelta(days=day, seconds=rng.randrange(86400))
        row = {
            "tenant_id": f"t{tenant:04d}",
            "event_id": f"e{i:012d}",
            "event_time": event_time,
            "ingested_at": start + timedelta(days=days),
            "units": units,
            "source_version": 1,
            "is_deleted": False,
        }
        buffer.append(row)
        if i < revision_limit:
            # event_time is immutable; modifications arrive in later batches.
            update = dict(
                row,
                source_version=3,
                units=units + Decimal("1.000000"),
                is_deleted=(i % 7 == 0),
                ingested_at=start + timedelta(days=days + 1),
            )
            revisions.extend([update, update.copy(), dict(row, source_version=2)])
        if len(buffer) >= file_rows:
            flush(buffer, 1)
            buffer = []
    if buffer:
        flush(buffer, 1)
    rng.shuffle(revisions)
    flush(revisions, 2)
    legal = []
    for i in range(max(2, rows // 100)):
        legal.append(
            {
                "tenant_id": f"t{i % tenants:04d}",
                "event_id": f"new{i:012d}",
                "event_time": start + timedelta(days=days, seconds=i),
                "ingested_at": start + timedelta(days=days + 2),
                "units": Decimal("3.125000"),
                "source_version": 1,
                "is_deleted": False,
            }
        )
    flush(legal, 3)
    manifest = root / "manifest.json"
    json_write(
        manifest,
        {
            "contract_version": 1,
            "dataset_id": f"saas_seed_{seed}",
            "high_watermark": 3,
            "timezone": "UTC",
            "files": entries,
        },
    )
    json_write(
        root / "distribution.json",
        {
            "seed": seed,
            "base_events": rows,
            "tenants": tenants,
            "days": days,
            "hot_tenant_probability": hot_share,
            "raw_rows": sum(e["rows"] for e in entries),
            "compressed_bytes": sum(e["bytes"] for e in entries),
            "file_count": len(entries),
            "start": start.date(),
            "end": (start + timedelta(days=days - 1)).date(),
            "late_revision_keys": revision_limit,
        },
    )
    # Separate SQL implementation persists expected results alongside the generator.
    from lakebridge.oracle import reference

    for watermark in (1, 2, 3):
        reference(manifest, watermark, "v1", root / "expected" / f"h{watermark}_v1")
    return manifest
