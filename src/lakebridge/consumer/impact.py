from __future__ import annotations

from datetime import date, timedelta

from lakebridge.consumer.diff_backend import DiffArtifact
from lakebridge.consumer.protocol import Scope


def affected_scope(artifact: DiffArtifact) -> list[dict]:
    """Consume the entire validated artifact, never the model's sample or proposed scope."""
    days = set()
    for row in artifact.rows():
        for side in ("before", "after"):
            value = row[side]
            if value is not None:
                days.add((value["tenant_id"], value["event_time"].date().isoformat()))
    return [{"tenant_id": tenant, "business_date": day} for tenant, day in sorted(days)]


def full_scope(scope: Scope) -> list[dict]:
    start, end = date.fromisoformat(scope.start), date.fromisoformat(scope.end)
    return [
        {"tenant_id": scope.tenant_id, "business_date": (start + timedelta(days=i)).isoformat()}
        for i in range((end - start).days)
    ]
