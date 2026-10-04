"""Consumer dependency guard for upstream maintenance.

All upstream release tags remain protected. The extra reference ledger makes
in-flight/unconfirmed consumer dependencies explicit, including after a consumer
database restore. It never claims to restore missing object-store data.
"""

from __future__ import annotations

import os

from lakebridge.common import LakeBridgeError
from lakebridge.consumer.protocol import ReleasePin
from lakebridge.consumer.store import Store


def verify_references(pipeline, table_name, store):
    protected = []
    for value in store.protected_pins():
        pin = ReleasePin.from_dict(value)
        for table in pin.tables:
            if table.name != table_name or table.table != pipeline.table(table_name):
                continue
            if pipeline.snapshot(table_name, pin.release_id) != int(table.snapshot_id):
                raise LakeBridgeError(f"Consumer dependency lost its release tag: {pin.release_id}")
            protected.append(
                {
                    "release_id": pin.release_id,
                    "table": table.table,
                    "snapshot_id": table.snapshot_id,
                }
            )
    return protected


def configured_references(pipeline, table_name):
    dsn = os.environ.get("LAKEBRIDGE_CONSUMER_DSN")
    if not dsn:
        return []
    # Fail closed if a configured consumer dependency ledger cannot be read.
    store = Store(dsn, os.environ.get("LAKEBRIDGE_CONSUMER_NAMESPACE", "consumer"))
    try:
        return verify_references(pipeline, table_name, store)
    finally:
        store.close()
