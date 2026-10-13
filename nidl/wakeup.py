from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone

from .planner import compatible_groups, execute_shared_group, time_watermark


def wake_event_id(source: str, tenant: str, key: str, watermark: str) -> str:
    return "wake_" + hashlib.sha256(f"{source}:{tenant}:{key}:{watermark}".encode()).hexdigest()[:28]


def dispatch_scheduled(store, engine, tenant: str, at: datetime | None = None):
    at = at or datetime.now(timezone.utc)
    workflow_rows = store.list("workflows", tenant)
    versions = {row["id"]: json.loads(row["body"])["metadata"]["version"] for row in workflow_rows}
    groups = compatible_groups(store.list("subscriptions", tenant), versions, at)
    results = []
    for key, subscriptions in groups.items():
        first = subscriptions[0][1]
        every = int(first.get("evaluation", {}).get("every_minutes", 15))
        watermark = time_watermark(at, every)
        event_id = wake_event_id("schedule", tenant, key, watermark)
        wake = store.claim_wakeup(event_id, tenant, "schedule", {"subscription_ids": [row["id"] for row, _ in subscriptions]}, key)
        if wake["duplicate"]:
            results.append({"event_id": event_id, "status": "DUPLICATE", "evaluation_key": key})
            continue
        try:
            result = execute_shared_group(store, engine, key, subscriptions, tenant, watermark)
            store.complete_wakeup(event_id, "SUCCEEDED", result)
            results.append({"event_id": event_id, "status": "SUCCEEDED", **result})
        except Exception as exc:
            result = {"error": str(exc)}
            store.complete_wakeup(event_id, "FAILED", result)
            results.append({"event_id": event_id, "status": "FAILED", **result})
    return results


def dispatch_signal(store, tenant: str, event_id: str, source: str, payload: dict):
    wake = store.claim_wakeup(event_id, tenant, source, payload)
    if wake["duplicate"]:
        return {"event_id": event_id, "status": "DUPLICATE"}
    store.complete_wakeup(event_id, "ACCEPTED", {"queued": True})
    return {"event_id": event_id, "status": "ACCEPTED"}
