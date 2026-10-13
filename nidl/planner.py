from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any

from .models import Workflow
from .notifications import route_incident


def evaluation_key(workflow_id: str, workflow_version: str, tenant: str, market: str, parameters: dict[str, Any], freshness_minutes: int, watermark: str) -> str:
    canonical = {"workflow_id": workflow_id, "workflow_version": workflow_version, "tenant": tenant, "market": market, "parameters": parameters, "freshness_minutes": freshness_minutes, "watermark": watermark}
    return hashlib.sha256(json.dumps(canonical, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def time_watermark(at: datetime, every_minutes: int) -> str:
    if every_minutes < 60:
        minute = (at.minute // every_minutes) * every_minutes
        return at.replace(minute=minute, second=0, microsecond=0).isoformat()
    bucket = int(at.timestamp()) // (every_minutes * 60)
    return datetime.fromtimestamp(bucket * every_minutes * 60, timezone.utc).isoformat()


def compatible_groups(subscription_rows: list[dict[str, Any]], workflow_versions: dict[str, str], at: datetime):
    groups: dict[str, list[tuple[dict[str, Any], dict[str, Any]]]] = {}
    for row in subscription_rows:
        body = json.loads(row["body"])
        status = body.get("status", "active" if body.get("enabled", True) else "paused")
        if status != "active" or not body.get("enabled", True):
            continue
        evaluation = body.get("evaluation", {})
        every = int(evaluation.get("every_minutes", 15))
        watermark = time_watermark(at, every)
        params = {**body.get("parameters", {}), "market": body["market"]}
        key = evaluation_key(body["workflow_id"], workflow_versions.get(body["workflow_id"], body.get("workflow_version") or "unknown"), body.get("tenant", "default"), body["market"], params, int(evaluation.get("freshness_minutes", every)), watermark)
        groups.setdefault(key, []).append((row, body))
    return groups


def execute_shared_group(store, engine, key: str, subscriptions: list[tuple[dict[str, Any], dict[str, Any]]], tenant: str, watermark: str):
    cached = store.cached_evaluation(key)
    if cached:
        result, run_id, reused = json.loads(cached["result"]), cached["run_id"], True
    else:
        _, first = subscriptions[0]
        workflow_row = store.get("workflows", first["workflow_id"], tenant)
        if not workflow_row:
            raise ValueError("Pinned workflow not found")
        workflow = Workflow.model_validate_json(workflow_row["body"])
        params = {**first.get("parameters", {}), "market": first["market"]}
        result = engine.run(workflow, params, tenant=tenant)
        run_id = store.save_run(first["workflow_id"], "SUCCEEDED", result, tenant)
        store.save_evaluation(key, tenant, first["workflow_id"], result, run_id, watermark)
        reused = False
    incident_id = result.get("outputs", {}).get("incident")
    routes = []
    if incident_id and incident_id != "dry-run":
        for row, _ in subscriptions:
            routes.append({"subscription_id": row["id"], "decision": route_incident(store, incident_id, row["id"])})
    return {"evaluation_key": key, "run_id": run_id, "reused": reused, "subscriber_count": len(subscriptions), "routes": routes}
