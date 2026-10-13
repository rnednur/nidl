from __future__ import annotations

import json
from datetime import datetime, timezone

from .compiler import compile_plan, validate_parameters
from .models import DeliveryPolicy, EvaluationPolicy, Subscription, SubscriptionArtifact, Workflow
from .okf import resolve_recipe
from .planner import evaluation_key
from .primitives import REGISTRY

TRANSITIONS = {
    "draft": {"active", "disabled", "retired"},
    "active": {"paused", "disabled", "error", "retired"},
    "paused": {"active", "disabled", "retired"},
    "disabled": {"active", "retired"},
    "error": {"active", "paused", "disabled", "retired"},
    "retired": set(),
}


def _minutes(value, default: int) -> int:
    if value is None:
        return default
    if isinstance(value, int):
        return value
    match = __import__("re").match(r"^(\d+)(m|h|d)$", str(value))
    if not match:
        raise ValueError(f"Invalid duration: {value}")
    amount, unit = int(match.group(1)), match.group(2)
    return amount * {"m": 1, "h": 60, "d": 1440}[unit]


def subscription_from_artifact(store, artifact: SubscriptionArtifact, tenant: str, owner: str):
    reference = artifact.spec["workflowRef"]
    name, requested_version = reference.rsplit("@", 1) if "@" in reference else (reference, None)
    candidates = []
    for row in store.list("workflows", tenant):
        workflow = json.loads(row["body"])
        if workflow["metadata"]["name"] == name and (not requested_version or workflow["metadata"]["version"] == requested_version):
            candidates.append((row, workflow))
    if not candidates:
        raise ValueError("Pinned workflow reference not found")
    notification = artifact.spec.get("notification", {})
    material = notification.get("materialChange", {})
    channels = notification.get("channels", ["inbox"])
    ai = artifact.spec.get("ai", {})
    evaluation = artifact.spec.get("evaluation", {})
    return Subscription(
        workflow_id=candidates[0][0]["id"], owner=owner, tenant=tenant,
        market=artifact.spec["scope"]["market"], parameters=artifact.spec.get("parameters", {}),
        evaluation=EvaluationPolicy(every_minutes=_minutes(evaluation.get("every"), 15), freshness_minutes=_minutes(evaluation.get("freshness"), 15)),
        channel=channels[0], destination_ref=notification.get("destinationRef"),
        policy=DeliveryPolicy(material_change_percent=material.get("affectedUsersIncreasePct", 20), stable_backoff_minutes=[_minutes(value, 15) for value in notification.get("stableBackoff", ["15m", "30m", "1h", "4h"])]),
        okf_bundle_ref=artifact.spec.get("okfBundleRef"), investigation_recipe=ai.get("investigationRecipe"),
        ai_mode=ai.get("mode", "conditional"), max_tool_calls=ai.get("maxToolCalls", 8), confirmed=False, status="draft",
    )


def preview_subscription(store, subscription: Subscription, tenant: str):
    workflow_row = store.get("workflows", subscription.workflow_id, tenant)
    if not workflow_row:
        raise ValueError("Workflow not found")
    workflow = Workflow.model_validate_json(workflow_row["body"])
    parameters = validate_parameters(workflow, {**subscription.parameters, "market": subscription.market})
    plan = compile_plan(workflow, set(REGISTRY))
    okf_bundle = resolve_recipe(subscription.okf_bundle_ref)
    investigation_recipe = resolve_recipe(subscription.investigation_recipe)
    every = subscription.evaluation.every_minutes
    key = evaluation_key(subscription.workflow_id, workflow.metadata["version"], tenant, subscription.market, parameters, subscription.evaluation.freshness_minutes, "preview")
    return {
        "valid": True,
        "workflow": {"id": subscription.workflow_id, "name": workflow.metadata["name"], "version": workflow.metadata["version"]},
        "resolved_parameters": parameters,
        "evaluation_key_preview": key,
        "estimated_cost_units": plan["estimated_cost_units"],
        "estimated_daily_evaluations": max(1, 1440 // every),
        "notification_channels": [subscription.channel],
        "okf_bundle": okf_bundle,
        "investigation_recipe": investigation_recipe,
        "requires_confirmation": not subscription.confirmed,
        "warnings": plan["warnings"],
    }


def validate_transition(current: str, requested: str):
    if requested == current:
        return
    if requested not in TRANSITIONS.get(current, set()):
        raise ValueError(f"Invalid subscription transition: {current} -> {requested}")


def rollback_subscription(store, subscription_id: str, version: int, tenant: str, actor: str):
    versions = store.subscription_versions(subscription_id)
    target = next((row for row in versions if int(row["version"]) == version), None)
    if not target:
        raise ValueError("Subscription version not found")
    body = json.loads(target["body"])
    return store.update_subscription(subscription_id, body, tenant, actor)
