import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from nidl.compiler import compile_plan
from nidl.conversation import create_proposal, interpret
from nidl.models import GatewayEvent, Subscription, SubscriptionArtifact, Workflow
from nidl.notifications import dispatch_delivery, evaluate_notification, route_incident
from nidl.primitives import REGISTRY
from nidl.runtime import Engine
from nidl.store import Store
from nidl.subscriptions import preview_subscription, rollback_subscription, subscription_from_artifact, validate_transition
from nidl.wakeup import dispatch_scheduled, dispatch_signal


EXAMPLE = Path(__file__).parent.parent / "examples/harq.json"


def setup_workflow(store):
    workflow = Workflow.model_validate_json(EXAMPLE.read_text())
    workflow_id = store.save("workflows", workflow.model_dump(by_alias=True), tenant="default", compiled=compile_plan(workflow, set(REGISTRY)))
    return workflow_id


def active_subscription(workflow_id, owner="alice"):
    return Subscription(workflow_id=workflow_id, owner=owner, market="atlanta", confirmed=True)


def test_subscription_preview_pins_workflow_and_cost(tmp_path):
    store = Store(str(tmp_path / "hermes.db"))
    workflow_id = setup_workflow(store)
    preview = preview_subscription(store, active_subscription(workflow_id), "default")
    assert preview["valid"] is True
    assert preview["workflow"]["version"] == "1.0.0"
    assert preview["estimated_daily_evaluations"] == 96


def test_okf_recipe_reference_is_resolved_at_preview(tmp_path, monkeypatch):
    store = Store(str(tmp_path / "hermes.db"))
    workflow_id = setup_workflow(store)
    monkeypatch.setenv("NIDL_OKF_CATALOG", str(Path(__file__).parent.parent / "examples/okf_catalog.json"))
    subscription = active_subscription(workflow_id)
    subscription.okf_bundle_ref = "ran-harq-watch@1"
    preview = preview_subscription(store, subscription, "default")
    assert preview["okf_bundle"]["digest"].startswith("sha256:")
    subscription.okf_bundle_ref = "missing@1"
    with pytest.raises(ValueError, match="Unknown OKF"):
        preview_subscription(store, subscription, "default")


def test_subscription_artifact_normalizes_and_validates(tmp_path, monkeypatch):
    store = Store(str(tmp_path / "hermes.db"))
    setup_workflow(store)
    monkeypatch.setenv("NIDL_OKF_CATALOG", str(Path(__file__).parent.parent / "examples/okf_catalog.json"))
    artifact = SubscriptionArtifact.model_validate_json((Path(__file__).parent.parent / "examples/subscription.json").read_text())
    subscription = subscription_from_artifact(store, artifact, "default", "alice")
    preview = preview_subscription(store, subscription, "default")
    assert subscription.evaluation.every_minutes == 5
    assert subscription.policy.stable_backoff_minutes == [15, 30, 60, 240]
    assert preview["workflow"]["version"] == "1.0.0"


def test_subscription_lifecycle_is_versioned_and_rollbackable(tmp_path):
    store = Store(str(tmp_path / "hermes.db"))
    workflow_id = setup_workflow(store)
    subscription = active_subscription(workflow_id)
    subscription_id = store.save("subscriptions", subscription.model_dump(), tenant="default")
    validate_transition("active", "paused")
    store.update_subscription(subscription_id, {"status": "paused", "enabled": False}, "default", "alice")
    assert len(store.subscription_versions(subscription_id)) == 2
    restored = rollback_subscription(store, subscription_id, 1, "default", "alice")
    assert json.loads(restored["body"])["status"] == "active"
    with pytest.raises(ValueError, match="Invalid"):
        validate_transition("retired", "active")


def test_two_subscribers_share_one_evaluation_and_receive_personalized_routes(tmp_path):
    store = Store(str(tmp_path / "hermes.db"))
    workflow_id = setup_workflow(store)
    for owner in ("alice", "bob"):
        store.save("subscriptions", active_subscription(workflow_id, owner).model_dump(), tenant="default")
    at = datetime(2026, 10, 12, 12, 0, tzinfo=timezone.utc)
    results = dispatch_scheduled(store, Engine(store), "default", at)
    assert results[0]["status"] == "SUCCEEDED"
    assert results[0]["subscriber_count"] == 2
    assert len(store.list("runs", "default")) == 1
    assert len(store.list("deliveries")) == 2
    duplicate = dispatch_scheduled(store, Engine(store), "default", at)
    assert duplicate[0]["status"] == "DUPLICATE"
    assert len(store.list("runs", "default")) == 1


def test_signal_wakeup_is_idempotent(tmp_path):
    store = Store(str(tmp_path / "hermes.db"))
    first = dispatch_signal(store, "default", "evt_alarm_1", "signal.received", {"cell": "A1"})
    second = dispatch_signal(store, "default", "evt_alarm_1", "signal.received", {"cell": "A1"})
    assert first["status"] == "ACCEPTED"
    assert second["status"] == "DUPLICATE"


def test_notification_pair_state_suppresses_duplicate_evidence(tmp_path):
    store = Store(str(tmp_path / "hermes.db"))
    workflow_id = setup_workflow(store)
    subscription_id = store.save("subscriptions", active_subscription(workflow_id).model_dump(), tenant="default")
    incident = store.upsert_incident("episode", {"estimated_users": 100}, "atlanta")
    first = route_incident(store, incident["id"], subscription_id)
    second = route_incident(store, incident["id"], subscription_id)
    assert first["deliver"] is True
    assert second["deliver"] is False
    assert second["reason"] == "duplicate_evidence"
    assert len(store.list("deliveries")) == 1


def test_stable_incident_respects_maximum_silence_cap():
    at = datetime(2026, 10, 12, 12, 0, tzinfo=timezone.utc)
    state = {"subscription_id": "sub", "last_sent_at": "2026-10-11T11:00:00+00:00", "last_evidence_fingerprint": "different", "notification_count": 2, "window_started_at": "2026-10-12T00:00:00+00:00", "last_affected_users": 100}
    decision = evaluate_notification({"id": "incident", "status": "ACTIVE_STABLE", "severity": 2, "affected_users": 100}, {"maximum_silence_minutes": 1440}, state, at)
    assert decision["deliver"] is True
    assert decision["reason"] == "maximum_silence_reached"


def test_failed_delivery_reaches_dead_letter(tmp_path, monkeypatch):
    store = Store(str(tmp_path / "hermes.db"))
    incident = store.upsert_incident("episode", {}, "atlanta")
    delivery = store.enqueue(incident["id"], "email", "noc@example.com")
    monkeypatch.delenv("NIDL_SMTP_HOST", raising=False)
    for _ in range(3):
        delivery = dispatch_delivery(store, store.get("deliveries", delivery["id"]))
    assert delivery["status"] == "DEAD_LETTER"
    assert delivery["attempts"] == 3


def test_resolved_incident_reopens_with_lineage(tmp_path):
    store = Store(str(tmp_path / "hermes.db"))
    incident = store.upsert_incident("episode", {"estimated_users": 10}, "atlanta")
    store.upsert_incident("episode", {"estimated_users": 0}, "atlanta")
    store.upsert_incident("episode", {"estimated_users": 0}, "atlanta")
    reopened = store.upsert_incident("episode", {"estimated_users": 25}, "atlanta")
    assert reopened["id"] == incident["id"]
    assert reopened["status"] == "REOPENED"


def test_conversation_produces_bounded_confirmable_actions(tmp_path):
    store = Store(str(tmp_path / "hermes.db"))
    subscription_id = "12345678-1234-1234-1234-123456789abc"
    intent = interpret(f"Pause subscription {subscription_id}", {})
    assert intent == {"action": "pause", "subscription_id": subscription_id, "patch": {"status": "paused", "enabled": False}, "read_only": False}
    proposal = create_proposal(store, "default", "alice", intent)
    assert proposal["requires_confirmation"] is True
    assert store.get("proposals", proposal["proposal_id"], "default")["status"] == "PENDING"


def test_gateway_envelope_is_typed():
    event = GatewayEvent.model_validate({"event_id": "evt_123", "type": "message.received", "channel": "slack", "actor_id": "alice", "tenant_id": "telco", "thread_id": "thread-1", "received_at": "2026-10-12T18:45:00Z", "text": "List my subscriptions", "auth_context_ref": "auth_1"})
    assert event.received_at.tzinfo is not None
    with pytest.raises(ValueError):
        GatewayEvent.model_validate({"event_id": "bad", "type": "message.received", "channel": "slack", "actor_id": "alice", "tenant_id": "telco", "thread_id": "thread-1", "received_at": "2026-10-12T18:45:00Z", "text": "x", "auth_context_ref": "auth_1"})
