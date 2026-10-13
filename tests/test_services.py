from datetime import datetime, timedelta, timezone

import pytest

from nidl.models import PresentationDefinition
from nidl.notifications import delivery_decision, delivery_fingerprint, dispatch_pending, render_message
from nidl.presentation import normalize_presentation
from nidl.scheduler import cron_due
from nidl.security import current_principal


def test_adaptive_delivery_observes_quiet_hours_and_critical_override():
    at = datetime(2026, 1, 1, 23, 0, tzinfo=timezone.utc)
    policy = {"quiet_hours_start": 22, "quiet_hours_end": 6, "critical_severity": 4}
    assert delivery_decision({"status": "ACTIVE_STABLE", "severity": 2}, policy, at=at)["reason"] == "quiet_hours"
    assert delivery_decision({"status": "ACTIVE_STABLE", "severity": 5}, policy, at=at)["deliver"] is True


def test_adaptive_delivery_backoff():
    at = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
    last = {"created": (at - timedelta(minutes=5)).isoformat()}
    decision = delivery_decision({"status": "ACTIVE_STABLE", "severity": 2}, {"min_interval_minutes": 30}, last, at)
    assert decision["deliver"] is False
    assert decision["reason"] == "cadence_suppressed"


def test_channel_rendering_and_fingerprint():
    incident = {"id": "i-1", "status": "NEW", "severity": 4, "market": "atlanta", "affected_users": 1200}
    assert render_message(incident, "email")["subject"].startswith("NIDL")
    assert render_message(incident, "slack")["blocks"][0]["type"] == "header"
    assert delivery_fingerprint(incident, "sub") == delivery_fingerprint(incident, "sub")


def test_inbox_delivery_worker(tmp_path):
    from nidl.store import Store

    store = Store(str(tmp_path / "delivery.db"))
    incident = store.upsert_incident("episode", {"estimated_users": 2}, "atlanta")
    store.enqueue(incident["id"], "inbox", None)
    result = dispatch_pending(store)
    assert result[0]["status"] == "SENT"


def test_presentation_is_safe_and_normalized():
    definition = PresentationDefinition.model_validate({"kind": "PresentationDefinition", "metadata": {"name": "detail"}, "components": [{"type": "metric_cards", "items": [{"label": "Users", "field": "impact.users"}]}, {"type": "text", "text": "<script>alert(1)</script>"}]})
    rendered = normalize_presentation(definition, {"impact": {"users": 42}})
    assert rendered["components"][0]["items"][0]["value"] == 42
    assert "<script>" not in rendered["components"][1]["text"]


def test_presentation_rejects_arbitrary_component():
    with pytest.raises(ValueError, match="Unsupported"):
        PresentationDefinition.model_validate({"kind": "PresentationDefinition", "metadata": {}, "components": [{"type": "javascript"}]})


def test_scheduler_cron_matching():
    at = datetime(2026, 1, 1, 12, 30, tzinfo=timezone.utc)
    assert cron_due("*/15 * * * *", at)
    assert not cron_due("0 * * * *", at)


def test_api_key_scope(monkeypatch):
    monkeypatch.setenv("NIDL_AUTH_MODE", "apikey")
    monkeypatch.setenv("NIDL_API_KEYS", '{"secret":{"subject":"alice","tenant":"telco","markets":["atlanta"],"roles":["operator"]}}')
    principal = current_principal("Bearer secret")
    assert principal.permits_market("atlanta")
    assert not principal.permits_market("boston")
