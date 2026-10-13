from __future__ import annotations

import hashlib
import json
import os
import smtplib
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from email.message import EmailMessage
from typing import Any


def delivery_decision(incident: dict[str, Any], policy: dict[str, Any], last_delivery: dict[str, Any] | None = None, at: datetime | None = None):
    at = at or datetime.now(timezone.utc)
    severity = int(incident.get("severity", 1))
    state = incident.get("status", "NEW")
    critical = severity >= int(policy.get("critical_severity", 4))
    quiet_start, quiet_end = policy.get("quiet_hours_start"), policy.get("quiet_hours_end")
    quiet = quiet_start is not None and quiet_end is not None and (quiet_start <= at.hour < quiet_end if quiet_start < quiet_end else at.hour >= quiet_start or at.hour < quiet_end)
    if quiet and not critical:
        return {"deliver": False, "reason": "quiet_hours"}
    if state in {"NEW", "RESOLVED"} or critical:
        return {"deliver": True, "reason": "material_state_change"}
    if not last_delivery:
        return {"deliver": True, "reason": "first_delivery"}
    last = datetime.fromisoformat(last_delivery["created"])
    eligible = last + timedelta(minutes=int(policy.get("min_interval_minutes", 30)))
    if at >= eligible:
        return {"deliver": True, "reason": "cadence_eligible"}
    return {"deliver": False, "reason": "cadence_suppressed", "next_eligible": eligible.isoformat()}


def delivery_fingerprint(incident: dict[str, Any], subscription_id: str) -> str:
    material = {"incident": incident.get("id"), "state": incident.get("status"), "severity": incident.get("severity"), "affected_users": incident.get("affected_users"), "subscription": subscription_id}
    return hashlib.sha256(json.dumps(material, sort_keys=True).encode()).hexdigest()


def evaluate_notification(incident: dict[str, Any], policy: dict[str, Any], state: dict[str, Any] | None = None, at: datetime | None = None):
    at = at or datetime.now(timezone.utc)
    state = state or {}
    fingerprint = delivery_fingerprint(incident, state.get("subscription_id", ""))
    prior_users = int(state.get("last_affected_users", 0) or 0)
    users = int(incident.get("affected_users", 0) or 0)
    increase = 100 if prior_users == 0 and users > 0 else ((users - prior_users) / max(prior_users, 1)) * 100
    material = increase >= float(policy.get("material_change_percent", 20))
    mandatory = incident.get("status") in {"NEW", "REOPENED", "RESOLVED"} or int(incident.get("severity", 1)) >= int(policy.get("critical_severity", 4))
    base = delivery_decision(incident, policy, {"created": state["last_sent_at"]} if state.get("last_sent_at") else None, at)
    if state.get("last_evidence_fingerprint") == fingerprint:
        return {"deliver": False, "reason": "duplicate_evidence", "fingerprint": fingerprint}
    window = datetime.fromisoformat(state["window_started_at"]) if state.get("window_started_at") else at
    count = int(state.get("notification_count", 0))
    if at - window >= timedelta(days=1):
        window, count = at, 0
    if count >= int(policy.get("max_notifications_per_day", 24)) and not mandatory:
        return {"deliver": False, "reason": "rate_limited", "fingerprint": fingerprint}
    if mandatory or material:
        return {"deliver": True, "reason": "mandatory_transition" if mandatory else "material_change", "fingerprint": fingerprint, "window_started_at": window.isoformat(), "notification_count": count}
    last = datetime.fromisoformat(state["last_sent_at"]) if state.get("last_sent_at") else None
    if last and at >= last + timedelta(minutes=int(policy.get("maximum_silence_minutes", 1440))):
        return {"deliver": True, "reason": "maximum_silence_reached", "fingerprint": fingerprint, "window_started_at": window.isoformat(), "notification_count": count}
    if not base["deliver"]:
        return {**base, "fingerprint": fingerprint}
    backoff = policy.get("stable_backoff_minutes", [15, 30, 60, 240])
    interval = backoff[min(count, len(backoff) - 1)]
    if last and at < last + timedelta(minutes=interval):
        return {"deliver": False, "reason": "stable_backoff", "next_eligible": (last + timedelta(minutes=interval)).isoformat(), "fingerprint": fingerprint}
    return {"deliver": True, "reason": "cadence_eligible", "fingerprint": fingerprint, "window_started_at": window.isoformat(), "notification_count": count}


def render_message(incident: dict[str, Any], channel: str):
    title = f"NIDL {incident.get('status', 'incident')}: {incident.get('market', 'unknown market')}"
    summary = f"Severity {incident.get('severity', '?')} · {incident.get('affected_users', 0):,} affected users"
    if channel == "slack":
        return {"blocks": [{"type": "header", "text": {"type": "plain_text", "text": title}}, {"type": "section", "text": {"type": "mrkdwn", "text": summary}}]}
    if channel == "email":
        return {"subject": title, "text": f"{title}\n\n{summary}\nIncident: {incident.get('id')}"}
    return {"title": title, "summary": summary, "incident_id": incident.get("id")}


def route_incident(store, incident_id: str, subscription_id: str, at: datetime | None = None):
    incident, subscription_row = store.get("incidents", incident_id), store.get("subscriptions", subscription_id)
    if not incident or not subscription_row:
        raise ValueError("Incident or subscription not found")
    subscription = json.loads(subscription_row["body"])
    state = store.notification_state(incident_id, subscription_id) or {"subscription_id": subscription_id}
    decision = evaluate_notification(incident, subscription.get("policy", {}), state, at)
    if not decision["deliver"]:
        store.record_activity(incident["tenant"], subscription.get("owner", "system"), "notification.suppressed", {"incident_id": incident_id, "subscription_id": subscription_id, "reason": decision["reason"]})
        return decision
    destination = subscription.get("destination")
    if not destination and subscription.get("destination_ref"):
        destination = json.loads(os.getenv("NIDL_DESTINATIONS", "{}")).get(subscription["destination_ref"])
        if not destination:
            raise ValueError("Subscription destination reference is not configured")
    delivery = store.enqueue(incident_id, subscription.get("channel", "inbox"), destination, subscription_id, decision["fingerprint"], decision["reason"])
    timestamp = (at or datetime.now(timezone.utc)).isoformat()
    store.save_notification_state(incident_id, subscription_id, {**state, "last_sent_at": timestamp, "last_evidence_fingerprint": decision["fingerprint"], "next_eligible_at": decision.get("next_eligible"), "notification_count": int(decision.get("notification_count", state.get("notification_count", 0))) + 1, "window_started_at": decision.get("window_started_at", state.get("window_started_at", timestamp)), "last_affected_users": incident.get("affected_users", 0)})
    store.record_activity(incident["tenant"], subscription.get("owner", "system"), "notification.enqueued", {"incident_id": incident_id, "subscription_id": subscription_id, "delivery_id": delivery["id"], "reason": decision["reason"]})
    return {**decision, "delivery": delivery}


def dispatch_delivery(store, delivery: dict[str, Any]):
    incident = store.get("incidents", delivery["incident_id"])
    if not incident:
        return store.update_delivery(delivery["id"], "FAILED", "incident_not_found")
    incident = {**incident, "evidence": json.loads(incident["evidence"])}
    channel, destination = delivery["channel"], delivery["destination"]
    try:
        if channel == "inbox":
            pass
        elif channel == "slack":
            parsed = urllib.parse.urlparse(destination)
            allowed = {host.strip() for host in os.getenv("NIDL_SLACK_HOSTS", "hooks.slack.com").split(",")}
            if parsed.scheme != "https" or parsed.hostname not in allowed:
                raise ValueError("Slack webhook host is not allowed")
            body = json.dumps(render_message(incident, "slack")).encode()
            request = urllib.request.Request(destination, data=body, headers={"Content-Type": "application/json"})
            with urllib.request.urlopen(request, timeout=10) as response:
                if response.status >= 300:
                    raise RuntimeError(f"Slack returned {response.status}")
        elif channel == "email":
            host = os.getenv("NIDL_SMTP_HOST")
            if not host:
                raise RuntimeError("NIDL_SMTP_HOST is not configured")
            content = render_message(incident, "email")
            message = EmailMessage()
            message["From"] = os.getenv("NIDL_EMAIL_FROM", "nidl@localhost")
            message["To"] = destination
            message["Subject"] = content["subject"]
            message.set_content(content["text"])
            with smtplib.SMTP(host, int(os.getenv("NIDL_SMTP_PORT", "25")), timeout=10) as smtp:
                if os.getenv("NIDL_SMTP_STARTTLS", "false").lower() == "true":
                    smtp.starttls()
                if os.getenv("NIDL_SMTP_USER"):
                    smtp.login(os.environ["NIDL_SMTP_USER"], os.getenv("NIDL_SMTP_PASSWORD", ""))
                smtp.send_message(message)
        else:
            raise ValueError("Unsupported delivery channel")
        return store.update_delivery(delivery["id"], "SENT")
    except Exception as exc:
        attempts = int(delivery.get("attempts", 0)) + 1
        if attempts >= int(delivery.get("max_attempts", 3)):
            result = store.update_delivery(delivery["id"], "DEAD_LETTER", str(exc)[:500])
        else:
            retry_at = (datetime.now(timezone.utc) + timedelta(minutes=2 ** attempts)).isoformat()
            result = store.update_delivery(delivery["id"], "PENDING", str(exc)[:500], retry_at)
        if incident:
            store.audit(incident["tenant"], "notification-worker", "delivery.failed", delivery["id"], {"attempt": attempts, "status": result["status"], "error": str(exc)[:500]})
        return result


def dispatch_pending(store, limit: int = 50):
    current = datetime.now(timezone.utc).isoformat()
    pending = [row for row in store.list("deliveries", limit=limit) if row["status"] == "PENDING" and (not row.get("next_attempt") or row["next_attempt"] <= current)]
    return [dispatch_delivery(store, row) for row in pending]
