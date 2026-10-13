from __future__ import annotations

import json
import os
import re
import urllib.request
from datetime import datetime, timedelta, timezone


SUBSCRIPTION_ID = re.compile(r"\b[0-9a-f]{8}-[0-9a-f-]{27,36}\b", re.I)


def _configured_agent(text: str, context: dict):
    endpoint = os.getenv("NIDL_CONVERSATION_ENDPOINT")
    if not endpoint:
        return None
    if not endpoint.startswith("https://"):
        raise ValueError("Conversation agent endpoint must use HTTPS")
    request = urllib.request.Request(endpoint, data=json.dumps({"text": text, "context": context}).encode(), headers={"Content-Type": "application/json", "Authorization": "Bearer " + os.getenv("NIDL_AGENT_TOKEN", "")})
    with urllib.request.urlopen(request, timeout=15) as response:
        result = json.load(response)
    if result.get("action") not in {"list", "pause", "resume", "disable", "retire", "create", "explain"}:
        raise ValueError("Conversation agent returned an unsupported action")
    return result


def interpret(text: str, context: dict):
    agent_result = _configured_agent(text, context)
    if agent_result:
        return agent_result
    normalized = " ".join(text.lower().split())
    if "list" in normalized and ("subscription" in normalized or "watch" in normalized):
        return {"action": "list", "read_only": True}
    match = SUBSCRIPTION_ID.search(text)
    for verb, status in (("pause", "paused"), ("resume", "active"), ("disable", "disabled"), ("retire", "retired")):
        if verb in normalized and match:
            return {"action": verb, "subscription_id": match.group(0), "patch": {"status": status, "enabled": status == "active"}, "read_only": False}
    if ("why" in normalized or "explain" in normalized) and match:
        return {"action": "explain", "incident_id": match.group(0), "read_only": True}
    watch = re.search(r"watch\s+([a-z0-9_-]+)\s+(?:with|using)\s+([a-z0-9_.@-]+)", normalized)
    if watch:
        return {"action": "create", "market": watch.group(1), "workflow": watch.group(2), "read_only": False}
    return {"action": "clarify", "read_only": True, "message": "I can list subscriptions, create 'watch MARKET using WORKFLOW', or pause/resume an exact subscription ID."}


def create_proposal(store, tenant: str, actor: str, intent: dict):
    expires = (datetime.now(timezone.utc) + timedelta(minutes=15)).isoformat()
    proposal_id = store.save_proposal(tenant, actor, intent["action"], intent, expires)
    return {"proposal_id": proposal_id, "expires_at": expires, "intent": intent, "requires_confirmation": True}
