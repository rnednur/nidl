from __future__ import annotations

import json
import os
from pathlib import Path

from fastapi import Depends, FastAPI, HTTPException
from fastapi.responses import FileResponse

from .compiler import compile_plan
from .conversation import create_proposal, interpret
from .models import AckRequest, ConversationConfirmation, ConversationRequest, DurableRunRequest, GatewayEvent, PresentationDefinition, RunRequest, Subscription, SubscriptionArtifact, SubscriptionPatch, SubscriptionPreviewRequest, Workflow
from .notifications import delivery_decision, dispatch_pending, render_message, route_incident
from .presentation import normalize_presentation
from .primitives import CONTRACTS, REGISTRY
from .runtime import Engine
from .scheduler import run_due_subscriptions
from .security import Principal, current_principal, require_market, require_role
from .store import Store
from .subscriptions import preview_subscription, rollback_subscription, subscription_from_artifact, validate_transition
from .temporal_runtime import TemporalNotInstalled, TemporalService, build_execution_input, configuration as temporal_configuration
from .wakeup import dispatch_signal

app = FastAPI(title="NIDL API", version="0.4.0", description="Network Intelligence Definition Language control and execution API")
store = Store(os.getenv("NIDL_DB", "nidl.db"))
queries_path = Path(os.getenv("NIDL_QUERIES", "queries.json"))
queries = json.loads(queries_path.read_text()) if queries_path.exists() else {}
engine = Engine(store, queries)
web = Path(__file__).resolve().parent.parent / "web"


def decoded(row):
    result = dict(row)
    for key in ("body", "compiled", "result", "evidence", "detail"):
        if result.get(key):
            try:
                result[key] = json.loads(result[key])
            except (TypeError, json.JSONDecodeError):
                pass
    return result


@app.get("/")
def home():
    return FileResponse(web / "index.html")


@app.get("/health")
def health():
    return {"status": "ok", "version": app.version, "auth_mode": os.getenv("NIDL_AUTH_MODE", "disabled"), "temporal": temporal_configuration()}


@app.get("/v1/dashboard")
def dashboard(principal: Principal = Depends(current_principal)):
    return store.summary(principal.tenant)


@app.get("/v1/primitives")
def primitives(principal: Principal = Depends(current_principal)):
    return list(CONTRACTS.values())


@app.post("/v1/workflows/validate")
def validate(workflow: Workflow, principal: Principal = Depends(current_principal)):
    try:
        return {"valid": True, "plan": compile_plan(workflow, set(REGISTRY))}
    except ValueError as exc:
        raise HTTPException(422, str(exc)) from exc


@app.post("/v1/workflows/compile")
def compile_definition(workflow: Workflow, principal: Principal = Depends(current_principal)):
    require_role(principal, "publisher")
    return compile_plan(workflow, set(REGISTRY))


@app.post("/v1/workflows", status_code=201)
def create_workflow(workflow: Workflow, principal: Principal = Depends(current_principal)):
    require_role(principal, "publisher")
    plan = compile_plan(workflow, set(REGISTRY))
    workflow_id = store.save("workflows", workflow.model_dump(by_alias=True), tenant=principal.tenant, compiled=plan)
    store.audit(principal.tenant, principal.subject, "workflow.publish", workflow_id, {"plan_id": plan["plan_id"]})
    return {"id": workflow_id, "plan": plan}


@app.get("/v1/workflows")
def workflows(principal: Principal = Depends(current_principal)):
    return [decoded(row) for row in store.list("workflows", principal.tenant)]


@app.get("/v1/workflows/{workflow_id}")
def workflow(workflow_id: str, principal: Principal = Depends(current_principal)):
    row = store.get("workflows", workflow_id, principal.tenant)
    if not row:
        raise HTTPException(404, "Workflow not found")
    return decoded(row)


@app.post("/v1/workflows/{workflow_id}/run")
def run(workflow_id: str, request: RunRequest, principal: Principal = Depends(current_principal)):
    row = store.get("workflows", workflow_id, principal.tenant)
    if not row:
        raise HTTPException(404, "Workflow not found")
    definition = Workflow.model_validate_json(row["body"])
    market = request.parameters.get("market", "demo")
    require_market(principal, market)
    try:
        result = engine.run(definition, request.parameters, market=market, dry_run=request.dry_run, tenant=principal.tenant)
        run_id = store.save_run(workflow_id, "SUCCEEDED", result, principal.tenant)
        store.audit(principal.tenant, principal.subject, "workflow.run", run_id, {"workflow_id": workflow_id, "dry_run": request.dry_run})
        return {"id": run_id, "status": "SUCCEEDED", **result}
    except Exception as exc:
        run_id = store.save_run(workflow_id, "FAILED", {"error": str(exc)}, principal.tenant)
        raise HTTPException(400, {"run_id": run_id, "error": str(exc)}) from exc


@app.post("/v1/workflows/{workflow_id}/replay")
def replay(workflow_id: str, request: RunRequest, principal: Principal = Depends(current_principal)):
    request.dry_run = True
    return run(workflow_id, request, principal)


@app.post("/v1/workflows/{workflow_id}/run/durable", status_code=202)
async def run_durable(workflow_id: str, request: DurableRunRequest, principal: Principal = Depends(current_principal)):
    row = store.get("workflows", workflow_id, principal.tenant)
    if not row:
        raise HTTPException(404, "Workflow not found")
    market = request.parameters.get("market", "demo")
    require_market(principal, market)
    try:
        execution = build_execution_input(
            workflow_id,
            json.loads(row["body"]),
            request.parameters,
            principal.tenant,
            market,
            request.dry_run,
            request.request_id,
        )
        existing = store.get("runs", execution["run_id"], principal.tenant)
        if existing:
            return {"id": execution["run_id"], "status": existing["status"], "duplicate": True}
        store.create_run(execution["run_id"], workflow_id, principal.tenant, result={"orchestrator": "temporal", "submission": "pending"})
        started = await TemporalService().start(execution)
        store.audit(principal.tenant, principal.subject, "workflow.run.durable", execution["run_id"], {"workflow_id": workflow_id, "temporal_workflow_id": started["temporal_workflow_id"]})
        return {"id": execution["run_id"], **started}
    except (TemporalNotInstalled, OSError, ConnectionError, RuntimeError) as exc:
        if "execution" in locals():
            store.update_run(execution["run_id"], "SUBMISSION_FAILED", {"orchestrator": "temporal", "error": str(exc)}, principal.tenant)
        raise HTTPException(503, f"Temporal is unavailable: {exc}") from exc
    except ValueError as exc:
        raise HTTPException(422, str(exc)) from exc


@app.get("/v1/durable/runs/{run_id}")
async def durable_run_state(run_id: str, principal: Principal = Depends(current_principal)):
    row = store.get("runs", run_id, principal.tenant)
    if not row:
        raise HTTPException(404, "Run not found")
    result = decoded(row)
    if row["status"] in {"PENDING", "RUNNING"}:
        try:
            result["temporal_state"] = await TemporalService().state(run_id)
        except Exception as exc:
            result["temporal_state"] = {"status": "UNAVAILABLE", "detail": str(exc)}
    return result


@app.post("/v1/durable/runs/{run_id}/cancel", status_code=202)
async def cancel_durable_run(run_id: str, principal: Principal = Depends(current_principal)):
    require_role(principal, "operator")
    row = store.get("runs", run_id, principal.tenant)
    if not row:
        raise HTTPException(404, "Run not found")
    if row["status"] not in {"PENDING", "RUNNING"}:
        raise HTTPException(409, f"Run is already {row['status']}")
    try:
        result = await TemporalService().cancel(run_id)
    except (TemporalNotInstalled, OSError, ConnectionError, RuntimeError) as exc:
        raise HTTPException(503, f"Temporal is unavailable: {exc}") from exc
    store.audit(principal.tenant, principal.subject, "workflow.run.cancel", run_id, {})
    return result


@app.post("/v1/subscriptions/preview")
def subscription_preview(request: SubscriptionPreviewRequest | SubscriptionArtifact, principal: Principal = Depends(current_principal)):
    try:
        subscription = request.subscription if isinstance(request, SubscriptionPreviewRequest) else subscription_from_artifact(store, request, principal.tenant, principal.subject)
    except ValueError as exc:
        raise HTTPException(422, str(exc)) from exc
    require_market(principal, subscription.market)
    if subscription.tenant != principal.tenant:
        raise HTTPException(403, "Subscription tenant does not match principal")
    try:
        return {**preview_subscription(store, subscription, principal.tenant), "normalized_subscription": subscription.model_dump(mode="json")}
    except ValueError as exc:
        raise HTTPException(422, str(exc)) from exc


@app.post("/v1/subscriptions", status_code=201)
def subscribe(subscription: Subscription, principal: Principal = Depends(current_principal)):
    require_market(principal, subscription.market)
    if subscription.tenant != principal.tenant:
        raise HTTPException(403, "Subscription tenant does not match principal")
    workflow_row = store.get("workflows", subscription.workflow_id, principal.tenant)
    if not workflow_row:
        raise HTTPException(404, "Workflow not found")
    preview = preview_subscription(store, subscription, principal.tenant)
    body = subscription.model_dump()
    body["workflow_version"] = preview["workflow"]["version"]
    subscription_id = store.save("subscriptions", body, tenant=principal.tenant)
    store.audit(principal.tenant, principal.subject, "subscription.create", subscription_id, {"status": body["status"], "workflow_version": body["workflow_version"]})
    return {"id": subscription_id, "status": body["status"], "preview": preview}


@app.get("/v1/subscriptions")
def subscriptions(principal: Principal = Depends(current_principal)):
    return [decoded(row) for row in store.list("subscriptions", principal.tenant)]


@app.patch("/v1/subscriptions/{subscription_id}")
def update_subscription(subscription_id: str, patch: SubscriptionPatch, principal: Principal = Depends(current_principal)):
    current = store.get("subscriptions", subscription_id, principal.tenant)
    if not current:
        raise HTTPException(404, "Subscription not found")
    current_body = json.loads(current["body"])
    if current_body.get("owner") != principal.subject and not principal.has_role("admin"):
        raise HTTPException(403, "Only the owner or an administrator may update this subscription")
    update = patch.model_dump(exclude_unset=True)
    if patch.status:
        try:
            validate_transition(current_body.get("status", "active"), patch.status)
        except ValueError as exc:
            raise HTTPException(409, str(exc)) from exc
    row = store.update_subscription(subscription_id, update, principal.tenant, principal.subject)
    store.audit(principal.tenant, principal.subject, "subscription.update", subscription_id, patch.model_dump(exclude_unset=True))
    return decoded(row)


@app.get("/v1/subscriptions/{subscription_id}/versions")
def subscription_history(subscription_id: str, principal: Principal = Depends(current_principal)):
    if not store.get("subscriptions", subscription_id, principal.tenant):
        raise HTTPException(404, "Subscription not found")
    return [decoded(row) for row in store.subscription_versions(subscription_id)]


@app.post("/v1/subscriptions/{subscription_id}/rollback/{version}")
def subscription_rollback(subscription_id: str, version: int, principal: Principal = Depends(current_principal)):
    require_role(principal, "operator")
    try:
        return decoded(rollback_subscription(store, subscription_id, version, principal.tenant, principal.subject))
    except ValueError as exc:
        raise HTTPException(404, str(exc)) from exc


@app.post("/v1/presentations", status_code=201)
def create_presentation(definition: PresentationDefinition, principal: Principal = Depends(current_principal)):
    require_role(principal, "publisher")
    return {"id": store.save("presentations", definition.model_dump(), tenant=principal.tenant)}


@app.post("/v1/presentations/{presentation_id}/render")
def render_presentation(presentation_id: str, result: dict, principal: Principal = Depends(current_principal)):
    row = store.get("presentations", presentation_id, principal.tenant)
    if not row:
        raise HTTPException(404, "Presentation not found")
    return normalize_presentation(PresentationDefinition.model_validate_json(row["body"]), result)


@app.get("/v1/incidents")
def incidents(principal: Principal = Depends(current_principal)):
    return [decoded(row) for row in store.list("incidents", principal.tenant)]


@app.get("/v1/incidents/{incident_id}")
def get_incident(incident_id: str, principal: Principal = Depends(current_principal)):
    row = store.get("incidents", incident_id, principal.tenant)
    if not row:
        raise HTTPException(404, "Incident not found")
    return {**decoded(row), "timeline": [decoded(event) for event in store.incident_timeline(incident_id)]}


@app.get("/v1/incidents/{incident_id}/timeline")
def timeline(incident_id: str, principal: Principal = Depends(current_principal)):
    if not store.get("incidents", incident_id, principal.tenant):
        raise HTTPException(404, "Incident not found")
    return [decoded(row) for row in store.incident_timeline(incident_id)]


@app.post("/v1/incidents/{incident_id}/ack")
def acknowledge(incident_id: str, request: AckRequest, principal: Principal = Depends(current_principal)):
    row = store.acknowledge(incident_id, principal.subject, request.note, principal.tenant)
    if not row:
        raise HTTPException(404, "Incident not found")
    return decoded(row)


@app.post("/v1/incidents/{incident_id}/investigate")
def investigate(incident_id: str, payload: dict, principal: Principal = Depends(current_principal)):
    incident = store.get("incidents", incident_id, principal.tenant)
    if not incident:
        raise HTTPException(404, "Incident not found")
    result = REGISTRY["agent.investigate@1"]({"evidence": json.loads(incident["evidence"]), "instruction": payload.get("instruction", "Investigate the incident evidence")}, {"store": store, "tenant": principal.tenant, "market": incident["market"]})
    store.append_incident_event(incident_id, "INVESTIGATION_COMPLETED", {"requested_by": principal.subject, "result": result})
    store.record_activity(principal.tenant, principal.subject, "incident.investigated", {"incident_id": incident_id, "status": result.get("status", "completed")})
    return result


@app.post("/v1/incidents/{incident_id}/route/{subscription_id}")
def route_incident_to_subscription(incident_id: str, subscription_id: str, principal: Principal = Depends(current_principal)):
    require_role(principal, "operator")
    try:
        return route_incident(store, incident_id, subscription_id)
    except ValueError as exc:
        raise HTTPException(404, str(exc)) from exc


@app.post("/v1/policies/simulate")
def simulate_policy(payload: dict, principal: Principal = Depends(current_principal)):
    decision = delivery_decision(payload["incident"], payload.get("policy", {}), payload.get("last_delivery"))
    return {**decision, "preview": render_message(payload["incident"], payload.get("channel", "inbox"))}


@app.post("/v1/scheduler/tick")
def scheduler_tick(principal: Principal = Depends(current_principal)):
    require_role(principal, "operator")
    return {"executed": run_due_subscriptions(store, engine, principal.tenant)}


@app.get("/v1/wakeups")
def wakeups(principal: Principal = Depends(current_principal)):
    return [decoded(row) for row in store.list("wake_events", principal.tenant)]


@app.post("/v1/gateway/events")
def gateway_event(event: GatewayEvent, principal: Principal = Depends(current_principal)):
    if event.tenant_id != principal.tenant or event.actor_id != principal.subject:
        raise HTTPException(403, "Gateway identity does not match the authenticated principal")
    if event.type == "message.received":
        return conversation_preview(ConversationRequest(text=event.text, channel=event.channel, thread_id=event.thread_id), principal)
    return dispatch_signal(store, principal.tenant, event.event_id, event.type, event.model_dump(mode="json"))


@app.post("/v1/conversations/preview")
def conversation_preview(request: ConversationRequest, principal: Principal = Depends(current_principal)):
    intent = interpret(request.text, {"tenant": principal.tenant, "actor": principal.subject, "markets": sorted(principal.markets)})
    action = intent["action"]
    if action == "list":
        return {"intent": intent, "subscriptions": [decoded(row) for row in store.list("subscriptions", principal.tenant)], "requires_confirmation": False}
    if action == "explain":
        incident = store.get("incidents", intent["incident_id"], principal.tenant)
        if not incident:
            raise HTTPException(404, "Incident not found")
        return {"intent": intent, "incident": decoded(incident), "timeline": [decoded(row) for row in store.incident_timeline(intent["incident_id"])], "requires_confirmation": False}
    if action == "create":
        require_market(principal, intent["market"])
        candidates = [row for row in store.list("workflows", principal.tenant) if row["id"] == intent["workflow"] or json.loads(row["body"])["metadata"]["name"] == intent["workflow"].split("@")[0]]
        if not candidates:
            raise HTTPException(404, "Requested workflow not found")
        subscription = Subscription(workflow_id=candidates[0]["id"], owner=principal.subject, tenant=principal.tenant, market=intent["market"], confirmed=False)
        intent["subscription"] = subscription.model_dump(mode="json")
        intent["preview"] = preview_subscription(store, subscription, principal.tenant)
    elif action in {"pause", "resume", "disable", "retire"}:
        row = store.get("subscriptions", intent["subscription_id"], principal.tenant)
        if not row:
            raise HTTPException(404, "Subscription not found")
        body = json.loads(row["body"])
        if body.get("owner") != principal.subject and not principal.has_role("admin"):
            raise HTTPException(403, "Subscription belongs to another principal")
        try:
            validate_transition(body.get("status", "active"), intent["patch"]["status"])
        except ValueError as exc:
            raise HTTPException(409, str(exc)) from exc
    else:
        return {"intent": intent, "requires_confirmation": False}
    proposal = create_proposal(store, principal.tenant, principal.subject, intent)
    store.record_activity(principal.tenant, principal.subject, "conversation.proposed", {"thread_id": request.thread_id, "proposal_id": proposal["proposal_id"], "action": action})
    return proposal


@app.post("/v1/conversations/confirm")
def conversation_confirm(request: ConversationConfirmation, principal: Principal = Depends(current_principal)):
    proposal = store.get("proposals", request.proposal_id, principal.tenant)
    if not proposal or proposal["actor"] != principal.subject or proposal["status"] != "PENDING":
        raise HTTPException(404, "Pending proposal not found")
    if proposal["expires"] < __import__("datetime").datetime.now(__import__("datetime").timezone.utc).isoformat():
        store.resolve_proposal(request.proposal_id, principal.tenant, "EXPIRED")
        raise HTTPException(410, "Proposal expired")
    if not request.confirmed:
        store.resolve_proposal(request.proposal_id, principal.tenant, "REJECTED")
        return {"status": "REJECTED"}
    intent = json.loads(proposal["body"])
    if intent["action"] == "create":
        body = {**intent["subscription"], "confirmed": True, "status": "active"}
        subscription = Subscription.model_validate(body)
        result = subscribe(subscription, principal)
    else:
        row = store.update_subscription(intent["subscription_id"], intent["patch"], principal.tenant, principal.subject)
        result = decoded(row)
    store.resolve_proposal(request.proposal_id, principal.tenant, "CONFIRMED")
    store.record_activity(principal.tenant, principal.subject, "conversation.confirmed", {"proposal_id": request.proposal_id, "action": intent["action"]})
    return {"status": "CONFIRMED", "result": result}


@app.get("/v1/agent/profile")
def agent_profile(principal: Principal = Depends(current_principal)):
    row = store.get_profile(principal.subject, principal.tenant)
    return decoded(row) if row else {"principal_id": principal.subject, "tenant": principal.tenant, "body": {"preferences": {}, "communication": {}}}


@app.patch("/v1/agent/profile")
def update_agent_profile(body: dict, principal: Principal = Depends(current_principal)):
    row = store.save_profile(principal.subject, principal.tenant, body)
    store.record_activity(principal.tenant, principal.subject, "profile.updated", {"fields": sorted(body)})
    return decoded(row)


@app.get("/v1/agent/activity")
def agent_activity(principal: Principal = Depends(current_principal)):
    return [decoded(row) for row in store.list("agent_activity", principal.tenant) if row["principal_id"] == principal.subject]


@app.get("/v1/runs")
def runs(principal: Principal = Depends(current_principal)):
    return [decoded(row) for row in store.list("runs", principal.tenant)]


@app.get("/v1/runs/{run_id}")
def get_run(run_id: str, principal: Principal = Depends(current_principal)):
    row = store.get("runs", run_id, principal.tenant)
    if not row:
        raise HTTPException(404, "Run not found")
    return decoded(row)


@app.get("/v1/deliveries")
def deliveries(principal: Principal = Depends(current_principal)):
    return [decoded(row) for row in store.list("deliveries")]


@app.post("/v1/deliveries/dispatch")
def dispatch_deliveries(principal: Principal = Depends(current_principal)):
    require_role(principal, "operator")
    return {"deliveries": dispatch_pending(store)}
