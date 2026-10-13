from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator, model_validator


class Step(BaseModel):
    id: str = Field(pattern=r"^[a-z][a-z0-9_-]{0,63}$")
    uses: str
    needs: list[str] = Field(default_factory=list)
    with_: dict[str, Any] = Field(default_factory=dict, alias="with")
    when: bool | str = True
    retry: int = Field(default=0, ge=0, le=5)
    timeout_seconds: int = Field(default=60, ge=1, le=3600)

    model_config = {"populate_by_name": True}


class Workflow(BaseModel):
    apiVersion: Literal["network.ai/v1"] = "network.ai/v1"
    kind: Literal["NetworkWorkflow"] = "NetworkWorkflow"
    metadata: dict[str, Any]
    parameters: dict[str, dict[str, Any]] = Field(default_factory=dict)
    triggers: list[dict[str, Any]] = Field(default_factory=list)
    steps: list[Step]
    outputs: dict[str, Any] = Field(default_factory=dict)
    presentation_ref: str | None = None

    @model_validator(mode="after")
    def validate_graph(self):
        for field in ("name", "version", "owner"):
            if not self.metadata.get(field):
                raise ValueError(f"metadata.{field} is required")
        ids = [step.id for step in self.steps]
        if not ids or len(ids) != len(set(ids)):
            raise ValueError("Step ids must be unique and nonempty")
        known = set(ids)
        for step in self.steps:
            if step.id in step.needs or any(dep not in known for dep in step.needs):
                raise ValueError(f"Invalid dependencies for {step.id}")
        return self


class DeliveryPolicy(BaseModel):
    min_interval_minutes: int = Field(default=30, ge=0, le=10080)
    digest_after_minutes: int = Field(default=240, ge=1, le=10080)
    quiet_hours_start: int | None = Field(default=None, ge=0, le=23)
    quiet_hours_end: int | None = Field(default=None, ge=0, le=23)
    critical_severity: int = Field(default=4, ge=1, le=5)
    material_change_percent: float = Field(default=20, ge=0, le=1000)
    stable_backoff_minutes: list[int] = Field(default_factory=lambda: [15, 30, 60, 240])
    maximum_silence_minutes: int = Field(default=1440, ge=1, le=10080)
    max_notifications_per_day: int = Field(default=24, ge=1, le=1000)

    @field_validator("stable_backoff_minutes")
    @classmethod
    def ordered_backoff(cls, value):
        if not value or any(item < 1 or item > 10080 for item in value) or value != sorted(value):
            raise ValueError("stable_backoff_minutes must be an ascending list of positive minutes")
        return value


class EvaluationPolicy(BaseModel):
    every_minutes: int = Field(default=15, ge=1, le=10080)
    freshness_minutes: int = Field(default=15, ge=1, le=10080)
    schedule: str | None = None


class Subscription(BaseModel):
    workflow_id: str
    owner: str
    tenant: str = "default"
    market: str
    parameters: dict[str, Any] = Field(default_factory=dict)
    schedule: str = "*/15 * * * *"
    timezone: str = "UTC"
    channel: Literal["inbox", "email", "slack"] = "inbox"
    destination: str | None = None
    destination_ref: str | None = None
    policy: DeliveryPolicy = Field(default_factory=DeliveryPolicy)
    evaluation: EvaluationPolicy = Field(default_factory=EvaluationPolicy)
    status: Literal["draft", "active", "paused", "disabled", "error", "retired"] = "draft"
    confirmed: bool = False
    workflow_version: str | None = None
    okf_bundle_ref: str | None = None
    investigation_recipe: str | None = None
    ai_mode: Literal["disabled", "conditional", "always"] = "conditional"
    max_tool_calls: int = Field(default=8, ge=0, le=32)
    enabled: bool = True

    @model_validator(mode="after")
    def require_destination(self):
        if self.channel != "inbox" and self.confirmed and not (self.destination or self.destination_ref):
            raise ValueError(f"destination or destination_ref is required to activate {self.channel}")
        if self.confirmed and self.status == "draft":
            self.status = "active"
        if not self.enabled and self.status == "active":
            self.status = "paused"
        return self


class SubscriptionPatch(BaseModel):
    enabled: bool | None = None
    schedule: str | None = None
    channel: Literal["inbox", "email", "slack"] | None = None
    destination: str | None = None
    destination_ref: str | None = None
    parameters: dict[str, Any] | None = None
    policy: DeliveryPolicy | None = None
    evaluation: EvaluationPolicy | None = None
    status: Literal["draft", "active", "paused", "disabled", "error", "retired"] | None = None
    confirmed: bool | None = None
    owner: str | None = None
    okf_bundle_ref: str | None = None
    investigation_recipe: str | None = None


class PresentationDefinition(BaseModel):
    apiVersion: Literal["network.ai/v1"] = "network.ai/v1"
    kind: Literal["PresentationDefinition"] = "PresentationDefinition"
    metadata: dict[str, Any]
    components: list[dict[str, Any]]

    @model_validator(mode="after")
    def validate_components(self):
        allowed = {"metric_cards", "line_chart", "h3_map", "ranked_table", "investigation_link", "text"}
        for component in self.components:
            if component.get("type") not in allowed:
                raise ValueError(f"Unsupported presentation component: {component.get('type')}")
        return self


class RunRequest(BaseModel):
    parameters: dict[str, Any] = Field(default_factory=dict)
    dry_run: bool = False
    replay_at: str | None = None


class DurableRunRequest(RunRequest):
    request_id: str | None = Field(default=None, pattern=r"^[A-Za-z0-9._:-]{1,128}$")


class AckRequest(BaseModel):
    note: str | None = Field(default=None, max_length=1000)


class SubscriptionPreviewRequest(BaseModel):
    subscription: Subscription


class SubscriptionArtifact(BaseModel):
    apiVersion: Literal["nidl.io/v1"]
    kind: Literal["Subscription"]
    metadata: dict[str, Any]
    spec: dict[str, Any]

    @model_validator(mode="after")
    def required_contract(self):
        if not self.metadata.get("name"):
            raise ValueError("metadata.name is required")
        for field in ("workflowRef", "scope"):
            if field not in self.spec:
                raise ValueError(f"spec.{field} is required")
        if not self.spec["scope"].get("market"):
            raise ValueError("spec.scope.market is required")
        return self


class GatewayEvent(BaseModel):
    event_id: str = Field(pattern=r"^evt_[A-Za-z0-9_-]+$")
    type: Literal["message.received", "signal.received", "recovery.received"]
    channel: Literal["web", "slack", "email", "system"]
    actor_id: str
    tenant_id: str
    thread_id: str
    received_at: datetime
    text: str = Field(max_length=10000)
    auth_context_ref: str


class ConversationRequest(BaseModel):
    text: str = Field(min_length=1, max_length=10000)
    channel: Literal["web", "slack", "email"] = "web"
    thread_id: str = "default"


class ConversationConfirmation(BaseModel):
    proposal_id: str
    confirmed: bool = True
