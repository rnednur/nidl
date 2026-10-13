# NIDL — Network Intelligence Definition Language

NIDL is an open-source framework for defining, validating, executing, and operating telecom network-intelligence workflows. It combines deterministic analytics primitives with incident correlation, personalized delivery policies, safe presentation definitions, and optional evidence-bounded agent investigation.

Version 0.4 adds Temporal-backed durable execution to the Hermes-inspired personal-agent and deterministic NIDL runtime. It is a functional local control plane and worker stack; see **Known gaps** below for production boundaries.

## Run locally

NIDL requires Python 3.11 or newer.

```bash
python3.11 -m venv .venv
source .venv/bin/activate
pip install -e '.[dev]'
uvicorn nidl.api:app --reload
```

Open <http://localhost:8000> for the operations console or <http://localhost:8000/docs> for the API.

### Run the durable Temporal stack

Docker Compose starts NIDL, a separate NIDL worker, Temporal, PostgreSQL for Temporal, and Temporal UI:

```bash
docker compose up --build
```

- NIDL console: <http://localhost:8000>
- NIDL API documentation: <http://localhost:8000/docs>
- Temporal UI: <http://localhost:8080>

Publish a workflow in the console and select **Temporal**, or call `POST /v1/workflows/{workflow_id}/run/durable`. Supplying `request_id` makes client retries idempotent. Durable run state is available at `GET /v1/durable/runs/{run_id}` and an operator can request cooperative cancellation with `POST /v1/durable/runs/{run_id}/cancel`.

To connect to an existing Temporal cluster instead, install the optional worker dependency and run the worker separately:

```bash
pip install -e '.[temporal]'
export NIDL_TEMPORAL_ADDRESS=temporal.example.net:7233
export NIDL_TEMPORAL_NAMESPACE=nidl-production
export NIDL_TEMPORAL_TASK_QUEUE=nidl-workflows
export NIDL_TEMPORAL_TLS=true
nidl-temporal-worker
```

Run the verified test suite with:

```bash
pytest -q
```

## Implemented and tested

- Typed JSON workflow model, DAG validation, reference checking, parameter constraints, schedule syntax validation, deterministic compiled-plan hashes, cost previews, and immutable publication records.
- Runtime step diagnostics, conditional skipping, bounded retries, dry runs, side-effect suppression, approved StarRocks query templates, and registered primitive dispatch.
- Tenant-aware SQLite persistence with legacy schema migration, audit records, subscription revisions, execution records, and idempotent delivery records.
- Incident episode identity, lifecycle transitions, impact tracking, evidence updates, acknowledgement, and append-only timelines.
- Adaptive notification decisions for quiet hours, critical overrides, and cadence suppression; normalized inbox, email, and Slack rendering; SMTP and allowlisted Slack webhook transports.
- Safe presentation definitions using an allowlisted component registry and field bindings without arbitrary HTML or JavaScript.
- Optional API-key authentication with tenant, market, and role checks. Authentication is disabled for local development unless explicitly enabled.
- Manual scheduler ticks for subscribed workflows and a bounded HTTPS agent bridge requiring structured hypotheses.
- A responsive operations console with overview metrics, workflow publication and execution, incident queue, run diagnostics, subscriptions, and delivery outbox views.
- Subscription preview, explicit confirmation, lifecycle transitions, immutable version history, rollback, workflow-version pinning, and validated OKF recipe references.
- Idempotent schedule and signal wake-ups, authorization-safe shared evaluation keys, one-run/many-subscriber fan-out, and zero-LLM recurring execution.
- Per-incident/per-subscription notification state, stable backoff, material-change overrides, daily rate limits, delivery retries, and dead-letter status.
- Typed multi-channel gateway envelopes, deterministic conversational job-management proposals, personal profiles, and an agent activity timeline.
- Temporal workflow submission with stable workflow IDs, one durable activity per compiled NIDL step, per-step retry and timeout policies, heartbeats, persisted progress, state queries, cooperative cancellation, and separate API/worker deployment.

## Key API operations

- `POST /v1/workflows/validate` — validate and preview a compiled plan.
- `POST /v1/workflows/compile` — produce an immutable executable plan.
- `POST /v1/workflows` — publish a workflow version.
- `POST /v1/workflows/{id}/run` and `/replay` — execute or dry-run.
- `POST /v1/workflows/{id}/run/durable` — submit a validated execution to Temporal.
- `GET /v1/durable/runs/{id}` and `POST /cancel` — inspect or cancel a durable run.
- `POST` and `PATCH /v1/subscriptions/{id}` — create and revise subscriber policy.
- `POST /v1/subscriptions/preview` — validate scope, parameters, pinned workflow, OKF recipes, and estimated cost before confirmation.
- `POST /v1/conversations/preview` and `/confirm` — preview and confirm bounded create, pause, resume, disable, and retire actions.
- `POST /v1/gateway/events` — normalize authenticated web, Slack, email, signal, and recovery events.
- `GET /v1/incidents/{id}/timeline` and `POST /ack` — inspect and acknowledge an episode.
- `POST /v1/policies/simulate` — preview adaptive delivery decisions.
- `POST /v1/scheduler/tick` — evaluate due subscriptions.
- `POST /v1/deliveries/dispatch` — process the notification outbox.

## Configuration

StarRocks uses `STARROCKS_HOST`, `STARROCKS_PORT`, `STARROCKS_USER`, `STARROCKS_PASSWORD`, `STARROCKS_DATABASE`, and an `NIDL_QUERIES` JSON file of approved parameterized SELECT templates.

Enable API-key security with `NIDL_AUTH_MODE=apikey` and `NIDL_API_KEYS`, a JSON map from keys to `{subject, tenant, markets, roles}`. Use a secrets manager in deployed environments; do not commit keys.

Slack delivery accepts only HTTPS webhook hosts in `NIDL_SLACK_HOSTS` (default `hooks.slack.com`). Email uses `NIDL_SMTP_HOST`, `NIDL_SMTP_PORT`, `NIDL_SMTP_STARTTLS`, `NIDL_SMTP_USER`, `NIDL_SMTP_PASSWORD`, and `NIDL_EMAIL_FROM`.

The optional agent bridge uses `NIDL_AGENT_ENDPOINT` (HTTPS only) and `NIDL_AGENT_TOKEN`. A missing endpoint degrades to a deterministic `not_configured` result.

Set `NIDL_OKF_CATALOG` to a JSON object keyed by immutable recipe reference. Subscription preview rejects unknown or unresolvable OKF bundles. `examples/okf_catalog.json` shows the local contract.

Temporal uses `NIDL_TEMPORAL_ADDRESS`, `NIDL_TEMPORAL_NAMESPACE`, `NIDL_TEMPORAL_TASK_QUEUE`, and `NIDL_TEMPORAL_TLS`. The API and worker must share the application data store and the same approved query/secret configuration. Temporal Cloud authentication is not yet wired; the current TLS flag covers server TLS but not API-key or mTLS credential loading.

## Known gaps

- Manual NIDL runs now have a verified Temporal path with durable per-step retries and cooperative cancellation. Subscription schedule creation and signal-to-workflow mapping still use the in-process dispatcher rather than Temporal Schedules and durable signal workflows.
- SQLite supports local development and migration but does not replace PostgreSQL, a transactional event bus, dead-letter processing, or high availability.
- Email and Slack transport code is implemented, but live delivery requires credentials and has not been verified against external services in this repository.
- StarRocks and the agent bridge require production endpoints and schemas, so their live integrations remain unverified.
- Correlation provides stable episode identity, lifecycle transitions including reopen, and simple grouping; merge/split logic and cross-domain topology/temporal RCA remain incomplete.
- The compiler does not yet perform full dataset schema, unit, time-grain, geography-grain, SQL cost, or cross-market lineage analysis.
- API-key RBAC is a deployable baseline, not SSO/OIDC, artifact signing, approval workflow, or a complete ABAC policy engine.
- The bundled Temporal stack is for local verification. Production still needs Temporal Cloud or an HA self-hosted cluster, authentication/mTLS, encrypted application persistence, metrics, and retention policies.
- The console supports bounded conversational subscription management, but has no general natural-language workflow authoring, visual DAG editor, live H3 map, or time-series charts yet.

These boundaries are intentional and are not represented as complete until integration and failure-mode tests exist.
