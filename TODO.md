# NIDL Roadmap

This roadmap tracks work that is not yet verified complete. Check an item only after its acceptance criteria and relevant automated tests pass.

## P0 — Durable production execution

- [ ] Move subscription scheduling to Temporal Schedules.
  - Create, update, pause, resume, and delete schedules when subscriptions change.
  - Preserve evaluation sharing for compatible subscriptions.
  - Prove schedule recovery after API and worker restarts.
- [ ] Route signal and recovery events into durable Temporal workflows.
  - Map authenticated gateway events to affected subscriptions and workflows.
  - Deduplicate repeated events by event ID.
  - Test out-of-order signal and recovery delivery.
- [ ] Add cooperative cancellation inside long-running primitives.
  - Heartbeat query and agent activities with useful progress details.
  - Stop database queries and external calls when cancellation is requested.
  - Test cancellation during an active step, not only between steps.
- [ ] Replace the NIDL SQLite production path with PostgreSQL.
  - Add migrations, transactions, connection pooling, and concurrency tests.
  - Retain SQLite as the zero-dependency local development option.
- [ ] Add Temporal Cloud and secured self-hosted connectivity.
  - Support API-key authentication, mTLS certificates, namespace configuration, and secret references.
  - Document credential rotation without worker downtime.
- [ ] Add worker deployment safety.
  - Graceful shutdown, readiness/liveness probes, build IDs, compatible worker versioning, and rolling-upgrade tests.

## P0 — Security and tenancy

- [ ] Add OIDC/SSO and short-lived service identities.
- [ ] Add policy-based authorization for tenant, market, dataset, workflow, and destination scopes.
- [ ] Store credentials only through a secrets manager; remove direct production secret environment variables.
- [ ] Sign published workflow and subscription artifacts and verify signatures before execution.
- [ ] Add immutable audit export with actor, decision, policy version, and correlation identifiers.
- [ ] Complete cross-tenant isolation and authorization tests for every API operation and background worker.

## P1 — Telecom intelligence

- [ ] Add topology-aware RAN, transport, and core correlation.
- [ ] Implement incident episode merge and split with preserved lineage.
- [ ] Add temporal and spatial root-cause scoring with explainable evidence.
- [ ] Add dependency-aware blast-radius and customer-impact estimation.
- [ ] Add recovery confirmation windows and flapping detection.
- [ ] Build replay comparisons against historical windows and prior workflow versions.
- [ ] Add representative telecom fixture datasets and expected incident goldens.

## P1 — Compiler and data contracts

- [ ] Validate dataset schemas and field references during compilation.
- [ ] Add unit, time-grain, geography-grain, and market compatibility checks.
- [ ] Add data lineage and sensitivity labels to compiled plans.
- [ ] Add SQL explain/cost checks and configurable execution budgets.
- [ ] Add workflow compatibility rules and migration tooling between DSL versions.
- [ ] Add a machine-readable JSON Schema for every public NIDL artifact.

## P1 — Integrations and delivery

- [ ] Verify StarRocks against production-like schemas, query limits, cancellation, and failure modes.
- [ ] Verify Slack delivery, interaction callbacks, destination authorization, and signature validation.
- [ ] Verify SMTP delivery, bounce handling, retry policy, and message threading.
- [ ] Integrate production OKF resolution with immutable version and digest enforcement.
- [ ] Integrate the LangGraph agent service with evidence bounds, tool-call budgets, and trace capture.
- [ ] Add durable notification workers and dead-letter replay controls.

## P1 — Observability and operations

- [ ] Export OpenTelemetry traces across API, Temporal workflow, activities, database, and external calls.
- [ ] Add Prometheus metrics and dashboards for schedules, lag, retries, failures, incidents, and deliveries.
- [ ] Add structured logs with tenant-safe correlation IDs and redaction.
- [ ] Define SLOs and alerts for evaluation freshness, delivery latency, and workflow failures.
- [ ] Add retention, archival, backup, restore, and disaster-recovery procedures.
- [ ] Add load, soak, chaos, and worker-restart tests.

## P2 — Web console and user experience

- [ ] Add a visual DAG editor with compiler feedback and plan diffs.
- [ ] Add live time-series charts, H3 maps, topology views, and incident timelines.
- [ ] Add durable-run detail with Temporal history, current activity, attempts, and cancellation state.
- [ ] Add subscription schedule management and timezone previews.
- [ ] Add notification preview, delivery replay, and dead-letter controls.
- [ ] Add accessible responsive states, keyboard navigation, and browser test coverage.
- [ ] Add general natural-language workflow authoring with explicit review and confirmation.

## P2 — Open-source readiness

- [ ] Add `CONTRIBUTING.md`, code of conduct, security policy, issue templates, and pull-request template.
- [ ] Add release automation, changelog generation, semantic versioning, and signed artifacts.
- [ ] Publish versioned container images and Python packages with provenance/SBOM attestations.
- [ ] Add end-to-end example environments and operator tutorials.
- [ ] Document extension APIs for primitives, connectors, presentations, and agent tools.

## Definition of done

A capability is complete only when:

1. Its public contract and failure behavior are documented.
2. Unit tests and appropriate integration or browser tests pass in CI.
3. Tenant isolation, authorization, idempotency, retry, and cancellation behavior are tested where applicable.
4. Operational telemetry and actionable error messages exist.
5. The capability has been exercised against a representative environment rather than only mocked dependencies.
