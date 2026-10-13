# ADR 0002: Temporal for durable NIDL execution

- Status: Accepted
- Date: 2026-10-12

## Context

The local NIDL runtime executes a compiled DAG in one process. That is useful for previews and development, but it cannot recover execution after a process restart, durably retry an individual step, expose orchestration state, or accept a persisted cancellation request.

## Decision

Temporal is the production orchestration path. The API compiles and validates the NIDL definition before submission and records an immutable, JSON-serializable execution payload in Temporal history. `NIDLExecutionWorkflow` then runs each ordered NIDL step as a separate activity.

Each activity has the step's declared timeout and retry limit. Side effects retain NIDL's application-level idempotency keys. A stable `request_id` becomes both the NIDL run ID and the Temporal workflow ID, making submission safe to repeat. The workflow exposes a state query and a cancellation signal; cancellation is cooperative at step boundaries. The SQLite run record is a read model, while Temporal history is the durable orchestration source of truth.

The in-process engine remains available for validation, dry runs, tests, and minimal local installations. Production operators should run separate API and worker processes and use a managed or highly available Temporal cluster.

## Consequences

- Worker restarts do not lose workflow progress.
- Retries occur at the failed NIDL step rather than restarting the entire DAG.
- API and worker deployment can scale independently.
- Deployments must provide Temporal namespace, task queue, TLS, and authentication settings.
- Long-running primitives must heartbeat and cooperate with activity cancellation for sub-step cancellation; current built-in primitives are short-lived, and the workflow guarantees cancellation between steps.
- SQLite remains a local read model. PostgreSQL-backed application persistence and a production secrets system are still required for high availability.
