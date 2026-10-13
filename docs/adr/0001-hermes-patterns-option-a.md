# Adopt Hermes Patterns Without the Hermes Runtime

## Status

Accepted for NIDL 0.3.

## Decision

NIDL retains LangGraph and OKF as optional control-plane and investigation integrations. The deterministic NIDL compiler, runtime, incident engine, shared evaluation planner, wake-up dispatcher, notification ledger, and channel adapters remain framework-neutral.

Recurring evaluations do not invoke an LLM unless a published workflow explicitly contains an approved agent step. Conversational changes produce bounded proposals that require validation and confirmation before mutating a subscription.

## Consequences

- Personal agents are persistent profiles and activity records, not dedicated worker processes.
- Schedule and signal events use idempotent wake-up records.
- Compatible subscriptions share one authorized evaluation and fan out through separate notification policies.
- Workflow and OKF versions are pinned when a subscription is activated.
- Conversation history is not the system of record for scheduling or delivery state.
- A future Hermes adapter may call the same gateway and conversation APIs without becoming the execution runtime.
