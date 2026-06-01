# Task Breakdown

Update this file as implementation progresses. Check off completed tasks in the same change that implements them.

Canonical cross-repo tasks live in `../ai-assist-architecture/implementation-task-breakdown.md`. This repo owns the session-events portions of `EVT-*`, action status event delivery, `OPS-003`, and `REPO-001` items, grounded by `../ai-assist-architecture/lld-session-events-transport.md`. `REPO-001 bootstrap` names the original temporary Node.js package setup; the pending final `REPO-001` item is the language/runtime/package-structure decision from the workspace task list. Any `INFRA-004`, `OPS-004`, `OPS-005`, or `E2E-005` item listed here is the session-events-owned deployment, metrics, redaction-check, or operational-validation slice of that cross-cutting task.

Migration status: The repo has been migrated from the temporary JavaScript ESM bootstrap to Python initially for the current local package scope. Revisit Java only if long-running high-concurrency requirements justify it. Broad new feature work may continue in Python after the parent migration checkpoint.

## Completed Bootstrap

- [x] REPO-001 bootstrap: create dependency-light Node.js ESM package with direct `node:test` coverage commands.
- [x] EVT-002 repo-local: implement transport-neutral `SessionEvent` envelope validation with event, correlation, tenant/user/session, type, payload, timestamp, and sequence fields.
- [x] EVT-002 repo-local: reject sensitive payload keys before publish.
- [x] EVT-003 repo-local: implement SSE formatting helper with `id`, event type, and JSON `data` fields.
- [x] EVT-003 repo-local: implement reconnect, replay-window, sequence-gap, and deduplication helpers without exactly-once assumptions.
- [x] EVT-004 repo-local: implement in-memory event sink and publisher helpers decoupled from orchestration workflow logic.
- [x] EVT-005 repo-local: keep WebSocket out of the MVP implementation while preserving transport-neutral event primitives.
- [x] ACTION-002 / ACTION-003 repo-local: allow `action.proposed` and `action.status_changed` events as typed transport payloads.
- [x] OPS-003 bootstrap: reject obvious secret/raw-content payload keys and keep transport helpers free of content logging.
- [x] Repo hygiene: document tests and coverage commands, and ignore prompts, feedback, coverage output, dependencies, and build artifacts.

## Pending Architecture Tasks

- [ ] REPO-001: decide final language/runtime, framework, package manager, package layout, migration cost, deployment target, and test strategy for this repo.
- [x] REPO-002: migrate the session-events bootstrap to a Python package layout initially, with equivalent behavior and tests, before broad new feature work continues; revisit Java only if long-running high-concurrency requirements justify it.
- [ ] EVT-002: align `SessionEvent` envelope and payload validation with versioned shared contracts after `ai-assist-contracts` publishes them.
- [ ] EVT-003: add authenticated SSE HTTP route adapter with server-derived identity and session authorization checks.
- [x] EVT-003: add client reconnect behavior contract tests for `Last-Event-ID`, unavailable replay, duplicate event IDs, and HTTP state refresh guidance.
- [ ] EVT-003 / E2E-002: add integration tests for authenticated SSE streaming of progress, assistant delta, assistant final, error, and action events.
- [ ] EVT-003: ensure assistant delta, assistant final, progress, error, action proposed, and action status changed event types are covered by shared payload tests.
- [x] EVT-004: add event publisher interface for orchestration with typed publisher failure categories.
- [ ] EVT-004: verify publisher failures cannot corrupt durable command/action state owned by other services.
- [ ] EVT-004 / E2E-003: add integration tests for orchestration publisher handoff, action proposed events, and action status changed events.
- [x] EVT-005: document future WebSocket trigger checklist and adapter constraints in repo-local adapter notes when transport work starts.
- [x] OPS-003: add metadata-only logging rules for stream opens, closes, errors, replay misses, and sequence gaps.
- [ ] OPS-004: add metrics for stream opens, closes, duration, disconnects, replay misses, sequence gaps, and transport errors.
- [ ] OPS-004 / INFRA-004: add deployment pipeline checks for SSE route config, stream timeout settings, replay buffer settings, metrics, and log redaction.
- [ ] OPS-005 / E2E-005: add operational validation for stream failure, replay misses, disconnect spikes, publisher dependency failure, and sequence-gap recovery.
- [ ] Quality: raise line coverage to at least 95% after route/publisher adapters are added.

## Future Production Tasks

- [ ] EVT-005: add WebSocket adapter only when a documented product trigger exists.
- [ ] EVT-005: add multi-client subscription authorization if WebSocket is introduced.
- [ ] EVT-003: add durable event history only if replay requirements exceed the short-lived buffer policy.
