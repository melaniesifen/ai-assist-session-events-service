# ai-assist-session-events-service

Transport-neutral session event primitives for the AI Assist Platform.

## Boundary

This service owns:

- `SessionEvent` envelope validation.
- In-memory event publishing and subscription primitives for local tests and future adapters.
- SSE formatting helpers.
- Stdlib HTTP runtime wrapper for the canonical authenticated SSE route.
- Reconnect, replay, sequence-gap, and deduplication helpers.

This service does not own:

- Prompt construction.
- Provider calls.
- Proposed-action lifecycle decisions.
- Document mutations.
- Durable HTTP apply-action behavior.

## Current Implementation

The package is dependency-free Python using only the standard library. It exports transport helpers plus a stdlib HTTP runtime for the canonical SSE route.

`src/ai_assist_session_events/http_sse.py` provides a framework-neutral authenticated stream adapter. `src/ai_assist_session_events/http_runtime.py` wraps that adapter in a runnable stdlib HTTP runtime for `GET /sessions/{sessionId}/events`. `src/ai_assist_session_events/http_app.py` exposes that runtime through the deployable dogfood HTTP adapter. The runtime requires a server-derived auth context, defaulting to trusted upstream headers `X-AI-Assist-Tenant-Id` and `X-AI-Assist-User-Id`, emits `text/event-stream`, supports `Last-Event-ID` replay, heartbeat keepalive frames, disconnect close logs, and metadata-only lifecycle records.

SSE is a best-effort display path. Durable state must still be fetched over HTTP after reconnects, browser refreshes, or sequence gaps.

Publisher callers can use `try_publish` to receive typed publisher failure categories without throwing. Stream adapters should build metadata-only lifecycle records with `create_stream_log_record`; these helpers reject known sensitive keys and discard unsupported metadata fields.

Future WebSocket trigger checks and adapter constraints are tracked in [ADAPTER-NOTES.md](ADAPTER-NOTES.md).

## Task Breakdown

Implementation tasks are tracked in [TASKS.md](TASKS.md). Update the checkboxes there in the same change that implements or verifies a task.

## Testing And Coverage

Run the unit tests with:

```sh
PYTHONPATH=src python3 -m unittest discover -s tests
```

No third-party test runner is required. No repo-local coverage command is currently defined for the dependency-free Python bootstrap. If later tooling writes coverage, cache, virtualenv, dependency, or build output, those generated paths are ignored by `.gitignore`.
