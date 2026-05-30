# AGENTS.md

## Repo Purpose

`ai-assist-session-events-service` owns transport-neutral `SessionEvent` delivery. MVP uses SSE; future WebSocket support must be an adapter over the same event contract.

## Agent Instructions

- Read `README.md`, `ai-assist-platform-context.md`, and `../ai-assist-architecture/lld-session-events-transport.md` before changing behavior.
- Keep business workflow logic, prompt construction, provider calls, and document mutation out of this repo.
- Validate event envelopes and typed payloads before publishing.
- Treat SSE as a display/update path only. Durable state and mutations remain HTTP-owned elsewhere.
- Support deduplication and reconnect behavior without assuming exactly-once delivery.
- Do not log secrets or raw prompt/document/model-response content in transport logs.
- Add tests for envelope validation, duplicate events, sequence gaps, replay unavailable, SSE formatting, and sensitive payload rejection.

## Commands

- Run tests with `node --test`.
- `npm` may not be available in this environment; prefer the direct Node command.

## Review Notes

Before committing, review whether transport helpers remain independent of SSE-specific assumptions and whether future WebSocket can reuse the event contract.
