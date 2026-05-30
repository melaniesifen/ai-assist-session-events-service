# ai-assist-session-events-service

Transport-neutral session event primitives for the AI Assist Platform.

## Boundary

This service owns:

- `SessionEvent` envelope validation.
- In-memory event publishing and subscription primitives for local tests and future adapters.
- SSE formatting helpers.
- Reconnect, replay, sequence-gap, and deduplication helpers.

This service does not own:

- Prompt construction.
- Provider calls.
- Proposed-action lifecycle decisions.
- Document mutations.
- Durable HTTP apply-action behavior.

## Current Implementation

The package is dependency-light Node.js ESM. It exports domain helpers only; it does not start an HTTP server. Future HTTP and SSE adapters should wrap these helpers and keep authenticated stream access checks at the adapter boundary.

SSE is a best-effort display path. Durable state must still be fetched over HTTP after reconnects, browser refreshes, or sequence gaps.

## Tests

Run:

```sh
node --test
```

The test suite uses `node:test` and installs no packages.
