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

Publisher callers can use `tryPublish` to receive typed publisher failure categories without throwing. Stream adapters should build metadata-only lifecycle records with `createStreamLogRecord`; these helpers reject known sensitive keys and discard unsupported metadata fields.

Future WebSocket trigger checks and adapter constraints are tracked in [ADAPTER-NOTES.md](ADAPTER-NOTES.md).

## Task Breakdown

Implementation tasks are tracked in [TASKS.md](TASKS.md). Update the checkboxes there in the same change that implements or verifies a task.

## Testing And Coverage

Run the unit tests with either command:

```sh
node --test
npm test
```

View the built-in coverage report in the terminal:

```sh
node --experimental-test-coverage --test
npm run coverage
```

The coverage command uses Node's built-in test runner and prints a text report. If later tooling writes HTML, LCOV, TAP, JUnit, or build output, those generated paths are ignored by `.gitignore`.
