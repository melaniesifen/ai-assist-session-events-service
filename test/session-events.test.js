import assert from "node:assert/strict";
import test from "node:test";

import {
  InMemorySessionEventSink,
  InMemorySessionEventPublisher,
  SessionEventValidationError,
  assertValidSessionEvent,
  createEventDeduplicator,
  detectSequenceGap,
  formatSseEvent,
  replayStatusForLastEventId,
  validateSessionEvent
} from "../src/index.js";

const BASE_EVENT = Object.freeze({
  eventId: "evt_001",
  tenantId: "tenant_001",
  userId: "user_001",
  sessionId: "session_001",
  requestId: "req_001",
  correlationId: "corr_001",
  type: "progress",
  sequence: 1,
  createdAt: "2026-05-29T00:00:00.000Z",
  payload: {
    stage: "context.loading",
    status: "STARTED",
    messageCode: "CONTEXT_LOADING"
  }
});

test("validates a transport-neutral SessionEvent envelope", () => {
  assert.deepEqual(validateSessionEvent(BASE_EVENT), { valid: true, issues: [] });
});

test("rejects invalid envelope fields and sensitive payload keys", () => {
  const result = validateSessionEvent({
    ...BASE_EVENT,
    tenantId: " ",
    type: "assistant.delta",
    payload: {
      messageId: "msg_001",
      delta: "hello",
      index: 0,
      providerKey: "secret"
    }
  });

  assert.equal(result.valid, false);
  assert(result.issues.some((issue) => issue.path === "tenantId"));
  assert(result.issues.some((issue) => issue.code === "forbidden_payload_key"));
});

test("throws typed validation errors for invalid events", () => {
  assert.throws(
    () => assertValidSessionEvent({ ...BASE_EVENT, payload: {} }),
    (error) => error instanceof SessionEventValidationError
      && error.issues.some((issue) => issue.path === "payload.stage")
  );
});

test("publishes, subscribes, and replays events after Last-Event-ID", () => {
  const publisher = new InMemorySessionEventPublisher();
  const first = { ...BASE_EVENT, eventId: "evt_001", sequence: 1 };
  const second = { ...BASE_EVENT, eventId: "evt_002", sequence: 2 };
  const third = { ...BASE_EVENT, eventId: "evt_003", sequence: 3 };
  publisher.publish(first);
  publisher.publish(second);

  const received = [];
  const subscription = publisher.subscribe(BASE_EVENT.sessionId, (event) => received.push(event), {
    lastEventId: "evt_001"
  });

  assert.deepEqual(received.map((event) => event.eventId), ["evt_002"]);
  assert.equal(subscription.replayStatus, "PARTIAL_REPLAY");
  assert.deepEqual(subscription.replayed.map((event) => event.eventId), ["evt_002"]);

  publisher.publish(third);
  assert.deepEqual(received.map((event) => event.eventId), ["evt_002", "evt_003"]);

  subscription.unsubscribe();
  publisher.publish({ ...BASE_EVENT, eventId: "evt_004", sequence: 4 });
  assert.deepEqual(received.map((event) => event.eventId), ["evt_002", "evt_003"]);
});

test("surfaces unavailable replay after the requested event leaves the buffer", () => {
  const publisher = new InMemorySessionEventPublisher({
    sink: new InMemorySessionEventSink({ maxEventsPerSession: 1 })
  });
  publisher.publish({ ...BASE_EVENT, eventId: "evt_001", sequence: 1 });
  publisher.publish({ ...BASE_EVENT, eventId: "evt_002", sequence: 2 });

  const subscription = publisher.subscribe(BASE_EVENT.sessionId, () => {}, {
    lastEventId: "evt_001"
  });

  assert.equal(subscription.replayStatus, "REPLAY_UNAVAILABLE");
  assert.deepEqual(subscription.replayed, []);
});

test("rejects duplicate event IDs within the retained session buffer", () => {
  const publisher = new InMemorySessionEventPublisher();
  publisher.publish(BASE_EVENT);

  assert.throws(
    () => publisher.publish({ ...BASE_EVENT, payload: { ...BASE_EVENT.payload, status: "DONE" } }),
    (error) => error instanceof SessionEventValidationError
      && error.issues.some((issue) => issue.code === "duplicate_event_id")
  );
});

test("rejects event IDs that would inject additional SSE field lines", () => {
  const injected = { ...BASE_EVENT, eventId: "evt_001\nretry: 0" };
  const result = validateSessionEvent(injected);

  assert.equal(result.valid, false);
  assert(result.issues.some((issue) => issue.path === "eventId"));
  assert.throws(() => formatSseEvent(injected), SessionEventValidationError);
});

test("formats SSE id, event, and JSON data fields", () => {
  const formatted = formatSseEvent(BASE_EVENT);
  assert.match(formatted, /^id: evt_001\n/u);
  assert.match(formatted, /\nevent: progress\n/u);
  assert.match(formatted, /\ndata: \{"eventId":"evt_001"/u);
  assert.match(formatted, /\n\n$/u);
});

test("deduplicates reconnect deliveries by eventId", () => {
  const deduplicator = createEventDeduplicator({ initialEventIds: ["evt_001"] });
  assert.equal(deduplicator.shouldProcess(BASE_EVENT), false);
  assert.equal(deduplicator.shouldProcess({ ...BASE_EVENT, eventId: "evt_002" }), true);
  assert.equal(deduplicator.shouldProcess({ ...BASE_EVENT, eventId: "evt_002" }), false);
});

test("detects sequence gaps and unavailable replay windows", () => {
  assert.deepEqual(detectSequenceGap(1, { ...BASE_EVENT, sequence: 3 }), {
    hasGap: true,
    expectedSequence: 2,
    actualSequence: 3
  });

  assert.deepEqual(
    replayStatusForLastEventId([{ ...BASE_EVENT, eventId: "evt_002" }], "evt_001"),
    { status: "REPLAY_UNAVAILABLE", events: [] }
  );
});
