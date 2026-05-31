import assert from "node:assert/strict";
import test from "node:test";

import {
  InMemorySessionEventSink,
  InMemorySessionEventPublisher,
  PUBLISHER_FAILURE_CATEGORIES,
  STREAM_LOG_OPERATIONS,
  SessionEventPublisherError,
  SessionEventValidationError,
  assertValidSessionEvent,
  createEventDeduplicator,
  createStreamLogRecord,
  detectSequenceGap,
  formatSseEvent,
  reconnectRecoveryGuidance,
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

test("validates every supported typed payload shape", () => {
  const cases = [
    {
      type: "assistant.delta",
      payload: { messageId: "msg_001", delta: "hello", index: 0 }
    },
    {
      type: "assistant.final",
      payload: { messageId: "msg_001", finishReason: "stop", usage: { outputTokens: 10 } }
    },
    {
      type: "progress",
      payload: { stage: "provider.generating", status: "STARTED", messageCode: "PROVIDER_GENERATING" }
    },
    {
      type: "error",
      payload: { errorCode: "PROVIDER_TIMEOUT", category: "DEPENDENCY", retryable: true, message: "Try again." }
    },
    {
      type: "action.proposed",
      payload: {
        actionId: "act_001",
        actionType: "google_docs.replace_text",
        resourceRef: { provider: "google_docs", resourceId: "doc_001" },
        summary: "Replace selected text.",
        expiresAt: "2026-05-30T00:00:00.000Z"
      }
    },
    {
      type: "action.status_changed",
      payload: { actionId: "act_001", previousStatus: "PROPOSED", status: "APPROVED", reasonCode: "USER_APPROVED" }
    }
  ];

  for (const [index, eventCase] of cases.entries()) {
    assert.deepEqual(
      validateSessionEvent({
        ...BASE_EVENT,
        eventId: `evt_typed_${index}`,
        type: eventCase.type,
        payload: eventCase.payload
      }),
      { valid: true, issues: [] }
    );
  }
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
    (error) => error instanceof SessionEventPublisherError
      && error.category === PUBLISHER_FAILURE_CATEGORIES.PERSISTENCE
      && error.cause instanceof SessionEventValidationError
      && error.cause.issues.some((issue) => issue.code === "duplicate_event_id")
  );
});

test("returns typed publisher failure results without throwing", () => {
  const publisher = new InMemorySessionEventPublisher();
  const result = publisher.tryPublish({ ...BASE_EVENT, payload: {} });

  assert.equal(result.ok, false);
  assert(result.error instanceof SessionEventPublisherError);
  assert.equal(result.error.category, PUBLISHER_FAILURE_CATEGORIES.VALIDATION);
  assert.equal(result.error.operation, "validate");
});

test("reports subscriber delivery failures as non-authoritative diagnostics", () => {
  const publisher = new InMemorySessionEventPublisher();
  const delivered = [];
  publisher.subscribe(BASE_EVENT.sessionId, () => {
    throw new Error("subscriber unavailable");
  }, { replay: false });
  publisher.subscribe(BASE_EVENT.sessionId, (event) => delivered.push(event.eventId), { replay: false });

  const result = publisher.tryPublish(BASE_EVENT);

  assert.equal(result.ok, true);
  assert.equal(result.deliveryFailures.length, 1);
  assert.deepEqual(publisher.list(BASE_EVENT.sessionId).map((event) => event.eventId), ["evt_001"]);
  assert.deepEqual(delivered, ["evt_001"]);
});

test("does not throw from publish when best-effort subscriber delivery fails", () => {
  const publisher = new InMemorySessionEventPublisher();
  publisher.subscribe(BASE_EVENT.sessionId, () => {
    throw new Error("closed stream");
  }, { replay: false });

  assert.doesNotThrow(() => publisher.publish(BASE_EVENT));
  assert.deepEqual(publisher.list(BASE_EVENT.sessionId).map((event) => event.eventId), ["evt_001"]);
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
  const sequenceGap = detectSequenceGap(1, { ...BASE_EVENT, sequence: 3 });

  assert.deepEqual(sequenceGap, {
    hasGap: true,
    expectedSequence: 2,
    actualSequence: 3
  });

  assert.deepEqual(
    replayStatusForLastEventId([{ ...BASE_EVENT, eventId: "evt_002" }], "evt_001"),
    { status: "REPLAY_UNAVAILABLE", events: [] }
  );

  assert.deepEqual(reconnectRecoveryGuidance({ replayStatus: "PARTIAL_REPLAY", sequenceGap }), {
    shouldRefreshDurableState: true,
    reasonCode: "SEQUENCE_GAP",
    messageCode: "REFRESH_SESSION_STATE"
  });
  assert.deepEqual(reconnectRecoveryGuidance({ replayStatus: "REPLAY_UNAVAILABLE" }), {
    shouldRefreshDurableState: true,
    reasonCode: "REPLAY_UNAVAILABLE",
    messageCode: "REFRESH_SESSION_STATE"
  });
  assert.deepEqual(
    reconnectRecoveryGuidance({
      replayStatus: replayStatusForLastEventId([{ ...BASE_EVENT, eventId: "evt_002" }], "evt_001")
    }),
    {
      shouldRefreshDurableState: true,
      reasonCode: "REPLAY_UNAVAILABLE",
      messageCode: "REFRESH_SESSION_STATE"
    }
  );
  assert.deepEqual(reconnectRecoveryGuidance({ replayStatus: "PARTIAL_REPLAY" }), {
    shouldRefreshDurableState: false,
    reasonCode: "STREAM_CONTINUITY_OK",
    messageCode: "CONTINUE_STREAM"
  });
});

test("creates metadata-only stream lifecycle log records", () => {
  assert.deepEqual(
    createStreamLogRecord(STREAM_LOG_OPERATIONS.REPLAY_MISS, {
      tenantId: "tenant_001",
      userId: "user_001",
      sessionId: "session_001",
      requestId: "req_001",
      correlationId: "corr_001",
      route: "/sessions/session_001/events",
      lastEventId: "evt_001",
      replayStatus: "REPLAY_UNAVAILABLE",
      ignoredField: "not logged"
    }, { now: () => "2026-05-29T00:00:00.000Z" }),
    {
      timestamp: "2026-05-29T00:00:00.000Z",
      service: "ai-assist-session-events-service",
      operation: STREAM_LOG_OPERATIONS.REPLAY_MISS,
      tenantId: "tenant_001",
      userId: "user_001",
      sessionId: "session_001",
      requestId: "req_001",
      correlationId: "corr_001",
      route: "/sessions/session_001/events",
      lastEventId: "evt_001",
      replayStatus: "REPLAY_UNAVAILABLE"
    }
  );
});

test("rejects sensitive stream log metadata keys at any depth", () => {
  assert.throws(
    () => createStreamLogRecord(STREAM_LOG_OPERATIONS.ERROR, {
      tenantId: "tenant_001",
      errorCode: "STREAM_FAILED",
      details: { oauthToken: "secret" }
    }),
    /oauthToken/u
  );
});
