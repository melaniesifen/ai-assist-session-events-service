export const STREAM_LOG_OPERATIONS = Object.freeze({
  OPEN: "stream.open",
  CLOSE: "stream.close",
  ERROR: "stream.error",
  REPLAY_MISS: "stream.replay_miss",
  SEQUENCE_GAP: "stream.sequence_gap"
});

const SERVICE_NAME = "ai-assist-session-events-service";

const ALLOWED_LOG_FIELDS = Object.freeze([
  "timestamp",
  "service",
  "tenantId",
  "userId",
  "sessionId",
  "requestId",
  "correlationId",
  "route",
  "operation",
  "statusCode",
  "durationMs",
  "errorCategory",
  "errorCode",
  "lastEventId",
  "expectedSequence",
  "actualSequence",
  "replayStatus",
  "disconnectReason"
]);

const FORBIDDEN_LOG_KEYS = Object.freeze([
  "prompt",
  "documentText",
  "selectedText",
  "modelResponse",
  "screenshot",
  "ocrText",
  "accessibilityTree",
  "providerKey",
  "apiKey",
  "oauthToken",
  "accessToken",
  "refreshToken",
  "decryptedPayload",
  "decryptedSessionSecret",
  "cookie",
  "authorization",
  "bearerToken"
]);

export function createStreamLogRecord(operation, metadata = {}, { now = () => new Date().toISOString() } = {}) {
  if (!Object.values(STREAM_LOG_OPERATIONS).includes(operation)) {
    throw new TypeError("operation is not a supported stream log operation");
  }
  if (!isPlainObject(metadata)) {
    throw new TypeError("metadata must be an object");
  }

  assertNoForbiddenLogKeys(metadata);

  const record = {
    timestamp: metadata.timestamp ?? now(),
    service: SERVICE_NAME,
    operation
  };

  for (const field of ALLOWED_LOG_FIELDS) {
    if (field === "timestamp" || field === "service" || field === "operation") {
      continue;
    }
    if (metadata[field] !== undefined) {
      record[field] = metadata[field];
    }
  }

  return record;
}

function assertNoForbiddenLogKeys(value, path = "metadata") {
  if (!isPlainObject(value) && !Array.isArray(value)) {
    return;
  }
  const entries = Array.isArray(value) ? value.entries() : Object.entries(value);
  for (const [key, child] of entries) {
    if (FORBIDDEN_LOG_KEYS.includes(String(key))) {
      throw new TypeError(`${path}.${key} is not allowed in stream log metadata`);
    }
    assertNoForbiddenLogKeys(child, `${path}.${key}`);
  }
}

function isPlainObject(value) {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}
