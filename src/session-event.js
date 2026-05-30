import { SessionEventValidationError } from "./errors.js";

export const SESSION_EVENT_TYPES = Object.freeze([
  "assistant.delta",
  "assistant.final",
  "progress",
  "error",
  "action.proposed",
  "action.status_changed"
]);

export const ERROR_CATEGORIES = Object.freeze([
  "AUTHENTICATION",
  "AUTHORIZATION",
  "RATE_LIMITED",
  "VALIDATION",
  "CONSENT_REQUIRED",
  "CONFLICT",
  "DEPENDENCY",
  "PROVIDER_QUOTA",
  "KMS",
  "OAUTH",
  "CONNECTOR",
  "POLICY",
  "INTERNAL"
]);

const REQUIRED_STRING_FIELDS = Object.freeze([
  "eventId",
  "tenantId",
  "userId",
  "sessionId",
  "requestId",
  "correlationId",
  "type",
  "createdAt"
]);

const FORBIDDEN_PAYLOAD_KEYS = Object.freeze([
  "providerKey",
  "apiKey",
  "oauthToken",
  "accessToken",
  "refreshToken",
  "decryptedPayload",
  "sessionSecret"
]);

const MAX_PAYLOAD_BYTES = 64 * 1024;

export function validateSessionEvent(event) {
  const issues = [];

  if (!isPlainObject(event)) {
    return {
      valid: false,
      issues: [{ path: "$", code: "invalid_type", message: "event must be an object" }]
    };
  }

  for (const field of REQUIRED_STRING_FIELDS) {
    if (!isNonBlankString(event[field])) {
      issues.push({ path: field, code: "required_string", message: `${field} must be a non-empty string` });
    }
  }
  if (isNonBlankString(event.eventId) && !isSseFieldValue(event.eventId)) {
    issues.push({ path: "eventId", code: "invalid_event_id", message: "eventId must not contain line breaks" });
  }

  if (!SESSION_EVENT_TYPES.includes(event.type)) {
    issues.push({ path: "type", code: "invalid_enum", message: "type is not supported" });
  }

  if (event.sequence !== undefined && (!Number.isSafeInteger(event.sequence) || event.sequence < 0)) {
    issues.push({ path: "sequence", code: "invalid_sequence", message: "sequence must be a non-negative safe integer" });
  }

  if (isNonBlankString(event.createdAt) && Number.isNaN(Date.parse(event.createdAt))) {
    issues.push({ path: "createdAt", code: "invalid_timestamp", message: "createdAt must be an ISO-compatible timestamp" });
  }

  if (!isPlainObject(event.payload)) {
    issues.push({ path: "payload", code: "invalid_payload", message: "payload must be an object" });
  } else {
    const payloadSize = Buffer.byteLength(JSON.stringify(event.payload), "utf8");
    if (payloadSize > MAX_PAYLOAD_BYTES) {
      issues.push({ path: "payload", code: "payload_too_large", message: "payload exceeds the maximum size" });
    }
    issues.push(...findForbiddenPayloadKeys(event.payload));
    issues.push(...validatePayload(event.type, event.payload));
  }

  return { valid: issues.length === 0, issues };
}

export function assertValidSessionEvent(event) {
  const result = validateSessionEvent(event);
  if (!result.valid) {
    throw new SessionEventValidationError(result.issues);
  }
  return event;
}

export function createSessionEvent(input, { now = () => new Date().toISOString() } = {}) {
  const event = {
    ...input,
    createdAt: input.createdAt ?? now(),
    payload: input.payload ?? {}
  };
  return assertValidSessionEvent(event);
}

function validatePayload(type, payload) {
  switch (type) {
    case "assistant.delta":
      return requireFields(payload, [
        ["messageId", isNonBlankString],
        ["delta", isString],
        ["index", isNonNegativeInteger]
      ]);
    case "assistant.final":
      return requireFields(payload, [
        ["messageId", isNonBlankString],
        ["finishReason", isNonBlankString]
      ]).concat(payload.usage === undefined || isPlainObject(payload.usage)
        ? []
        : [{ path: "payload.usage", code: "invalid_type", message: "usage must be an object when present" }]);
    case "progress":
      return requireFields(payload, [
        ["stage", isNonBlankString],
        ["status", isNonBlankString],
        ["messageCode", isNonBlankString]
      ]);
    case "error":
      return requireFields(payload, [
        ["errorCode", isNonBlankString],
        ["category", (value) => ERROR_CATEGORIES.includes(value)],
        ["retryable", (value) => typeof value === "boolean"],
        ["message", isNonBlankString]
      ]);
    case "action.proposed":
      return requireFields(payload, [
        ["actionId", isNonBlankString],
        ["actionType", isNonBlankString],
        ["resourceRef", isPlainObject],
        ["summary", isNonBlankString],
        ["expiresAt", isIsoDateString]
      ]);
    case "action.status_changed":
      return requireFields(payload, [
        ["actionId", isNonBlankString],
        ["previousStatus", isNonBlankString],
        ["status", isNonBlankString],
        ["reasonCode", isNonBlankString]
      ]);
    default:
      return [];
  }
}

function requireFields(payload, rules) {
  return rules.flatMap(([field, predicate]) => predicate(payload[field])
    ? []
    : [{ path: `payload.${field}`, code: "invalid_field", message: `${field} is invalid or missing` }]);
}

function findForbiddenPayloadKeys(value, path = "payload") {
  if (!isPlainObject(value) && !Array.isArray(value)) {
    return [];
  }
  const entries = Array.isArray(value) ? value.entries() : Object.entries(value);
  const issues = [];
  for (const [key, child] of entries) {
    const keyPath = `${path}.${key}`;
    if (FORBIDDEN_PAYLOAD_KEYS.includes(String(key))) {
      issues.push({ path: keyPath, code: "forbidden_payload_key", message: "payload contains a forbidden sensitive key" });
    }
    issues.push(...findForbiddenPayloadKeys(child, keyPath));
  }
  return issues;
}

function isString(value) {
  return typeof value === "string";
}

function isNonBlankString(value) {
  return typeof value === "string" && value.trim().length > 0;
}

function isSseFieldValue(value) {
  return !/[\r\n]/u.test(value);
}

function isNonNegativeInteger(value) {
  return Number.isSafeInteger(value) && value >= 0;
}

function isIsoDateString(value) {
  return isNonBlankString(value) && !Number.isNaN(Date.parse(value));
}

function isPlainObject(value) {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}
