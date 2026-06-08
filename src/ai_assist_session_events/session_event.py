from __future__ import annotations

from datetime import datetime
import json
from typing import Any, Callable

from .errors import SessionEventValidationError

SESSION_EVENT_TYPES = (
    "assistant.delta",
    "assistant.final",
    "progress",
    "error",
    "action.proposed",
    "action.status_changed",
)

ERROR_CATEGORIES = (
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
    "INTERNAL",
)

PROGRESS_STATUSES = (
    "started",
    "in_progress",
    "completed",
    "skipped",
)

CONNECTORS = ("google_docs",)

PROPOSED_ACTION_TYPES = (
    "REPLACE_TEXT",
    "INSERT_TEXT",
)

PROPOSED_ACTION_STATUSES = (
    "PROPOSED",
    "APPROVED",
    "APPLIED",
    "REJECTED",
    "EXPIRED",
    "CONFLICTED",
    "FAILED",
)

REQUIRED_STRING_FIELDS = (
    "eventId",
    "tenantId",
    "userId",
    "sessionId",
    "requestId",
    "correlationId",
    "type",
    "createdAt",
)

FORBIDDEN_PAYLOAD_KEYS = frozenset(
    (
        "providerKey",
        "apiKey",
        "oauthToken",
        "accessToken",
        "refreshToken",
        "decryptedPayload",
        "decryptedActionPayload",
        "decryptedSessionSecret",
        "sessionSecret",
        "prompt",
        "rawPrompt",
        "documentText",
        "selectedText",
        "modelResponse",
        "screenshot",
        "ocrText",
        "accessibilityTree",
        "authorization",
        "authorizationHeader",
        "bearerToken",
        "actionPayload",
        "cookie",
    )
)

MAX_PAYLOAD_BYTES = 64 * 1024
MAX_SAFE_INTEGER = (2**53) - 1


def create_progress_event(
    envelope: dict[str, Any],
    *,
    stage: str,
    status: str,
    message_code: str,
    now: Callable[[], str] | None = None,
) -> dict[str, Any]:
    return _create_typed_session_event(
        "progress",
        envelope,
        {"stage": stage, "status": status, "messageCode": message_code},
        now=now,
    )


def create_assistant_delta_event(
    envelope: dict[str, Any],
    *,
    message_id: str,
    delta: str,
    index: int,
    now: Callable[[], str] | None = None,
) -> dict[str, Any]:
    return _create_typed_session_event(
        "assistant.delta",
        envelope,
        {"messageId": message_id, "delta": delta, "index": index},
        now=now,
    )


def create_assistant_final_event(
    envelope: dict[str, Any],
    *,
    message_id: str,
    finish_reason: str,
    usage: dict[str, Any] | None = None,
    now: Callable[[], str] | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {"messageId": message_id, "finishReason": finish_reason}
    if usage is not None:
        payload["usage"] = usage
    return _create_typed_session_event("assistant.final", envelope, payload, now=now)


def create_safe_error_event(
    envelope: dict[str, Any],
    *,
    error_code: str,
    category: str,
    retryable: bool,
    message: str,
    metadata: dict[str, Any] | None = None,
    now: Callable[[], str] | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "errorCode": error_code,
        "category": category,
        "retryable": retryable,
        "message": message,
    }
    if metadata is not None:
        payload["metadata"] = metadata
    return _create_typed_session_event(
        "error",
        envelope,
        payload,
        now=now,
    )


def create_action_proposed_event(
    envelope: dict[str, Any],
    *,
    action_id: str,
    action_type: str,
    resource_ref: dict[str, Any],
    summary: str,
    expires_at: str,
    now: Callable[[], str] | None = None,
) -> dict[str, Any]:
    return _create_typed_session_event(
        "action.proposed",
        envelope,
        {
            "actionId": action_id,
            "actionType": action_type,
            "resourceRef": resource_ref,
            "summary": summary,
            "expiresAt": expires_at,
        },
        now=now,
    )


def create_action_status_changed_event(
    envelope: dict[str, Any],
    *,
    action_id: str,
    previous_status: str,
    status: str,
    reason_code: str,
    now: Callable[[], str] | None = None,
) -> dict[str, Any]:
    return _create_typed_session_event(
        "action.status_changed",
        envelope,
        {
            "actionId": action_id,
            "previousStatus": previous_status,
            "status": status,
            "reasonCode": reason_code,
        },
        now=now,
    )


def validate_session_event(event: Any) -> dict[str, Any]:
    issues: list[dict[str, str]] = []

    if not _is_plain_object(event):
        return {
            "valid": False,
            "issues": [{"path": "$", "code": "invalid_type", "message": "event must be an object"}],
        }

    for field in REQUIRED_STRING_FIELDS:
        if not _is_non_blank_string(event.get(field)):
            issues.append(
                {
                    "path": field,
                    "code": "required_string",
                    "message": f"{field} must be a non-empty string",
                }
            )

    if _is_non_blank_string(event.get("eventId")) and not _is_sse_field_value(event["eventId"]):
        issues.append(
            {
                "path": "eventId",
                "code": "invalid_event_id",
                "message": "eventId must not contain line breaks",
            }
        )

    if event.get("type") not in SESSION_EVENT_TYPES:
        issues.append({"path": "type", "code": "invalid_enum", "message": "type is not supported"})

    sequence = event.get("sequence")
    if sequence is not None and not _is_non_negative_integer(sequence):
        issues.append(
            {
                "path": "sequence",
                "code": "invalid_sequence",
                "message": "sequence must be a non-negative safe integer",
            }
        )

    created_at = event.get("createdAt")
    if _is_non_blank_string(created_at) and not _is_iso_date_string(created_at):
        issues.append(
            {
                "path": "createdAt",
                "code": "invalid_timestamp",
                "message": "createdAt must be an ISO-compatible timestamp",
            }
        )

    payload = event.get("payload")
    if not _is_plain_object(payload):
        issues.append({"path": "payload", "code": "invalid_payload", "message": "payload must be an object"})
    else:
        payload_size = len(json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8"))
        if payload_size > MAX_PAYLOAD_BYTES:
            issues.append({"path": "payload", "code": "payload_too_large", "message": "payload exceeds the maximum size"})
        issues.extend(_find_forbidden_payload_keys(payload))
        issues.extend(_validate_payload(event.get("type"), payload))

    return {"valid": not issues, "issues": issues}


def assert_valid_session_event(event: dict[str, Any]) -> dict[str, Any]:
    result = validate_session_event(event)
    if not result["valid"]:
        raise SessionEventValidationError(result["issues"])
    return event


def create_session_event(input_event: dict[str, Any], *, now: Callable[[], str] | None = None) -> dict[str, Any]:
    clock = now or (lambda: datetime.now().astimezone().isoformat())
    event = {**input_event}
    if event.get("createdAt") is None:
        event["createdAt"] = clock()
    if event.get("payload") is None:
        event["payload"] = {}
    return assert_valid_session_event(event)


def _create_typed_session_event(
    event_type: str,
    envelope: dict[str, Any],
    payload: dict[str, Any],
    *,
    now: Callable[[], str] | None = None,
) -> dict[str, Any]:
    return create_session_event({**envelope, "type": event_type, "payload": payload}, now=now)


def _validate_payload(event_type: str | None, payload: dict[str, Any]) -> list[dict[str, str]]:
    if event_type == "assistant.delta":
        return _require_fields(
            payload,
            (
                ("messageId", _is_non_blank_string),
                ("delta", lambda value: isinstance(value, str)),
                ("index", _is_non_negative_integer),
            ),
        )
    if event_type == "assistant.final":
        issues = _require_fields(
            payload,
            (
                ("messageId", _is_non_blank_string),
                ("finishReason", _is_non_blank_string),
            ),
        )
        if "usage" in payload and not _is_plain_object(payload["usage"]):
            issues.append({"path": "payload.usage", "code": "invalid_type", "message": "usage must be an object when present"})
        return issues
    if event_type == "progress":
        return _require_fields(
            payload,
            (
                ("stage", _is_non_blank_string),
                ("status", lambda value: value in PROGRESS_STATUSES),
                ("messageCode", _is_non_blank_string),
            ),
        )
    if event_type == "error":
        return _require_fields(
            payload,
            (
                ("errorCode", _is_non_blank_string),
                ("category", lambda value: value in ERROR_CATEGORIES),
                ("retryable", lambda value: isinstance(value, bool)),
                ("message", _is_non_blank_string),
            ),
        )
    if event_type == "action.proposed":
        issues = _require_fields(
            payload,
            (
                ("actionId", _is_non_blank_string),
                ("actionType", lambda value: value in PROPOSED_ACTION_TYPES),
                ("resourceRef", _is_plain_object),
                ("summary", _is_non_blank_string),
                ("expiresAt", _is_iso_date_string),
            ),
        )
        if _is_plain_object(payload.get("resourceRef")):
            issues.extend(_validate_resource_ref(payload["resourceRef"]))
        return issues
    if event_type == "action.status_changed":
        return _require_fields(
            payload,
            (
                ("actionId", _is_non_blank_string),
                ("previousStatus", lambda value: value in PROPOSED_ACTION_STATUSES),
                ("status", lambda value: value in PROPOSED_ACTION_STATUSES),
                ("reasonCode", _is_non_blank_string),
            ),
        )
    return []


def _validate_resource_ref(resource_ref: dict[str, Any]) -> list[dict[str, str]]:
    issues = _require_fields_with_prefix(
        resource_ref,
        (
            ("connector", lambda value: value in CONNECTORS),
            ("resourceId", _is_non_blank_string),
            ("resourceType", _is_non_blank_string),
        ),
        path_prefix="payload.resourceRef",
    )
    for optional_field in ("displayName", "externalUrl"):
        if optional_field in resource_ref and not _is_non_blank_string(resource_ref[optional_field]):
            issues.append(
                {
                    "path": f"payload.resourceRef.{optional_field}",
                    "code": "invalid_field",
                    "message": f"{optional_field} is invalid",
                }
            )
    return issues


def _require_fields(payload: dict[str, Any], rules: tuple[tuple[str, Callable[[Any], bool]], ...]) -> list[dict[str, str]]:
    return _require_fields_with_prefix(payload, rules, path_prefix="payload")


def _require_fields_with_prefix(
    payload: dict[str, Any],
    rules: tuple[tuple[str, Callable[[Any], bool]], ...],
    *,
    path_prefix: str,
) -> list[dict[str, str]]:
    issues = []
    for field, predicate in rules:
        if not predicate(payload.get(field)):
            issues.append({"path": f"{path_prefix}.{field}", "code": "invalid_field", "message": f"{field} is invalid or missing"})
    return issues


def _find_forbidden_payload_keys(value: Any, path: str = "payload") -> list[dict[str, str]]:
    if not _is_plain_object(value) and not isinstance(value, list):
        return []

    entries = enumerate(value) if isinstance(value, list) else value.items()
    issues = []
    for key, child in entries:
        key_path = f"{path}.{key}"
        if str(key) in FORBIDDEN_PAYLOAD_KEYS:
            issues.append(
                {
                    "path": key_path,
                    "code": "forbidden_payload_key",
                    "message": "payload contains a forbidden sensitive key",
                }
            )
        issues.extend(_find_forbidden_payload_keys(child, key_path))
    return issues


def _is_non_blank_string(value: Any) -> bool:
    return isinstance(value, str) and len(value.strip()) > 0


def _is_sse_field_value(value: str) -> bool:
    return "\n" not in value and "\r" not in value


def _is_non_negative_integer(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and 0 <= value <= MAX_SAFE_INTEGER


def _is_iso_date_string(value: Any) -> bool:
    if not _is_non_blank_string(value):
        return False
    candidate = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        datetime.fromisoformat(candidate)
    except ValueError:
        return False
    return True


def _is_plain_object(value: Any) -> bool:
    return isinstance(value, dict)
