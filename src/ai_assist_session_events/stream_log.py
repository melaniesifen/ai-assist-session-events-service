from __future__ import annotations

from datetime import datetime
from typing import Any, Callable

STREAM_LOG_OPERATIONS = {
    "OPEN": "stream.open",
    "CLOSE": "stream.close",
    "ERROR": "stream.error",
    "REPLAY_MISS": "stream.replay_miss",
    "SEQUENCE_GAP": "stream.sequence_gap",
}

SERVICE_NAME = "ai-assist-session-events-service"

ALLOWED_LOG_FIELDS = (
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
    "disconnectReason",
)

FORBIDDEN_LOG_KEYS = frozenset(
    (
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
        "bearerToken",
    )
)


def create_stream_log_record(operation: str, metadata=None, *, now: Callable[[], str] | None = None) -> dict[str, Any]:
    if operation not in STREAM_LOG_OPERATIONS.values():
        raise TypeError("operation is not a supported stream log operation")
    if metadata is None:
        metadata = {}
    if not isinstance(metadata, dict):
        raise TypeError("metadata must be an object")

    _assert_no_forbidden_log_keys(metadata)
    clock = now or (lambda: datetime.now().astimezone().isoformat())
    timestamp = metadata.get("timestamp")
    record = {
        "timestamp": clock() if timestamp is None else timestamp,
        "service": SERVICE_NAME,
        "operation": operation,
    }

    for field in ALLOWED_LOG_FIELDS:
        if field in ("timestamp", "service", "operation"):
            continue
        if field in metadata:
            record[field] = metadata[field]

    return record


def _assert_no_forbidden_log_keys(value: Any, path: str = "metadata") -> None:
    if not isinstance(value, (dict, list)):
        return
    entries = enumerate(value) if isinstance(value, list) else value.items()
    for key, child in entries:
        if str(key) in FORBIDDEN_LOG_KEYS:
            raise TypeError(f"{path}.{key} is not allowed in stream log metadata")
        _assert_no_forbidden_log_keys(child, f"{path}.{key}")
