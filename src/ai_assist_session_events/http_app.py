from __future__ import annotations

import json
from typing import Any

from .errors import SessionEventPublisherError
from .http_runtime import SessionEventsHttpRuntime
from .publisher import InMemorySessionEventPublisher


_PUBLISHER = InMemorySessionEventPublisher()
_STREAM_LOGS: list[dict[str, Any]] = []
_RUNTIME = SessionEventsHttpRuntime(publisher=_PUBLISHER, record_log=_STREAM_LOGS.append)


def handle_http_request(
    *,
    method: str,
    path: str,
    headers: dict[str, str] | None = None,
    query_string: str = "",
    body: bytes | None = None,
) -> dict[str, Any]:
    del query_string
    if method.upper() == "POST" and path == "/internal/session-events":
        return _handle_internal_publish(body)

    try:
        result = _RUNTIME.open_stream(
            {
                "method": method,
                "path": path,
                "headers": headers or {},
            }
        )
    except Exception:
        return _json_response(404, "SSE_ROUTE_NOT_FOUND", "No session-events route matches this request.")

    response: dict[str, Any] = {
        "status": result.response.status_code,
        "headers": result.response.headers,
    }
    if result.stream is not None:
        response["stream"] = _RecordingStream(result.stream)
    return response


def publish_session_event(event: dict[str, Any]) -> dict[str, Any]:
    return _PUBLISHER.publish(event)


def stream_log_records() -> tuple[dict[str, Any], ...]:
    return tuple(_STREAM_LOGS)


def reset_runtime_for_tests() -> None:
    global _PUBLISHER, _RUNTIME
    _PUBLISHER = InMemorySessionEventPublisher()
    _STREAM_LOGS.clear()
    _RUNTIME = SessionEventsHttpRuntime(publisher=_PUBLISHER, record_log=_STREAM_LOGS.append)


class _RecordingStream:
    def __init__(self, stream: Any) -> None:
        self._stream = stream
        self._closed = False

    def pop_pending(self) -> tuple[str, ...]:
        return self._stream.pop_pending()

    def heartbeat(self) -> str:
        return self._stream.heartbeat()

    def close(self, *, disconnect_reason: str = "client_disconnect", duration_ms: int | None = None) -> dict[str, Any]:
        record = self._stream.close(disconnect_reason=disconnect_reason, duration_ms=duration_ms)
        if not self._closed:
            _STREAM_LOGS.append(record)
            self._closed = True
        return record


def _handle_internal_publish(body: bytes | None) -> dict[str, Any]:
    try:
        if not body:
            return _json_response(400, "SESSION_EVENT_BODY_REQUIRED", "Session event publish requires a JSON body.")
        event = json.loads(body.decode("utf-8"))
        publish_session_event(event)
        return _json_response(202, "SESSION_EVENT_ACCEPTED", "Session event accepted.")
    except (UnicodeDecodeError, json.JSONDecodeError):
        return _json_response(400, "SESSION_EVENT_JSON_INVALID", "Session event publish body must be valid JSON.")
    except SessionEventPublisherError as error:
        return _json_response(
            400,
            "SESSION_EVENT_REJECTED",
            "Session event failed validation or duplicate suppression.",
            details={
                "category": error.category,
                "operation": error.operation,
                "causeCode": getattr(error.cause, "code", type(error.cause).__name__),
            },
        )


def _json_response(status: int, code: str, message: str, *, details: dict[str, Any] | None = None) -> dict[str, Any]:
    error: dict[str, Any] = {
        "code": code,
        "category": "VALIDATION" if status < 500 else "DEPENDENCY",
        "message": message,
        "retryable": False,
    }
    if details:
        error["details"] = details
    return {
        "status": status,
        "headers": {"Content-Type": "application/json", "Cache-Control": "no-store"},
        "body": json.dumps({"error": error}, separators=(",", ":"), sort_keys=True).encode("utf-8"),
    }
