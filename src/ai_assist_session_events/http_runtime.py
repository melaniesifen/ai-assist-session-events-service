from __future__ import annotations

import json
import re
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable
from uuid import uuid4

from .http_sse import (
    SSE_ROUTE_TEMPLATE,
    SessionAuthorization,
    SseHttpResponse,
    SseHttpStream,
    SseHttpStreamAdapter,
    SseStreamOpenResult,
)
from .publisher import InMemorySessionEventPublisher

HEADER_TENANT_ID = "x-ai-assist-tenant-id"
HEADER_USER_ID = "x-ai-assist-user-id"
HEADER_REQUEST_ID = "x-request-id"
HEADER_CORRELATION_ID = "x-correlation-id"
SSE_ROUTE_PATTERN = re.compile(r"^/sessions/(?P<session_id>[^/]+)/events$")


class SessionEventsHttpRuntime:
    """Stdlib HTTP runtime for the canonical authenticated SSE route."""

    def __init__(
        self,
        *,
        publisher: InMemorySessionEventPublisher,
        authorize_session: Callable[[dict[str, Any]], SessionAuthorization] | None = None,
        request_id_generator: Callable[[], str] | None = None,
        correlation_id_generator: Callable[[], str] | None = None,
        now: Callable[[], str] | None = None,
        record_log: Callable[[dict[str, Any]], None] | None = None,
    ) -> None:
        self.request_id_generator = request_id_generator or (lambda: f"req_{uuid4().hex}")
        self.correlation_id_generator = correlation_id_generator or (lambda: f"corr_{uuid4().hex}")
        self.record_log = record_log or (lambda _record: None)
        self.adapter = SseHttpStreamAdapter(
            publisher=publisher,
            authorize_session=authorize_session or self._trusted_header_authorization,
            now=now,
        )

    def open_stream(self, request: dict[str, Any]) -> SseStreamOpenResult:
        routed = route_stream_request(request)
        result = self.adapter.open_stream(
            {
                "sessionId": routed["sessionId"],
                "headers": routed["headers"],
                "method": routed["method"],
                "path": routed["path"],
            }
        )
        for record in result.logs:
            self.record_log(record)
        return result

    def _trusted_header_authorization(self, request: dict[str, Any]) -> SessionAuthorization:
        headers = normalize_headers(request.get("headers", {}))
        tenant_id = headers.get(HEADER_TENANT_ID)
        user_id = headers.get(HEADER_USER_ID)
        session_id = request.get("sessionId")
        request_id = headers.get(HEADER_REQUEST_ID) or self.request_id_generator()
        correlation_id = headers.get(HEADER_CORRELATION_ID) or self.correlation_id_generator()
        if not tenant_id or not tenant_id.strip() or not user_id or not user_id.strip():
            return SessionAuthorization(
                allowed=False,
                session_id=session_id,
                request_id=request_id,
                correlation_id=correlation_id,
                status_code=401,
                error_category="AUTHENTICATION",
                error_code="AUTH_CONTEXT_REQUIRED",
            )
        return SessionAuthorization(
            allowed=True,
            tenant_id=tenant_id,
            user_id=user_id,
            session_id=session_id,
            request_id=request_id,
            correlation_id=correlation_id,
        )


def route_stream_request(request: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(request, dict):
        raise TypeError("request must be an object")
    method = request.get("method")
    path = request.get("path")
    if not isinstance(method, str) or method.upper() != "GET":
        raise TypeError("SSE stream route requires GET")
    if not isinstance(path, str):
        raise TypeError("request path must be a string")
    match = SSE_ROUTE_PATTERN.match(path)
    if not match:
        raise TypeError(f"request path must match {SSE_ROUTE_TEMPLATE}")
    return {
        "method": method.upper(),
        "path": path,
        "sessionId": match.group("session_id"),
        "headers": normalize_headers(request.get("headers", {})),
    }


def normalize_headers(headers: Any) -> dict[str, str]:
    if headers is None:
        return {}
    if not isinstance(headers, dict):
        raise TypeError("request headers must be an object")
    normalized: dict[str, str] = {}
    for key, value in headers.items():
        if isinstance(key, str) and isinstance(value, str):
            normalized[key.lower()] = value
    return normalized


def create_http_handler(
    runtime: SessionEventsHttpRuntime,
    *,
    heartbeat_interval_seconds: float = 15.0,
    poll_interval_seconds: float = 0.25,
) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802 - stdlib handler method name
            try:
                result = runtime.open_stream(
                    {
                        "method": self.command,
                        "path": self.path.split("?", 1)[0],
                        "headers": dict(self.headers.items()),
                    }
                )
            except Exception:
                write_json_error(self, status_code=404, code="SSE_ROUTE_NOT_FOUND")
                return

            send_response_headers(self, result.response)
            if result.stream is None:
                return

            try:
                write_stream_loop(
                    self,
                    result.stream,
                    heartbeat_interval_seconds=heartbeat_interval_seconds,
                    poll_interval_seconds=poll_interval_seconds,
                )
            finally:
                runtime.record_log(result.stream.close(disconnect_reason="client_disconnect"))

        def log_message(self, _format: str, *_args: Any) -> None:
            return

    return Handler


def send_response_headers(handler: BaseHTTPRequestHandler, response: SseHttpResponse) -> None:
    handler.send_response(response.status_code)
    for key, value in response.headers.items():
        handler.send_header(key, value)
    handler.end_headers()


def write_stream_loop(
    handler: BaseHTTPRequestHandler,
    stream: SseHttpStream,
    *,
    heartbeat_interval_seconds: float,
    poll_interval_seconds: float,
) -> None:
    last_heartbeat = time.monotonic()
    while True:
        for chunk in stream.pop_pending():
            handler.wfile.write(chunk.encode("utf-8"))
            handler.wfile.flush()
        now = time.monotonic()
        if now - last_heartbeat >= heartbeat_interval_seconds:
            frame = stream.heartbeat()
            if frame:
                handler.wfile.write(frame.encode("utf-8"))
                handler.wfile.flush()
            last_heartbeat = now
        time.sleep(poll_interval_seconds)


def write_json_error(handler: BaseHTTPRequestHandler, *, status_code: int, code: str) -> None:
    body = json.dumps({"error": {"code": code}}, separators=(",", ":")).encode("utf-8")
    handler.send_response(status_code)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Cache-Control", "no-store")
    handler.send_header("Content-Length", str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)


def serve_http(
    runtime: SessionEventsHttpRuntime,
    *,
    host: str = "127.0.0.1",
    port: int = 8081,
    heartbeat_interval_seconds: float = 15.0,
) -> ThreadingHTTPServer:
    server = ThreadingHTTPServer(
        (host, port),
        create_http_handler(runtime, heartbeat_interval_seconds=heartbeat_interval_seconds),
    )
    server.serve_forever()
    return server
