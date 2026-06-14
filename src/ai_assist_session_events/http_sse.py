from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable

from .publisher import InMemorySessionEventPublisher, Subscription
from .reconnect import reconnect_recovery_guidance
from .session_event import create_progress_event
from .sse import format_sse_event
from .stream_log import STREAM_LOG_OPERATIONS, create_stream_log_record

SSE_ROUTE_TEMPLATE = "/sessions/{sessionId}/events"
SSE_CONTENT_TYPE = "text/event-stream; charset=utf-8"
SSE_HEARTBEAT_FRAME = ": keepalive\n\n"
REFRESH_STAGE = "stream.replay"


@dataclass(frozen=True)
class SessionAuthorization:
    allowed: bool
    tenant_id: str | None = None
    user_id: str | None = None
    session_id: str | None = None
    request_id: str | None = None
    correlation_id: str | None = None
    status_code: int = 403
    error_category: str | None = None
    error_code: str | None = None
    log_metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class SseHttpResponse:
    status_code: int
    headers: dict[str, str]


@dataclass(frozen=True)
class SseStreamOpenResult:
    response: SseHttpResponse
    stream: "SseHttpStream | None"
    logs: tuple[dict[str, Any], ...]
    replay_status: str | None = None


class SseHttpStream:
    def __init__(
        self,
        *,
        authorization: SessionAuthorization,
        route: str,
        subscription: Subscription,
        chunks: list[str],
        now: Callable[[], str] | None = None,
    ):
        self._authorization = authorization
        self._route = route
        self._subscription = subscription
        self._chunks = chunks
        self._now = now
        self._closed = False
        self._close_log: dict[str, Any] | None = None

    def pop_pending(self) -> tuple[str, ...]:
        chunks = tuple(self._chunks)
        self._chunks.clear()
        return chunks

    def heartbeat(self) -> str:
        if self._closed:
            return ""
        self._chunks.append(SSE_HEARTBEAT_FRAME)
        return SSE_HEARTBEAT_FRAME

    def close(self, *, disconnect_reason: str = "client_disconnect", duration_ms: int | None = None) -> dict[str, Any]:
        if self._closed and self._close_log is not None:
            return self._close_log

        self._subscription.unsubscribe()
        metadata = _base_log_metadata(self._authorization, self._route)
        metadata.update({"statusCode": 200, "disconnectReason": disconnect_reason})
        if duration_ms is not None:
            metadata["durationMs"] = duration_ms
        self._close_log = create_stream_log_record(
            STREAM_LOG_OPERATIONS["CLOSE"],
            metadata,
            now=self._now,
        )
        self._closed = True
        return self._close_log


class SseHttpStreamAdapter:
    def __init__(
        self,
        *,
        publisher: InMemorySessionEventPublisher,
        authorize_session: Callable[[dict[str, Any]], SessionAuthorization],
        now: Callable[[], str] | None = None,
    ):
        if not callable(authorize_session):
            raise TypeError("authorize_session must be callable")
        self._publisher = publisher
        self._authorize_session = authorize_session
        self._now = now

    def open_stream(self, request: dict[str, Any]) -> SseStreamOpenResult:
        if not isinstance(request, dict):
            raise TypeError("request must be an object")

        authorization = self._authorize_session(request)
        if not isinstance(authorization, SessionAuthorization):
            raise TypeError("authorize_session must return SessionAuthorization")

        requested_session_id = _requested_session_id_from(request)
        route = _route_for(requested_session_id or authorization.session_id)
        if not authorization.allowed:
            log = create_stream_log_record(
                STREAM_LOG_OPERATIONS["ERROR"],
                {
                    **authorization.log_metadata,
                    **_base_log_metadata(authorization, route),
                    "statusCode": authorization.status_code,
                    "errorCategory": authorization.error_category or "AUTHORIZATION",
                    "errorCode": authorization.error_code or "STREAM_AUTHORIZATION_DENIED",
                },
                now=self._now,
            )
            return SseStreamOpenResult(
                response=SseHttpResponse(
                    status_code=authorization.status_code,
                    headers={"Cache-Control": "no-store"},
                ),
                stream=None,
                logs=(log,),
            )

        _assert_authorized_session_complete(authorization)
        route = _route_for(authorization.session_id)

        if requested_session_id is not None and requested_session_id != authorization.session_id:
            log = create_stream_log_record(
                STREAM_LOG_OPERATIONS["ERROR"],
                {
                    **authorization.log_metadata,
                    **_base_log_metadata(authorization, route),
                    "statusCode": 403,
                    "errorCategory": "AUTHORIZATION",
                    "errorCode": "SESSION_AUTHORIZATION_MISMATCH",
                },
                now=self._now,
            )
            return SseStreamOpenResult(
                response=SseHttpResponse(
                    status_code=403,
                    headers={"Cache-Control": "no-store"},
                ),
                stream=None,
                logs=(log,),
            )

        chunks: list[str] = []

        def handle_event(event: dict[str, Any]) -> None:
            chunks.append(format_sse_event(event))

        last_event_id = _last_event_id_from(request)
        subscription = self._publisher.subscribe(
            authorization.session_id,
            handle_event,
            last_event_id=last_event_id,
            replay=True,
        )

        logs = [
            create_stream_log_record(
                STREAM_LOG_OPERATIONS["OPEN"],
                {
                    **authorization.log_metadata,
                    **_base_log_metadata(authorization, route),
                    "statusCode": 200,
                    "lastEventId": last_event_id,
                    "replayStatus": subscription.replay_status,
                },
                now=self._now,
            )
        ]

        if subscription.replay_status == "REPLAY_UNAVAILABLE":
            logs.append(
                create_stream_log_record(
                    STREAM_LOG_OPERATIONS["REPLAY_MISS"],
                    {
                        **authorization.log_metadata,
                        **_base_log_metadata(authorization, route),
                        "statusCode": 200,
                        "lastEventId": last_event_id,
                        "replayStatus": subscription.replay_status,
                    },
                    now=self._now,
                )
            )
            chunks.append(format_sse_event(_create_refresh_guidance_event(authorization, now=self._now)))

        stream = SseHttpStream(
            authorization=authorization,
            route=route,
            subscription=subscription,
            chunks=chunks,
            now=self._now,
        )
        return SseStreamOpenResult(
            response=SseHttpResponse(status_code=200, headers=create_sse_response_headers()),
            stream=stream,
            logs=tuple(logs),
            replay_status=subscription.replay_status,
        )


def create_sse_response_headers() -> dict[str, str]:
    return {
        "Content-Type": SSE_CONTENT_TYPE,
        "Cache-Control": "no-cache, no-transform",
        "Connection": "keep-alive",
        "X-Accel-Buffering": "no",
    }


def _assert_authorized_session_complete(authorization: SessionAuthorization) -> None:
    missing = [
        field_name
        for field_name in (
            "tenant_id",
            "user_id",
            "session_id",
            "request_id",
            "correlation_id",
        )
        if not getattr(authorization, field_name)
    ]
    if missing:
        raise TypeError(f"allowed session authorization is missing: {', '.join(missing)}")


def _requested_session_id_from(request: dict[str, Any]) -> str | None:
    session_id = request.get("sessionId")
    if session_id is None:
        return None
    if not isinstance(session_id, str) or not session_id.strip():
        raise TypeError("request sessionId must be a non-empty string when present")
    return session_id


def _last_event_id_from(request: dict[str, Any]) -> str | None:
    headers = request.get("headers") or {}
    if not isinstance(headers, dict):
        raise TypeError("request headers must be an object")
    for key, value in headers.items():
        if str(key).lower() == "last-event-id":
            if value is None or value == "":
                return None
            if not isinstance(value, str):
                raise TypeError("Last-Event-ID header must be a string when present")
            return value
    return None


def _route_for(session_id: str | None) -> str:
    return SSE_ROUTE_TEMPLATE.format(sessionId=session_id or "unknown")


def _base_log_metadata(authorization: SessionAuthorization, route: str) -> dict[str, Any]:
    metadata: dict[str, Any] = {"route": route}
    if authorization.tenant_id is not None:
        metadata["tenantId"] = authorization.tenant_id
    if authorization.user_id is not None:
        metadata["userId"] = authorization.user_id
    if authorization.session_id is not None:
        metadata["sessionId"] = authorization.session_id
    if authorization.request_id is not None:
        metadata["requestId"] = authorization.request_id
    if authorization.correlation_id is not None:
        metadata["correlationId"] = authorization.correlation_id
    return metadata


def _create_refresh_guidance_event(
    authorization: SessionAuthorization,
    *,
    now: Callable[[], str] | None = None,
) -> dict[str, Any]:
    guidance = reconnect_recovery_guidance(replay_status="REPLAY_UNAVAILABLE")
    return create_progress_event(
        {
            "eventId": f"evt_stream_refresh_required_{authorization.request_id}",
            "tenantId": authorization.tenant_id,
            "userId": authorization.user_id,
            "sessionId": authorization.session_id,
            "requestId": authorization.request_id,
            "correlationId": authorization.correlation_id,
        },
        stage=REFRESH_STAGE,
        status="skipped",
        message_code=guidance["messageCode"],
        now=now,
    )
