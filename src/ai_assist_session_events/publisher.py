from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from .errors import SessionEventPublisherError
from .event_sink import InMemorySessionEventSink
from .session_event import assert_valid_session_event

PUBLISHER_FAILURE_CATEGORIES = {
    "VALIDATION": "VALIDATION",
    "PERSISTENCE": "PERSISTENCE",
}


@dataclass(frozen=True)
class PublisherResult:
    ok: bool
    event: dict[str, Any] | None = None
    error: SessionEventPublisherError | None = None
    delivery_failures: tuple[BaseException, ...] = ()


@dataclass(frozen=True)
class Subscription:
    replay_status: str
    replayed: list[dict[str, Any]]
    unsubscribe: Callable[[], None]


class InMemorySessionEventPublisher:
    def __init__(self, *, sink=None):
        self._sink = sink or InMemorySessionEventSink()
        self._subscribers_by_session: dict[str, set[Callable[[dict[str, Any]], None]]] = {}

    def publish(self, event: dict[str, Any]) -> dict[str, Any]:
        result = self.try_publish(event)
        if not result.ok:
            raise result.error
        return result.event

    def try_publish(self, event: dict[str, Any]) -> PublisherResult:
        try:
            assert_valid_session_event(event)
        except Exception as error:
            return PublisherResult(
                ok=False,
                error=SessionEventPublisherError(
                    category=PUBLISHER_FAILURE_CATEGORIES["VALIDATION"],
                    operation="validate",
                    cause=error,
                ),
            )

        try:
            self._sink.append(event)
        except Exception as error:
            return PublisherResult(
                ok=False,
                error=SessionEventPublisherError(
                    category=PUBLISHER_FAILURE_CATEGORIES["PERSISTENCE"],
                    operation="append",
                    cause=error,
                ),
            )

        delivery_failures = []
        for subscriber in tuple(self._subscribers_by_session.get(event["sessionId"], ())):
            try:
                subscriber(event)
            except Exception as error:
                delivery_failures.append(error)

        return PublisherResult(ok=True, event=event, delivery_failures=tuple(delivery_failures))

    def subscribe(self, session_id: str, handler, *, last_event_id=None, replay=True) -> Subscription:
        if not callable(handler):
            raise TypeError("handler must be a function")

        subscribers = self._subscribers_by_session.setdefault(session_id, set())
        subscribers.add(handler)

        replay_result = (
            self._sink.replay_after(session_id, last_event_id)
            if replay
            else {"status": "REPLAY_DISABLED", "events": []}
        )
        for event in replay_result["events"]:
            handler(event)

        def unsubscribe() -> None:
            current = self._subscribers_by_session.get(session_id)
            if not current:
                return
            current.discard(handler)
            if not current:
                del self._subscribers_by_session[session_id]

        return Subscription(
            replay_status=replay_result["status"],
            replayed=list(replay_result["events"]),
            unsubscribe=unsubscribe,
        )

    def list(self, session_id: str) -> list[dict[str, Any]]:
        return self._sink.list(session_id)
