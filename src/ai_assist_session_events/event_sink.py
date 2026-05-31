from __future__ import annotations

from typing import Any

from .errors import SessionEventValidationError
from .reconnect import replay_status_for_last_event_id
from .session_event import MAX_SAFE_INTEGER, assert_valid_session_event

DEFAULT_BUFFER_SIZE = 100


class InMemorySessionEventSink:
    def __init__(self, *, max_events_per_session=DEFAULT_BUFFER_SIZE):
        if (
            not isinstance(max_events_per_session, int)
            or isinstance(max_events_per_session, bool)
            or max_events_per_session < 1
            or max_events_per_session > MAX_SAFE_INTEGER
        ):
            raise TypeError("max_events_per_session must be a positive safe integer")
        self._max_events_per_session = max_events_per_session
        self._events_by_session: dict[str, list[dict[str, Any]]] = {}

    def append(self, event: dict[str, Any]) -> dict[str, Any]:
        assert_valid_session_event(event)
        session_events = self._events_by_session.get(event["sessionId"], [])
        if any(existing["eventId"] == event["eventId"] for existing in session_events):
            raise SessionEventValidationError(
                [{"path": "eventId", "code": "duplicate_event_id", "message": "eventId already exists for this session"}]
            )
        self._events_by_session[event["sessionId"]] = (session_events + [event])[-self._max_events_per_session :]
        return event

    def list(self, session_id: str) -> list[dict[str, Any]]:
        return list(self._events_by_session.get(session_id, []))

    def list_after(self, session_id: str, last_event_id=None) -> list[dict[str, Any]]:
        return self.replay_after(session_id, last_event_id)["events"]

    def replay_after(self, session_id: str, last_event_id=None) -> dict[str, Any]:
        return replay_status_for_last_event_id(self.list(session_id), last_event_id)

    def has_event(self, session_id: str, event_id: str) -> bool:
        return any(event["eventId"] == event_id for event in self.list(session_id))
