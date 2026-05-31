from __future__ import annotations

from collections import OrderedDict
from typing import Any

from .session_event import MAX_SAFE_INTEGER


class EventDeduplicator:
    def __init__(self, *, initial_event_ids=None, max_tracked_event_ids=1000):
        if (
            not isinstance(max_tracked_event_ids, int)
            or isinstance(max_tracked_event_ids, bool)
            or max_tracked_event_ids < 1
            or max_tracked_event_ids > MAX_SAFE_INTEGER
        ):
            raise TypeError("max_tracked_event_ids must be a positive safe integer")
        self._max_tracked_event_ids = max_tracked_event_ids
        self._seen: OrderedDict[str, None] = OrderedDict()
        for event_id in initial_event_ids or ():
            self._remember(event_id)

    def _remember(self, event_id: str) -> None:
        if event_id in self._seen:
            return
        self._seen[event_id] = None
        while len(self._seen) > self._max_tracked_event_ids:
            self._seen.popitem(last=False)

    def should_process(self, event: dict[str, Any]) -> bool:
        event_id = event["eventId"]
        if event_id in self._seen:
            return False
        self._remember(event_id)
        return True

    def has_seen(self, event_id: str) -> bool:
        return event_id in self._seen


def create_event_deduplicator(*, initial_event_ids=None, max_tracked_event_ids=1000) -> EventDeduplicator:
    return EventDeduplicator(initial_event_ids=initial_event_ids, max_tracked_event_ids=max_tracked_event_ids)


def detect_sequence_gap(previous_sequence, next_event: dict[str, Any]) -> dict[str, Any]:
    next_sequence = next_event.get("sequence")
    if previous_sequence is None or next_sequence is None:
        return {"hasGap": False}
    expected_sequence = previous_sequence + 1
    return {
        "hasGap": next_sequence != expected_sequence,
        "expectedSequence": expected_sequence,
        "actualSequence": next_sequence,
    }


def replay_status_for_last_event_id(events: list[dict[str, Any]], last_event_id=None) -> dict[str, Any]:
    if not last_event_id:
        return {"status": "FULL_REPLAY", "events": events}
    for index, event in enumerate(events):
        if event.get("eventId") == last_event_id:
            return {"status": "PARTIAL_REPLAY", "events": events[index + 1 :]}
    return {"status": "REPLAY_UNAVAILABLE", "events": []}


def reconnect_recovery_guidance(*, replay_status=None, sequence_gap=None) -> dict[str, Any]:
    if sequence_gap and sequence_gap.get("hasGap"):
        return {
            "shouldRefreshDurableState": True,
            "reasonCode": "SEQUENCE_GAP",
            "messageCode": "REFRESH_SESSION_STATE",
        }
    replay_status_code = replay_status if isinstance(replay_status, str) else (replay_status or {}).get("status")
    if replay_status_code == "REPLAY_UNAVAILABLE":
        return {
            "shouldRefreshDurableState": True,
            "reasonCode": "REPLAY_UNAVAILABLE",
            "messageCode": "REFRESH_SESSION_STATE",
        }
    return {
        "shouldRefreshDurableState": False,
        "reasonCode": "STREAM_CONTINUITY_OK",
        "messageCode": "CONTINUE_STREAM",
    }
