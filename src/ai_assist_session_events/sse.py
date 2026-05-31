from __future__ import annotations

import json

from .session_event import assert_valid_session_event


def format_sse_event(event: dict) -> str:
    assert_valid_session_event(event)
    serialized_event = json.dumps(event, separators=(",", ":"))
    lines = [
        f"id: {event['eventId']}",
        f"event: {event['type']}",
        *[f"data: {line}" for line in serialized_event.splitlines()],
        "",
    ]
    return "\n".join(lines) + "\n"


def format_sse_retry(milliseconds: int) -> str:
    if not isinstance(milliseconds, int) or isinstance(milliseconds, bool) or milliseconds < 0:
        raise TypeError("milliseconds must be a non-negative safe integer")
    return f"retry: {milliseconds}\n\n"
