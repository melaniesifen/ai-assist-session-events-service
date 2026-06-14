import json
import unittest

from ai_assist_session_events import (
    SSE_CONTENT_TYPE,
    SSE_HEARTBEAT_FRAME,
    STREAM_LOG_OPERATIONS,
    InMemorySessionEventPublisher,
    SessionAuthorization,
    SseHttpStreamAdapter,
)

from common import BASE_EVENT, event_with


NOW = "2026-05-29T00:00:00.000Z"


def allowed_authorization(**overrides):
    values = {
        "allowed": True,
        "tenant_id": "tenant_001",
        "user_id": "user_001",
        "session_id": "session_001",
        "request_id": "req_001",
        "correlation_id": "corr_001",
    }
    values.update(overrides)
    return SessionAuthorization(**values)


class HttpSseAdapterTest(unittest.TestCase):
    def test_denies_before_stream_open_when_authorization_fails(self):
        calls = []

        def authorize(request):
            calls.append(request)
            return SessionAuthorization(
                allowed=False,
                session_id="session_001",
                request_id="req_001",
                correlation_id="corr_001",
                status_code=403,
                error_category="AUTHORIZATION",
                error_code="SESSION_ACCESS_DENIED",
            )

        adapter = SseHttpStreamAdapter(
            publisher=InMemorySessionEventPublisher(),
            authorize_session=authorize,
            now=lambda: NOW,
        )

        result = adapter.open_stream({"sessionId": "session_001"})

        self.assertEqual(calls, [{"sessionId": "session_001"}])
        self.assertEqual(result.response.status_code, 403)
        self.assertEqual(result.response.headers, {"Cache-Control": "no-store"})
        self.assertIsNone(result.stream)
        self.assertEqual(result.logs[0]["operation"], STREAM_LOG_OPERATIONS["ERROR"])
        self.assertEqual(result.logs[0]["errorCode"], "SESSION_ACCESS_DENIED")

    def test_denies_when_requested_session_does_not_match_authorized_session(self):
        publisher = InMemorySessionEventPublisher()
        publisher.publish(event_with(sessionId="session_authorized"))
        adapter = SseHttpStreamAdapter(
            publisher=publisher,
            authorize_session=lambda request: allowed_authorization(session_id="session_authorized"),
            now=lambda: NOW,
        )

        result = adapter.open_stream({"sessionId": "session_requested"})

        self.assertEqual(result.response.status_code, 403)
        self.assertIsNone(result.stream)
        self.assertEqual(result.logs[0]["operation"], STREAM_LOG_OPERATIONS["ERROR"])
        self.assertEqual(result.logs[0]["errorCategory"], "AUTHORIZATION")
        self.assertEqual(result.logs[0]["errorCode"], "SESSION_AUTHORIZATION_MISMATCH")
        self.assertEqual(result.logs[0]["sessionId"], "session_authorized")

    def test_returns_text_event_stream_response_metadata(self):
        adapter = SseHttpStreamAdapter(
            publisher=InMemorySessionEventPublisher(),
            authorize_session=lambda request: allowed_authorization(),
            now=lambda: NOW,
        )

        result = adapter.open_stream({"sessionId": "session_001"})

        self.assertEqual(result.response.status_code, 200)
        self.assertEqual(result.response.headers["Content-Type"], SSE_CONTENT_TYPE)
        self.assertEqual(result.response.headers["Cache-Control"], "no-cache, no-transform")
        self.assertEqual(result.response.headers["Connection"], "keep-alive")
        self.assertEqual(result.response.headers["X-Accel-Buffering"], "no")
        self.assertEqual(result.logs[0]["operation"], STREAM_LOG_OPERATIONS["OPEN"])

    def test_formats_replayed_session_events_with_sse_id_event_and_data(self):
        publisher = InMemorySessionEventPublisher()
        publisher.publish(BASE_EVENT)
        adapter = SseHttpStreamAdapter(
            publisher=publisher,
            authorize_session=lambda request: allowed_authorization(),
            now=lambda: NOW,
        )

        result = adapter.open_stream({"sessionId": "session_001"})
        chunks = result.stream.pop_pending()

        self.assertEqual(result.replay_status, "FULL_REPLAY")
        self.assertEqual(len(chunks), 1)
        self.assertTrue(chunks[0].startswith("id: evt_001\n"))
        self.assertIn("\nevent: progress\n", chunks[0])
        self.assertIn('\ndata: {"eventId":"evt_001"', chunks[0])

    def test_heartbeat_adds_keepalive_comment_frame(self):
        adapter = SseHttpStreamAdapter(
            publisher=InMemorySessionEventPublisher(),
            authorize_session=lambda request: allowed_authorization(),
            now=lambda: NOW,
        )
        result = adapter.open_stream({"sessionId": "session_001"})

        self.assertEqual(result.stream.heartbeat(), SSE_HEARTBEAT_FRAME)
        self.assertEqual(result.stream.pop_pending(), (SSE_HEARTBEAT_FRAME,))

    def test_heartbeat_after_close_does_not_enqueue_frames(self):
        adapter = SseHttpStreamAdapter(
            publisher=InMemorySessionEventPublisher(),
            authorize_session=lambda request: allowed_authorization(),
            now=lambda: NOW,
        )
        result = adapter.open_stream({"sessionId": "session_001"})
        result.stream.close()

        self.assertEqual(result.stream.heartbeat(), "")
        self.assertEqual(result.stream.pop_pending(), ())

    def test_close_unsubscribes_and_records_metadata_only_disconnect_log(self):
        publisher = InMemorySessionEventPublisher()
        adapter = SseHttpStreamAdapter(
            publisher=publisher,
            authorize_session=lambda request: allowed_authorization(),
            now=lambda: NOW,
        )
        result = adapter.open_stream({"sessionId": "session_001"})

        close_log = result.stream.close(disconnect_reason="client_disconnect", duration_ms=1500)
        publisher.publish(event_with(eventId="evt_after_close", sequence=2))

        self.assertEqual(close_log["operation"], STREAM_LOG_OPERATIONS["CLOSE"])
        self.assertEqual(close_log["disconnectReason"], "client_disconnect")
        self.assertEqual(close_log["durationMs"], 1500)
        self.assertEqual(result.stream.pop_pending(), ())

    def test_replay_miss_emits_refresh_guidance_and_log(self):
        publisher = InMemorySessionEventPublisher()
        publisher.publish(BASE_EVENT)
        adapter = SseHttpStreamAdapter(
            publisher=publisher,
            authorize_session=lambda request: allowed_authorization(),
            now=lambda: NOW,
        )

        result = adapter.open_stream(
            {
                "sessionId": "session_001",
                "headers": {"Last-Event-ID": "evt_missing"},
            }
        )
        chunks = result.stream.pop_pending()
        guidance_json = chunks[0].split("data: ", 1)[1].strip()
        guidance_event = json.loads(guidance_json)

        self.assertEqual(result.replay_status, "REPLAY_UNAVAILABLE")
        self.assertEqual(result.logs[1]["operation"], STREAM_LOG_OPERATIONS["REPLAY_MISS"])
        self.assertEqual(result.logs[1]["lastEventId"], "evt_missing")
        self.assertTrue(chunks[0].startswith("id: evt_stream_refresh_required_req_001\n"))
        self.assertEqual(guidance_event["type"], "progress")
        self.assertEqual(guidance_event["payload"]["messageCode"], "REFRESH_SESSION_STATE")
        self.assertEqual(guidance_event["payload"]["status"], "skipped")

    def test_last_event_id_header_lookup_is_case_insensitive(self):
        publisher = InMemorySessionEventPublisher()
        publisher.publish(BASE_EVENT)
        publisher.publish(event_with(eventId="evt_002", sequence=2))
        adapter = SseHttpStreamAdapter(
            publisher=publisher,
            authorize_session=lambda request: allowed_authorization(),
            now=lambda: NOW,
        )

        result = adapter.open_stream(
            {
                "sessionId": "session_001",
                "headers": {"Last-Event-Id": "evt_001"},
            }
        )
        chunks = result.stream.pop_pending()

        self.assertEqual(result.replay_status, "PARTIAL_REPLAY")
        self.assertEqual(len(chunks), 1)
        self.assertTrue(chunks[0].startswith("id: evt_002\n"))

    def test_rejects_non_string_last_event_id_header(self):
        adapter = SseHttpStreamAdapter(
            publisher=InMemorySessionEventPublisher(),
            authorize_session=lambda request: allowed_authorization(),
            now=lambda: NOW,
        )

        with self.assertRaisesRegex(TypeError, "Last-Event-ID"):
            adapter.open_stream({"sessionId": "session_001", "headers": {"Last-Event-ID": 123}})

    def test_rejects_sensitive_authorization_log_metadata(self):
        adapter = SseHttpStreamAdapter(
            publisher=InMemorySessionEventPublisher(),
            authorize_session=lambda request: allowed_authorization(
                log_metadata={"details": {"accessToken": "secret"}}
            ),
            now=lambda: NOW,
        )

        with self.assertRaisesRegex(TypeError, "accessToken"):
            adapter.open_stream({"sessionId": "session_001"})


if __name__ == "__main__":
    unittest.main()
