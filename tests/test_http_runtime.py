import unittest

from ai_assist_session_events import (
    InMemorySessionEventPublisher,
    SessionAuthorization,
    SessionEventsHttpRuntime,
)

from common import BASE_EVENT, event_with


AUTH_HEADERS = {
    "X-AI-Assist-Tenant-Id": "tenant_001",
    "X-AI-Assist-User-Id": "user_001",
    "X-Request-Id": "req_http",
    "X-Correlation-Id": "corr_http",
}


class SessionEventsHttpRuntimeTest(unittest.TestCase):
    def test_opens_canonical_sessions_events_route_with_trusted_header_auth(self):
        logs = []
        publisher = InMemorySessionEventPublisher()
        publisher.publish(BASE_EVENT)
        runtime = SessionEventsHttpRuntime(publisher=publisher, record_log=logs.append)

        result = runtime.open_stream(
            {
                "method": "GET",
                "path": "/sessions/session_001/events",
                "headers": AUTH_HEADERS,
            }
        )

        self.assertEqual(result.response.status_code, 200)
        self.assertEqual(result.stream.pop_pending()[0].splitlines()[0], "id: evt_001")
        self.assertEqual(logs[0]["operation"], "stream.open")
        self.assertEqual(logs[0]["tenantId"], "tenant_001")
        self.assertEqual(logs[0]["requestId"], "req_http")

    def test_missing_trusted_auth_context_denies_before_stream_open(self):
        logs = []
        runtime = SessionEventsHttpRuntime(
            publisher=InMemorySessionEventPublisher(),
            request_id_generator=lambda: "req_generated",
            correlation_id_generator=lambda: "corr_generated",
            record_log=logs.append,
        )

        result = runtime.open_stream(
            {
                "method": "GET",
                "path": "/sessions/session_001/events",
                "headers": {},
            }
        )

        self.assertEqual(result.response.status_code, 401)
        self.assertIsNone(result.stream)
        self.assertEqual(logs[0]["errorCategory"], "AUTHENTICATION")
        self.assertEqual(logs[0]["errorCode"], "AUTH_CONTEXT_REQUIRED")
        self.assertEqual(logs[0]["requestId"], "req_generated")

    def test_last_event_id_replays_only_later_events(self):
        publisher = InMemorySessionEventPublisher()
        publisher.publish(BASE_EVENT)
        publisher.publish(event_with(eventId="evt_002", sequence=2))
        runtime = SessionEventsHttpRuntime(publisher=publisher)

        result = runtime.open_stream(
            {
                "method": "GET",
                "path": "/sessions/session_001/events",
                "headers": {**AUTH_HEADERS, "Last-Event-ID": "evt_001"},
            }
        )

        chunks = result.stream.pop_pending()
        self.assertEqual(result.replay_status, "PARTIAL_REPLAY")
        self.assertEqual(len(chunks), 1)
        self.assertTrue(chunks[0].startswith("id: evt_002\n"))

    def test_rejects_resource_session_alias_route(self):
        runtime = SessionEventsHttpRuntime(publisher=InMemorySessionEventPublisher())

        with self.assertRaisesRegex(TypeError, "/sessions/\\{sessionId\\}/events"):
            runtime.open_stream(
                {
                    "method": "GET",
                    "path": "/resource-sessions/session_001/events",
                    "headers": AUTH_HEADERS,
                }
            )

    def test_injected_authorizer_can_reject_cross_session_access(self):
        def authorize(_request):
            return SessionAuthorization(
                allowed=True,
                tenant_id="tenant_001",
                user_id="user_001",
                session_id="session_authorized",
                request_id="req_001",
                correlation_id="corr_001",
            )

        runtime = SessionEventsHttpRuntime(
            publisher=InMemorySessionEventPublisher(),
            authorize_session=authorize,
        )

        result = runtime.open_stream(
            {
                "method": "GET",
                "path": "/sessions/session_requested/events",
                "headers": AUTH_HEADERS,
            }
        )

        self.assertEqual(result.response.status_code, 403)
        self.assertIsNone(result.stream)


if __name__ == "__main__":
    unittest.main()
