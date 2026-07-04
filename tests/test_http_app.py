import json
import unittest

from ai_assist_session_events import http_app

from common import BASE_EVENT, event_with


AUTH_HEADERS = {
    "X-AI-Assist-Tenant-Id": "tenant_001",
    "X-AI-Assist-User-Id": "user_001",
    "X-Request-Id": "req_http_app",
    "X-Correlation-Id": "corr_http_app",
}


class SessionEventsHttpAppTest(unittest.TestCase):
    def setUp(self):
        http_app.reset_runtime_for_tests()

    def test_stream_route_returns_real_sse_stream_with_event_ids(self):
        http_app.publish_session_event(BASE_EVENT)

        response = http_app.handle_http_request(
            method="GET",
            path="/sessions/session_001/events",
            headers=AUTH_HEADERS,
        )

        self.assertEqual(response["status"], 200)
        self.assertEqual(response["headers"]["Content-Type"], "text/event-stream; charset=utf-8")
        chunks = response["stream"].pop_pending()
        self.assertEqual(chunks[0].splitlines()[0], "id: evt_001")
        self.assertIn('"type":"progress"', chunks[0])

    def test_last_event_id_replays_only_later_event(self):
        http_app.publish_session_event(BASE_EVENT)
        http_app.publish_session_event(event_with(eventId="evt_002", sequence=2))

        response = http_app.handle_http_request(
            method="GET",
            path="/sessions/session_001/events",
            headers={**AUTH_HEADERS, "Last-Event-ID": "evt_001"},
        )

        chunks = response["stream"].pop_pending()
        self.assertEqual(len(chunks), 1)
        self.assertTrue(chunks[0].startswith("id: evt_002\n"))

    def test_last_event_id_replay_miss_sends_refresh_guidance(self):
        response = http_app.handle_http_request(
            method="GET",
            path="/sessions/session_001/events",
            headers={**AUTH_HEADERS, "Last-Event-ID": "evt_missing"},
        )

        chunks = response["stream"].pop_pending()
        self.assertEqual(response["status"], 200)
        self.assertIn("REFRESH_SESSION_STATE", chunks[0])
        self.assertEqual(http_app.stream_log_records()[1]["operation"], "stream.replay_miss")

    def test_duplicate_event_is_rejected_by_internal_publish(self):
        body = json.dumps(BASE_EVENT).encode("utf-8")
        first = http_app.handle_http_request(method="POST", path="/internal/session-events", body=body)
        second = http_app.handle_http_request(method="POST", path="/internal/session-events", body=body)

        self.assertEqual(first["status"], 202)
        self.assertEqual(second["status"], 400)
        error = json.loads(second["body"].decode("utf-8"))["error"]
        self.assertEqual(error["code"], "SESSION_EVENT_REJECTED")
        self.assertEqual(error["details"]["category"], "PERSISTENCE")

    def test_malformed_event_is_rejected_by_internal_publish(self):
        response = http_app.handle_http_request(
            method="POST",
            path="/internal/session-events",
            body=json.dumps({"eventId": "evt_bad"}).encode("utf-8"),
        )

        self.assertEqual(response["status"], 400)
        error = json.loads(response["body"].decode("utf-8"))["error"]
        self.assertEqual(error["code"], "SESSION_EVENT_REJECTED")
        self.assertEqual(error["details"]["category"], "VALIDATION")

    def test_close_records_disconnect_log(self):
        response = http_app.handle_http_request(
            method="GET",
            path="/sessions/session_001/events",
            headers=AUTH_HEADERS,
        )

        close_log = response["stream"].close(disconnect_reason="client_disconnect", duration_ms=10)

        self.assertEqual(close_log["operation"], "stream.close")
        self.assertEqual(close_log["disconnectReason"], "client_disconnect")
        self.assertEqual(http_app.stream_log_records()[-1]["operation"], "stream.close")


if __name__ == "__main__":
    unittest.main()
