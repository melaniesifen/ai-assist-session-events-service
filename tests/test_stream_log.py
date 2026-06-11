import unittest

from ai_assist_session_events import STREAM_LOG_OPERATIONS, create_stream_log_record


class StreamLogTest(unittest.TestCase):
    def test_creates_metadata_only_stream_lifecycle_log_records(self):
        self.assertEqual(
            create_stream_log_record(
                STREAM_LOG_OPERATIONS["REPLAY_MISS"],
                {
                    "tenantId": "tenant_001",
                    "userId": "user_001",
                    "sessionId": "session_001",
                    "requestId": "req_001",
                    "correlationId": "corr_001",
                    "route": "/sessions/session_001/events",
                    "lastEventId": "evt_001",
                    "replayStatus": "REPLAY_UNAVAILABLE",
                    "ignoredField": "not logged",
                },
                now=lambda: "2026-05-29T00:00:00.000Z",
            ),
            {
                "timestamp": "2026-05-29T00:00:00.000Z",
                "service": "ai-assist-session-events-service",
                "operation": STREAM_LOG_OPERATIONS["REPLAY_MISS"],
                "tenantId": "tenant_001",
                "userId": "user_001",
                "sessionId": "session_001",
                "requestId": "req_001",
                "correlationId": "corr_001",
                "route": "/sessions/session_001/events",
                "lastEventId": "evt_001",
                "replayStatus": "REPLAY_UNAVAILABLE",
            },
        )

    def test_stream_log_timestamp_none_uses_clock_default(self):
        self.assertEqual(
            create_stream_log_record(
                STREAM_LOG_OPERATIONS["OPEN"],
                {"timestamp": None},
                now=lambda: "2026-05-29T00:00:00.000Z",
            )["timestamp"],
            "2026-05-29T00:00:00.000Z",
        )

    def test_rejects_sensitive_stream_log_metadata_keys_at_any_depth(self):
        with self.assertRaisesRegex(TypeError, "oauthToken"):
            create_stream_log_record(
                STREAM_LOG_OPERATIONS["ERROR"],
                {
                    "tenantId": "tenant_001",
                    "errorCode": "STREAM_FAILED",
                    "details": {"oauthToken": "secret"},
                },
            )


if __name__ == "__main__":
    unittest.main()
