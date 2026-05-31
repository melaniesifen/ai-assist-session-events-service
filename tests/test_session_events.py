import copy
import unittest

from ai_assist_session_events import (
    InMemorySessionEventPublisher,
    InMemorySessionEventSink,
    PUBLISHER_FAILURE_CATEGORIES,
    STREAM_LOG_OPERATIONS,
    SessionEventPublisherError,
    SessionEventValidationError,
    assert_valid_session_event,
    create_event_deduplicator,
    create_session_event,
    create_stream_log_record,
    detect_sequence_gap,
    format_sse_event,
    reconnect_recovery_guidance,
    replay_status_for_last_event_id,
    validate_session_event,
)


BASE_EVENT = {
    "eventId": "evt_001",
    "tenantId": "tenant_001",
    "userId": "user_001",
    "sessionId": "session_001",
    "requestId": "req_001",
    "correlationId": "corr_001",
    "type": "progress",
    "sequence": 1,
    "createdAt": "2026-05-29T00:00:00.000Z",
    "payload": {
        "stage": "context.loading",
        "status": "STARTED",
        "messageCode": "CONTEXT_LOADING",
    },
}


def event_with(**overrides):
    event = copy.deepcopy(BASE_EVENT)
    event.update(overrides)
    return event


class SessionEventsTest(unittest.TestCase):
    def test_validates_transport_neutral_session_event_envelope(self):
        self.assertEqual(validate_session_event(BASE_EVENT), {"valid": True, "issues": []})

    def test_rejects_invalid_envelope_fields_and_sensitive_payload_keys(self):
        result = validate_session_event(
            event_with(
                tenantId=" ",
                type="assistant.delta",
                payload={
                    "messageId": "msg_001",
                    "delta": "hello",
                    "index": 0,
                    "providerKey": "secret",
                },
            )
        )

        self.assertFalse(result["valid"])
        self.assertTrue(any(issue["path"] == "tenantId" for issue in result["issues"]))
        self.assertTrue(any(issue["code"] == "forbidden_payload_key" for issue in result["issues"]))

    def test_throws_typed_validation_errors_for_invalid_events(self):
        with self.assertRaises(SessionEventValidationError) as caught:
            assert_valid_session_event(event_with(payload={}))
        self.assertTrue(any(issue["path"] == "payload.stage" for issue in caught.exception.issues))

    def test_create_session_event_applies_missing_and_none_defaults(self):
        event_without_defaults = copy.deepcopy(BASE_EVENT)
        del event_without_defaults["createdAt"]
        created = create_session_event(event_without_defaults, now=lambda: "2026-05-30T00:00:00.000Z")

        self.assertEqual(created["createdAt"], "2026-05-30T00:00:00.000Z")

        with self.assertRaises(SessionEventValidationError) as caught:
            create_session_event(event_with(createdAt=None, payload=None), now=lambda: "2026-05-31T00:00:00.000Z")

        self.assertTrue(any(issue["path"] == "payload.stage" for issue in caught.exception.issues))
        self.assertFalse(any(issue["path"] == "createdAt" for issue in caught.exception.issues))
        self.assertFalse(any(issue["code"] == "invalid_payload" for issue in caught.exception.issues))

    def test_validates_every_supported_typed_payload_shape(self):
        cases = [
            ("assistant.delta", {"messageId": "msg_001", "delta": "hello", "index": 0}),
            ("assistant.final", {"messageId": "msg_001", "finishReason": "stop", "usage": {"outputTokens": 10}}),
            ("progress", {"stage": "provider.generating", "status": "STARTED", "messageCode": "PROVIDER_GENERATING"}),
            ("error", {"errorCode": "PROVIDER_TIMEOUT", "category": "DEPENDENCY", "retryable": True, "message": "Try again."}),
            (
                "action.proposed",
                {
                    "actionId": "act_001",
                    "actionType": "google_docs.replace_text",
                    "resourceRef": {"provider": "google_docs", "resourceId": "doc_001"},
                    "summary": "Replace selected text.",
                    "expiresAt": "2026-05-30T00:00:00.000Z",
                },
            ),
            (
                "action.status_changed",
                {"actionId": "act_001", "previousStatus": "PROPOSED", "status": "APPROVED", "reasonCode": "USER_APPROVED"},
            ),
        ]

        for index, (event_type, payload) in enumerate(cases):
            with self.subTest(event_type=event_type):
                self.assertEqual(
                    validate_session_event(event_with(eventId=f"evt_typed_{index}", type=event_type, payload=payload)),
                    {"valid": True, "issues": []},
                )

    def test_publishes_subscribes_and_replays_events_after_last_event_id(self):
        publisher = InMemorySessionEventPublisher()
        first = event_with(eventId="evt_001", sequence=1)
        second = event_with(eventId="evt_002", sequence=2)
        third = event_with(eventId="evt_003", sequence=3)
        publisher.publish(first)
        publisher.publish(second)

        received = []
        subscription = publisher.subscribe(BASE_EVENT["sessionId"], lambda event: received.append(event), last_event_id="evt_001")

        self.assertEqual([event["eventId"] for event in received], ["evt_002"])
        self.assertEqual(subscription.replay_status, "PARTIAL_REPLAY")
        self.assertEqual([event["eventId"] for event in subscription.replayed], ["evt_002"])

        publisher.publish(third)
        self.assertEqual([event["eventId"] for event in received], ["evt_002", "evt_003"])

        subscription.unsubscribe()
        publisher.publish(event_with(eventId="evt_004", sequence=4))
        self.assertEqual([event["eventId"] for event in received], ["evt_002", "evt_003"])

    def test_surfaces_unavailable_replay_after_requested_event_leaves_buffer(self):
        publisher = InMemorySessionEventPublisher(sink=InMemorySessionEventSink(max_events_per_session=1))
        publisher.publish(event_with(eventId="evt_001", sequence=1))
        publisher.publish(event_with(eventId="evt_002", sequence=2))

        subscription = publisher.subscribe(BASE_EVENT["sessionId"], lambda event: None, last_event_id="evt_001")

        self.assertEqual(subscription.replay_status, "REPLAY_UNAVAILABLE")
        self.assertEqual(subscription.replayed, [])

    def test_rejects_unsafe_replay_and_dedupe_buffer_sizes(self):
        for invalid_size in (0, True, 2**53):
            with self.subTest(helper="sink", invalid_size=invalid_size):
                with self.assertRaises(TypeError):
                    InMemorySessionEventSink(max_events_per_session=invalid_size)

            with self.subTest(helper="deduplicator", invalid_size=invalid_size):
                with self.assertRaises(TypeError):
                    create_event_deduplicator(max_tracked_event_ids=invalid_size)

        InMemorySessionEventSink(max_events_per_session=(2**53) - 1)
        create_event_deduplicator(max_tracked_event_ids=(2**53) - 1)

    def test_rejects_duplicate_event_ids_within_retained_session_buffer(self):
        publisher = InMemorySessionEventPublisher()
        publisher.publish(BASE_EVENT)

        with self.assertRaises(SessionEventPublisherError) as caught:
            publisher.publish(event_with(payload={**BASE_EVENT["payload"], "status": "DONE"}))

        error = caught.exception
        self.assertEqual(error.category, PUBLISHER_FAILURE_CATEGORIES["PERSISTENCE"])
        self.assertIsInstance(error.cause, SessionEventValidationError)
        self.assertTrue(any(issue["code"] == "duplicate_event_id" for issue in error.cause.issues))

    def test_returns_typed_publisher_failure_results_without_throwing(self):
        publisher = InMemorySessionEventPublisher()
        result = publisher.try_publish(event_with(payload={}))

        self.assertFalse(result.ok)
        self.assertIsInstance(result.error, SessionEventPublisherError)
        self.assertEqual(result.error.category, PUBLISHER_FAILURE_CATEGORIES["VALIDATION"])
        self.assertEqual(result.error.operation, "validate")

    def test_reports_subscriber_delivery_failures_as_non_authoritative_diagnostics(self):
        publisher = InMemorySessionEventPublisher()
        delivered = []

        def failing_handler(_event):
            raise RuntimeError("subscriber unavailable")

        publisher.subscribe(BASE_EVENT["sessionId"], failing_handler, replay=False)
        publisher.subscribe(BASE_EVENT["sessionId"], lambda event: delivered.append(event["eventId"]), replay=False)

        result = publisher.try_publish(BASE_EVENT)

        self.assertTrue(result.ok)
        self.assertEqual(len(result.delivery_failures), 1)
        self.assertEqual([event["eventId"] for event in publisher.list(BASE_EVENT["sessionId"])], ["evt_001"])
        self.assertEqual(delivered, ["evt_001"])

    def test_does_not_throw_from_publish_when_best_effort_subscriber_delivery_fails(self):
        publisher = InMemorySessionEventPublisher()
        publisher.subscribe(BASE_EVENT["sessionId"], lambda _event: (_ for _ in ()).throw(RuntimeError("closed stream")), replay=False)

        publisher.publish(BASE_EVENT)
        self.assertEqual([event["eventId"] for event in publisher.list(BASE_EVENT["sessionId"])], ["evt_001"])

    def test_rejects_event_ids_that_would_inject_additional_sse_field_lines(self):
        injected = event_with(eventId="evt_001\nretry: 0")
        result = validate_session_event(injected)

        self.assertFalse(result["valid"])
        self.assertTrue(any(issue["path"] == "eventId" for issue in result["issues"]))
        with self.assertRaises(SessionEventValidationError):
            format_sse_event(injected)

    def test_formats_sse_id_event_and_json_data_fields(self):
        formatted = format_sse_event(BASE_EVENT)
        self.assertTrue(formatted.startswith("id: evt_001\n"))
        self.assertIn("\nevent: progress\n", formatted)
        self.assertIn('\ndata: {"eventId":"evt_001"', formatted)
        self.assertTrue(formatted.endswith("\n\n"))

    def test_deduplicates_reconnect_deliveries_by_event_id(self):
        deduplicator = create_event_deduplicator(initial_event_ids=["evt_001"])
        self.assertFalse(deduplicator.should_process(BASE_EVENT))
        self.assertTrue(deduplicator.should_process(event_with(eventId="evt_002")))
        self.assertFalse(deduplicator.should_process(event_with(eventId="evt_002")))

    def test_detects_sequence_gaps_and_unavailable_replay_windows(self):
        sequence_gap = detect_sequence_gap(1, event_with(sequence=3))

        self.assertEqual(sequence_gap, {"hasGap": True, "expectedSequence": 2, "actualSequence": 3})
        self.assertEqual(
            replay_status_for_last_event_id([event_with(eventId="evt_002")], "evt_001"),
            {"status": "REPLAY_UNAVAILABLE", "events": []},
        )
        self.assertEqual(
            reconnect_recovery_guidance(replay_status="PARTIAL_REPLAY", sequence_gap=sequence_gap),
            {
                "shouldRefreshDurableState": True,
                "reasonCode": "SEQUENCE_GAP",
                "messageCode": "REFRESH_SESSION_STATE",
            },
        )
        self.assertEqual(
            reconnect_recovery_guidance(replay_status="REPLAY_UNAVAILABLE"),
            {
                "shouldRefreshDurableState": True,
                "reasonCode": "REPLAY_UNAVAILABLE",
                "messageCode": "REFRESH_SESSION_STATE",
            },
        )
        self.assertEqual(
            reconnect_recovery_guidance(
                replay_status=replay_status_for_last_event_id([event_with(eventId="evt_002")], "evt_001")
            ),
            {
                "shouldRefreshDurableState": True,
                "reasonCode": "REPLAY_UNAVAILABLE",
                "messageCode": "REFRESH_SESSION_STATE",
            },
        )
        self.assertEqual(
            reconnect_recovery_guidance(replay_status="PARTIAL_REPLAY"),
            {
                "shouldRefreshDurableState": False,
                "reasonCode": "STREAM_CONTINUITY_OK",
                "messageCode": "CONTINUE_STREAM",
            },
        )

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
