import copy
import unittest

from ai_assist_session_events import (
    InMemorySessionEventPublisher,
    InMemorySessionEventSink,
    PUBLISHER_FAILURE_CATEGORIES,
    STREAM_LOG_OPERATIONS,
    SessionEventPublisherError,
    SessionEventValidationError,
    create_action_proposed_event,
    create_action_status_changed_event,
    assert_valid_session_event,
    create_assistant_delta_event,
    create_assistant_final_event,
    create_event_deduplicator,
    create_progress_event,
    create_safe_error_event,
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
        "status": "started",
        "messageCode": "CONTEXT_LOADING",
    },
}


BASE_ENVELOPE = {
    "eventId": "evt_001",
    "tenantId": "tenant_001",
    "userId": "user_001",
    "sessionId": "session_001",
    "requestId": "req_001",
    "correlationId": "corr_001",
    "sequence": 1,
}

GOOGLE_DOCS_RESOURCE_REF = {
    "connector": "google_docs",
    "resourceId": "doc_001",
    "resourceType": "document",
    "displayName": "Quarterly plan",
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
            ("progress", {"stage": "provider.generating", "status": "in_progress", "messageCode": "PROVIDER_GENERATING"}),
            ("error", {"errorCode": "PROVIDER_TIMEOUT", "category": "DEPENDENCY", "retryable": True, "message": "Try again."}),
            (
                "action.proposed",
                {
                    "actionId": "act_001",
                    "actionType": "REPLACE_TEXT",
                    "resourceRef": GOOGLE_DOCS_RESOURCE_REF,
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

    def test_creates_full_m5_stream_event_envelopes_for_sse_delivery(self):
        cases = [
            create_progress_event(
                {**BASE_ENVELOPE, "eventId": "evt_progress", "sequence": 1},
                stage="context.loading",
                status="started",
                message_code="CONTEXT_LOADING",
                now=lambda: "2026-05-29T00:00:00.000Z",
            ),
            create_assistant_delta_event(
                {**BASE_ENVELOPE, "eventId": "evt_delta", "sequence": 2},
                message_id="msg_001",
                delta="hello",
                index=0,
                now=lambda: "2026-05-29T00:00:01.000Z",
            ),
            create_assistant_final_event(
                {**BASE_ENVELOPE, "eventId": "evt_final", "sequence": 3},
                message_id="msg_001",
                finish_reason="stop",
                usage={"outputTokens": 10},
                now=lambda: "2026-05-29T00:00:02.000Z",
            ),
            create_safe_error_event(
                {**BASE_ENVELOPE, "eventId": "evt_error", "sequence": 4},
                error_code="PROVIDER_UNAVAILABLE",
                category="DEPENDENCY",
                retryable=True,
                message="The assistant service is unavailable. Try again.",
                now=lambda: "2026-05-29T00:00:03.000Z",
            ),
        ]

        self.assertEqual(
            [event["type"] for event in cases],
            ["progress", "assistant.delta", "assistant.final", "error"],
        )
        for event in cases:
            with self.subTest(event_type=event["type"]):
                self.assertEqual(validate_session_event(event), {"valid": True, "issues": []})
                formatted = format_sse_event(event)
                self.assertIn(f"id: {event['eventId']}\n", formatted)
                self.assertIn(f"event: {event['type']}\n", formatted)
                self.assertIn(f'data: {{"eventId":"{event["eventId"]}"', formatted)

    def test_creates_full_action_event_envelopes_for_sse_delivery(self):
        cases = [
            create_action_proposed_event(
                {**BASE_ENVELOPE, "eventId": "evt_action_proposed", "sequence": 5},
                action_id="act_001",
                action_type="REPLACE_TEXT",
                resource_ref=GOOGLE_DOCS_RESOURCE_REF,
                summary="Replace selected text.",
                expires_at="2026-05-30T00:00:00.000Z",
                now=lambda: "2026-05-29T00:00:04.000Z",
            ),
            create_action_status_changed_event(
                {**BASE_ENVELOPE, "eventId": "evt_action_status", "sequence": 6},
                action_id="act_001",
                previous_status="PROPOSED",
                status="APPROVED",
                reason_code="USER_APPROVED",
                now=lambda: "2026-05-29T00:00:05.000Z",
            ),
        ]

        self.assertEqual([event["type"] for event in cases], ["action.proposed", "action.status_changed"])
        for event in cases:
            with self.subTest(event_type=event["type"]):
                self.assertEqual(validate_session_event(event), {"valid": True, "issues": []})
                formatted = format_sse_event(event)
                self.assertIn(f"id: {event['eventId']}\n", formatted)
                self.assertIn(f"event: {event['type']}\n", formatted)
                self.assertIn(f'data: {{"eventId":"{event["eventId"]}"', formatted)

    def test_action_proposed_requires_contract_shaped_payload(self):
        invalid_cases = [
            (
                "unsupported_action_type",
                {
                    "actionId": "act_001",
                    "actionType": "google_docs.replace_text",
                    "resourceRef": GOOGLE_DOCS_RESOURCE_REF,
                    "summary": "Replace selected text.",
                    "expiresAt": "2026-05-30T00:00:00.000Z",
                },
                "payload.actionType",
            ),
            (
                "incomplete_resource_ref",
                {
                    "actionId": "act_001",
                    "actionType": "REPLACE_TEXT",
                    "resourceRef": {"resourceId": "doc_001"},
                    "summary": "Replace selected text.",
                    "expiresAt": "2026-05-30T00:00:00.000Z",
                },
                "payload.resourceRef.connector",
            ),
            (
                "unsupported_connector",
                {
                    "actionId": "act_001",
                    "actionType": "REPLACE_TEXT",
                    "resourceRef": {**GOOGLE_DOCS_RESOURCE_REF, "connector": "made_up_connector"},
                    "summary": "Replace selected text.",
                    "expiresAt": "2026-05-30T00:00:00.000Z",
                },
                "payload.resourceRef.connector",
            ),
            (
                "blank_optional_resource_display_name",
                {
                    "actionId": "act_001",
                    "actionType": "REPLACE_TEXT",
                    "resourceRef": {**GOOGLE_DOCS_RESOURCE_REF, "displayName": " "},
                    "summary": "Replace selected text.",
                    "expiresAt": "2026-05-30T00:00:00.000Z",
                },
                "payload.resourceRef.displayName",
            ),
            (
                "invalid_expiry",
                {
                    "actionId": "act_001",
                    "actionType": "REPLACE_TEXT",
                    "resourceRef": GOOGLE_DOCS_RESOURCE_REF,
                    "summary": "Replace selected text.",
                    "expiresAt": "tomorrow",
                },
                "payload.expiresAt",
            ),
        ]

        for name, payload, path in invalid_cases:
            with self.subTest(name=name):
                result = validate_session_event(event_with(type="action.proposed", payload=payload))
                self.assertFalse(result["valid"])
                self.assertTrue(any(issue["path"] == path for issue in result["issues"]))

    def test_action_status_changed_requires_known_statuses(self):
        result = validate_session_event(
            event_with(
                type="action.status_changed",
                payload={
                    "actionId": "act_001",
                    "previousStatus": "proposed",
                    "status": "DONE",
                    "reasonCode": "USER_APPROVED",
                },
            )
        )

        self.assertFalse(result["valid"])
        self.assertTrue(any(issue["path"] == "payload.previousStatus" for issue in result["issues"]))
        self.assertTrue(any(issue["path"] == "payload.status" for issue in result["issues"]))

    def test_action_status_changed_accepts_contract_apply_lifecycle_states(self):
        apply_result_cases = [
            ("APPLIED", "APPLY_SUCCEEDED"),
            ("CONFLICTED", "TARGET_CONFLICTED"),
            ("FAILED", "APPLY_FAILED"),
            ("EXPIRED", "ACTION_EXPIRED"),
            ("FAILED", "AUTHORIZATION_DENIED"),
            ("FAILED", "OAUTH_RECONNECT_REQUIRED"),
        ]

        for index, (status, reason_code) in enumerate(apply_result_cases):
            with self.subTest(status=status):
                event = create_action_status_changed_event(
                    {**BASE_ENVELOPE, "eventId": f"evt_apply_status_{index}", "sequence": index + 10},
                    action_id="act_001",
                    previous_status="APPROVED",
                    status=status,
                    reason_code=reason_code,
                    now=lambda: "2026-06-07T00:00:00.000Z",
                )

                self.assertEqual(validate_session_event(event), {"valid": True, "issues": []})

    def test_action_status_changed_rejects_non_contract_apply_status_labels(self):
        for field_name in ("previousStatus", "status"):
            with self.subTest(field_name=field_name):
                payload = {
                    "actionId": "act_001",
                    "previousStatus": "APPROVED",
                    "status": "FAILED",
                    "reasonCode": "OAUTH_RECONNECT_REQUIRED",
                    field_name: "RECONNECT_REQUIRED",
                }
                result = validate_session_event(event_with(type="action.status_changed", payload=payload))

                self.assertFalse(result["valid"])
                self.assertTrue(any(issue["path"] == f"payload.{field_name}" for issue in result["issues"]))

    def test_action_events_reject_sensitive_payload_fields_at_any_depth(self):
        result = validate_session_event(
            event_with(
                type="action.proposed",
                payload={
                    "actionId": "act_001",
                    "actionType": "REPLACE_TEXT",
                    "resourceRef": GOOGLE_DOCS_RESOURCE_REF,
                    "summary": "Replace selected text.",
                    "expiresAt": "2026-05-30T00:00:00.000Z",
                    "metadata": {"decryptedActionPayload": {"replacementText": "secret"}},
                },
            )
        )

        self.assertFalse(result["valid"])
        self.assertTrue(any(issue["path"] == "payload.metadata.decryptedActionPayload" for issue in result["issues"]))

    def test_action_status_changed_rejects_sensitive_apply_payload_fields_before_publish(self):
        sensitive_apply_fields = [
            ("applyPayload", {"applyPayload": {"operation": "replace"}}),
            ("mutationPayload", {"metadata": {"mutationPayload": {"operation": "insert"}}}),
            ("replacementText", {"metadata": {"result": {"replacementText": "plaintext"}}}),
            ("insertionText", {"metadata": {"result": {"insertionText": "plaintext"}}}),
            ("originalText", {"metadata": {"target": {"originalText": "plaintext"}}}),
            ("newText", {"metadata": {"diff": {"newText": "plaintext"}}}),
            ("oldText", {"metadata": {"diff": {"oldText": "plaintext"}}}),
        ]

        for field_name, extra_payload in sensitive_apply_fields:
            with self.subTest(field_name=field_name):
                payload = {
                    "actionId": "act_001",
                    "previousStatus": "APPROVED",
                    "status": "APPLIED",
                    "reasonCode": "APPLY_SUCCEEDED",
                    **extra_payload,
                }
                result = InMemorySessionEventPublisher().try_publish(
                    event_with(type="action.status_changed", payload=payload)
                )

                self.assertFalse(result.ok)
                self.assertEqual(result.error.operation, "validate")
                self.assertTrue(any(issue["code"] == "forbidden_payload_key" for issue in result.error.cause.issues))

    def test_publishes_replays_and_deduplicates_action_events(self):
        publisher = InMemorySessionEventPublisher()
        proposed = create_action_proposed_event(
            {**BASE_ENVELOPE, "eventId": "evt_action_proposed", "sequence": 5},
            action_id="act_001",
            action_type="REPLACE_TEXT",
            resource_ref=GOOGLE_DOCS_RESOURCE_REF,
            summary="Replace selected text.",
            expires_at="2026-05-30T00:00:00.000Z",
            now=lambda: "2026-05-29T00:00:04.000Z",
        )
        status_changed = create_action_status_changed_event(
            {**BASE_ENVELOPE, "eventId": "evt_action_status", "sequence": 6},
            action_id="act_001",
            previous_status="PROPOSED",
            status="APPROVED",
            reason_code="USER_APPROVED",
            now=lambda: "2026-05-29T00:00:05.000Z",
        )

        publisher.publish(proposed)
        publisher.publish(status_changed)

        replay = publisher.subscribe(BASE_EVENT["sessionId"], lambda _event: None, last_event_id="evt_action_proposed")
        deduplicator = create_event_deduplicator(initial_event_ids=["evt_action_proposed"])

        self.assertEqual(replay.replay_status, "PARTIAL_REPLAY")
        self.assertEqual([event["eventId"] for event in replay.replayed], ["evt_action_status"])
        self.assertFalse(deduplicator.should_process(proposed))
        self.assertTrue(deduplicator.should_process(status_changed))

    def test_typed_error_event_constructor_rejects_forbidden_payload_keys(self):
        with self.assertRaises(SessionEventValidationError) as caught:
            create_safe_error_event(
                BASE_ENVELOPE,
                error_code="PROVIDER_UNAVAILABLE",
                category="DEPENDENCY",
                retryable=True,
                message="Try again.",
                metadata={"providerKey": "secret"},
                now=lambda: "2026-05-29T00:00:00.000Z",
            )

        self.assertTrue(any(issue["path"] == "payload.metadata.providerKey" for issue in caught.exception.issues))

    def test_rejects_progress_status_values_outside_shared_contract(self):
        result = validate_session_event(
            event_with(
                payload={
                    "stage": "context.loading",
                    "status": "STARTED",
                    "messageCode": "CONTEXT_LOADING",
                }
            )
        )

        self.assertFalse(result["valid"])
        self.assertTrue(any(issue["path"] == "payload.status" for issue in result["issues"]))

    def test_typed_error_event_constructor_rejects_sensitive_error_metadata_keys(self):
        sensitive_metadata_cases = [
            ("documentText", {"documentText": "raw document text"}),
            ("authorizationHeader", {"authorizationHeader": "Bearer secret"}),
            ("nestedSelectedText", {"dependency": {"selectedText": "raw selection"}}),
        ]

        for _, metadata in sensitive_metadata_cases:
            with self.subTest(metadata=metadata), self.assertRaises(SessionEventValidationError) as caught:
                create_safe_error_event(
                    BASE_ENVELOPE,
                    error_code="PROVIDER_UNAVAILABLE",
                    category="DEPENDENCY",
                    retryable=True,
                    message="Try again.",
                    metadata=metadata,
                    now=lambda: "2026-05-29T00:00:00.000Z",
                )

            self.assertTrue(any(issue["code"] == "forbidden_payload_key" for issue in caught.exception.issues))

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
            publisher.publish(event_with(payload={**BASE_EVENT["payload"], "messageCode": "CONTEXT_STARTED"}))

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
