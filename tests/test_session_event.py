import copy
import unittest

from ai_assist_session_events import (
    InMemorySessionEventPublisher,
    SessionEventValidationError,
    create_action_proposed_event,
    create_action_status_changed_event,
    assert_valid_session_event,
    create_assistant_delta_event,
    create_assistant_final_event,
    create_progress_event,
    create_safe_error_event,
    create_session_event,
    format_sse_event,
    validate_session_event,
)

from common import BASE_ENVELOPE, BASE_EVENT, GOOGLE_DOCS_RESOURCE_REF, event_with


class SessionEventTest(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
