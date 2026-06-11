import unittest

from ai_assist_session_events import (
    InMemorySessionEventPublisher,
    InMemorySessionEventSink,
    PUBLISHER_FAILURE_CATEGORIES,
    SessionEventPublisherError,
    SessionEventValidationError,
    create_action_proposed_event,
    create_action_status_changed_event,
    create_event_deduplicator,
)

from common import BASE_ENVELOPE, BASE_EVENT, GOOGLE_DOCS_RESOURCE_REF, event_with


class PublisherTest(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
