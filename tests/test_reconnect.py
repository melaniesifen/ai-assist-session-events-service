import unittest

from ai_assist_session_events import (
    InMemorySessionEventSink,
    create_event_deduplicator,
    detect_sequence_gap,
    reconnect_recovery_guidance,
    replay_status_for_last_event_id,
)

from common import BASE_EVENT, event_with


class ReconnectTest(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
