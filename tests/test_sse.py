import unittest

from ai_assist_session_events import SessionEventValidationError, format_sse_event, validate_session_event

from common import BASE_EVENT, event_with


class SseTest(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
