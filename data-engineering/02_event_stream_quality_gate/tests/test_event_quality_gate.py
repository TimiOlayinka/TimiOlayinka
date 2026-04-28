import unittest
from pathlib import Path
import sys


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from event_quality_gate import validate_batch, validate_event


CONTRACT = {
    "contract_name": "test_contract",
    "required_fields": {
        "event_id": "string",
        "event_name": "string",
        "payload": "object",
    },
    "payload_required_fields": {
        "entity_id": "string",
        "event_value": "number",
    },
    "accepted_event_names": ["entity_created"],
}


class EventQualityGateTests(unittest.TestCase):
    def test_accepts_valid_event(self):
        result = validate_event(
            {
                "event_id": "evt_001",
                "event_name": "entity_created",
                "payload": {"entity_id": "ent_001", "event_value": 1.5},
            },
            CONTRACT,
        )

        self.assertTrue(result.accepted)
        self.assertEqual(result.rejection_reasons, ())

    def test_rejects_missing_payload_field(self):
        result = validate_event(
            {
                "event_id": "evt_002",
                "event_name": "entity_created",
                "payload": {"event_value": 1},
            },
            CONTRACT,
        )

        self.assertFalse(result.accepted)
        self.assertIn("missing required payload field: entity_id", result.rejection_reasons)

    def test_rejects_unaccepted_event_name(self):
        result = validate_event(
            {
                "event_id": "evt_003",
                "event_name": "entity_removed",
                "payload": {"entity_id": "ent_002", "event_value": 1},
            },
            CONTRACT,
        )

        self.assertFalse(result.accepted)
        self.assertIn("event_name is not accepted: entity_removed", result.rejection_reasons)

    def test_batch_summary_counts_rejections(self):
        summary = validate_batch(
            [
                {
                    "event_id": "evt_001",
                    "event_name": "entity_created",
                    "payload": {"entity_id": "ent_001", "event_value": 1},
                },
                {
                    "event_id": "evt_002",
                    "event_name": "entity_created",
                    "payload": {"event_value": "bad"},
                },
            ],
            CONTRACT,
        )

        self.assertEqual(summary["total_records"], 2)
        self.assertEqual(summary["accepted_records"], 1)
        self.assertEqual(summary["rejected_records"], 1)
        self.assertEqual(len(summary["quarantine"]), 1)


if __name__ == "__main__":
    unittest.main()
