"""Validate event payloads against a small JSON contract."""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


TYPE_CHECKS = {
    "object": lambda value: isinstance(value, dict),
    "number": lambda value: isinstance(value, (int, float)) and not isinstance(value, bool),
    "string": lambda value: isinstance(value, str) and value.strip() != "",
}


@dataclass(frozen=True)
class ValidationResult:
    accepted: bool
    event: dict[str, Any]
    rejection_reasons: tuple[str, ...]


def load_json(path: str | Path) -> dict[str, Any]:
    with Path(path).open("r", encoding="utf-8") as file:
        return json.load(file)


def load_jsonl(path: str | Path) -> list[dict[str, Any]]:
    events = []
    with Path(path).open("r", encoding="utf-8") as file:
        for line_number, line in enumerate(file, start=1):
            if not line.strip():
                continue
            try:
                events.append(json.loads(line))
            except json.JSONDecodeError as exc:
                raise ValueError(f"Line {line_number} is not valid JSON: {exc}") from exc
    return events


def _check_field_type(value: Any, expected_type: str) -> bool:
    if expected_type not in TYPE_CHECKS:
        raise ValueError(f"Unsupported contract type: {expected_type}")
    return TYPE_CHECKS[expected_type](value)


def validate_event(event: dict[str, Any], contract: dict[str, Any]) -> ValidationResult:
    reasons: list[str] = []

    for field, expected_type in contract.get("required_fields", {}).items():
        if field not in event:
            reasons.append(f"missing required field: {field}")
            continue
        if not _check_field_type(event[field], expected_type):
            reasons.append(f"invalid type for field: {field}")

    accepted_event_names = set(contract.get("accepted_event_names", []))
    if accepted_event_names and event.get("event_name") not in accepted_event_names:
        reasons.append(f"event_name is not accepted: {event.get('event_name')}")

    payload = event.get("payload", {})
    if isinstance(payload, dict):
        for field, expected_type in contract.get("payload_required_fields", {}).items():
            if field not in payload:
                reasons.append(f"missing required payload field: {field}")
                continue
            if not _check_field_type(payload[field], expected_type):
                reasons.append(f"invalid type for payload field: {field}")

    return ValidationResult(
        accepted=len(reasons) == 0,
        event=event,
        rejection_reasons=tuple(reasons),
    )


def validate_batch(events: list[dict[str, Any]], contract: dict[str, Any]) -> dict[str, Any]:
    results = [validate_event(event, contract) for event in events]
    rejected = [result for result in results if not result.accepted]
    reason_counts: dict[str, int] = {}

    for result in rejected:
        for reason in result.rejection_reasons:
            reason_counts[reason] = reason_counts.get(reason, 0) + 1

    return {
        "contract_name": contract.get("contract_name", "unnamed_contract"),
        "total_records": len(results),
        "accepted_records": len(results) - len(rejected),
        "rejected_records": len(rejected),
        "rejection_reason_counts": dict(sorted(reason_counts.items())),
        "quarantine": [
            {
                "event_id": result.event.get("event_id"),
                "event_name": result.event.get("event_name"),
                "rejection_reasons": list(result.rejection_reasons),
            }
            for result in rejected
        ],
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate a JSONL event batch against a JSON contract.")
    parser.add_argument("--contract", required=True, help="Path to event_contract.json")
    parser.add_argument("--input", required=True, help="Path to input events.jsonl")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    summary = validate_batch(load_jsonl(args.input), load_json(args.contract))
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
