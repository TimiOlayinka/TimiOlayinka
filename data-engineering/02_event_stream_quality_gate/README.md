# Event Stream Quality Gate

This project demonstrates a small quality gate for event-style payloads before they are written to a raw analytical store. It validates required fields, applies simple type checks, and separates accepted records from quarantined records with clear rejection reasons.

## Structure

```text
02_event_stream_quality_gate/
  config/
    event_contract.json
  sample_payloads/
    events.jsonl
  src/
    event_quality_gate.py
  tests/
    test_event_quality_gate.py
```

## What It Demonstrates

- Contract-led validation for streaming or micro-batch payloads.
- Deterministic quarantine output for invalid records.
- Summary metrics that can be pushed to monitoring.
- A compact CLI that can validate local JSONL files without external services.

## Run

```bash
python data-engineering/02_event_stream_quality_gate/src/event_quality_gate.py \
  --contract data-engineering/02_event_stream_quality_gate/config/event_contract.json \
  --input data-engineering/02_event_stream_quality_gate/sample_payloads/events.jsonl
```

The script prints validation metrics and rejected records as JSON.
