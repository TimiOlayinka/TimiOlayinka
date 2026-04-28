# Metadata Driven Ingestion

This project shows a lightweight orchestration pattern for file, SQL, API, and quality-check tasks. The aim is to keep run order, retry policy, dependencies, and runtime parameters in metadata so new pipelines can be added without changing orchestration code.

## Structure

```text
01_metadata_driven_ingestion/
  config/
    pipeline_config.json
  src/
    metadata_pipeline.py
  tests/
    test_metadata_pipeline.py
```

## What It Demonstrates

- Config-first pipeline design with task groups and dependencies.
- Deterministic run-plan generation for sequential and parallel task stages.
- Lightweight command templating for execution IDs and task parameters.
- Unit tests for dependency validation and command rendering.

## Run

```bash
python data-engineering/01_metadata_driven_ingestion/src/metadata_pipeline.py \
  --config data-engineering/01_metadata_driven_ingestion/config/pipeline_config.json \
  --execution-id demo_20260429 \
  --dry-run
```

The command prints the resolved execution plan as JSON. It does not connect to any external platform.
