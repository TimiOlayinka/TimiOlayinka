# Data Engineering Projects

This folder contains four anonymized, self-contained projects. Each one is small enough to review quickly but complete enough to show how I structure production data engineering work.

## Projects

1. `01_metadata_driven_ingestion` builds a resolved execution plan from pipeline metadata.
2. `02_event_stream_quality_gate` validates incoming event payloads and produces quarantine metrics.
3. `03_sql_lineage_audit` extracts source-to-target dependencies from SQL files.
4. `04_warehouse_change_control` validates a release manifest covering DDL, update scripts, and check scripts.

## Review Path

Start with each project `README.md`, then read `src/` and `tests/` together. The SQL projects include deployable-style files under `sample_sql/`, `deployments/`, and `maintenance/`.
