# SQL Lineage Audit

This project scans SQL files and extracts table-to-table lineage edges. It is designed as a portable audit utility for warehouse repositories where teams need to understand upstream dependencies before changing a model.

## Structure

```text
03_sql_lineage_audit/
  sample_sql/
    publish_entity_metrics.sql
    transform_entity_metrics.sql
  src/
    sql_lineage_audit.py
  tests/
    test_sql_lineage_audit.py
```

## What It Demonstrates

- Static SQL scanning for `CREATE`, `INSERT`, `FROM`, and `JOIN` dependencies.
- CTE filtering so internal query aliases are not reported as physical tables.
- JSON output that can be loaded into a governance table or reviewed in CI.
- Unit tests for comments, quoted identifiers, CTEs, and multi-file scanning.

## Run

```bash
python data-engineering/03_sql_lineage_audit/src/sql_lineage_audit.py \
  --root data-engineering/03_sql_lineage_audit/sample_sql
```

The script prints a sorted JSON list of lineage edges.
