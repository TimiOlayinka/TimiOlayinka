# Warehouse Change Control

This project demonstrates a controlled pattern for warehouse changes: DDL files, update scripts, validation checks, and a manifest that ties the release together. The validator is intended to catch missing files and naming drift before a change is reviewed.

## Structure

```text
04_warehouse_change_control/
  deployments/
    ddls/
      DPLAT-104_ddls.sql
  maintenance/
    check_scripts/
      check-entity_snapshot.sql
    update_scripts/
      update-entity_snapshot.sql
  manifest/
    change_manifest.json
  src/
    change_manifest.py
  tests/
    test_change_manifest.py
```

## What It Demonstrates

- Release manifest validation before deployment review.
- SQL naming convention checks for DDL, update, and check scripts.
- A small warehouse object pattern with staged updates and post-change checks.
- Unit tests for required metadata, file existence, and naming rules.

## Run

```bash
python data-engineering/04_warehouse_change_control/src/change_manifest.py \
  --root data-engineering/04_warehouse_change_control \
  --manifest data-engineering/04_warehouse_change_control/manifest/change_manifest.json
```

The command exits with a non-zero status if the manifest is incomplete or references missing files.
