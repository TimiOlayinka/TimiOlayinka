"""Run all portfolio project tests without relying on package discovery."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
TEST_FILES = [
    "data-engineering/01_redshift_warehouse_modernisation/tests/test_metadata_pipeline.py",
    "data-engineering/02_gcp_realtime_pipelines/tests/test_event_quality_gate.py",
    "data-engineering/03_global_logistics_analytics/tests/test_sql_lineage_audit.py",
    "data-engineering/04_enterprise_data_quality/tests/test_change_manifest.py",
]


def main() -> int:
    failed = []
    for test_file in TEST_FILES:
        print(f"\n==> {test_file}")
        result = subprocess.run([sys.executable, test_file], cwd=REPO_ROOT)
        if result.returncode != 0:
            failed.append(test_file)

    if failed:
        print("\nFailed test files:")
        for test_file in failed:
            print(f"- {test_file}")
        return 1

    print("\nAll project tests passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
