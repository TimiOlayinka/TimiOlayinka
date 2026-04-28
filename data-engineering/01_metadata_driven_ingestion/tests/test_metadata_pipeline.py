import unittest
from pathlib import Path
import sys


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from metadata_pipeline import build_execution_plan, build_run_groups, render_command


class MetadataPipelineTests(unittest.TestCase):
    def test_builds_ordered_run_groups(self):
        config = {
            "pipeline": "demo_pipeline",
            "defaults": {"retry_count": 1, "timeout_seconds": 60},
            "tasks": {
                "stage": {"task_run_order": 20, "task_type": "sql", "command": "stage"},
                "extract": {"task_run_order": 10, "task_type": "file", "command": "extract"},
            },
        }

        groups = build_run_groups(config)

        self.assertEqual([group.task_run_order for group in groups], [10, 20])
        self.assertEqual(groups[0].tasks[0].task_id, "extract")

    def test_rejects_unknown_dependency(self):
        config = {
            "tasks": {
                "stage": {
                    "task_run_order": 20,
                    "task_type": "sql",
                    "command": "stage",
                    "depends_on": ["missing"],
                }
            }
        }

        with self.assertRaisesRegex(ValueError, "unknown task"):
            build_run_groups(config)

    def test_renders_nested_command_tokens(self):
        command = "load --date {{ params.business_date }} --execution {{ execution_id }}"
        rendered = render_command(
            command,
            {"execution_id": "run_001", "params": {"business_date": "2026-04-29"}},
        )

        self.assertEqual(rendered, "load --date 2026-04-29 --execution run_001")

    def test_build_execution_plan_renders_commands(self):
        config = {
            "pipeline": "demo_pipeline",
            "tasks": {
                "extract": {
                    "task_run_order": 10,
                    "task_type": "file",
                    "command": "extract --run {{ execution_id }}",
                }
            },
        }

        plan = build_execution_plan(config, "run_001")

        self.assertEqual(plan["pipeline"], "demo_pipeline")
        self.assertEqual(plan["run_groups"][0]["tasks"][0]["command"], "extract --run run_001")


if __name__ == "__main__":
    unittest.main()
