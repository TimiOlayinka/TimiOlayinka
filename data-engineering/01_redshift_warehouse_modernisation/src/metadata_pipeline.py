"""Build a deterministic execution plan from pipeline metadata."""

from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any


TOKEN_PATTERN = re.compile(r"{{\s*([A-Za-z0-9_.]+)\s*}}")


@dataclass(frozen=True)
class Task:
    task_id: str
    task_type: str
    task_run_order: int
    command: str
    depends_on: tuple[str, ...]
    retry_count: int
    timeout_seconds: int
    params: dict[str, Any]


@dataclass(frozen=True)
class RunGroup:
    task_run_order: int
    tasks: tuple[Task, ...]


def load_config(path: str | Path) -> dict[str, Any]:
    with Path(path).open("r", encoding="utf-8") as config_file:
        return json.load(config_file)


def _task_from_config(task_id: str, raw_task: dict[str, Any], defaults: dict[str, Any]) -> Task:
    missing = [key for key in ("task_run_order", "task_type", "command") if key not in raw_task]
    if missing:
        raise ValueError(f"Task {task_id!r} is missing required keys: {', '.join(missing)}")

    depends_on = raw_task.get("depends_on", [])
    if isinstance(depends_on, str):
        depends_on = [depends_on]

    return Task(
        task_id=task_id,
        task_type=str(raw_task["task_type"]),
        task_run_order=int(raw_task["task_run_order"]),
        command=str(raw_task["command"]),
        depends_on=tuple(depends_on),
        retry_count=int(raw_task.get("retry_count", defaults.get("retry_count", 0))),
        timeout_seconds=int(raw_task.get("timeout_seconds", defaults.get("timeout_seconds", 0))),
        params=dict(raw_task.get("params", {})),
    )


def build_run_groups(config: dict[str, Any]) -> list[RunGroup]:
    raw_tasks = config.get("tasks", {})
    if not raw_tasks:
        raise ValueError("Pipeline config must define at least one task.")

    defaults = dict(config.get("defaults", {}))
    tasks = [_task_from_config(task_id, raw_task, defaults) for task_id, raw_task in raw_tasks.items()]
    task_by_id = {task.task_id: task for task in tasks}

    if len(task_by_id) != len(tasks):
        raise ValueError("Task IDs must be unique.")

    for task in tasks:
        unknown = sorted(set(task.depends_on) - set(task_by_id))
        if unknown:
            raise ValueError(f"Task {task.task_id!r} depends on unknown task(s): {', '.join(unknown)}")

        late_dependencies = [
            dep for dep in task.depends_on if task_by_id[dep].task_run_order >= task.task_run_order
        ]
        if late_dependencies:
            raise ValueError(
                f"Task {task.task_id!r} has dependencies not scheduled before it: "
                f"{', '.join(late_dependencies)}"
            )

    grouped: dict[int, list[Task]] = {}
    for task in tasks:
        grouped.setdefault(task.task_run_order, []).append(task)

    return [
        RunGroup(task_run_order=run_order, tasks=tuple(sorted(group, key=lambda item: item.task_id)))
        for run_order, group in sorted(grouped.items())
    ]


def render_command(command: str, context: dict[str, Any]) -> str:
    def replace_token(match: re.Match[str]) -> str:
        path = match.group(1).split(".")
        value: Any = context
        for key in path:
            if not isinstance(value, dict) or key not in value:
                raise KeyError(f"Template token {match.group(0)!r} could not be resolved.")
            value = value[key]
        return str(value)

    return TOKEN_PATTERN.sub(replace_token, command)


def build_execution_plan(config: dict[str, Any], execution_id: str) -> dict[str, Any]:
    groups = build_run_groups(config)
    pipeline = str(config.get("pipeline", "unnamed_pipeline"))

    rendered_groups = []
    for group in groups:
        rendered_tasks = []
        for task in group.tasks:
            context = {
                "execution_id": execution_id,
                "pipeline": pipeline,
                "task_id": task.task_id,
                "params": task.params,
            }
            rendered_tasks.append(
                {
                    "task_id": task.task_id,
                    "task_type": task.task_type,
                    "depends_on": list(task.depends_on),
                    "retry_count": task.retry_count,
                    "timeout_seconds": task.timeout_seconds,
                    "command": render_command(task.command, context),
                }
            )
        rendered_groups.append({"task_run_order": group.task_run_order, "tasks": rendered_tasks})

    return {"pipeline": pipeline, "execution_id": execution_id, "run_groups": rendered_groups}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build a metadata-driven pipeline execution plan.")
    parser.add_argument("--config", required=True, help="Path to pipeline_config.json")
    parser.add_argument("--execution-id", required=True, help="Execution identifier to inject into task commands.")
    parser.add_argument("--dry-run", action="store_true", help="Print the plan without executing commands.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    plan = build_execution_plan(load_config(args.config), args.execution_id)
    print(json.dumps(plan, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
