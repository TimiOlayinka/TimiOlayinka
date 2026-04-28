"""Validate warehouse change-control manifest files."""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any


REQUIRED_FIELDS = {
    "change_id",
    "summary",
    "risk_level",
    "deployment_files",
    "update_scripts",
    "check_scripts",
    "rollback_note",
}
CHANGE_ID_PATTERN = re.compile(r"^[A-Z]+-\d+$")
DDL_PATTERN = re.compile(r"^deployments/ddls/[A-Z]+-\d+_ddls\.sql$")
UPDATE_PATTERN = re.compile(r"^maintenance/update_scripts/update-[a-z0-9_]+\.sql$")
CHECK_PATTERN = re.compile(r"^maintenance/check_scripts/check-[a-z0-9_]+\.sql$")


def load_manifest(path: str | Path) -> dict[str, Any]:
    with Path(path).open("r", encoding="utf-8") as file:
        return json.load(file)


def _validate_required_fields(manifest: dict[str, Any]) -> list[str]:
    missing = sorted(REQUIRED_FIELDS - set(manifest))
    return [f"Missing required field: {field}" for field in missing]


def _validate_non_empty_list(manifest: dict[str, Any], field: str) -> list[str]:
    value = manifest.get(field)
    if not isinstance(value, list) or not value:
        return [f"{field} must be a non-empty list"]
    if any(not isinstance(item, str) or not item.strip() for item in value):
        return [f"{field} must contain non-empty string paths"]
    return []


def _validate_pattern(paths: list[str], pattern: re.Pattern[str], field: str) -> list[str]:
    return [f"{field} path does not follow naming convention: {path}" for path in paths if not pattern.match(path)]


def _validate_files_exist(root: Path, paths: list[str], field: str) -> list[str]:
    errors = []
    for relative_path in paths:
        candidate = root / relative_path
        if not candidate.is_file():
            errors.append(f"{field} file does not exist: {relative_path}")
    return errors


def validate_manifest(root: str | Path, manifest: dict[str, Any]) -> list[str]:
    root_path = Path(root)
    errors = _validate_required_fields(manifest)

    change_id = str(manifest.get("change_id", ""))
    if not CHANGE_ID_PATTERN.match(change_id):
        errors.append("change_id must follow PREFIX-123 format")

    for field in ("deployment_files", "update_scripts", "check_scripts"):
        errors.extend(_validate_non_empty_list(manifest, field))

    deployment_files = list(manifest.get("deployment_files", []))
    update_scripts = list(manifest.get("update_scripts", []))
    check_scripts = list(manifest.get("check_scripts", []))

    errors.extend(_validate_pattern(deployment_files, DDL_PATTERN, "deployment_files"))
    errors.extend(_validate_pattern(update_scripts, UPDATE_PATTERN, "update_scripts"))
    errors.extend(_validate_pattern(check_scripts, CHECK_PATTERN, "check_scripts"))
    errors.extend(_validate_files_exist(root_path, deployment_files, "deployment_files"))
    errors.extend(_validate_files_exist(root_path, update_scripts, "update_scripts"))
    errors.extend(_validate_files_exist(root_path, check_scripts, "check_scripts"))

    rollback_note = str(manifest.get("rollback_note", "")).strip()
    if len(rollback_note) < 20:
        errors.append("rollback_note must describe a practical rollback decision")

    return errors


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate a warehouse change-control manifest.")
    parser.add_argument("--root", required=True, help="Project root used to resolve manifest file paths.")
    parser.add_argument("--manifest", required=True, help="Path to change_manifest.json")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    errors = validate_manifest(args.root, load_manifest(args.manifest))
    result = {"valid": not errors, "errors": errors}
    print(json.dumps(result, indent=2))
    return 0 if not errors else 1


if __name__ == "__main__":
    raise SystemExit(main())
