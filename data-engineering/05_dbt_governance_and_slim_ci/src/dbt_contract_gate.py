"""Validate dbt model governance contracts, FinOps physical tuning, and trusted data coverage SLAs."""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


GOVERNED_LAYERS = {"odl", "adl", "marts"}
CERTIFIED_LAYERS = {"adl", "marts"}
FACT_PREFIXES = ("fact_", "fct_")
REQUIRED_PK_TESTS = {"unique", "not_null"}


@dataclass(frozen=True)
class AuditReport:
    total_models: int
    compliant_models: int
    trusted_coverage_pct: float
    min_coverage_pct: float
    passed: bool
    violations: list[str]


def load_registry(path: str | Path) -> dict[str, Any]:
    with Path(path).open("r", encoding="utf-8") as handle:
        return json.load(handle)


def validate_sources(sources: list[dict[str, Any]]) -> list[str]:
    violations: list[str] = []
    for source in sources:
        name = str(source.get("name", "<unnamed_source>"))
        freshness = source.get("freshness") or {}
        warn_hours = freshness.get("warn_after_hours")
        error_hours = freshness.get("error_after_hours")
        if not isinstance(warn_hours, (int, float)) or warn_hours <= 0:
            violations.append(f"Source '{name}' is missing a valid freshness.warn_after_hours SLA")
        if not isinstance(error_hours, (int, float)) or error_hours <= 0:
            violations.append(f"Source '{name}' is missing a valid freshness.error_after_hours SLA")
        if (
            isinstance(warn_hours, (int, float))
            and isinstance(error_hours, (int, float))
            and warn_hours >= error_hours
        ):
            violations.append(
                f"Source '{name}' freshness warn_after_hours ({warn_hours}) must be less than error_after_hours ({error_hours})"
            )
    return violations


def _has_finops_tuning(finops: dict[str, Any]) -> bool:
    has_bigquery = bool(finops.get("partition_by")) and bool(finops.get("cluster_by"))
    has_redshift = bool(finops.get("dist_key")) and bool(finops.get("sort_keys"))
    has_snowflake = bool(finops.get("cluster_by"))
    return has_bigquery or has_redshift or has_snowflake


def validate_model(model: dict[str, Any]) -> list[str]:
    violations: list[str] = []
    name = str(model.get("name", "<unnamed_model>"))
    layer = str(model.get("layer", "")).lower()
    materialized = str(model.get("materialized", "")).lower()
    columns = model.get("columns") or []

    if not str(model.get("description", "")).strip():
        violations.append(f"Model '{name}' is missing a model-level description")

    if layer in GOVERNED_LAYERS:
        pk_columns = [col for col in columns if col.get("is_primary_key")]
        if not pk_columns:
            violations.append(f"Model '{name}' ({layer}) must declare a primary key column")
        else:
            for pk_col in pk_columns:
                col_name = pk_col.get("name", "<unnamed_col>")
                tests = set(pk_col.get("tests") or [])
                missing_tests = sorted(REQUIRED_PK_TESTS - tests)
                if missing_tests:
                    violations.append(
                        f"Model '{name}' primary key '{col_name}' is missing required tests: {', '.join(missing_tests)}"
                    )

    if layer in CERTIFIED_LAYERS:
        if not model.get("contract_enforced", False):
            violations.append(f"Certified model '{name}' ({layer}) must enable contract_enforced=true")
        if not str(model.get("owner", "")).strip():
            violations.append(f"Certified model '{name}' ({layer}) must specify an owner")
        for col in columns:
            col_name = str(col.get("name", "<unnamed_col>"))
            if not str(col.get("data_type", "")).strip():
                violations.append(f"Certified model '{name}' column '{col_name}' is missing an explicit data_type")
            if not str(col.get("description", "")).strip():
                violations.append(f"Certified model '{name}' column '{col_name}' is missing a description")

    if name.startswith(FACT_PREFIXES) and materialized in {"incremental", "table"}:
        finops = model.get("finops") or {}
        if not _has_finops_tuning(finops):
            violations.append(
                f"Fact model '{name}' ({materialized}) must define FinOps physical tuning "
                "(BigQuery partition_by+cluster_by or Redshift dist_key+sort_keys)"
            )

    return violations


def evaluate_registry(registry: dict[str, Any], min_coverage_pct: float = 95.0) -> AuditReport:
    sources = list(registry.get("sources") or [])
    models = list(registry.get("models") or [])

    violations = validate_sources(sources)
    compliant_models = 0

    for model in models:
        model_violations = validate_model(model)
        if not model_violations:
            compliant_models += 1
        violations.extend(model_violations)

    total_models = len(models)
    trusted_coverage_pct = round((compliant_models / total_models) * 100.0, 2) if total_models else 0.0
    if trusted_coverage_pct < min_coverage_pct:
        violations.append(
            f"Trusted data coverage {trusted_coverage_pct:.2f}% is below SLA threshold {min_coverage_pct:.2f}%"
        )

    passed = len(violations) == 0 and trusted_coverage_pct >= min_coverage_pct
    return AuditReport(
        total_models=total_models,
        compliant_models=compliant_models,
        trusted_coverage_pct=trusted_coverage_pct,
        min_coverage_pct=min_coverage_pct,
        passed=passed,
        violations=violations,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the dbt governance, FinOps, and data contract quality gate.")
    parser.add_argument("--registry", required=True, help="Path to schema_manifest.json")
    parser.add_argument("--min-coverage", type=float, default=95.0, help="Minimum trusted coverage percentage SLA")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    report = evaluate_registry(load_registry(args.registry), min_coverage_pct=args.min_coverage)
    print(json.dumps(asdict(report), indent=2))
    return 0 if report.passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
