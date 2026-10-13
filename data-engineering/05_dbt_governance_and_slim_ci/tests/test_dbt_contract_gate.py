import copy
import sys
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from dbt_contract_gate import evaluate_registry, load_registry, validate_model, validate_sources


class DbtContractGateTests(unittest.TestCase):
    def setUp(self):
        self.manifest_path = PROJECT_ROOT / "sample_dbt_project" / "schema_manifest.json"
        self.registry = load_registry(self.manifest_path)

    def test_sample_production_registry_passes_with_100_percent_coverage(self):
        report = evaluate_registry(self.registry, min_coverage_pct=95.0)

        self.assertTrue(report.passed)
        self.assertEqual(report.total_models, 3)
        self.assertEqual(report.compliant_models, 3)
        self.assertEqual(report.trusted_coverage_pct, 100.0)
        self.assertEqual(report.violations, [])

    def test_missing_primary_key_tests_are_flagged(self):
        broken_model = copy.deepcopy(self.registry["models"][0])
        broken_model["columns"][0]["tests"] = ["not_null"]  # missing 'unique'

        violations = validate_model(broken_model)

        self.assertIn(
            "Model 'fct_otif_parcel_fulfillment' primary key 'parcel_fulfillment_sk' is missing required tests: unique",
            violations,
        )

    def test_unenforced_contract_and_missing_finops_tuning_on_fact_model_are_flagged(self):
        broken_model = copy.deepcopy(self.registry["models"][0])
        broken_model["contract_enforced"] = False
        broken_model["finops"] = {}

        violations = validate_model(broken_model)

        self.assertIn(
            "Certified model 'fct_otif_parcel_fulfillment' (marts) must enable contract_enforced=true",
            violations,
        )
        self.assertTrue(
            any("must define FinOps physical tuning" in v for v in violations),
            f"Expected FinOps violation in {violations}",
        )

    def test_invalid_source_freshness_sla_is_flagged(self):
        broken_sources = [
            {
                "name": "legacy_erp_orders",
                "freshness": {"warn_after_hours": 24, "error_after_hours": 12},
            }
        ]

        violations = validate_sources(broken_sources)

        self.assertIn(
            "Source 'legacy_erp_orders' freshness warn_after_hours (24) must be less than error_after_hours (12)",
            violations,
        )

    def test_coverage_below_threshold_fails_gate(self):
        mutated_registry = copy.deepcopy(self.registry)
        mutated_registry["models"][0]["contract_enforced"] = False

        report = evaluate_registry(mutated_registry, min_coverage_pct=95.0)

        self.assertFalse(report.passed)
        self.assertEqual(report.trusted_coverage_pct, 66.67)
        self.assertTrue(
            any("below SLA threshold 95.00%" in v for v in report.violations),
            f"Expected coverage threshold failure in {report.violations}",
        )


if __name__ == "__main__":
    unittest.main()
