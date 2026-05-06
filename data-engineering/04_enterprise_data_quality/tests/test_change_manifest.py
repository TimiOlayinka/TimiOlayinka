import unittest
from pathlib import Path
import sys


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from change_manifest import load_manifest, validate_manifest


class ChangeManifestTests(unittest.TestCase):
    def test_valid_manifest_has_no_errors(self):
        manifest = load_manifest(PROJECT_ROOT / "manifest" / "change_manifest.json")

        errors = validate_manifest(PROJECT_ROOT, manifest)

        self.assertEqual(errors, [])

    def test_missing_required_fields_are_reported(self):
        errors = validate_manifest(PROJECT_ROOT, {"change_id": "DPLAT-105"})

        self.assertIn("Missing required field: summary", errors)
        self.assertIn("deployment_files must be a non-empty list", errors)

    def test_invalid_naming_conventions_are_reported(self):
        manifest = {
            "change_id": "bad-id",
            "summary": "Example",
            "risk_level": "low",
            "deployment_files": ["deployments/ddls/bad.sql"],
            "update_scripts": ["maintenance/update_scripts/entity_snapshot.sql"],
            "check_scripts": ["maintenance/check_scripts/entity_snapshot.sql"],
            "rollback_note": "This rollback note is long enough to pass the length check.",
        }

        errors = validate_manifest(PROJECT_ROOT, manifest)

        self.assertIn("change_id must follow PREFIX-123 format", errors)
        self.assertIn(
            "deployment_files path does not follow naming convention: deployments/ddls/bad.sql",
            errors,
        )
        self.assertIn(
            "update_scripts path does not follow naming convention: maintenance/update_scripts/entity_snapshot.sql",
            errors,
        )
        self.assertIn(
            "check_scripts path does not follow naming convention: maintenance/check_scripts/entity_snapshot.sql",
            errors,
        )

    def test_missing_referenced_files_are_reported(self):
        manifest = load_manifest(PROJECT_ROOT / "manifest" / "change_manifest.json")
        manifest["check_scripts"] = ["maintenance/check_scripts/check-missing.sql"]

        errors = validate_manifest(PROJECT_ROOT, manifest)

        self.assertIn("check_scripts file does not exist: maintenance/check_scripts/check-missing.sql", errors)


if __name__ == "__main__":
    unittest.main()
