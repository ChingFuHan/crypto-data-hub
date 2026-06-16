import json
from pathlib import Path
import tempfile
import unittest

from datahub.validation.registry import validate_registry_file


class RegistryValidationTest(unittest.TestCase):
    def test_current_registry_passes(self):
        report = validate_registry_file("dataset_registry.json")
        self.assertFalse(report.has_failures, report.render())

    def test_invalid_registry_missing_required_field_fails(self):
        registry = json.loads(Path("dataset_registry.json").read_text(encoding="utf-8"))
        registry["datasets"][0].pop("dataset_id")
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "dataset_registry.json"
            path.write_text(json.dumps(registry), encoding="utf-8")
            report = validate_registry_file(path)
        failed_ids = {check.rule_id for check in report.error_summary}
        self.assertIn("REGISTRY-REQUIRED-FIELD", failed_ids)


if __name__ == "__main__":
    unittest.main()
