import json
from pathlib import Path
import unittest

from datahub.validation import naming


class NamingValidationTest(unittest.TestCase):
    def test_basic_naming_helpers(self):
        self.assertTrue(naming.is_dataset_id("reference.universe.metadata"))
        self.assertFalse(naming.is_dataset_id("Reference.Universe.Metadata"))
        self.assertTrue(naming.is_semver("v0.4.0"))
        self.assertFalse(naming.is_semver("0.4.0"))
        self.assertTrue(naming.is_utc_timestamp("2026-06-16T00:00:00Z"))
        self.assertFalse(naming.is_utc_timestamp("2026-06-16T00:00:00"))

    def test_current_registry_naming_passes(self):
        registry = json.loads(Path("dataset_registry.json").read_text(encoding="utf-8"))
        report = naming.validate_registry_naming(registry)
        self.assertFalse(report.has_failures, report.render())


if __name__ == "__main__":
    unittest.main()
