import unittest

from datahub.validation.lifecycle import is_valid_transition, validate_lifecycle


class LifecycleValidationTest(unittest.TestCase):
    def test_transition_table(self):
        self.assertTrue(is_valid_transition("draft", "active"))
        self.assertTrue(is_valid_transition("deprecated", "active"))
        self.assertFalse(is_valid_transition("active", "archived"))
        self.assertFalse(is_valid_transition("archived", "active"))

    def test_current_draft_lifecycle_passes(self):
        report = validate_lifecycle("dataset_registry.json", ".")
        self.assertFalse(report.has_failures, report.render())
        rule_ids = {check.rule_id for check in report.checks}
        self.assertIn("LIFECYCLE-DRAFT-CONTRACT", rule_ids)
        self.assertIn("LIFECYCLE-DRAFT-CATALOG", rule_ids)


if __name__ == "__main__":
    unittest.main()
