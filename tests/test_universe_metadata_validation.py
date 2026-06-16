from pathlib import Path
import unittest

from datahub.validation.universe_metadata import validate_fixture


FIXTURE_DIR = Path("tests/fixtures/universe_metadata")


class UniverseMetadataValidationTest(unittest.TestCase):
    def test_valid_fixture_passes(self):
        report = validate_fixture(FIXTURE_DIR / "valid_universe_metadata.json")
        self.assertFalse(report.has_failures, report.render())

    def test_invalid_fixtures_fail_with_expected_rule_ids(self):
        cases = {
            "duplicate_instrument_id.json": "UM-Q2",
            "invalid_timestamp_order.json": "UM-Q4",
            "invalid_active_delisted_invariant.json": "UM-Q3",
            "invalid_successor_reference.json": "UM-Q6",
            "cyclic_successor_graph.json": "UM-Q6",
            "broken_point_in_time_reconstruction.json": "UM-PIT",
        }
        for filename, expected_rule_id in cases.items():
            with self.subTest(filename=filename):
                report = validate_fixture(FIXTURE_DIR / filename)
                failed_ids = {check.rule_id for check in report.error_summary}
                self.assertIn(expected_rule_id, failed_ids, report.render())


if __name__ == "__main__":
    unittest.main()
