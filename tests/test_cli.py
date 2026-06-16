from pathlib import Path
import subprocess
import sys
import unittest


class ValidationCliTest(unittest.TestCase):
    def run_cli(self, *args):
        return subprocess.run(
            [sys.executable, "-m", "datahub.validation", *args],
            cwd=Path(__file__).resolve().parents[1],
            text=True,
            capture_output=True,
            check=False,
        )

    def test_registry_success_exit_code_zero(self):
        result = self.run_cli("--target", "registry")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_universe_fixture_failure_exit_code_one(self):
        result = self.run_cli(
            "--target",
            "universe-metadata",
            "--fixture",
            "tests/fixtures/universe_metadata/duplicate_instrument_id.json",
        )
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)

    def test_invalid_invocation_exit_code_two(self):
        result = self.run_cli("--target", "universe-metadata")
        self.assertEqual(result.returncode, 2, result.stdout + result.stderr)


if __name__ == "__main__":
    unittest.main()
