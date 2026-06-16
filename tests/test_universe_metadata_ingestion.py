import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from datahub.ingestion import universe_metadata as ingestion
from datahub.validation.universe_metadata import validate_fixture


FIXTURE_DIR = Path("tests/fixtures/ingestion/universe_metadata")
RETRIEVED_AT = "2026-06-16T00:00:00Z"


def load_fixture(name):
    return json.loads((FIXTURE_DIR / name).read_text(encoding="utf-8"))


class UniverseMetadataIngestionTest(unittest.TestCase):
    def test_deterministic_instrument_id(self):
        payload = load_fixture("minimal_exchange_info.json")
        envelope = {
            "raw_response": payload,
            "retrieved_at": RETRIEVED_AT,
            "raw_response_checksum": ingestion.sha256_json(payload),
        }
        rows = ingestion.normalize_exchange_info(envelope)
        self.assertEqual(
            rows[0]["instrument_id"],
            "binance.usd_m_futures.perpetual.btcusdt.20190908",
        )

    def test_normalization_from_raw_fixture(self):
        payload = load_fixture("exchange_info_with_multiple_symbols.json")
        envelope = {
            "raw_response": payload,
            "retrieved_at": RETRIEVED_AT,
            "raw_response_checksum": ingestion.sha256_json(payload),
        }
        rows = ingestion.normalize_exchange_info(envelope)
        self.assertEqual(len(rows), 2)
        self.assertTrue(all(row["status"] == "active" for row in rows))
        self.assertTrue(all(row["contract_size"] == "1" for row in rows))

    def test_missing_optional_fields_normalize_as_nulls(self):
        payload = load_fixture("exchange_info_missing_optional_fields.json")
        envelope = {
            "raw_response": payload,
            "retrieved_at": RETRIEVED_AT,
            "raw_response_checksum": ingestion.sha256_json(payload),
        }
        rows = ingestion.normalize_exchange_info(envelope)
        self.assertIsNone(rows[0]["tick_size"])
        self.assertIsNone(rows[0]["step_size"])

    def test_manifest_generation_and_validation_integration(self):
        payload = load_fixture("minimal_exchange_info.json")
        with tempfile.TemporaryDirectory() as tmpdir:
            repo = Path(tmpdir)
            raw_path, _, _ = ingestion.write_raw_snapshot(
                payload, repo_root=repo, retrieved_at=RETRIEVED_AT
            )
            result = ingestion.normalize_from_snapshot(raw_path, repo_root=repo)
            manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
            self.assertEqual(manifest["dataset_id"], ingestion.DATASET_ID)
            self.assertEqual(manifest["validation_status"]["status"], "passed")
            self.assertEqual(manifest["row_count"], 1)
            report = validate_fixture(result.artifact_path)
            self.assertFalse(report.has_failures, report.render())

    def test_checksum_generation_is_stable(self):
        payload = load_fixture("minimal_exchange_info.json")
        first = ingestion.sha256_json(payload)
        second = ingestion.sha256_json(payload)
        self.assertEqual(first, second)

    def test_offline_mode_works_and_is_idempotent(self):
        payload = load_fixture("minimal_exchange_info.json")
        with tempfile.TemporaryDirectory() as tmpdir:
            repo = Path(tmpdir)
            raw_path, _, _ = ingestion.write_raw_snapshot(
                payload, repo_root=repo, retrieved_at=RETRIEVED_AT
            )
            first = ingestion.run_all(True, str(raw_path), repo_root=repo)
            artifact_first = first.artifact_path.read_text(encoding="utf-8")
            manifest_first = first.manifest_path.read_text(encoding="utf-8")
            second = ingestion.run_all(True, str(raw_path), repo_root=repo)
            self.assertEqual(artifact_first, second.artifact_path.read_text(encoding="utf-8"))
            self.assertEqual(manifest_first, second.manifest_path.read_text(encoding="utf-8"))

    def test_raw_snapshot_naming_and_reuse(self):
        payload = load_fixture("minimal_exchange_info.json")
        with tempfile.TemporaryDirectory() as tmpdir:
            repo = Path(tmpdir)
            first_path, _, first_reused = ingestion.write_raw_snapshot(
                payload, repo_root=repo, retrieved_at=RETRIEVED_AT
            )
            second_path, _, second_reused = ingestion.write_raw_snapshot(
                payload, repo_root=repo, retrieved_at="2026-06-16T00:05:00Z"
            )
            self.assertFalse(first_reused)
            self.assertTrue(second_reused)
            self.assertEqual(first_path, second_path)
            self.assertRegex(
                first_path.name,
                r"^exchange_info_20260616T000000Z_[0-9a-f]{12}\.json$",
            )

    def test_ingestion_cli_offline_success_exit_code_zero(self):
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "datahub.ingestion.universe_metadata",
                "--offline",
                "--all",
            ],
            cwd=Path(__file__).resolve().parents[1],
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_ingestion_cli_invalid_invocation_exit_code_two(self):
        result = subprocess.run(
            [sys.executable, "-m", "datahub.ingestion.universe_metadata"],
            cwd=Path(__file__).resolve().parents[1],
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(result.returncode, 2, result.stdout + result.stderr)


if __name__ == "__main__":
    unittest.main()
