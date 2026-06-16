"""Tests for the Binance USD-M Futures Kline ingestion pipeline."""

import hashlib
import io
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
import zipfile

from datahub.ingestion import binance_um_klines as kl
from datahub.validation.binance_um_klines import validate_klines_manifest

REPO_ROOT = Path(__file__).resolve().parents[1]
FIXTURE_DIR = Path("tests/fixtures/ingestion/binance_um_klines")
SAMPLE_ROW = "1577836800000,1.0,2.0,0.5,1.5,10.0,1577923199999,15.0,3,5.0,7.5,0\n"


# --------------------------------------------------------------------------- #
# Fake archive builder
# --------------------------------------------------------------------------- #


def make_zip_bytes(member_name: str) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(member_name, SAMPLE_ROW)
    return buffer.getvalue()


def add_archive_file(root, source, symbol, interval, period, tamper=False):
    zip_name = f"{symbol}-{interval}-{period}.zip"
    csv_name = f"{symbol}-{interval}-{period}.csv"
    zip_bytes = make_zip_bytes(csv_name)
    directory = Path(root) / "data" / "futures" / "um" / source / "klines" / symbol / interval
    directory.mkdir(parents=True, exist_ok=True)
    (directory / zip_name).write_bytes(zip_bytes)
    digest = hashlib.sha256(zip_bytes).hexdigest()
    if tamper:
        digest = "0" * 64
    (directory / (zip_name + ".CHECKSUM")).write_text(
        f"{digest}  {zip_name}\n", encoding="utf-8"
    )


def build_fake_archive(root, *, tamper_bbb=False):
    # AAAUSDT: monthly 2020-01, 2020-02 + daily covered (2020-01-15) + daily delta (2020-03-05)
    add_archive_file(root, "monthly", "AAAUSDT", "1d", "2020-01")
    add_archive_file(root, "monthly", "AAAUSDT", "1d", "2020-02")
    add_archive_file(root, "daily", "AAAUSDT", "1d", "2020-01-15")
    add_archive_file(root, "daily", "AAAUSDT", "1d", "2020-03-05")
    # BBBUSDT: monthly only
    add_archive_file(root, "monthly", "BBBUSDT", "1d", "2020-01", tamper=tamper_bbb)


def make_config(tmp, **overrides):
    params = dict(
        interval="1d",
        local_root=Path(tmp) / "local_data" / "binance_um_klines",
        archive_source="both",
        include_full_daily_history=False,
        resume=False,
        dry_run=False,
        workers=4,
        timeout=5,
        retries=1,
        symbols_file=None,
        max_symbols=None,
    )
    params.update(overrides)
    return kl.Config(**params)


# --------------------------------------------------------------------------- #
# Unit tests — pure functions
# --------------------------------------------------------------------------- #


class IntervalTests(unittest.TestCase):
    def test_supported_intervals_constant(self):
        self.assertEqual(
            kl.ALLOWED_INTERVALS, ("1d", "4h", "1h", "15m", "5m", "1m")
        )

    def test_validate_interval_accepts_all_supported(self):
        for interval in ("1d", "4h", "1h", "15m", "5m", "1m"):
            self.assertEqual(kl.validate_interval(interval), interval)

    def test_validate_interval_rejects_unsupported(self):
        with self.assertRaises(kl.KlinesCommandError):
            kl.validate_interval("2d")

    def test_cli_unsupported_interval_exit_2_prints_allowed(self):
        result = subprocess.run(
            [sys.executable, "-m", "datahub.ingestion.binance_um_klines",
             "--interval", "2d", "--discover"],
            cwd=REPO_ROOT, text=True, capture_output=True, check=False,
        )
        self.assertEqual(result.returncode, 2, result.stdout + result.stderr)
        self.assertIn("1d 4h 1h 15m 5m 1m", result.stderr)


class ParsingTests(unittest.TestCase):
    def test_parse_archive_period_monthly_and_daily(self):
        self.assertEqual(
            kl.parse_archive_period("BTCUSDT-1d-2020-01.zip", "BTCUSDT", "1d"),
            "2020-01",
        )
        self.assertEqual(
            kl.parse_archive_period("BTCUSDT-1d-2020-01-05.zip", "BTCUSDT", "1d"),
            "2020-01-05",
        )

    def test_parse_archive_period_handles_interval_with_digit(self):
        self.assertEqual(
            kl.parse_archive_period("ETHUSDT-15m-2021-07.zip", "ETHUSDT", "15m"),
            "2021-07",
        )

    def test_parse_checksum_text(self):
        text = (FIXTURE_DIR / "sample.CHECKSUM").read_text(encoding="utf-8")
        self.assertEqual(
            kl.parse_checksum_text(text),
            "83f80bb879d556dc26e10bd8dca5d6a1d4484247dd1a536431fad767f7978268",
        )

    def test_verify_bytes_pass_and_fail(self):
        payload = b"hello-kline"
        digest = hashlib.sha256(payload).hexdigest()
        ok, expected, actual = kl.verify_bytes(payload, f"{digest}  x.zip\n")
        self.assertTrue(ok)
        self.assertEqual(expected, actual)
        bad, _, _ = kl.verify_bytes(payload, "deadbeef  x.zip\n")
        self.assertFalse(bad)

    def test_period_bounds(self):
        self.assertEqual(kl.period_bounds("2020-02"), ("2020-02-01", "2020-02-29"))
        self.assertEqual(kl.period_bounds("2021-07-09"), ("2021-07-09", "2021-07-09"))


class PathTests(unittest.TestCase):
    def test_local_path_generation_includes_interval(self):
        with tempfile.TemporaryDirectory() as tmp:
            for interval in ("1d", "1m"):
                config = make_config(tmp, interval=interval)
                self.assertIn(f"interval={interval}", str(config.interval_root))
                zip_path = config.raw_zip_path("monthly", "BTCUSDT", "BTCUSDT-1d-2020-01.zip")
                self.assertIn(f"interval={interval}", str(zip_path))
                self.assertIn("raw/monthly/BTCUSDT", str(zip_path))

    def test_variant_id(self):
        with tempfile.TemporaryDirectory() as tmp:
            config = make_config(tmp, interval="4h")
            self.assertEqual(config.variant_id, "market.binance.um.klines.4h")


class S3ParseTests(unittest.TestCase):
    def test_s3_listing_parse_contents(self):
        xml = (FIXTURE_DIR / "sample_s3_listing.xml").read_bytes()
        backend = kl.HttpArchiveBackend()
        backend._get = lambda url: xml  # type: ignore[method-assign]
        contents, prefixes = backend.list_objects("data/futures/um/monthly/klines/BTCUSDT/1d/")
        keys = {entry.key for entry in contents}
        self.assertIn("data/futures/um/monthly/klines/BTCUSDT/1d/BTCUSDT-1d-2020-01.zip", keys)
        self.assertEqual(prefixes, [])

    def test_s3_listing_parse_symbols(self):
        xml = (FIXTURE_DIR / "sample_symbols_listing.xml").read_bytes()
        backend = kl.HttpArchiveBackend()
        backend._get = lambda url: xml  # type: ignore[method-assign]
        symbols = kl.discover_symbols(backend, "monthly", "1d")
        self.assertEqual(symbols, ["BTCUSDT", "ETHUSDT", "SOLUSDT"])


# --------------------------------------------------------------------------- #
# Integration tests — full pipeline against a local archive emulation
# --------------------------------------------------------------------------- #


class DiscoveryTests(unittest.TestCase):
    def test_symbol_discovery_from_archive_index(self):
        with tempfile.TemporaryDirectory() as tmp:
            build_fake_archive(tmp)
            backend = kl.LocalArchiveBackend(tmp)
            self.assertEqual(
                sorted(kl.discover_symbols(backend, "monthly", "1d")),
                ["AAAUSDT", "BBBUSDT"],
            )

    def test_discover_classifies_daily_overlap_default(self):
        with tempfile.TemporaryDirectory() as tmp:
            build_fake_archive(tmp)
            backend = kl.LocalArchiveBackend(tmp)
            config = make_config(tmp)
            records, symbols = kl.discover(config, backend, "2026-06-16T00:00:00Z")
            self.assertEqual(symbols, ["AAAUSDT", "BBBUSDT"])
            by_period = {
                (r["archive_package_source"], r["archive_period"]): r
                for r in records
                if r["symbol"] == "AAAUSDT"
            }
            covered = by_period[("daily", "2020-01-15")]
            delta = by_period[("daily", "2020-03-05")]
            self.assertEqual(covered["daily_status"], kl.DAILY_SKIPPED_BY_DEFAULT)
            self.assertTrue(covered["covered_by_monthly"])
            self.assertEqual(covered["download_decision"], kl.SKIP)
            self.assertEqual(delta["daily_status"], kl.DAILY_REQUIRED_DELTA)
            self.assertFalse(delta["covered_by_monthly"])
            self.assertEqual(delta["download_decision"], kl.DOWNLOAD)

    def test_include_full_daily_history_flag(self):
        with tempfile.TemporaryDirectory() as tmp:
            build_fake_archive(tmp)
            backend = kl.LocalArchiveBackend(tmp)
            config = make_config(tmp, include_full_daily_history=True)
            records, _ = kl.discover(config, backend, "2026-06-16T00:00:00Z")
            covered = next(
                r for r in records
                if r["symbol"] == "AAAUSDT" and r["archive_period"] == "2020-01-15"
            )
            self.assertEqual(covered["daily_status"], kl.DAILY_INCLUDED_FULL)
            self.assertEqual(covered["download_decision"], kl.DOWNLOAD)

    def test_max_symbols_and_archive_source_monthly(self):
        with tempfile.TemporaryDirectory() as tmp:
            build_fake_archive(tmp)
            backend = kl.LocalArchiveBackend(tmp)
            config = make_config(tmp, archive_source="monthly", max_symbols=1)
            records, symbols = kl.discover(config, backend, "2026-06-16T00:00:00Z")
            self.assertEqual(symbols, ["AAAUSDT"])
            self.assertTrue(all(r["archive_package_source"] == "monthly" for r in records))


class PipelineTests(unittest.TestCase):
    def _run_all(self, tmp, **overrides):
        build_fake_archive(tmp, tamper_bbb=overrides.pop("tamper_bbb", False))
        backend = kl.LocalArchiveBackend(tmp)
        config = make_config(tmp, **overrides)
        return kl.run_pipeline(config, backend, {"all"}, now="2026-06-16T00:00:00Z"), config

    def test_all_downloads_and_verifies(self):
        with tempfile.TemporaryDirectory() as tmp:
            result, config = self._run_all(tmp)
            manifest = result.manifest
            # AAAUSDT: 2 monthly + 1 daily delta downloaded, 1 daily covered skipped.
            # BBBUSDT: 1 monthly. Total downloaded = 4, skipped overlap = 1.
            self.assertEqual(manifest["downloaded_count"], 4)
            self.assertEqual(manifest["verified_count"], 4)
            self.assertEqual(manifest["checksum_failed_count"], 0)
            self.assertEqual(manifest["skipped_daily_overlap_count"], 1)
            self.assertEqual(manifest["file_count"], 5)
            # downloaded zips exist on disk
            for record in kl.load_jsonl(config.manifest_dir / "files.jsonl"):
                if record["download_status"] == kl.DL_DOWNLOADED:
                    self.assertTrue(Path(record["local_zip_path"]).exists())

    def test_resume_rerun_skips_verified_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            self._run_all(tmp)
            backend = kl.LocalArchiveBackend(tmp)
            config = make_config(tmp, resume=True)
            result = kl.run_pipeline(config, backend, {"all"}, now="2026-06-16T01:00:00Z")
            self.assertEqual(result.manifest["downloaded_count"], 0)
            self.assertEqual(result.manifest["verified_count"], 4)
            self.assertEqual(result.manifest["skipped_count"], 5)  # 4 verified + 1 overlap

    def test_skip_existing_verified_single_record(self):
        with tempfile.TemporaryDirectory() as tmp:
            self._run_all(tmp)
            backend = kl.LocalArchiveBackend(tmp)
            config = make_config(tmp)
            records, _ = kl.discover(config, backend, "2026-06-16T02:00:00Z")
            monthly = next(
                r for r in records
                if r["symbol"] == "BBBUSDT" and r["archive_package_source"] == "monthly"
            )
            processed = kl.process_record(config, backend, monthly)
            self.assertEqual(processed["checksum_status"], kl.CHK_SKIPPED_EXISTING)
            self.assertEqual(processed["download_status"], kl.DL_SKIPPED_EXISTING)

    def test_checksum_mismatch_detected(self):
        with tempfile.TemporaryDirectory() as tmp:
            result, config = self._run_all(tmp, tamper_bbb=True)
            self.assertEqual(result.manifest["checksum_failed_count"], 1)
            failures = kl.load_jsonl(config.reports_dir / "checksum_failures.jsonl")
            self.assertEqual(len(failures), 1)
            self.assertEqual(failures[0]["symbol"], "BBBUSDT")

    def test_daily_recent_delta_default_does_not_download_covered(self):
        with tempfile.TemporaryDirectory() as tmp:
            result, config = self._run_all(tmp)
            files = {
                (r["archive_period"]): r
                for r in kl.load_jsonl(config.manifest_dir / "files.jsonl")
                if r["symbol"] == "AAAUSDT" and r["archive_package_source"] == "daily"
            }
            self.assertEqual(files["2020-01-15"]["download_status"], kl.DL_SKIPPED_OVERLAP)
            self.assertEqual(files["2020-03-05"]["download_status"], kl.DL_DOWNLOADED)

    def test_manifest_generation_fields(self):
        with tempfile.TemporaryDirectory() as tmp:
            result, config = self._run_all(tmp)
            manifest = json.loads((config.manifest_dir / "manifest.json").read_text())
            for field in (
                "dataset_id", "dataset_variant_id", "interval", "local_root",
                "run_id", "symbol_count", "file_count", "downloaded_count",
                "verified_count", "skipped_count", "failed_count",
                "checksum_failed_count", "missing_count", "total_bytes",
                "date_min", "date_max", "archive_package_sources",
                "include_full_daily_history", "daily_delta_policy",
                "validation_summary", "primary_key",
            ):
                self.assertIn(field, manifest)
            self.assertEqual(manifest["dataset_id"], "market.binance.um.klines")
            self.assertEqual(manifest["dataset_variant_id"], "market.binance.um.klines.1d")
            self.assertEqual(manifest["primary_key"], ["symbol", "interval", "open_time"])

    def test_coverage_report_generation(self):
        with tempfile.TemporaryDirectory() as tmp:
            result, config = self._run_all(tmp)
            coverage = json.loads((config.reports_dir / "coverage_summary.json").read_text())
            for field in (
                "interval", "discovered_symbol_count", "monthly_archive_symbol_count",
                "daily_archive_symbol_count", "total_archive_file_count",
                "verified_file_count", "failed_file_count", "checksum_failed_count",
                "missing_count", "skipped_daily_overlap_count", "date_min",
                "date_max", "known_gaps",
            ):
                self.assertIn(field, coverage)
            self.assertEqual(coverage["discovered_symbol_count"], 2)
            self.assertTrue((config.reports_dir / "missing_files.jsonl").exists())
            self.assertTrue((config.reports_dir / "run_summary.md").exists())

    def test_research_access_manifest_generation(self):
        with tempfile.TemporaryDirectory() as tmp:
            result, config = self._run_all(tmp)
            research = json.loads((config.catalog_dir / "research_access.json").read_text())
            for field in (
                "dataset_id", "interval", "supported_intervals", "primary_key",
                "local_root", "manifest_path", "file_manifest_path", "raw_layout",
                "schema", "warning", "current_universe_warning",
            ):
                self.assertIn(field, research)
            self.assertEqual(research["supported_intervals"], list(kl.ALLOWED_INTERVALS))
            self.assertIn("not committed", research["warning"].lower())

    def test_dry_run_writes_nothing(self):
        with tempfile.TemporaryDirectory() as tmp:
            build_fake_archive(tmp)
            backend = kl.LocalArchiveBackend(tmp)
            config = make_config(tmp, dry_run=True)
            result = kl.run_pipeline(config, backend, {"discover"}, now="2026-06-16T00:00:00Z")
            self.assertIsNone(result.manifest)
            self.assertFalse(config.catalog_dir.exists())


class ValidationIntegrationTests(unittest.TestCase):
    def test_validation_integration_passes(self):
        with tempfile.TemporaryDirectory() as tmp:
            build_fake_archive(tmp)
            backend = kl.LocalArchiveBackend(tmp)
            config = make_config(tmp)
            kl.run_pipeline(config, backend, {"all"}, now="2026-06-16T00:00:00Z")
            report = validate_klines_manifest(
                config.manifest_dir / "manifest.json", "1d", REPO_ROOT
            )
            self.assertFalse(report.has_failures, report.render())

    def test_validation_detects_checksum_failure(self):
        with tempfile.TemporaryDirectory() as tmp:
            build_fake_archive(tmp, tamper_bbb=True)
            backend = kl.LocalArchiveBackend(tmp)
            config = make_config(tmp)
            kl.run_pipeline(config, backend, {"all"}, now="2026-06-16T00:00:00Z")
            report = validate_klines_manifest(
                config.manifest_dir / "manifest.json", "1d", REPO_ROOT
            )
            self.assertTrue(report.has_failures)

    def test_validation_interval_mismatch_fails(self):
        with tempfile.TemporaryDirectory() as tmp:
            build_fake_archive(tmp)
            backend = kl.LocalArchiveBackend(tmp)
            config = make_config(tmp)
            kl.run_pipeline(config, backend, {"all"}, now="2026-06-16T00:00:00Z")
            report = validate_klines_manifest(
                config.manifest_dir / "manifest.json", "4h", REPO_ROOT
            )
            self.assertTrue(report.has_failures)

    def test_validation_target_requires_manifest_exit_2(self):
        result = subprocess.run(
            [sys.executable, "-m", "datahub.validation",
             "--target", "binance-um-klines", "--interval", "1d"],
            cwd=REPO_ROOT, text=True, capture_output=True, check=False,
        )
        self.assertEqual(result.returncode, 2, result.stdout + result.stderr)

    def test_validation_all_remains_clone_safe(self):
        result = subprocess.run(
            [sys.executable, "-m", "datahub.validation", "--all"],
            cwd=REPO_ROOT, text=True, capture_output=True, check=False,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)


class RepoPolicyTests(unittest.TestCase):
    def test_gitignore_contains_local_data(self):
        gitignore = (REPO_ROOT / ".gitignore").read_text(encoding="utf-8")
        entries = {line.strip() for line in gitignore.splitlines()}
        self.assertIn("local_data/", entries)


if __name__ == "__main__":
    unittest.main()
