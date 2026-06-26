"""Partial current-dataset symbol-missing hardening tests.

Scenario: the historical seed has a symbol but the current dataset does not.
That must be diagnosed as *partial current symbol missing* (repairable by
copying the seed symbol into current) rather than a historical
``bootstrap_required`` gap.

Covers:
* A. seed missing + current missing -> bootstrap_required
* B. seed exists + current missing -> initialize current symbol from seed
* C. current exists -> already_available, never overwritten / re-copied
* D. --run-startup-backfill-once initializes the current symbol before planning
* E. tempdir only (no real local_data writes)
* F. seed missing is never mistaken for a partial-current case
* G. a failed atomic copy leaves no half-written target dir

All tests use temp dirs and mock any REST access.
"""

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from datahub import live_update as lu

try:
    import pyarrow.parquet as pq
except ImportError:  # pragma: no cover
    pq = None


REPO_ROOT = Path(__file__).resolve().parents[1]
BASE_OT = 1638747600000  # 2021-12-06T00:00:00Z
ONE_MIN = lu.interval_milliseconds("1m")


def make_record(open_time, *, interval="1m", symbol="ETHUSDT", close=1.5):
    interval_ms = lu.interval_milliseconds(interval)
    return lu.KlineRecord.build(
        symbol=symbol,
        interval=interval,
        open_time=open_time,
        open=1.0,
        high=max(2.0, close),
        low=0.5,
        close=close,
        volume=10.0,
        close_time=open_time + interval_ms - 1,
        quote_volume=15.0,
        trade_count=3,
        taker_buy_base_volume=5.0,
        taker_buy_quote_volume=7.5,
        source_archive="seed",
        archive_source="seed",
        archive_period="2021-12-06",
    )


def rest_row(open_time, *, interval="1m", close=1.5):
    interval_ms = lu.interval_milliseconds(interval)
    return [
        open_time, "1.0", str(max(2.0, close)), "0.5", str(close), "10.0",
        open_time + interval_ms - 1, "15.0", 3, "5.0", "7.5", "0",
    ]


def make_paths(tmp):
    return lu.LiveUpdatePaths(
        repo_root=Path(tmp),
        seed_dataset_root=Path("seed"),
        current_dataset_root=Path("current"),
    )


def seed_writer(tmp):
    # Reuse the partition-merge writer to lay down a seed parquet by pointing the
    # "current" root at the seed location.
    return lu.LiveUpdatePaths(
        repo_root=Path(tmp),
        current_dataset_root=Path("seed"),
    )


def write_seed_symbol(tmp, *, symbol="ETHUSDT", interval="1m", open_times=(BASE_OT,)):
    writer = seed_writer(tmp)
    records = [make_record(ot, interval=interval, symbol=symbol) for ot in open_times]
    lu.merge_records_to_current_partition(records, writer)


@unittest.skipIf(pq is None, "pyarrow is required for current symbol tests")
class EnsureCurrentSymbolTests(unittest.TestCase):
    def test_seed_missing_and_current_missing_is_bootstrap_required(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = make_paths(tmp)
            result = lu.ensure_current_symbol_from_seed("1m", "ETHUSDT", paths)
            self.assertEqual(
                result.status, lu.CURRENT_SYMBOL_BOOTSTRAP_REQUIRED
            )
            self.assertFalse(result.current_symbol_root.exists())

    def test_seed_exists_current_missing_initializes_from_seed(self):
        with tempfile.TemporaryDirectory() as tmp:
            write_seed_symbol(tmp, open_times=(BASE_OT, BASE_OT + ONE_MIN))
            paths = make_paths(tmp)
            self.assertFalse(paths.current_parquet_root("1m").exists())

            result = lu.ensure_current_symbol_from_seed("1m", "ETHUSDT", paths)
            self.assertEqual(
                result.status, lu.CURRENT_SYMBOL_INITIALIZED_FROM_SEED
            )
            current_symbol_root = (
                paths.current_parquet_root("1m") / "symbol=ETHUSDT"
            )
            self.assertTrue(current_symbol_root.exists())
            self.assertTrue(any(current_symbol_root.rglob("*.parquet")))
            # Max open_time now resolvable from the current dataset.
            self.assertEqual(
                lu.max_open_time_from_current_dataset("1m", "ETHUSDT", paths),
                BASE_OT + ONE_MIN,
            )
            # No leftover temp dir.
            leftovers = [
                p
                for p in paths.current_parquet_root("1m").iterdir()
                if ".tmp-" in p.name
            ]
            self.assertEqual(leftovers, [])

    def test_current_exists_is_already_available_and_not_overwritten(self):
        with tempfile.TemporaryDirectory() as tmp:
            write_seed_symbol(tmp, open_times=(BASE_OT,))
            paths = make_paths(tmp)
            # Pre-populate current with a DIFFERENT close so we can detect an
            # accidental overwrite from the seed.
            lu.merge_records_to_current_partition(
                [make_record(BASE_OT, symbol="ETHUSDT", close=99.0)], paths
            )
            target = paths.current_partition_file(
                make_record(BASE_OT, symbol="ETHUSDT").partition_key()
            )
            rows_before = pq.ParquetFile(target).read().to_pylist()

            result = lu.ensure_current_symbol_from_seed("1m", "ETHUSDT", paths)
            self.assertEqual(
                result.status, lu.CURRENT_SYMBOL_ALREADY_AVAILABLE
            )
            rows_after = pq.ParquetFile(target).read().to_pylist()
            self.assertEqual(rows_before, rows_after)
            self.assertEqual(rows_after[0]["close"], 99.0)

    def test_only_requested_symbols_are_touched(self):
        with tempfile.TemporaryDirectory() as tmp:
            write_seed_symbol(tmp, symbol="ETHUSDT")
            write_seed_symbol(tmp, symbol="BTCUSDT")
            paths = make_paths(tmp)
            lu.ensure_current_symbols_from_seed("1m", ["ETHUSDT"], paths)
            current_root = paths.current_parquet_root("1m")
            self.assertTrue((current_root / "symbol=ETHUSDT").exists())
            self.assertFalse((current_root / "symbol=BTCUSDT").exists())

    def test_failed_copy_leaves_no_half_written_target(self):
        with tempfile.TemporaryDirectory() as tmp:
            write_seed_symbol(tmp, open_times=(BASE_OT,))
            paths = make_paths(tmp)
            current_symbol_root = (
                paths.current_parquet_root("1m") / "symbol=ETHUSDT"
            )
            with patch("shutil.copytree", side_effect=OSError("disk full")):
                with self.assertRaises(OSError):
                    lu.ensure_current_symbol_from_seed("1m", "ETHUSDT", paths)
            # Target never created; no leftover temp dirs that could be mistaken
            # for a complete current symbol.
            self.assertFalse(current_symbol_root.exists())
            parquet_root = paths.current_parquet_root("1m")
            if parquet_root.exists():
                self.assertEqual(
                    [p for p in parquet_root.iterdir() if ".tmp-" in p.name], []
                )


@unittest.skipIf(pq is None, "pyarrow is required for current symbol tests")
class StartupBackfillRepairsPartialCurrentTests(unittest.TestCase):
    def test_run_startup_backfill_once_initializes_then_backfills(self):
        with tempfile.TemporaryDirectory() as tmp:
            write_seed_symbol(tmp, open_times=(BASE_OT,))
            paths = make_paths(tmp)
            self.assertFalse(paths.current_parquet_root("1m").exists())

            now_ms = BASE_OT + 4 * ONE_MIN
            latest = lu.calculate_latest_closed_open_time("1m", now_ms)
            gap_rows = [
                rest_row(ot)
                for ot in range(BASE_OT + ONE_MIN, latest + ONE_MIN, ONE_MIN)
            ]
            fake_fetch = MagicMock()
            fake_fetch.status = "ok"
            fake_fetch.rows = gap_rows
            fake_fetch.error = None

            with patch.object(lu, "fetch_rest_klines", return_value=fake_fetch):
                results = lu.run_startup_backfill_once(
                    ("1m",),
                    ["ETHUSDT"],
                    paths,
                    now_ms=now_ms,
                    sleep_func=lambda _s: None,
                )
            self.assertEqual(len(results), 1)
            result = results[0]
            # Not bootstrap_required: the current symbol was initialized from seed.
            self.assertNotEqual(result.status, "bootstrap_required")
            self.assertIn(result.status, ("ok", "ok_with_warnings"))
            self.assertGreaterEqual(result.merged_row_count, 1)
            # Current dataset now reaches the latest closed bar.
            self.assertEqual(
                lu.max_open_time_from_current_dataset("1m", "ETHUSDT", paths),
                latest,
            )

    def test_run_startup_backfill_once_seed_missing_stays_bootstrap_required(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = make_paths(tmp)
            # No seed, no current. REST must not be called for a bootstrap symbol.
            with patch.object(
                lu, "fetch_rest_klines", side_effect=AssertionError("no REST")
            ):
                results = lu.run_startup_backfill_once(
                    ("1m",),
                    ["ETHUSDT"],
                    paths,
                    now_ms=BASE_OT + 4 * ONE_MIN,
                    sleep_func=lambda _s: None,
                )
            self.assertEqual(results[0].status, "bootstrap_required")
            self.assertFalse(
                (paths.current_parquet_root("1m") / "symbol=ETHUSDT").exists()
            )


@unittest.skipIf(pq is None, "pyarrow is required for current symbol CLI tests")
class InitializeCurrentDatasetCliTests(unittest.TestCase):
    def _run(self, tmp, extra):
        return subprocess.run(
            [
                sys.executable,
                "scripts/live_update.py",
                "--repo-root",
                tmp,
                "--seed-dataset-root",
                "seed",
                "--current-dataset-root",
                "current",
                "--interval",
                "1m",
                "--initialize-current-dataset",
                *extra,
            ],
            cwd=REPO_ROOT,
            text=True,
            capture_output=True,
            check=False,
            timeout=60,
        )

    def test_initialize_symbol_from_seed(self):
        with tempfile.TemporaryDirectory() as tmp:
            write_seed_symbol(tmp, open_times=(BASE_OT,))
            result = self._run(tmp, ["--symbols", "ETHUSDT"])
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            payload = json.loads(result.stdout)
            entry = payload["symbol_results"]["1m"][0]
            self.assertEqual(entry["symbol"], "ETHUSDT")
            self.assertEqual(
                entry["status"], lu.CURRENT_SYMBOL_INITIALIZED_FROM_SEED
            )

    def test_initialize_symbol_already_available(self):
        with tempfile.TemporaryDirectory() as tmp:
            write_seed_symbol(tmp, open_times=(BASE_OT,))
            self._run(tmp, ["--symbols", "ETHUSDT"])
            result = self._run(tmp, ["--symbols", "ETHUSDT"])
            payload = json.loads(result.stdout)
            entry = payload["symbol_results"]["1m"][0]
            self.assertEqual(
                entry["status"], lu.CURRENT_SYMBOL_ALREADY_AVAILABLE
            )

    def test_initialize_symbol_seed_missing_is_bootstrap_required(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = self._run(tmp, ["--symbols", "ETHUSDT"])
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            payload = json.loads(result.stdout)
            entry = payload["symbol_results"]["1m"][0]
            self.assertEqual(
                entry["status"], lu.CURRENT_SYMBOL_BOOTSTRAP_REQUIRED
            )


if __name__ == "__main__":
    unittest.main()
