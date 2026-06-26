"""Current layout migration dry-run / precheck tests.

The precheck is read-only: it reads parquet to report per-symbol layout status,
row counts, open_time range, duplicate counts, and the expected canonical
year/month partitions. It never moves, deletes, overwrites, or rewrites data and
never contacts Binance. ``--symbols all`` / omitted symbols here mean LOCAL
current dataset discovery, not exchange-wide resolution.

All tests use temp dirs; none write the repo's real local_data.
"""

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from datahub import live_update as lu

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError:  # pragma: no cover
    pa = None
    pq = None


REPO_ROOT = Path(__file__).resolve().parents[1]
BASE_OT = 1638747600000  # 2021-12-06T00:00:00Z (Taipei 2021-12)
JAN_OT = 1641340800000   # 2022-01-05T00:00:00Z (Taipei 2022-01)
ONE_MIN = lu.interval_milliseconds("1m")


def make_record(open_time, *, symbol="ETHUSDT", interval="1m", close=1.5):
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


def make_paths(tmp):
    return lu.LiveUpdatePaths(
        repo_root=Path(tmp),
        seed_dataset_root=Path("seed"),
        current_dataset_root=Path("current"),
    )


def write_year_only(root, symbol, records):
    """Legacy year-only layout: symbol=S/year=YYYY/part-000.parquet (one per year)."""
    by_year = {}
    for rec in records:
        by_year.setdefault(rec.year, []).append(rec)
    for year, recs in by_year.items():
        target = root / f"symbol={symbol}" / f"year={year}" / "part-000.parquet"
        target.parent.mkdir(parents=True, exist_ok=True)
        rows = [r.physical_dict() for r in sorted(recs, key=lambda r: r.open_time)]
        table = pa.table(
            {name: [row[name] for row in rows] for name in lu.CURRENT_PHYSICAL_COLUMNS},
            schema=lu._current_parquet_schema(),
        )
        pq.write_table(table, target)


def snapshot_files(root):
    return sorted(str(p) for p in Path(root).rglob("*") if p.is_file())


@unittest.skipIf(pq is None, "pyarrow required")
class PrecheckStatusTests(unittest.TestCase):
    def test_year_only_only_needs_migration(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = make_paths(tmp)
            write_year_only(
                paths.current_parquet_root("1m"), "ETHUSDT", [make_record(BASE_OT)]
            )
            pc = lu.precheck_symbol_layout_migration("1m", "ETHUSDT", paths)
            self.assertEqual(pc["status"], lu.CURRENT_LAYOUT_MIGRATION_YEAR_ONLY)
            self.assertEqual(pc["year_only_file_count"], 1)
            self.assertEqual(pc["year_month_file_count"], 0)
            self.assertIn("convert year-only", pc["recommended_action"])

    def test_mixed_needs_migration(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = make_paths(tmp)
            lu.merge_records_to_current_partition([make_record(BASE_OT)], paths)
            write_year_only(
                paths.current_parquet_root("1m"), "ETHUSDT", [make_record(JAN_OT)]
            )
            pc = lu.precheck_symbol_layout_migration("1m", "ETHUSDT", paths)
            self.assertEqual(pc["status"], lu.CURRENT_LAYOUT_MIGRATION_MIXED)
            self.assertGreaterEqual(pc["year_only_file_count"], 1)
            self.assertGreaterEqual(pc["year_month_file_count"], 1)
            action = pc["recommended_action"]
            self.assertIn("merge year-only + year/month", action)
            self.assertIn("deduplicate by open_time", action)
            self.assertIn("verify row count and continuity", action)

    def test_year_month_only_no_migration(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = make_paths(tmp)
            lu.merge_records_to_current_partition([make_record(BASE_OT)], paths)
            pc = lu.precheck_symbol_layout_migration("1m", "ETHUSDT", paths)
            self.assertEqual(pc["status"], lu.CURRENT_LAYOUT_MIGRATION_NONE)
            self.assertEqual(pc["year_only_file_count"], 0)
            self.assertEqual(pc["year_month_file_count"], 1)

    def test_expected_canonical_partitions_span_months(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = make_paths(tmp)
            # Year-only seed spanning Dec 2021 and Jan 2022.
            write_year_only(
                paths.current_parquet_root("1m"),
                "ETHUSDT",
                [make_record(BASE_OT), make_record(JAN_OT)],
            )
            pc = lu.precheck_symbol_layout_migration("1m", "ETHUSDT", paths)
            self.assertEqual(pc["expected_canonical_partition_count"], 2)
            partitions = {p["partition"] for p in pc["expected_canonical_partitions"]}
            self.assertEqual(partitions, {"year=2021/month=12", "year=2022/month=01"})

    def test_duplicate_open_time_counted(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = make_paths(tmp)
            # Two parquet files (different years) but include a duplicate open_time
            # via a raw second copy in the same year file.
            write_year_only(
                paths.current_parquet_root("1m"),
                "ETHUSDT",
                [make_record(BASE_OT), make_record(BASE_OT)],
            )
            pc = lu.precheck_symbol_layout_migration("1m", "ETHUSDT", paths)
            self.assertEqual(pc["row_count"], 2)
            self.assertEqual(pc["duplicate_open_time_count"], 1)
            self.assertEqual(pc["min_open_time"], BASE_OT)
            self.assertEqual(pc["max_open_time"], BASE_OT)


@unittest.skipIf(pq is None, "pyarrow required")
class PrecheckDiscoveryTests(unittest.TestCase):
    def test_omitted_symbols_scan_local_current_dataset(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = make_paths(tmp)
            write_year_only(
                paths.current_parquet_root("1m"), "BTCUSDT", [make_record(BASE_OT, symbol="BTCUSDT")]
            )
            write_year_only(
                paths.current_parquet_root("1m"), "ETHUSDT", [make_record(BASE_OT)]
            )
            result = lu.build_current_layout_migration_precheck("1m", [], paths)
            self.assertEqual(result["symbols"], ["BTCUSDT", "ETHUSDT"])
            self.assertEqual(result["status"], lu.CURRENT_LAYOUT_MIGRATION_YEAR_ONLY)
            self.assertEqual(
                result["symbols_needing_migration"], ["BTCUSDT", "ETHUSDT"]
            )

    def test_precheck_does_not_write(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = make_paths(tmp)
            write_year_only(
                paths.current_parquet_root("1m"), "ETHUSDT", [make_record(BASE_OT)]
            )
            before = snapshot_files(Path(tmp))
            lu.build_current_layout_migration_precheck("1m", ["ETHUSDT"], paths)
            self.assertEqual(snapshot_files(Path(tmp)), before)


@unittest.skipIf(pq is None, "pyarrow required")
class PlanCliTests(unittest.TestCase):
    def _run(self, tmp, extra):
        return subprocess.run(
            [
                sys.executable, "scripts/live_update.py",
                "--repo-root", tmp,
                "--seed-dataset-root", "seed",
                "--current-dataset-root", "current",
                "--interval", "1m",
                "--plan-current-layout-migration",
                *extra,
            ],
            cwd=REPO_ROOT,
            text=True,
            capture_output=True,
            check=False,
            timeout=60,
        )

    def test_cli_outputs_json_and_writes_nothing(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = make_paths(tmp)
            lu.merge_records_to_current_partition([make_record(BASE_OT)], paths)
            write_year_only(
                paths.current_parquet_root("1m"), "ETHUSDT", [make_record(JAN_OT)]
            )
            before = snapshot_files(Path(tmp))
            result = self._run(tmp, ["--symbols", "ETHUSDT"])
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            payload = json.loads(result.stdout)
            self.assertEqual(
                payload["overall_status"], lu.CURRENT_LAYOUT_MIGRATION_MIXED
            )
            entry = payload["interval_results"]["1m"]["prechecks"][0]
            self.assertEqual(entry["symbol"], "ETHUSDT")
            self.assertEqual(entry["status"], lu.CURRENT_LAYOUT_MIGRATION_MIXED)
            # Read-only: writes nothing.
            self.assertEqual(snapshot_files(Path(tmp)), before)

    def test_cli_omitted_symbols_no_network(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = make_paths(tmp)
            write_year_only(
                paths.current_parquet_root("1m"), "ETHUSDT", [make_record(BASE_OT)]
            )
            # No --symbols: must scan local current dataset, not hit exchangeInfo.
            result = self._run(tmp, [])
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            payload = json.loads(result.stdout)
            self.assertEqual(
                payload["interval_results"]["1m"]["symbols"], ["ETHUSDT"]
            )

    def test_cli_all_uses_local_discovery_not_exchange_info(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = make_paths(tmp)
            write_year_only(
                paths.current_parquet_root("1m"), "ETHUSDT", [make_record(BASE_OT)]
            )
            # --symbols all here is LOCAL discovery; point the REST base URL at an
            # unroutable host so any accidental exchangeInfo call would fail.
            result = subprocess.run(
                [
                    sys.executable, "scripts/live_update.py",
                    "--repo-root", tmp,
                    "--seed-dataset-root", "seed",
                    "--current-dataset-root", "current",
                    "--interval", "1m",
                    "--symbols", "all",
                    "--plan-current-layout-migration",
                    "--binance-rest-base-url", "http://127.0.0.1:9",
                    "--http-timeout", "2",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
                check=False,
                timeout=60,
            )
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            payload = json.loads(result.stdout)
            self.assertEqual(
                payload["interval_results"]["1m"]["symbols"], ["ETHUSDT"]
            )


if __name__ == "__main__":
    unittest.main()
