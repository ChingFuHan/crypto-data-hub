"""Current dataset partition layout governance tests.

Canonical current layout is year/month:

    current/interval=<I>/parquet/symbol=<S>/year=<YYYY>/month=<MM>/part-000.parquet

Covers:
* initialize-from-seed converts a year-only seed into year/month (no year-only
  parquet left in current), preserving rows and partitioning by open_time month
* current exists -> not overwritten / not re-converted
* failed conversion leaves no half-written target (see
  tests/test_live_update_current_symbol.py)
* audit_current_partition_layout detects year-only / year-month / mixed
* --audit-current-layout CLI outputs JSON and writes nothing

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
BASE_OT = 1638747600000  # 2021-12-06T00:00:00Z (Taipei 2021-12, month 12)
JAN_OT = 1641340800000   # 2022-01-05T00:00:00Z (Taipei 2022-01, month 01)
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
    """Write a legacy year-only layout: symbol=S/year=YYYY/part-000.parquet."""
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
class InitializeConvertsLayoutTests(unittest.TestCase):
    def test_year_only_seed_initializes_as_year_month(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = make_paths(tmp)
            write_year_only(
                paths.seed_parquet_root("1m"),
                "ETHUSDT",
                [make_record(BASE_OT), make_record(BASE_OT + ONE_MIN)],
            )
            result = lu.ensure_current_symbol_from_seed("1m", "ETHUSDT", paths)
            self.assertEqual(
                result.status, lu.CURRENT_SYMBOL_INITIALIZED_FROM_SEED
            )
            symbol_root = paths.current_parquet_root("1m") / "symbol=ETHUSDT"
            files = [
                (p, "month=" in str(p.relative_to(symbol_root)))
                for p in symbol_root.rglob("*.parquet")
            ]
            self.assertTrue(files)
            # Every current file is year/month; no year-only file remains.
            self.assertTrue(all(has_month for _p, has_month in files))
            # Explicitly: no symbol=ETHUSDT/year=2021/part-000.parquet
            self.assertFalse(
                (symbol_root / "year=2021" / "part-000.parquet").exists()
            )
            self.assertTrue(
                (symbol_root / "year=2021" / "month=12" / "part-000.parquet").exists()
            )

    def test_conversion_preserves_row_count(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = make_paths(tmp)
            records = [make_record(BASE_OT + i * ONE_MIN) for i in range(5)]
            write_year_only(paths.seed_parquet_root("1m"), "ETHUSDT", records)
            lu.ensure_current_symbol_from_seed("1m", "ETHUSDT", paths)
            rows = lu.read_current_symbol_open_times("1m", "ETHUSDT", paths)
            self.assertEqual(len(rows), 5)

    def test_conversion_partitions_by_open_time_month(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = make_paths(tmp)
            write_year_only(
                paths.seed_parquet_root("1m"),
                "ETHUSDT",
                [make_record(BASE_OT), make_record(JAN_OT)],
            )
            lu.ensure_current_symbol_from_seed("1m", "ETHUSDT", paths)
            symbol_root = paths.current_parquet_root("1m") / "symbol=ETHUSDT"
            self.assertTrue(
                (symbol_root / "year=2021" / "month=12" / "part-000.parquet").exists()
            )
            self.assertTrue(
                (symbol_root / "year=2022" / "month=01" / "part-000.parquet").exists()
            )

    def test_current_exists_not_reconverted(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = make_paths(tmp)
            write_year_only(paths.seed_parquet_root("1m"), "ETHUSDT", [make_record(BASE_OT)])
            lu.merge_records_to_current_partition(
                [make_record(BASE_OT, close=99.0)], paths
            )
            target = paths.current_partition_file(make_record(BASE_OT).partition_key())
            before = pq.ParquetFile(target).read().to_pylist()
            result = lu.ensure_current_symbol_from_seed("1m", "ETHUSDT", paths)
            self.assertEqual(result.status, lu.CURRENT_SYMBOL_ALREADY_AVAILABLE)
            after = pq.ParquetFile(target).read().to_pylist()
            self.assertEqual(before, after)


@unittest.skipIf(pq is None, "pyarrow required")
class AuditLayoutTests(unittest.TestCase):
    def test_audit_detects_year_only(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = make_paths(tmp)
            write_year_only(
                paths.current_parquet_root("1m"), "ETHUSDT", [make_record(BASE_OT)]
            )
            audit = lu.audit_current_partition_layout("1m", ["ETHUSDT"], paths)
            self.assertEqual(audit["year_only_file_count"], 1)
            self.assertEqual(audit["year_month_file_count"], 0)
            self.assertEqual(audit["mixed_symbol_count"], 0)
            self.assertEqual(audit["status"], lu.CURRENT_LAYOUT_OK)

    def test_audit_detects_year_month(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = make_paths(tmp)
            lu.merge_records_to_current_partition([make_record(BASE_OT)], paths)
            audit = lu.audit_current_partition_layout("1m", ["ETHUSDT"], paths)
            self.assertEqual(audit["year_only_file_count"], 0)
            self.assertEqual(audit["year_month_file_count"], 1)
            self.assertEqual(audit["status"], lu.CURRENT_LAYOUT_OK)

    def test_audit_detects_mixed(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = make_paths(tmp)
            # year/month for Dec 2021, plus a stray year-only file for 2022.
            lu.merge_records_to_current_partition([make_record(BASE_OT)], paths)
            write_year_only(
                paths.current_parquet_root("1m"), "ETHUSDT", [make_record(JAN_OT)]
            )
            audit = lu.audit_current_partition_layout("1m", ["ETHUSDT"], paths)
            self.assertGreaterEqual(audit["year_only_file_count"], 1)
            self.assertGreaterEqual(audit["year_month_file_count"], 1)
            self.assertEqual(audit["mixed_symbol_count"], 1)
            self.assertIn("ETHUSDT", audit["mixed_symbols"])
            self.assertEqual(audit["status"], lu.CURRENT_LAYOUT_MIXED)
            self.assertFalse(lu.validate_current_partition_layout("1m", ["ETHUSDT"], paths))

    def test_plan_migration_is_dry_run_and_lists_year_only(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = make_paths(tmp)
            write_year_only(
                paths.current_parquet_root("1m"), "ETHUSDT", [make_record(BASE_OT)]
            )
            before = snapshot_files(Path(tmp))
            plan = lu.plan_current_layout_migration("1m", ["ETHUSDT"], paths)
            self.assertEqual(plan["would_convert_file_count"], 1)
            self.assertEqual(len(plan["would_convert_files"]), 1)
            # Dry-run: nothing written or moved.
            self.assertEqual(snapshot_files(Path(tmp)), before)


@unittest.skipIf(pq is None, "pyarrow required")
class AuditCliTests(unittest.TestCase):
    def test_audit_current_layout_cli_outputs_json_and_writes_nothing(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = make_paths(tmp)
            lu.merge_records_to_current_partition([make_record(BASE_OT)], paths)
            write_year_only(
                paths.current_parquet_root("1m"), "ETHUSDT", [make_record(JAN_OT)]
            )
            before = snapshot_files(Path(tmp))
            result = subprocess.run(
                [
                    sys.executable, "scripts/live_update.py",
                    "--repo-root", tmp,
                    "--seed-dataset-root", "seed",
                    "--current-dataset-root", "current",
                    "--interval", "1m",
                    "--symbols", "ETHUSDT",
                    "--audit-current-layout",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
                check=False,
                timeout=60,
            )
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            payload = json.loads(result.stdout)
            self.assertEqual(payload["overall_status"], lu.CURRENT_LAYOUT_MIXED)
            entry = payload["interval_results"]["1m"]
            self.assertIn("ETHUSDT", entry["mixed_symbols"])
            # CLI audit must not write or move any file.
            self.assertEqual(snapshot_files(Path(tmp)), before)


if __name__ == "__main__":
    unittest.main()
