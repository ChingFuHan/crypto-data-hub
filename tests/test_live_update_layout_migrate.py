"""Single-symbol current layout migration tests.

migrate_current_symbol_layout rewrites one current symbol from year-only / mixed
layout into canonical year/month. Dry-run (default) writes nothing; execute
stages + verifies + backs up + promotes, leaving the original untouched on
verification failure.

All tests use temp dirs; none write the repo's real local_data.
"""

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from datahub import live_update as lu

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError:  # pragma: no cover
    pa = None
    pq = None


REPO_ROOT = Path(__file__).resolve().parents[1]
BASE_OT = 1638747600000  # 2021-12 (Taipei)
JAN_OT = 1641340800000   # 2022-01 (Taipei)
ONE_MIN = lu.interval_milliseconds("1m")


def make_record(open_time, *, symbol="URNMUSDT", close=1.5):
    return lu.KlineRecord.build(
        symbol=symbol, interval="1m", open_time=open_time,
        open=1.0, high=max(2.0, close), low=0.5, close=close, volume=10.0,
        close_time=open_time + ONE_MIN - 1, quote_volume=15.0, trade_count=3,
        taker_buy_base_volume=5.0, taker_buy_quote_volume=7.5,
        source_archive="seed", archive_source="seed", archive_period="2021-12-06",
    )


def make_paths(tmp):
    return lu.LiveUpdatePaths(
        repo_root=Path(tmp), seed_dataset_root=Path("seed"),
        current_dataset_root=Path("current"),
    )


def write_year_only(root, symbol, records):
    by_year = {}
    for rec in records:
        by_year.setdefault(rec.year, []).append(rec)
    for year, recs in by_year.items():
        target = root / f"symbol={symbol}" / f"year={year}" / "part-000.parquet"
        target.parent.mkdir(parents=True, exist_ok=True)
        rows = [r.physical_dict() for r in sorted(recs, key=lambda r: r.open_time)]
        pq.write_table(
            pa.table(
                {n: [row[n] for row in rows] for n in lu.CURRENT_PHYSICAL_COLUMNS},
                schema=lu._current_parquet_schema(),
            ),
            target,
        )


def snapshot(root):
    return sorted(str(p) for p in Path(root).rglob("*") if p.is_file())


@unittest.skipIf(pq is None, "pyarrow required")
class MigrateHelperTests(unittest.TestCase):
    def test_dry_run_writes_nothing(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = make_paths(tmp)
            write_year_only(paths.current_parquet_root("1m"), "URNMUSDT", [make_record(BASE_OT)])
            before = snapshot(Path(tmp))
            result = lu.migrate_current_symbol_layout("1m", "URNMUSDT", paths, execute=False)
            self.assertEqual(result["status"], lu.MIGRATE_PLANNED)
            self.assertTrue(result["dry_run"])
            self.assertIsNone(result["stage_path"])
            self.assertIsNone(result["backup_path"])
            self.assertEqual(snapshot(Path(tmp)), before)

    def test_year_only_migrates_to_year_month(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = make_paths(tmp)
            write_year_only(
                paths.current_parquet_root("1m"), "URNMUSDT",
                [make_record(BASE_OT), make_record(JAN_OT)],
            )
            result = lu.migrate_current_symbol_layout(
                "1m", "URNMUSDT", paths, execute=True, stamp="TS"
            )
            self.assertEqual(result["status"], lu.MIGRATE_DONE)
            symbol_root = paths.current_parquet_root("1m") / "symbol=URNMUSDT"
            files = list(symbol_root.rglob("*.parquet"))
            self.assertTrue(files)
            self.assertTrue(
                all("month=" in str(f.relative_to(symbol_root)) for f in files)
            )
            self.assertEqual(
                lu.precheck_symbol_layout_migration("1m", "URNMUSDT", paths)["status"],
                lu.CURRENT_LAYOUT_MIGRATION_NONE,
            )

    def test_mixed_migrates_to_year_month(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = make_paths(tmp)
            lu.merge_records_to_current_partition([make_record(BASE_OT)], paths)
            write_year_only(
                paths.current_parquet_root("1m"), "URNMUSDT", [make_record(JAN_OT)]
            )
            result = lu.migrate_current_symbol_layout(
                "1m", "URNMUSDT", paths, execute=True, stamp="TS"
            )
            self.assertEqual(result["status"], lu.MIGRATE_DONE)
            self.assertEqual(result["written_partition_count"], 2)
            audit = lu.audit_current_partition_layout("1m", ["URNMUSDT"], paths)
            self.assertEqual(audit["status"], lu.CURRENT_LAYOUT_OK)
            self.assertEqual(audit["year_only_file_count"], 0)

    def test_duplicate_open_time_deduplicated(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = make_paths(tmp)
            write_year_only(
                paths.current_parquet_root("1m"), "URNMUSDT",
                [make_record(BASE_OT), make_record(BASE_OT, close=9.0)],
            )
            result = lu.migrate_current_symbol_layout(
                "1m", "URNMUSDT", paths, execute=True, stamp="TS"
            )
            self.assertEqual(result["status"], lu.MIGRATE_DONE)
            self.assertEqual(result["row_count_before"], 2)
            self.assertEqual(result["duplicate_open_time_before"], 1)
            self.assertEqual(result["duplicate_replaced_count"], 1)
            self.assertEqual(result["row_count_after"], 1)
            self.assertEqual(result["duplicate_open_time_after"], 0)

    def test_execute_creates_backup(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = make_paths(tmp)
            write_year_only(paths.current_parquet_root("1m"), "URNMUSDT", [make_record(BASE_OT)])
            result = lu.migrate_current_symbol_layout(
                "1m", "URNMUSDT", paths, execute=True, stamp="TS"
            )
            self.assertEqual(result["status"], lu.MIGRATE_DONE)
            self.assertTrue(Path(result["backup_path"]).exists())
            # Backup is year-only (the original); new source is year/month.
            backup = Path(result["backup_path"])
            self.assertTrue((backup / "year=2021" / "part-000.parquet").exists())

    def test_verification_failure_leaves_original_untouched(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = make_paths(tmp)
            write_year_only(
                paths.current_parquet_root("1m"), "URNMUSDT",
                [make_record(BASE_OT), make_record(JAN_OT)],
            )
            source = paths.current_parquet_root("1m") / "symbol=URNMUSDT"
            before = snapshot(source)
            # Force the stage to be missing a row -> row_count_after mismatch.
            with patch.object(
                lu, "_seed_symbol_records", return_value=[make_record(BASE_OT)]
            ):
                result = lu.migrate_current_symbol_layout(
                    "1m", "URNMUSDT", paths, execute=True, stamp="TS"
                )
            self.assertEqual(result["status"], lu.MIGRATE_VERIFICATION_FAILED)
            self.assertTrue(result["warnings"])
            # Original intact, no stage/backup left behind.
            self.assertEqual(snapshot(source), before)
            self.assertFalse(Path(result["stage_path"]).exists())
            self.assertFalse(Path(result["backup_path"]).exists())


@unittest.skipIf(pq is None, "pyarrow required")
class MigrateCliTests(unittest.TestCase):
    def _run(self, tmp, extra):
        return subprocess.run(
            [
                sys.executable, "scripts/live_update.py",
                "--repo-root", tmp,
                "--seed-dataset-root", "seed",
                "--current-dataset-root", "current",
                "--interval", "1m",
                "--migrate-current-layout",
                *extra,
            ],
            cwd=REPO_ROOT, text=True, capture_output=True, check=False, timeout=60,
        )

    def test_cli_dry_run_outputs_json_and_writes_nothing(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = make_paths(tmp)
            write_year_only(paths.current_parquet_root("1m"), "URNMUSDT", [make_record(BASE_OT)])
            before = snapshot(Path(tmp))
            result = self._run(tmp, ["--symbols", "URNMUSDT"])
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            payload = json.loads(result.stdout)
            self.assertTrue(payload["dry_run"])
            self.assertEqual(payload["results"][0]["status"], lu.MIGRATE_PLANNED)
            self.assertEqual(snapshot(Path(tmp)), before)

    def test_cli_execute_migrates(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = make_paths(tmp)
            write_year_only(paths.current_parquet_root("1m"), "URNMUSDT", [make_record(BASE_OT)])
            result = self._run(tmp, ["--symbols", "URNMUSDT", "--execute"])
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            payload = json.loads(result.stdout)
            self.assertFalse(payload["dry_run"])
            self.assertEqual(payload["results"][0]["status"], lu.MIGRATE_DONE)
            self.assertEqual(
                lu.audit_current_partition_layout("1m", ["URNMUSDT"], paths)["status"],
                lu.CURRENT_LAYOUT_OK,
            )

    def test_cli_requires_explicit_symbols(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = self._run(tmp, [])
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("--symbols", result.stderr)

    def test_cli_rejects_symbols_all(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = self._run(tmp, ["--symbols", "all"])
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("all", result.stderr)

    def test_cli_rejects_interval_all(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = subprocess.run(
                [
                    sys.executable, "scripts/live_update.py",
                    "--repo-root", tmp,
                    "--current-dataset-root", "current",
                    "--interval", "all",
                    "--symbols", "URNMUSDT",
                    "--migrate-current-layout",
                ],
                cwd=REPO_ROOT, text=True, capture_output=True, check=False, timeout=60,
            )
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("interval", result.stderr.lower())


if __name__ == "__main__":
    unittest.main()
