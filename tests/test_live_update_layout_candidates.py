"""Current layout migration candidate list / batch planner tests.

list_current_layout_migration_candidates is read-only: it scans the LOCAL
current dataset, ranks safe/cheap migrations first, and writes nothing.

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
BASE_OT = 1638747600000  # 2021-12 (Taipei)
JAN_OT = 1641340800000   # 2022-01 (Taipei)
ONE_MIN = lu.interval_milliseconds("1m")


def make_record(open_time, symbol, *, close=1.5):
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


def build_dataset(tmp):
    """AAOIUSDT (year-only, 1 row), ZBIG (year-only, 2 rows / 2 months),
    DUPS (year-only with a duplicate open_time), BTCUSDT (mixed), CANON (canonical)."""
    paths = make_paths(tmp)
    root = paths.current_parquet_root("1m")
    write_year_only(root, "AAOIUSDT", [make_record(BASE_OT, "AAOIUSDT")])
    write_year_only(root, "ZBIG", [make_record(BASE_OT, "ZBIG"), make_record(JAN_OT, "ZBIG")])
    write_year_only(root, "DUPS", [make_record(BASE_OT, "DUPS"), make_record(BASE_OT, "DUPS", close=9.0)])
    lu.merge_records_to_current_partition([make_record(BASE_OT, "CANON")], paths)
    lu.merge_records_to_current_partition([make_record(BASE_OT, "BTCUSDT")], paths)
    write_year_only(root, "BTCUSDT", [make_record(JAN_OT, "BTCUSDT")])  # -> mixed
    return paths


@unittest.skipIf(pq is None, "pyarrow required")
class CandidateListTests(unittest.TestCase):
    def test_default_lists_year_only_only(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = build_dataset(tmp)
            plan = lu.list_current_layout_migration_candidates("1m", paths)
            statuses = {c["status"] for c in plan["candidates"]}
            self.assertEqual(statuses, {lu.CURRENT_LAYOUT_MIGRATION_YEAR_ONLY})
            self.assertNotIn("BTCUSDT", plan["symbols"])  # mixed excluded
            self.assertNotIn("CANON", plan["symbols"])    # canonical excluded

    def test_ranking_safe_cheap_first_dups_last(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = build_dataset(tmp)
            plan = lu.list_current_layout_migration_candidates("1m", paths)
            # AAOIUSDT (1 row), ZBIG (2 rows), DUPS (has duplicate) -> dup last.
            self.assertEqual(plan["symbols"], ["AAOIUSDT", "ZBIG", "DUPS"])
            self.assertEqual(plan["candidates"][-1]["symbol"], "DUPS")
            self.assertGreater(
                plan["candidates"][-1]["duplicate_open_time_count"], 0
            )

    def test_include_mixed(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = build_dataset(tmp)
            plan = lu.list_current_layout_migration_candidates(
                "1m", paths, include_mixed=True
            )
            self.assertIn("BTCUSDT", plan["symbols"])
            statuses = {c["status"] for c in plan["candidates"]}
            self.assertIn(lu.CURRENT_LAYOUT_MIGRATION_MIXED, statuses)

    def test_limit(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = build_dataset(tmp)
            plan = lu.list_current_layout_migration_candidates("1m", paths, limit=2)
            self.assertEqual(plan["candidate_count"], 2)
            self.assertEqual(plan["total_matched"], 3)
            self.assertEqual(plan["symbols"], ["AAOIUSDT", "ZBIG"])

    def test_max_row_count(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = build_dataset(tmp)
            plan = lu.list_current_layout_migration_candidates(
                "1m", paths, max_row_count=1
            )
            # Only symbols with row_count <= 1 -> AAOIUSDT.
            self.assertEqual(plan["symbols"], ["AAOIUSDT"])

    def test_status_filter(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = build_dataset(tmp)
            plan = lu.list_current_layout_migration_candidates(
                "1m", paths, status=lu.CURRENT_LAYOUT_MIGRATION_MIXED
            )
            self.assertEqual(plan["symbols"], ["BTCUSDT"])

    def test_candidate_fields_present(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = build_dataset(tmp)
            plan = lu.list_current_layout_migration_candidates("1m", paths)
            c = plan["candidates"][0]
            for field in (
                "symbol", "interval", "status", "row_count",
                "year_only_file_count", "year_month_file_count",
                "expected_canonical_partition_count", "duplicate_open_time_count",
                "min_open_time", "max_open_time", "recommended_action",
            ):
                self.assertIn(field, c)

    def test_planner_writes_nothing(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = build_dataset(tmp)
            before = snapshot(Path(tmp))
            lu.list_current_layout_migration_candidates("1m", paths, include_mixed=True)
            self.assertEqual(snapshot(Path(tmp)), before)


@unittest.skipIf(pq is None, "pyarrow required")
class CandidateCliTests(unittest.TestCase):
    def _run(self, tmp, extra):
        return subprocess.run(
            [
                sys.executable, "scripts/live_update.py",
                "--repo-root", tmp,
                "--current-dataset-root", "current",
                "--interval", "1m",
                "--list-current-layout-migration-candidates",
                *extra,
            ],
            cwd=REPO_ROOT, text=True, capture_output=True, check=False, timeout=60,
        )

    def test_cli_json_output_and_writes_nothing(self):
        with tempfile.TemporaryDirectory() as tmp:
            build_dataset(tmp)
            before = snapshot(Path(tmp))
            result = self._run(tmp, ["--limit", "10", "--max-row-count", "300000"])
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            payload = json.loads(result.stdout)
            self.assertEqual(payload["symbols"], ["AAOIUSDT", "ZBIG", "DUPS"])
            self.assertEqual(snapshot(Path(tmp)), before)

    def test_cli_output_symbols_only(self):
        with tempfile.TemporaryDirectory() as tmp:
            build_dataset(tmp)
            result = self._run(
                tmp, ["--limit", "2", "--max-row-count", "300000", "--output-symbols-only"]
            )
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertEqual(result.stdout.strip(), "AAOIUSDT ZBIG")

    def test_cli_include_mixed(self):
        with tempfile.TemporaryDirectory() as tmp:
            build_dataset(tmp)
            result = self._run(tmp, ["--include-mixed", "--output-symbols-only"])
            self.assertIn("BTCUSDT", result.stdout.split())


if __name__ == "__main__":
    unittest.main()
