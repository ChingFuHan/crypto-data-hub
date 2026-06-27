"""Controlled current layout migration batch planner tests.

plan_current_layout_migration_batches is read-only: it reuses the candidate
planner, applies exclude filters, slices survivors into batches, and writes
NOTHING (no parquet / stage / backup / jsonl / state / registry). It never
contacts Binance. --dry-run-batches still writes nothing.

All tests use temp dirs; none touch the repo's real local_data.
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
    """Map of file path -> (size, mtime) for deep before/after comparison."""
    out = {}
    for p in Path(root).rglob("*"):
        if p.is_file():
            st = p.stat()
            out[str(p)] = (st.st_size, st.st_mtime_ns)
    return out


def build_dataset(tmp):
    """Plain year-only symbols plus the various excludable / non-candidate kinds."""
    paths = make_paths(tmp)
    root = paths.current_parquet_root("1m")
    # Plain year-only candidates (varying row_count for ranking).
    write_year_only(root, "AAAUSDT", [make_record(BASE_OT, "AAAUSDT")])
    write_year_only(root, "BBBUSDT", [make_record(BASE_OT, "BBBUSDT"), make_record(JAN_OT, "BBBUSDT")])
    write_year_only(root, "CCCUSDT", [make_record(BASE_OT, "CCCUSDT"), make_record(JAN_OT, "CCCUSDT")])
    write_year_only(root, "DDDUSDT", [make_record(BASE_OT, "DDDUSDT"), make_record(JAN_OT, "DDDUSDT")])
    # Excludable kinds (all year-only so only the flag suppresses them).
    write_year_only(root, "ETHUSDT_260925", [make_record(BASE_OT, "ETHUSDT_260925")])
    write_year_only(root, "CVXUSDTSETTLED", [make_record(BASE_OT, "CVXUSDTSETTLED")])
    write_year_only(root, "龙虾USDT", [make_record(BASE_OT, "龙虾USDT")])
    # ETHUSDT year-only -> default-excluded even though it is a clean candidate.
    write_year_only(root, "ETHUSDT", [make_record(BASE_OT, "ETHUSDT")])
    # CANON canonical (no migration needed -> never a candidate).
    lu.merge_records_to_current_partition([make_record(BASE_OT, "CANON")], paths)
    # BTCUSDT mixed (default-excluded). ZMIXUSDT mixed (non-default mixed).
    lu.merge_records_to_current_partition([make_record(BASE_OT, "BTCUSDT")], paths)
    write_year_only(root, "BTCUSDT", [make_record(JAN_OT, "BTCUSDT")])
    lu.merge_records_to_current_partition([make_record(BASE_OT, "ZMIXUSDT")], paths)
    write_year_only(root, "ZMIXUSDT", [make_record(JAN_OT, "ZMIXUSDT")])
    return paths


ALL_EXCLUDES = dict(
    exclude_delivery_contracts=True,
    exclude_settled=True,
    exclude_non_ascii=True,
)


@unittest.skipIf(pq is None, "pyarrow required")
class BatchPlannerTests(unittest.TestCase):
    def _plan(self, paths, **kw):
        return lu.plan_current_layout_migration_batches("1m", paths, **kw)

    def test_produces_batches_from_year_only(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = build_dataset(tmp)
            plan = self._plan(paths, batch_size=2, max_batches=5, **ALL_EXCLUDES)
            all_symbols = [s for b in plan["batches"] for s in b["symbols"]]
            self.assertEqual(all_symbols, ["AAAUSDT", "BBBUSDT", "CCCUSDT", "DDDUSDT"])

    def test_batch_size(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = build_dataset(tmp)
            plan = self._plan(paths, batch_size=2, max_batches=5, **ALL_EXCLUDES)
            self.assertEqual(plan["batches"][0]["symbol_count"], 2)
            self.assertEqual(plan["batches"][0]["symbols"], ["AAAUSDT", "BBBUSDT"])

    def test_max_batches(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = build_dataset(tmp)
            plan = self._plan(paths, batch_size=1, max_batches=2, **ALL_EXCLUDES)
            self.assertEqual(len(plan["batches"]), 2)
            self.assertEqual(plan["candidate_count_after_filters"], 2)

    def test_excludes_delivery_contracts(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = build_dataset(tmp)
            plan = self._plan(paths, batch_size=20, max_batches=5, exclude_delivery_contracts=True)
            syms = [s for b in plan["batches"] for s in b["symbols"]]
            self.assertNotIn("ETHUSDT_260925", syms)
            self.assertEqual(
                [e["symbol"] for e in plan["excluded"]["delivery_contracts"]],
                ["ETHUSDT_260925"],
            )

    def test_excludes_settled(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = build_dataset(tmp)
            plan = self._plan(paths, batch_size=20, max_batches=5, exclude_settled=True)
            syms = [s for b in plan["batches"] for s in b["symbols"]]
            self.assertNotIn("CVXUSDTSETTLED", syms)
            self.assertEqual(
                [e["symbol"] for e in plan["excluded"]["settled"]], ["CVXUSDTSETTLED"]
            )

    def test_excludes_non_ascii(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = build_dataset(tmp)
            plan = self._plan(paths, batch_size=20, max_batches=5, exclude_non_ascii=True)
            syms = [s for b in plan["batches"] for s in b["symbols"]]
            self.assertNotIn("龙虾USDT", syms)
            self.assertEqual(
                [e["symbol"] for e in plan["excluded"]["non_ascii"]], ["龙虾USDT"]
            )

    def test_excludes_explicit_symbols(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = build_dataset(tmp)
            plan = self._plan(
                paths, batch_size=20, max_batches=5,
                exclude_symbols=["BBBUSDT", "CCCUSDT"], **ALL_EXCLUDES,
            )
            syms = [s for b in plan["batches"] for s in b["symbols"]]
            self.assertNotIn("BBBUSDT", syms)
            self.assertNotIn("CCCUSDT", syms)
            self.assertEqual(
                sorted(e["symbol"] for e in plan["excluded"]["explicit_symbols"]),
                ["BBBUSDT", "CCCUSDT"],
            )

    def test_default_excludes_btc_eth(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = build_dataset(tmp)
            plan = self._plan(paths, batch_size=20, max_batches=5, **ALL_EXCLUDES)
            syms = [s for b in plan["batches"] for s in b["symbols"]]
            self.assertNotIn("BTCUSDT", syms)
            self.assertNotIn("ETHUSDT", syms)
            default_syms = {e["symbol"] for e in plan["excluded"]["default_symbols"]}
            # ETHUSDT (year-only) excluded by default; BTCUSDT also (it is mixed,
            # but default by-name exclusion is checked first).
            self.assertIn("ETHUSDT", default_syms)
            self.assertIn("BTCUSDT", default_syms)

    def test_default_excludes_mixed_layout(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = build_dataset(tmp)
            plan = self._plan(paths, batch_size=20, max_batches=5, **ALL_EXCLUDES)
            syms = [s for b in plan["batches"] for s in b["symbols"]]
            self.assertNotIn("ZMIXUSDT", syms)
            self.assertEqual(
                [e["symbol"] for e in plan["excluded"]["mixed_layout"]], ["ZMIXUSDT"]
            )
            self.assertTrue(plan["filters"]["exclude_mixed_layout"])

    def test_includes_only_year_only(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = build_dataset(tmp)
            plan = self._plan(paths, batch_size=20, max_batches=5, **ALL_EXCLUDES)
            self.assertEqual(plan["filters"]["include_statuses"], ["year_only_needs_migration"])
            if plan["batches"]:
                for b in plan["batches"]:
                    if b.get("dry_run_results"):
                        for r in b["dry_run_results"]:
                            self.assertNotEqual(r["status"], lu.CURRENT_LAYOUT_MIGRATION_MIXED)

    def test_excludes_canonical(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = build_dataset(tmp)
            plan = self._plan(paths, batch_size=20, max_batches=5, **ALL_EXCLUDES)
            syms = [s for b in plan["batches"] for s in b["symbols"]]
            self.assertNotIn("CANON", syms)

    def test_excludes_source_missing(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = build_dataset(tmp)
            plan = self._plan(paths, batch_size=20, max_batches=5, **ALL_EXCLUDES)
            syms = [s for b in plan["batches"] for s in b["symbols"]]
            # NEVERUSDT was never written -> not discovered, never a candidate.
            self.assertNotIn("NEVERUSDT", syms)

    def test_dry_run_batches_returns_results(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = build_dataset(tmp)
            plan = self._plan(
                paths, batch_size=2, max_batches=1, dry_run_batches=True, **ALL_EXCLUDES
            )
            results = plan["batches"][0]["dry_run_results"]
            self.assertEqual([r["symbol"] for r in results], ["AAAUSDT", "BBBUSDT"])
            for r in results:
                self.assertTrue(r["dry_run"])
                self.assertFalse(r["execute"])
                self.assertEqual(r["status"], lu.MIGRATE_PLANNED)

    def test_dry_run_batches_writes_nothing(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = build_dataset(tmp)
            before = snapshot(Path(tmp))
            self._plan(
                paths, batch_size=20, max_batches=5, dry_run_batches=True, **ALL_EXCLUDES
            )
            self.assertEqual(snapshot(Path(tmp)), before)

    def test_dry_run_no_stage_or_backup_dirs(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = build_dataset(tmp)
            self._plan(
                paths, batch_size=20, max_batches=5, dry_run_batches=True, **ALL_EXCLUDES
            )
            interval_root = paths.current_interval_root("1m")
            self.assertFalse((interval_root / lu.LAYOUT_MIGRATION_STAGE_DIR).exists())
            self.assertFalse((interval_root / lu.LAYOUT_MIGRATION_BACKUP_DIR).exists())

    def test_candidate_pool_filtered_before_slicing(self):
        # delivery/settled/non-ascii sort BEFORE plain symbols by row_count=1, yet
        # the small batch must still be filled with plain year-only symbols.
        with tempfile.TemporaryDirectory() as tmp:
            paths = build_dataset(tmp)
            plan = self._plan(paths, batch_size=2, max_batches=1, **ALL_EXCLUDES)
            self.assertEqual(plan["batches"][0]["symbols"], ["AAAUSDT", "BBBUSDT"])

    def test_candidate_scan_limit_hit_flag(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = build_dataset(tmp)
            plan = self._plan(paths, batch_size=2, max_batches=5, candidate_scan_limit=1)
            self.assertTrue(plan["filters"]["hit_candidate_scan_limit"])
            self.assertEqual(plan["filters"]["candidate_scan_limit"], 1)

    def test_candidate_scan_limit_not_hit(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = build_dataset(tmp)
            plan = self._plan(paths, batch_size=20, max_batches=5, candidate_scan_limit=1000)
            self.assertFalse(plan["filters"]["hit_candidate_scan_limit"])

    def test_commands_are_strings_only(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = build_dataset(tmp)
            before = snapshot(Path(tmp))
            plan = self._plan(paths, batch_size=2, max_batches=1, **ALL_EXCLUDES)
            cmd = plan["batches"][0]["commands"]
            self.assertIsInstance(cmd["dry_run"], str)
            self.assertIsInstance(cmd["execute"], str)
            self.assertIn("--migrate-current-layout", cmd["dry_run"])
            self.assertIn("--execute", cmd["execute"])
            # Building the plan executed no command -> nothing changed on disk.
            self.assertEqual(snapshot(Path(tmp)), before)

    def test_schema_fields_present(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = build_dataset(tmp)
            plan = self._plan(paths, batch_size=2, max_batches=1, **ALL_EXCLUDES)
            for field in (
                "interval", "mode", "read_only", "execute", "dry_run_batches",
                "filters", "candidate_count_before_filters",
                "candidate_count_after_filters", "excluded", "batches", "note",
            ):
                self.assertIn(field, plan)
            for field in (
                "batch_size", "max_batches", "max_row_count", "candidate_scan_limit",
                "hit_candidate_scan_limit", "exclude_delivery_contracts",
                "exclude_settled", "exclude_non_ascii", "exclude_symbols",
                "default_excluded_symbols", "include_statuses", "exclude_mixed_layout",
            ):
                self.assertIn(field, plan["filters"])
            for key in (
                "delivery_contracts", "settled", "non_ascii", "explicit_symbols",
                "mixed_layout", "default_symbols",
            ):
                self.assertIn(key, plan["excluded"])
            b = plan["batches"][0]
            for field in (
                "batch_no", "symbol_count", "symbols", "total_row_count",
                "total_expected_canonical_partition_count", "max_symbol_row_count",
                "max_expected_canonical_partition_count", "commands",
            ):
                self.assertIn(field, b)

    def test_invalid_batch_size_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = build_dataset(tmp)
            with self.assertRaises(lu.LiveUpdateCommandError):
                self._plan(paths, batch_size=0)

    def test_invalid_max_batches_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = build_dataset(tmp)
            with self.assertRaises(lu.LiveUpdateCommandError):
                self._plan(paths, batch_size=2, max_batches=0)

    def test_invalid_max_row_count_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = build_dataset(tmp)
            with self.assertRaises(lu.LiveUpdateCommandError):
                self._plan(paths, max_row_count=-1)

    def test_invalid_candidate_scan_limit_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = build_dataset(tmp)
            with self.assertRaises(lu.LiveUpdateCommandError):
                self._plan(paths, candidate_scan_limit=-5)


@unittest.skipIf(pq is None, "pyarrow required")
class BatchPlannerCliTests(unittest.TestCase):
    def _run(self, tmp, extra):
        return subprocess.run(
            [
                sys.executable, "scripts/live_update.py",
                "--repo-root", tmp,
                "--current-dataset-root", "current",
                "--interval", "1m",
                "--plan-current-layout-migration-batches",
                *extra,
            ],
            cwd=REPO_ROOT, text=True, capture_output=True, check=False, timeout=120,
        )

    def test_cli_json_and_writes_nothing(self):
        with tempfile.TemporaryDirectory() as tmp:
            build_dataset(tmp)
            before = snapshot(Path(tmp))
            result = self._run(tmp, [
                "--batch-size", "2", "--max-batches", "5",
                "--exclude-delivery-contracts", "--exclude-settled",
                "--exclude-non-ascii", "--exclude-symbols", "BTCUSDT,ETHUSDT",
                "--dry-run-batches",
            ])
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            payload = json.loads(result.stdout)
            self.assertEqual(payload["mode"], "plan_current_layout_migration_batches")
            self.assertTrue(payload["read_only"])
            self.assertFalse(payload["execute"])
            syms = [s for b in payload["batches"] for s in b["symbols"]]
            self.assertEqual(syms, ["AAAUSDT", "BBBUSDT", "CCCUSDT", "DDDUSDT"])
            self.assertEqual(snapshot(Path(tmp)), before)

    def test_cli_rejects_interval_all(self):
        with tempfile.TemporaryDirectory() as tmp:
            build_dataset(tmp)
            result = subprocess.run(
                [
                    sys.executable, "scripts/live_update.py",
                    "--repo-root", tmp, "--current-dataset-root", "current",
                    "--interval", "all",
                    "--plan-current-layout-migration-batches",
                ],
                cwd=REPO_ROOT, text=True, capture_output=True, check=False, timeout=60,
            )
            self.assertNotEqual(result.returncode, 0)

    def test_cli_rejects_invalid_interval(self):
        with tempfile.TemporaryDirectory() as tmp:
            build_dataset(tmp)
            result = self._run_interval(tmp, "garbage")
            self.assertNotEqual(result.returncode, 0)

    def _run_interval(self, tmp, interval):
        return subprocess.run(
            [
                sys.executable, "scripts/live_update.py",
                "--repo-root", tmp, "--current-dataset-root", "current",
                "--interval", interval,
                "--plan-current-layout-migration-batches",
            ],
            cwd=REPO_ROOT, text=True, capture_output=True, check=False, timeout=60,
        )

    def test_cli_rejects_invalid_batch_size(self):
        with tempfile.TemporaryDirectory() as tmp:
            build_dataset(tmp)
            result = self._run(tmp, ["--batch-size", "0"])
            self.assertNotEqual(result.returncode, 0)

    def test_cli_rejects_invalid_max_batches(self):
        with tempfile.TemporaryDirectory() as tmp:
            build_dataset(tmp)
            result = self._run(tmp, ["--max-batches", "0"])
            self.assertNotEqual(result.returncode, 0)

    def test_cli_rejects_invalid_max_row_count(self):
        with tempfile.TemporaryDirectory() as tmp:
            build_dataset(tmp)
            result = self._run(tmp, ["--max-row-count", "-1"])
            self.assertNotEqual(result.returncode, 0)

    def test_cli_rejects_invalid_candidate_scan_limit(self):
        with tempfile.TemporaryDirectory() as tmp:
            build_dataset(tmp)
            result = self._run(tmp, ["--candidate-scan-limit", "-1"])
            self.assertNotEqual(result.returncode, 0)

    def test_cli_rejects_incompatible_mode(self):
        with tempfile.TemporaryDirectory() as tmp:
            build_dataset(tmp)
            result = self._run(tmp, ["--audit-current-layout"])
            self.assertNotEqual(result.returncode, 0)

    def test_cli_rejects_symbols(self):
        with tempfile.TemporaryDirectory() as tmp:
            build_dataset(tmp)
            result = self._run(tmp, ["--symbols", "AAAUSDT"])
            self.assertNotEqual(result.returncode, 0)


if __name__ == "__main__":
    unittest.main()
