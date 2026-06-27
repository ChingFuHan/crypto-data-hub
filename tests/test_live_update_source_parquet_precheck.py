"""Source parquet readability precheck + quote-asset batch filter tests.

Two capabilities, both read-only and tempfile-only (never touch the repo's real
local_data, never contact Binance, never delete / migrate real data):

1. inspect_symbol_parquet_readability + the migration gate: a symbol with any
   unreadable (corrupt-footer) source parquet must NOT plan=planned, must NOT
   create a stage or backup, and must surface ``source_parquet_unreadable``.
2. plan_current_layout_migration_batches ``--quote-assets`` filter: restrict
   candidates to USDT (etc.) quote pairs, excluding USDC / BUSD pairs with a
   ``quote_asset_mismatch`` reason, without misjudging delivery contracts.
"""

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


def corrupt_footer(root, symbol):
    """Overwrite the trailing magic of every parquet under a symbol dir.

    Mirrors the known-bad KAITOUSDC part: head magic ``PAR1`` intact, footer
    magic destroyed, so pyarrow's "Parquet magic bytes not found in footer".
    """
    for path in (root / f"symbol={symbol}").rglob("*.parquet"):
        data = bytearray(path.read_bytes())
        data[-4:] = b"XXXX"
        path.write_bytes(bytes(data))


def snapshot(root):
    return sorted(str(p) for p in Path(root).rglob("*") if p.is_file())


@unittest.skipIf(pq is None, "pyarrow required")
class ReadabilityPrecheckTests(unittest.TestCase):
    def test_readable_source_dry_run_is_planned(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = make_paths(tmp)
            write_year_only(paths.current_parquet_root("1m"), "URNMUSDT", [make_record(BASE_OT, "URNMUSDT")])
            inspect = lu.inspect_symbol_parquet_readability("1m", "URNMUSDT", paths)
            self.assertEqual(inspect["unreadable_file_count"], 0)
            self.assertEqual(inspect["readable_file_count"], 1)
            self.assertEqual(inspect["source_file_count"], 1)
            self.assertTrue(inspect["files"][0]["parquet_magic_ok"])
            self.assertTrue(inspect["files"][0]["read_ok"])
            result = lu.migrate_current_symbol_layout("1m", "URNMUSDT", paths, execute=False)
            self.assertEqual(result["status"], lu.MIGRATE_PLANNED)

    def test_unreadable_bad_footer_dry_run_is_source_parquet_unreadable(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = make_paths(tmp)
            root = paths.current_parquet_root("1m")
            write_year_only(root, "URNMUSDT", [make_record(BASE_OT, "URNMUSDT")])
            corrupt_footer(root, "URNMUSDT")
            result = lu.migrate_current_symbol_layout("1m", "URNMUSDT", paths, execute=False)
            self.assertEqual(result["status"], lu.MIGRATE_SOURCE_PARQUET_UNREADABLE)
            self.assertEqual(result["unreadable_file_count"], 1)

    def test_unreadable_execute_creates_no_stage(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = make_paths(tmp)
            root = paths.current_parquet_root("1m")
            write_year_only(root, "URNMUSDT", [make_record(BASE_OT, "URNMUSDT")])
            corrupt_footer(root, "URNMUSDT")
            before = snapshot(Path(tmp))
            result = lu.migrate_current_symbol_layout("1m", "URNMUSDT", paths, execute=True)
            self.assertEqual(result["status"], lu.MIGRATE_SOURCE_PARQUET_UNREADABLE)
            self.assertIsNone(result["stage_path"])
            interval_root = paths.current_interval_root("1m")
            self.assertFalse((interval_root / lu.LAYOUT_MIGRATION_STAGE_DIR).exists())
            # Source untouched.
            self.assertEqual(snapshot(Path(tmp)), before)

    def test_unreadable_execute_creates_no_backup(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = make_paths(tmp)
            root = paths.current_parquet_root("1m")
            write_year_only(root, "URNMUSDT", [make_record(BASE_OT, "URNMUSDT")])
            corrupt_footer(root, "URNMUSDT")
            lu.migrate_current_symbol_layout("1m", "URNMUSDT", paths, execute=True)
            interval_root = paths.current_interval_root("1m")
            self.assertFalse((interval_root / lu.LAYOUT_MIGRATION_BACKUP_DIR).exists())

    def test_unreadable_files_detail_fields(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = make_paths(tmp)
            root = paths.current_parquet_root("1m")
            write_year_only(root, "URNMUSDT", [make_record(BASE_OT, "URNMUSDT")])
            corrupt_footer(root, "URNMUSDT")
            result = lu.migrate_current_symbol_layout("1m", "URNMUSDT", paths, execute=False)
            self.assertEqual(len(result["unreadable_files"]), 1)
            entry = result["unreadable_files"][0]
            for field in ("path", "size", "head4", "tail4", "parquet_magic_ok", "read_ok", "error"):
                self.assertIn(field, entry)
            self.assertEqual(entry["head4"], "PAR1")
            self.assertNotEqual(entry["tail4"], "PAR1")
            self.assertFalse(entry["parquet_magic_ok"])
            self.assertFalse(entry["read_ok"])
            self.assertIsInstance(entry["size"], int)
            self.assertIsNotNone(entry["error"])

    def test_batch_dry_run_surfaces_source_parquet_unreadable(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = make_paths(tmp)
            root = paths.current_parquet_root("1m")
            write_year_only(root, "URNMUSDT", [make_record(BASE_OT, "URNMUSDT")])
            corrupt_footer(root, "URNMUSDT")
            plan = lu.plan_current_layout_migration_batches(
                "1m", paths, batch_size=10, max_batches=2, dry_run_batches=True,
            )
            results = [r for b in plan["batches"] for r in b.get("dry_run_results", [])]
            statuses = {r["symbol"]: r["status"] for r in results}
            self.assertEqual(statuses.get("URNMUSDT"), lu.MIGRATE_SOURCE_PARQUET_UNREADABLE)


def build_quote_dataset(tmp):
    paths = make_paths(tmp)
    root = paths.current_parquet_root("1m")
    write_year_only(root, "AMZNUSDT", [make_record(BASE_OT, "AMZNUSDT")])
    write_year_only(root, "KAITOUSDC", [make_record(BASE_OT, "KAITOUSDC")])
    write_year_only(root, "BTCUSDC", [make_record(BASE_OT, "BTCUSDC")])
    write_year_only(root, "AAABUSD", [make_record(BASE_OT, "AAABUSD")])
    write_year_only(root, "BTCUSDT_230630", [make_record(BASE_OT, "BTCUSDT_230630")])
    return paths


@unittest.skipIf(pq is None, "pyarrow required")
class QuoteAssetFilterTests(unittest.TestCase):
    def _plan(self, paths, **kw):
        return lu.plan_current_layout_migration_batches("1m", paths, **kw)

    def _batch_symbols(self, plan):
        return [s for b in plan["batches"] for s in b["symbols"]]

    def test_usdt_includes_amznusdt(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = build_quote_dataset(tmp)
            plan = self._plan(paths, batch_size=20, max_batches=5, quote_assets=["USDT"])
            self.assertIn("AMZNUSDT", self._batch_symbols(plan))

    def test_usdt_excludes_kaitousdc(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = build_quote_dataset(tmp)
            plan = self._plan(paths, batch_size=20, max_batches=5, quote_assets=["USDT"])
            self.assertNotIn("KAITOUSDC", self._batch_symbols(plan))
            mismatched = {e["symbol"] for e in plan["excluded"]["quote_asset_mismatch"]}
            self.assertIn("KAITOUSDC", mismatched)

    def test_usdt_excludes_btcusdc(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = build_quote_dataset(tmp)
            plan = self._plan(paths, batch_size=20, max_batches=5, quote_assets=["USDT"])
            self.assertNotIn("BTCUSDC", self._batch_symbols(plan))

    def test_usdt_excludes_busd_pair(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = build_quote_dataset(tmp)
            plan = self._plan(paths, batch_size=20, max_batches=5, quote_assets=["USDT"])
            self.assertNotIn("AAABUSD", self._batch_symbols(plan))

    def test_delivery_contract_excluded_not_misjudged(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = build_quote_dataset(tmp)
            plan = self._plan(
                paths, batch_size=20, max_batches=5,
                quote_assets=["USDT"], exclude_delivery_contracts=True,
            )
            self.assertNotIn("BTCUSDT_230630", self._batch_symbols(plan))
            delivery = {e["symbol"] for e in plan["excluded"]["delivery_contracts"]}
            self.assertIn("BTCUSDT_230630", delivery)
            # Delivery filter wins -> NOT reported as a quote mismatch.
            quote_mismatch = {e["symbol"] for e in plan["excluded"]["quote_asset_mismatch"]}
            self.assertNotIn("BTCUSDT_230630", quote_mismatch)

    def test_quote_asset_mismatch_entry_fields(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = build_quote_dataset(tmp)
            plan = self._plan(paths, batch_size=20, max_batches=5, quote_assets=["USDT"])
            entries = plan["excluded"]["quote_asset_mismatch"]
            self.assertTrue(entries)
            kaito = next(e for e in entries if e["symbol"] == "KAITOUSDC")
            for field in ("symbol", "reason", "detected_quote_asset", "row_count",
                          "expected_canonical_partition_count"):
                self.assertIn(field, kaito)
            self.assertEqual(kaito["reason"], "quote_asset_mismatch")
            self.assertEqual(kaito["detected_quote_asset"], "USDC")

    def test_filters_report_quote_assets(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = build_quote_dataset(tmp)
            plan = self._plan(paths, batch_size=20, max_batches=5, quote_assets=["USDT"])
            self.assertEqual(plan["filters"]["quote_assets"], ["USDT"])

    def test_no_quote_filter_keeps_usdc(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = build_quote_dataset(tmp)
            plan = self._plan(paths, batch_size=20, max_batches=5)
            self.assertIn("KAITOUSDC", self._batch_symbols(plan))
            self.assertIsNone(plan["filters"]["quote_assets"])


class QuoteAssetParseTests(unittest.TestCase):
    def test_single(self):
        self.assertEqual(lu.parse_quote_assets_arg(["USDT"]), ["USDT"])

    def test_comma_separated(self):
        self.assertEqual(lu.parse_quote_assets_arg(["USDT,USDC"]), ["USDT", "USDC"])

    def test_space_separated(self):
        self.assertEqual(lu.parse_quote_assets_arg(["USDT USDC"]), ["USDT", "USDC"])

    def test_lowercase_normalized(self):
        self.assertEqual(lu.parse_quote_assets_arg(["usdt"]), ["USDT"])

    def test_none(self):
        self.assertEqual(lu.parse_quote_assets_arg(None), [])

    def test_detect_quote_asset(self):
        self.assertEqual(lu._detect_quote_asset("AMZNUSDT"), "USDT")
        self.assertEqual(lu._detect_quote_asset("KAITOUSDC"), "USDC")
        self.assertEqual(lu._detect_quote_asset("AAABUSD"), "BUSD")
        self.assertEqual(lu._detect_quote_asset("BTCUSDT_230630"), "USDT")
        self.assertIsNone(lu._detect_quote_asset("WEIRDPAIR"))


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
