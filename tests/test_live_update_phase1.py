"""Phase 1 tests for Binance UM Kline live-update primitives."""

from pathlib import Path
import json
import subprocess
import sys
import tempfile
import unittest

from datahub import live_update as lu

try:
    import pyarrow.parquet as pq
except ImportError:  # pragma: no cover
    pq = None


REPO_ROOT = Path(__file__).resolve().parents[1]


class IntervalTests(unittest.TestCase):
    def test_supported_intervals_are_cli_all_order(self):
        self.assertEqual(
            lu.SUPPORTED_INTERVALS,
            ("1m", "3m", "5m", "15m", "1h", "4h", "1d"),
        )

    def test_all_expands_to_api_safe_intervals(self):
        intervals = lu.expand_intervals("all")
        self.assertEqual(intervals, lu.SUPPORTED_INTERVALS)
        self.assertNotIn("all", intervals)

    def test_validate_interval_rejects_all(self):
        with self.assertRaises(lu.LiveUpdateCommandError):
            lu.validate_interval("all")


class KlineRecordTests(unittest.TestCase):
    def test_record_builds_taipei_fields_and_keys(self):
        rec = lu.KlineRecord.build(
            symbol="btcusdt",
            interval="1m",
            open_time=1638747660000,
            open=50000.0,
            high=50100.0,
            low=49900.0,
            close=50050.0,
            volume=1000.0,
            close_time=1638747719999,
            quote_volume=50050000.0,
            trade_count=100,
            taker_buy_base_volume=500.0,
            taker_buy_quote_volume=25025000.0,
            source_archive="live_websocket:kline",
            archive_source="live_websocket",
            archive_period="2021-12-06",
        )

        self.assertEqual(rec.symbol, "BTCUSDT")
        self.assertEqual(rec.open_time_utc, "2021-12-05T23:41:00")
        self.assertEqual(rec.open_time_taipei, "2021-12-06T07:41:00")
        self.assertEqual(rec.date, "2021-12-06")
        self.assertEqual(rec.year, 2021)
        self.assertEqual(rec.month, 12)
        self.assertEqual(rec.record_key().as_tuple(), ("BTCUSDT", "1m", 1638747660000))
        self.assertEqual(rec.partition_key().as_tuple(), ("1m", "BTCUSDT", 2021, 12))

    def test_logical_and_physical_dicts_match_partition_policy(self):
        rec = lu.KlineRecord.build(
            symbol="ETHUSDT",
            interval="1h",
            open_time=1638745200000,
            open=1.0,
            high=2.0,
            low=0.5,
            close=1.5,
            volume=10.0,
            close_time=1638748799999,
            quote_volume=15.0,
            trade_count=3,
            taker_buy_base_volume=5.0,
            taker_buy_quote_volume=7.5,
            source_archive="live_rest:/fapi/v1/klines",
            archive_source="live_rest",
            archive_period="2021-12-06",
        )

        logical = rec.logical_dict()
        physical = rec.physical_dict()
        self.assertIn("symbol", logical)
        self.assertIn("year", logical)
        self.assertNotIn("symbol", physical)
        self.assertNotIn("year", physical)
        self.assertEqual(physical["schema_version"], 1)
        self.assertEqual(physical["dataset_version"], "current-v1")


class PathTests(unittest.TestCase):
    def test_path_resolver_matches_live_update_layout(self):
        paths = lu.LiveUpdatePaths(repo_root=Path("/repo"))
        self.assertEqual(
            paths.current_parquet_root("1m"),
            Path("/repo/local_data/binance_um_klines_current/interval=1m/parquet"),
        )
        self.assertEqual(
            paths.closed_buffer_jsonl("1m", "2026-06-26"),
            Path(
                "/repo/local_data/live_update/binance_um_klines/"
                "interval=1m/closed_buffer/date=2026-06-26/closed.jsonl"
            ),
        )
        key = lu.PartitionKey(interval="1m", symbol="btcusdt", year=2026, month=6)
        self.assertEqual(
            paths.current_partition_file(key),
            Path(
                "/repo/local_data/binance_um_klines_current/interval=1m/parquet/"
                "symbol=BTCUSDT/year=2026/month=06/part-000.parquet"
            ),
        )

    def test_path_segment_parsers(self):
        self.assertEqual(lu.parse_interval_segment("interval=3m"), "3m")
        self.assertEqual(lu.parse_symbol_segment("symbol=btcusdt"), "BTCUSDT")
        self.assertEqual(lu.parse_date_segment("date=2026-06-26"), "2026-06-26")
        with self.assertRaises(lu.LiveUpdateCommandError):
            lu.parse_interval_segment("interval=all")


class ScriptTests(unittest.TestCase):
    def test_script_describe_layout_expands_all_without_runtime_start(self):
        result = subprocess.run(
            [
                sys.executable,
                "scripts/live_update.py",
                "--interval",
                "all",
                "--describe-layout",
            ],
            cwd=REPO_ROOT,
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        payload = json.loads(result.stdout)
        self.assertEqual(payload["requested_interval"], "all")
        self.assertEqual(payload["active_intervals"], list(lu.SUPPORTED_INTERVALS))
        self.assertTrue(payload["intervals_are_api_safe"])
        self.assertNotIn("all", payload["active_intervals"])


def make_record(open_time, *, close=1.5, source="live_websocket:kline"):
    return lu.KlineRecord.build(
        symbol="BTCUSDT",
        interval="1m",
        open_time=open_time,
        open=1.0,
        high=max(2.0, close),
        low=0.5,
        close=close,
        volume=10.0,
        close_time=open_time + lu.interval_milliseconds("1m") - 1,
        quote_volume=15.0,
        trade_count=3,
        taker_buy_base_volume=5.0,
        taker_buy_quote_volume=7.5,
        source_archive=source,
        archive_source=source.split(":", 1)[0],
        archive_period="2021-12-06",
    )


@unittest.skipIf(pq is None, "pyarrow is required for Phase 2 parquet tests")
class CurrentDatasetInitializationTests(unittest.TestCase):
    def test_initialize_current_dataset_copies_seed_and_writes_marker(self):
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            paths = lu.LiveUpdatePaths(repo_root=repo)
            seed_file = (
                paths.seed_parquet_root("1m")
                / "symbol=BTCUSDT"
                / "year=2021"
                / "month=12"
                / "part-000.parquet"
            )
            seed_file.parent.mkdir(parents=True, exist_ok=True)
            table = lu._table_from_physical_rows([make_record(1638747600000).physical_dict()])
            pq.write_table(table, seed_file)

            result = lu.ensure_current_dataset(
                "1m",
                paths,
                initialized_at_utc="2026-06-26T00:00:00Z",
            )

            self.assertEqual(result.status, "initialized")
            copied = (
                paths.current_parquet_root("1m")
                / "symbol=BTCUSDT"
                / "year=2021"
                / "month=12"
                / "part-000.parquet"
            )
            self.assertTrue(copied.exists())
            marker = json.loads(
                paths.current_initialized_marker("1m").read_text(encoding="utf-8")
            )
            self.assertEqual(marker["interval"], "1m")
            self.assertEqual(marker["method"], "copy")
            self.assertEqual(marker["schema_version"], 1)
            self.assertEqual(marker["dataset_version"], "current-v1")

            second = lu.ensure_current_dataset("1m", paths)
            self.assertEqual(second.status, "already_initialized")

    def test_initialize_current_dataset_reports_bootstrap_required(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = lu.LiveUpdatePaths(repo_root=Path(tmp))
            result = lu.ensure_current_dataset("3m", paths)

            self.assertEqual(result.status, "bootstrap_required")
            self.assertFalse(paths.current_parquet_root("3m").exists())

    def test_initialize_current_dataset_allows_empty_existing_current_root(self):
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            paths = lu.LiveUpdatePaths(repo_root=repo)
            seed_file = (
                paths.seed_parquet_root("1m")
                / "symbol=BTCUSDT"
                / "year=2021"
                / "month=12"
                / "part-000.parquet"
            )
            seed_file.parent.mkdir(parents=True, exist_ok=True)
            table = lu._table_from_physical_rows([make_record(1638747600000).physical_dict()])
            pq.write_table(table, seed_file)
            paths.current_parquet_root("1m").mkdir(parents=True)

            result = lu.ensure_current_dataset("1m", paths)

            self.assertEqual(result.status, "initialized")
            self.assertTrue(paths.current_initialized_marker("1m").exists())


@unittest.skipIf(pq is None, "pyarrow is required for Phase 2 parquet tests")
class CurrentDatasetMergeTests(unittest.TestCase):
    def test_merge_records_writes_sorted_partition(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = lu.LiveUpdatePaths(repo_root=Path(tmp))
            later = make_record(1638747660000, close=2.5)
            earlier = make_record(1638747600000, close=1.5)

            result = lu.merge_records_to_current_partition([later, earlier], paths)

            self.assertEqual(result.input_row_count, 2)
            self.assertEqual(result.existing_row_count, 0)
            self.assertEqual(result.output_row_count, 2)
            rows = pq.ParquetFile(result.target_path).read().to_pylist()
            self.assertEqual([row["open_time"] for row in rows], [1638747600000, 1638747660000])
            self.assertEqual(rows[0]["dataset_version"], "current-v1")

    def test_merge_records_deduplicates_with_last_received_wins(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = lu.LiveUpdatePaths(repo_root=Path(tmp))
            original = make_record(1638747600000, close=1.5, source="live_rest:/fapi/v1/klines")
            first = lu.merge_records_to_current_partition([original], paths)

            replacement = make_record(1638747600000, close=9.5, source="live_websocket:kline")
            new_bar = make_record(1638747660000, close=2.5)
            second = lu.merge_records_to_current_partition([replacement, new_bar], paths)

            self.assertEqual(first.output_row_count, 1)
            self.assertEqual(second.existing_row_count, 1)
            self.assertEqual(second.duplicate_replaced_count, 1)
            self.assertEqual(second.output_row_count, 2)
            rows = pq.ParquetFile(second.target_path).read().to_pylist()
            self.assertEqual([row["open_time"] for row in rows], [1638747600000, 1638747660000])
            self.assertEqual(rows[0]["close"], 9.5)
            self.assertEqual(rows[0]["archive_source"], "live_websocket")

    def test_merge_rejects_cross_partition_batch(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = lu.LiveUpdatePaths(repo_root=Path(tmp))
            june = make_record(1782432000000)
            july = make_record(1785110400000)

            with self.assertRaises(lu.LiveUpdateCommandError):
                lu.merge_records_to_current_partition([june, july], paths)


@unittest.skipIf(pq is None, "pyarrow is required for Phase 2 parquet tests")
class ScriptPhase2Tests(unittest.TestCase):
    def test_script_initialize_current_dataset_reports_bootstrap_required(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = subprocess.run(
                [
                    sys.executable,
                    "scripts/live_update.py",
                    "--repo-root",
                    tmp,
                    "--interval",
                    "1m",
                    "--initialize-current-dataset",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
                check=False,
            )

            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            payload = json.loads(result.stdout)
            self.assertEqual(payload["active_intervals"], ["1m"])
            self.assertEqual(payload["results"][0]["status"], "bootstrap_required")


@unittest.skipIf(pq is None, "pyarrow is required for Phase 3 current dataset tests")
class LiveUpdateStateTests(unittest.TestCase):
    def test_state_save_load_uses_expected_shape(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = lu.LiveUpdatePaths(repo_root=Path(tmp))
            state = lu.LiveUpdateState.create(
                "1m",
                paths,
                now_utc="2026-06-26T00:00:00Z",
            )
            lu.mark_symbol_buffered(
                state,
                "btcusdt",
                1638747600000,
                now_utc="2026-06-26T00:01:00Z",
            )
            path = lu.save_live_update_state(
                state,
                paths,
                now_utc="2026-06-26T00:02:00Z",
            )

            self.assertEqual(path, paths.state_json("1m"))
            self.assertTrue(path.exists())
            self.assertFalse(path.with_name(path.name + ".tmp").exists())
            loaded = lu.load_live_update_state("1m", paths)
            self.assertIsNotNone(loaded)
            self.assertEqual(loaded.interval, "1m")
            self.assertEqual(
                loaded.symbols["BTCUSDT"].last_buffered_open_time,
                1638747600000,
            )
            self.assertIsNone(loaded.symbols["BTCUSDT"].last_closed_open_time)

    def test_flush_result_is_only_path_that_updates_closed_state(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = lu.LiveUpdatePaths(repo_root=Path(tmp))
            state = lu.LiveUpdateState.create("1m", paths)
            rec = make_record(1638747600000)

            lu.mark_symbol_buffered(state, "BTCUSDT", rec.open_time)
            self.assertEqual(
                state.symbols["BTCUSDT"].last_buffered_open_time,
                rec.open_time,
            )
            self.assertIsNone(state.symbols["BTCUSDT"].last_closed_open_time)

            merge = lu.merge_records_to_current_partition([rec], paths)
            lu.apply_flush_result_to_state(
                state,
                "BTCUSDT",
                merge,
                closed_at_utc="2026-06-26T00:01:00Z",
                now_utc="2026-06-26T00:01:01Z",
            )

            symbol_state = state.symbols["BTCUSDT"]
            self.assertEqual(symbol_state.last_flushed_open_time, rec.open_time)
            self.assertEqual(symbol_state.last_closed_open_time, rec.open_time)
            self.assertEqual(symbol_state.merged_bar_count, 1)
            self.assertEqual(symbol_state.last_target_path, str(merge.target_path))


@unittest.skipIf(pq is None, "pyarrow is required for Phase 3 current dataset tests")
class StartupBackfillCalculationTests(unittest.TestCase):
    def test_resolve_last_closed_prefers_state_over_current_dataset(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = lu.LiveUpdatePaths(repo_root=Path(tmp))
            older = make_record(1638747600000)
            newer = make_record(1638747660000)
            lu.merge_records_to_current_partition([newer], paths)
            state = lu.LiveUpdateState.create("1m", paths)
            state.symbol_state("BTCUSDT").last_closed_open_time = older.open_time

            value, source = lu.resolve_last_closed_open_time(
                state,
                "1m",
                "BTCUSDT",
                paths,
            )

            self.assertEqual(value, older.open_time)
            self.assertEqual(source, "state")

    def test_resolve_last_closed_falls_back_to_current_dataset(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = lu.LiveUpdatePaths(repo_root=Path(tmp))
            earlier = make_record(1638747600000)
            later = make_record(1638747660000)
            lu.merge_records_to_current_partition([earlier, later], paths)
            state = lu.LiveUpdateState.create("1m", paths)

            value, source = lu.resolve_last_closed_open_time(
                state,
                "1m",
                "BTCUSDT",
                paths,
            )

            self.assertEqual(value, later.open_time)
            self.assertEqual(source, "current_dataset")

    def test_latest_closed_and_missing_bars_calculation(self):
        latest = lu.calculate_latest_closed_open_time(
            "1m",
            1638747785000,
            close_lag_ms=2000,
        )
        self.assertEqual(latest, 1638747720000)

        missing, start, end = lu.calculate_missing_bars(
            last_closed_open_time=1638747480000,
            latest_closed_open_time=latest,
            interval="1m",
        )
        self.assertEqual(missing, 4)
        self.assertEqual(start, 1638747540000)
        self.assertEqual(end, 1638747720000)

    def test_plan_startup_backfill_skeleton_does_not_fetch_rest(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = lu.LiveUpdatePaths(repo_root=Path(tmp))
            state = lu.LiveUpdateState.create(
                "1m",
                paths,
                now_utc="2026-06-26T00:00:00Z",
            )
            state.symbol_state("BTCUSDT").last_closed_open_time = 1638747480000
            lu.save_live_update_state(state, paths)

            plans = lu.plan_startup_backfill(
                ("1m",),
                ["BTCUSDT", "ETHUSDT"],
                paths,
                now_ms=1638747785000,
                close_lag_ms=2000,
            )

            self.assertEqual(len(plans), 1)
            interval_plan = plans[0]
            self.assertEqual(interval_plan.interval, "1m")
            self.assertEqual(interval_plan.init_result.status, "bootstrap_required")
            btc = interval_plan.plans[0]
            eth = interval_plan.plans[1]
            self.assertEqual(btc.status, "missing")
            self.assertEqual(btc.missing_bars, 4)
            self.assertEqual(btc.source, "state")
            self.assertEqual(eth.status, "bootstrap_required")
            self.assertEqual(eth.source, "bootstrap_required")


class ScriptPhase3Tests(unittest.TestCase):
    def test_script_plan_startup_backfill_requires_symbols(self):
        result = subprocess.run(
            [
                sys.executable,
                "scripts/live_update.py",
                "--interval",
                "1m",
                "--plan-startup-backfill",
                "--now-ms",
                "1638747785000",
            ],
            cwd=REPO_ROOT,
            text=True,
            capture_output=True,
            check=False,
        )

        self.assertEqual(result.returncode, 2)
        self.assertIn("--symbols is required", result.stderr)

    def test_script_plan_startup_backfill_outputs_plan_without_rest(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = subprocess.run(
                [
                    sys.executable,
                    "scripts/live_update.py",
                    "--repo-root",
                    tmp,
                    "--interval",
                    "1m",
                    "--symbols",
                    "BTCUSDT",
                    "--plan-startup-backfill",
                    "--now-ms",
                    "1638747785000",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
                check=False,
            )

            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            payload = json.loads(result.stdout)
            self.assertEqual(payload["active_intervals"], ["1m"])
            self.assertEqual(payload["plans"][0]["plans"][0]["symbol"], "BTCUSDT")
            self.assertEqual(payload["plans"][0]["plans"][0]["status"], "bootstrap_required")


if __name__ == "__main__":
    unittest.main()
