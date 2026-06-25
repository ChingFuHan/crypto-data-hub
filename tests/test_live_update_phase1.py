"""Phase 1 tests for Binance UM Kline live-update primitives."""

from pathlib import Path
import json
import subprocess
import sys
import unittest

from datahub import live_update as lu


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


if __name__ == "__main__":
    unittest.main()
