"""Phase 8 tests: continuity checks, shared validation, and acceptance modes.

Covers the Phase 8 deliverables from ``docs/live_update/08_VALIDATION_AND_TESTS.md``:

* continuity check (duplicate / missing / alignment / close_time / lag_bars)
* ``--check-continuity`` CLI mode and continuity summary JSON
* full KBar validation rule set shared by WebSocket / REST / webhook
* unclosed KBar never reaches closed_buffer / current dataset
* closed KBar flows closed_buffer -> parquet merge -> state update
* ``--interval all`` / ``--once`` / ``--run-startup-backfill-once`` /
  ``--describe-*`` modes still work
* tests never write into the repo real ``local_data`` (temp dirs only)
"""

from pathlib import Path
import io
import json
import subprocess
import sys
import tempfile
import unittest
import urllib.error
import urllib.request
from unittest.mock import patch

from datahub import live_update as lu

try:
    import pyarrow.parquet as pq
except ImportError:  # pragma: no cover
    pq = None


REPO_ROOT = Path(__file__).resolve().parents[1]


def make_record(
    open_time,
    *,
    interval="1m",
    symbol="BTCUSDT",
    close=1.5,
    open_=1.0,
    high=None,
    low=0.5,
    volume=10.0,
    quote_volume=15.0,
    trade_count=3,
    taker_base=5.0,
    taker_quote=7.5,
    source="live_websocket:kline",
    archive_source="live_websocket",
    close_time=None,
):
    interval_ms = lu.interval_milliseconds(interval)
    return lu.KlineRecord.build(
        symbol=symbol,
        interval=interval,
        open_time=open_time,
        open=open_,
        high=high if high is not None else max(2.0, close, open_),
        low=low,
        close=close,
        volume=volume,
        close_time=close_time if close_time is not None else open_time + interval_ms - 1,
        quote_volume=quote_volume,
        trade_count=trade_count,
        taker_buy_base_volume=taker_base,
        taker_buy_quote_volume=taker_quote,
        source_archive=source,
        archive_source=archive_source,
        archive_period="2021-12-06",
    )


def ws_payload(open_time, *, interval="1m", closed=False, symbol="BTCUSDT", close=1.5):
    return {
        "e": "kline",
        "E": open_time + 1000,
        "s": symbol.upper(),
        "k": {
            "t": open_time,
            "T": open_time + lu.interval_milliseconds(interval) - 1,
            "s": symbol.upper(),
            "i": interval,
            "f": 1,
            "L": 2,
            "o": "1.0",
            "c": str(close),
            "h": str(max(2.0, close)),
            "l": "0.5",
            "v": "10.0",
            "n": 3,
            "x": closed,
            "q": "15.0",
            "V": "5.0",
            "Q": "7.5",
            "B": "0",
        },
    }


def normalized_payload(
    open_time,
    *,
    interval="1m",
    closed=False,
    symbol="BTCUSDT",
    close=1.5,
    include_is_closed=True,
    close_time=None,
):
    payload = {
        "symbol": symbol.upper(),
        "interval": interval,
        "open_time": open_time,
        "open": "1.0",
        "high": str(max(2.0, close)),
        "low": "0.5",
        "close": str(close),
        "volume": "10.0",
        "close_time": close_time
        if close_time is not None
        else open_time + lu.interval_milliseconds(interval) - 1,
        "quote_volume": "15.0",
        "trade_count": 3,
        "taker_buy_base_volume": "5.0",
        "taker_buy_quote_volume": "7.5",
    }
    if include_is_closed:
        payload["is_closed"] = closed
    return payload


def rest_row(open_time, *, interval="1m", close=1.5, close_time=None):
    interval_ms = lu.interval_milliseconds(interval)
    return [
        open_time,
        "1.0",
        str(max(2.0, close)),
        "0.5",
        str(close),
        "10.0",
        close_time if close_time is not None else open_time + interval_ms - 1,
        "15.0",
        3,
        "5.0",
        "7.5",
        "0",
    ]


# 1m base open_time used across tests; 1638747600000 = 2021-12-06T00:00:00Z.
BASE_OT = 1638747600000
ONE_MIN = lu.interval_milliseconds("1m")


@unittest.skipIf(pq is None, "pyarrow is required for Phase 8 parquet continuity tests")
class ContinuityCheckTests(unittest.TestCase):
    def _build_current(self, tmp, records):
        paths = lu.LiveUpdatePaths(repo_root=Path(tmp))
        by_partition: dict[tuple, list[lu.KlineRecord]] = {}
        for rec in records:
            by_partition.setdefault(rec.partition_key().as_tuple(), []).append(rec)
        for batch in by_partition.values():
            lu.merge_records_to_current_partition(batch, paths)
        return paths

    def test_empty_symbol_returns_empty_status_with_lag_none(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = lu.LiveUpdatePaths(repo_root=Path(tmp))
            result = lu.check_continuity_for_symbol(
                "1m",
                "BTCUSDT",
                paths,
                now_ms=BASE_OT + 5 * ONE_MIN,
            )
            self.assertEqual(result.status, "empty")
            self.assertEqual(result.rows, 0)
            self.assertIsNone(result.min_open_time)
            self.assertIsNone(result.max_open_time)
            self.assertIsNone(result.lag_bars)
            self.assertEqual(result.duplicate_count, 0)
            self.assertEqual(result.missing_count, 0)
            self.assertIsNotNone(result.latest_closed_open_time)

    def test_continuous_series_reports_ok_with_zero_duplicates_and_missing(self):
        with tempfile.TemporaryDirectory() as tmp:
            records = [make_record(BASE_OT + i * ONE_MIN) for i in range(5)]
            paths = self._build_current(tmp, records)
            result = lu.check_continuity_for_symbol(
                "1m",
                "BTCUSDT",
                paths,
                now_ms=BASE_OT + 10 * ONE_MIN,
            )
            self.assertEqual(result.status, "ok")
            self.assertEqual(result.rows, 5)
            self.assertEqual(result.min_open_time, BASE_OT)
            self.assertEqual(result.max_open_time, BASE_OT + 4 * ONE_MIN)
            self.assertEqual(result.duplicate_count, 0)
            self.assertEqual(result.missing_count, 0)
            self.assertEqual(result.misaligned_count, 0)
            self.assertEqual(result.close_time_mismatch_count, 0)
            expected_latest = lu.calculate_latest_closed_open_time(
                "1m", BASE_OT + 10 * ONE_MIN
            )
            self.assertEqual(result.latest_closed_open_time, expected_latest)
            expected_lag = (expected_latest - (BASE_OT + 4 * ONE_MIN)) // ONE_MIN
            self.assertEqual(result.lag_bars, expected_lag)

    def test_duplicate_open_time_detected(self):
        with tempfile.TemporaryDirectory() as tmp:
            # The merge path deduplicates by primary key, so duplicates can only
            # appear via corruption / cross-writer bugs. Write a raw parquet with
            # two identical open_times to verify the checker catches it.
            paths = lu.LiveUpdatePaths(repo_root=Path(tmp))
            rec = make_record(BASE_OT)
            target = paths.current_partition_file(rec.partition_key())
            target.parent.mkdir(parents=True, exist_ok=True)
            import pyarrow as pa

            schema = lu._current_parquet_schema()
            row = rec.physical_dict()
            table = pa.Table.from_pylist([row, row], schema=schema)
            tmp_path = target.with_name(target.name + ".tmp")
            pq.write_table(table, tmp_path)
            tmp_path.replace(target)

            result = lu.check_continuity_for_symbol(
                "1m",
                "BTCUSDT",
                paths,
                now_ms=BASE_OT + 5 * ONE_MIN,
            )
            self.assertEqual(result.status, "gap_detected")
            self.assertEqual(result.rows, 2)
            self.assertEqual(result.duplicate_count, 1)
            self.assertEqual(result.missing_count, 0)

    def test_missing_open_time_gap_detected(self):
        with tempfile.TemporaryDirectory() as tmp:
            # 0,1, then skip 2,3, then 4 -> 2 missing bars
            records = [
                make_record(BASE_OT),
                make_record(BASE_OT + ONE_MIN),
                make_record(BASE_OT + 4 * ONE_MIN),
            ]
            paths = self._build_current(tmp, records)
            result = lu.check_continuity_for_symbol(
                "1m",
                "BTCUSDT",
                paths,
                now_ms=BASE_OT + 10 * ONE_MIN,
            )
            self.assertEqual(result.status, "gap_detected")
            self.assertEqual(result.missing_count, 2)
            self.assertEqual(result.duplicate_count, 0)

    def test_misaligned_open_time_detected(self):
        with tempfile.TemporaryDirectory() as tmp:
            # One bar whose open_time is not a multiple of interval_ms.
            records = [
                make_record(BASE_OT),
                make_record(BASE_OT + ONE_MIN, close_time=BASE_OT + ONE_MIN + ONE_MIN - 1),
            ]
            # Force a misaligned open_time by writing a raw row directly.
            paths = self._build_current(tmp, records)
            target = paths.current_partition_file(records[0].partition_key())
            rows = pq.ParquetFile(target).read().to_pylist()
            rows[1] = {**rows[1], "open_time": BASE_OT + 30_000}  # not aligned to 1m
            schema = lu._current_parquet_schema()
            import pyarrow as pa

            table = pa.Table.from_pylist(rows, schema=schema)
            tmp_path = target.with_name(target.name + ".tmp")
            pq.write_table(table, tmp_path)
            tmp_path.replace(target)

            result = lu.check_continuity_for_symbol(
                "1m",
                "BTCUSDT",
                paths,
                now_ms=BASE_OT + 5 * ONE_MIN,
            )
            self.assertEqual(result.status, "gap_detected")
            self.assertGreaterEqual(result.misaligned_count, 1)

    def test_close_time_mismatch_detected(self):
        with tempfile.TemporaryDirectory() as tmp:
            records = [make_record(BASE_OT)]
            paths = self._build_current(tmp, records)
            target = paths.current_partition_file(records[0].partition_key())
            rows = pq.ParquetFile(target).read().to_pylist()
            rows[0] = {**rows[0], "close_time": BASE_OT + ONE_MIN - 2}
            import pyarrow as pa

            schema = lu._current_parquet_schema()
            table = pa.Table.from_pylist(rows, schema=schema)
            tmp_path = target.with_name(target.name + ".tmp")
            pq.write_table(table, tmp_path)
            tmp_path.replace(target)

            result = lu.check_continuity_for_symbol(
                "1m",
                "BTCUSDT",
                paths,
                now_ms=BASE_OT + 5 * ONE_MIN,
            )
            self.assertEqual(result.status, "gap_detected")
            self.assertEqual(result.close_time_mismatch_count, 1)

    def test_lag_bars_uses_latest_closed_open_time(self):
        with tempfile.TemporaryDirectory() as tmp:
            records = [make_record(BASE_OT)]
            paths = self._build_current(tmp, records)
            now_ms = BASE_OT + 6 * ONE_MIN
            result = lu.check_continuity_for_symbol(
                "1m",
                "BTCUSDT",
                paths,
                now_ms=now_ms,
                close_lag_ms=2000,
            )
            expected_latest = lu.calculate_latest_closed_open_time("1m", now_ms)
            self.assertEqual(result.latest_closed_open_time, expected_latest)
            expected_lag = (expected_latest - BASE_OT) // ONE_MIN
            self.assertEqual(result.lag_bars, expected_lag)

    def test_run_continuity_check_across_intervals_and_symbols(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = lu.LiveUpdatePaths(repo_root=Path(tmp))
            lu.merge_records_to_current_partition(
                [make_record(BASE_OT, interval="1m", symbol="BTCUSDT")], paths
            )
            lu.merge_records_to_current_partition(
                [make_record(BASE_OT, interval="1m", symbol="ETHUSDT")], paths
            )
            results = lu.run_continuity_check(
                ("1m",),
                ["BTCUSDT", "ETHUSDT"],
                paths,
                now_ms=BASE_OT + 5 * ONE_MIN,
            )
            self.assertEqual(list(results.keys()), ["1m"])
            symbols = [item.symbol for item in results["1m"]]
            self.assertEqual(symbols, ["BTCUSDT", "ETHUSDT"])
            self.assertTrue(all(item.status == "ok" for item in results["1m"]))

    def test_discover_current_dataset_symbols_finds_existing_partitions(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = lu.LiveUpdatePaths(repo_root=Path(tmp))
            lu.merge_records_to_current_partition(
                [make_record(BASE_OT, symbol="BTCUSDT")], paths
            )
            lu.merge_records_to_current_partition(
                [make_record(BASE_OT, symbol="ETHUSDT")], paths
            )
            self.assertEqual(
                lu.discover_current_dataset_symbols("1m", paths),
                ["BTCUSDT", "ETHUSDT"],
            )

    def test_continuity_summary_payload_reports_overall_status(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = lu.LiveUpdatePaths(repo_root=Path(tmp))
            # Write a raw parquet with a missing-bar gap so overall is gap_detected.
            recs = [make_record(BASE_OT), make_record(BASE_OT + 4 * ONE_MIN)]
            by_part: dict[tuple, list[lu.KlineRecord]] = {}
            for rec in recs:
                by_part.setdefault(rec.partition_key().as_tuple(), []).append(rec)
            for batch in by_part.values():
                lu.merge_records_to_current_partition(batch, paths)

            results = lu.run_continuity_check(
                ("1m",),
                ["BTCUSDT"],
                paths,
                now_ms=BASE_OT + 5 * ONE_MIN,
            )
            payload = lu.continuity_summary_payload(
                results,
                requested_interval="1m",
                active_intervals=("1m",),
                symbols=["BTCUSDT"],
                now_ms=BASE_OT + 5 * ONE_MIN,
            )
            self.assertEqual(payload["requested_interval"], "1m")
            self.assertEqual(payload["overall_status"], "gap_detected")
            entry = payload["interval_results"]["1m"][0]
            for field in (
                "symbol",
                "interval",
                "rows",
                "min_open_time",
                "max_open_time",
                "duplicate_count",
                "missing_count",
                "latest_closed_open_time",
                "lag_bars",
                "status",
            ):
                self.assertIn(field, entry)
            self.assertEqual(entry["missing_count"], 3)


@unittest.skipIf(pq is None, "pyarrow is required for Phase 8 CLI continuity tests")
class CheckContinuityCliTests(unittest.TestCase):
    def test_check_continuity_with_symbols_is_network_free_and_outputs_summary(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = lu.LiveUpdatePaths(repo_root=Path(tmp))
            lu.merge_records_to_current_partition(
                [make_record(BASE_OT), make_record(BASE_OT + ONE_MIN)], paths
            )
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
                    "--check-continuity",
                    "--now-ms",
                    str(BASE_OT + 10 * ONE_MIN),
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
                check=False,
            )
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            payload = json.loads(result.stdout)
            self.assertEqual(payload["requested_interval"], "1m")
            self.assertEqual(payload["overall_status"], "ok")
            entry = payload["interval_results"]["1m"][0]
            self.assertEqual(entry["symbol"], "BTCUSDT")
            self.assertEqual(entry["status"], "ok")
            self.assertEqual(entry["duplicate_count"], 0)
            self.assertEqual(entry["missing_count"], 0)

    def test_check_continuity_all_intervals_expand_without_network(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = subprocess.run(
                [
                    sys.executable,
                    "scripts/live_update.py",
                    "--repo-root",
                    tmp,
                    "--interval",
                    "all",
                    "--symbols",
                    "BTCUSDT",
                    "--check-continuity",
                    "--now-ms",
                    str(BASE_OT + 10 * ONE_MIN),
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
                check=False,
            )
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            payload = json.loads(result.stdout)
            self.assertEqual(
                payload["active_intervals"], list(lu.SUPPORTED_INTERVALS)
            )
            # No current dataset -> every interval reports empty.
            for interval in lu.SUPPORTED_INTERVALS:
                self.assertEqual(
                    payload["interval_results"][interval][0]["status"], "empty"
                )
            self.assertEqual(payload["overall_status"], "empty")

    def test_check_continuity_discovers_symbols_from_current_dataset(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = lu.LiveUpdatePaths(repo_root=Path(tmp))
            lu.merge_records_to_current_partition(
                [make_record(BASE_OT, symbol="BTCUSDT")], paths
            )
            # No --symbols: should discover BTCUSDT from current dataset, no network.
            result = subprocess.run(
                [
                    sys.executable,
                    "scripts/live_update.py",
                    "--repo-root",
                    tmp,
                    "--interval",
                    "1m",
                    "--check-continuity",
                    "--now-ms",
                    str(BASE_OT + 5 * ONE_MIN),
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
                check=False,
                timeout=30,
            )
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            payload = json.loads(result.stdout)
            self.assertEqual(payload["symbols"], ["BTCUSDT"])
            self.assertEqual(
                payload["interval_results"]["1m"][0]["symbol"], "BTCUSDT"
            )

    def test_check_continuity_reports_gap_for_missing_bars(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = lu.LiveUpdatePaths(repo_root=Path(tmp))
            lu.merge_records_to_current_partition(
                [
                    make_record(BASE_OT),
                    make_record(BASE_OT + ONE_MIN),
                    make_record(BASE_OT + 4 * ONE_MIN),
                ],
                paths,
            )
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
                    "--check-continuity",
                    "--now-ms",
                    str(BASE_OT + 10 * ONE_MIN),
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
                check=False,
            )
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            payload = json.loads(result.stdout)
            self.assertEqual(payload["overall_status"], "gap_detected")
            entry = payload["interval_results"]["1m"][0]
            self.assertEqual(entry["missing_count"], 2)


class KBarValidationRuleTests(unittest.TestCase):
    """Full validation rule set from 08_VALIDATION_AND_TESTS.md section 1."""

    def _valid_record(self, **overrides):
        kwargs = dict(
            symbol="BTCUSDT",
            interval="1m",
            open_time=BASE_OT,
            open=50000.0,
            high=50100.0,
            low=49900.0,
            close=50050.0,
            volume=1000.0,
            close_time=BASE_OT + ONE_MIN - 1,
            quote_volume=50050000.0,
            trade_count=100,
            taker_buy_base_volume=500.0,
            taker_buy_quote_volume=25025000.0,
            source_archive="live_websocket:kline",
            archive_source="live_websocket",
            archive_period="2021-12-06",
        )
        kwargs.update(overrides)
        return lu.KlineRecord.build(**kwargs)

    def test_valid_record_has_no_errors(self):
        rec = self._valid_record()
        self.assertEqual(lu.validate_live_kline_record(rec, is_closed=True), [])

    def test_open_time_alignment_error(self):
        rec = self._valid_record(
            open_time=BASE_OT + 30_000,
            close_time=BASE_OT + 30_000 + ONE_MIN - 1,
        )
        errors = lu.validate_live_kline_record(rec, is_closed=True)
        self.assertTrue(any("aligned" in e for e in errors))

    def test_close_time_mismatch_error(self):
        rec = self._valid_record(close_time=BASE_OT + ONE_MIN - 2)
        errors = lu.validate_live_kline_record(rec, is_closed=True)
        self.assertTrue(any("close_time mismatch" in e for e in errors))

    def test_ohlc_high_below_open_close_low_error(self):
        rec = self._valid_record(open=5.0, high=2.0, low=1.0, close=4.0)
        errors = lu.validate_live_kline_record(rec, is_closed=True)
        self.assertTrue(any("OHLC invalid" in e and "high" in e for e in errors))

    def test_ohlc_low_above_open_close_high_error(self):
        rec = self._valid_record(open=1.0, high=5.0, low=6.0, close=2.0)
        errors = lu.validate_live_kline_record(rec, is_closed=True)
        self.assertTrue(any("OHLC invalid" in e and "low" in e for e in errors))

    def test_negative_volume_error(self):
        rec = self._valid_record(volume=-1.0)
        errors = lu.validate_live_kline_record(rec, is_closed=True)
        self.assertTrue(any("volume" in e for e in errors))

    def test_negative_quote_volume_error(self):
        rec = self._valid_record(quote_volume=-1.0)
        errors = lu.validate_live_kline_record(rec, is_closed=True)
        self.assertTrue(any("quote_volume" in e for e in errors))

    def test_negative_trade_count_error(self):
        rec = self._valid_record(trade_count=-1)
        errors = lu.validate_live_kline_record(rec, is_closed=True)
        self.assertTrue(any("trade_count" in e for e in errors))

    def test_negative_taker_base_volume_error(self):
        rec = self._valid_record(taker_buy_base_volume=-1.0)
        errors = lu.validate_live_kline_record(rec, is_closed=True)
        self.assertTrue(any("taker_buy_base_volume" in e for e in errors))

    def test_negative_taker_quote_volume_error(self):
        rec = self._valid_record(taker_buy_quote_volume=-1.0)
        errors = lu.validate_live_kline_record(rec, is_closed=True)
        self.assertTrue(any("taker_buy_quote_volume" in e for e in errors))

    def test_empty_symbol_error(self):
        rec = self._valid_record(symbol="")
        errors = lu.validate_live_kline_record(rec, is_closed=True)
        self.assertTrue(any("symbol" in e for e in errors))

    def test_unsupported_interval_rejected_at_build(self):
        with self.assertRaises(lu.LiveUpdateCommandError):
            self._valid_record(interval="2m")


class SharedValidationAcrossSourcesTests(unittest.TestCase):
    """Items 12-14: WS/REST/webhook share validation; unclosed/closed flow."""

    def test_websocket_rest_webhook_all_use_same_validate_function(self):
        # The single shared gate is validate_live_kline_record; each source
        # routes events through _event_validation_errors -> that function.
        ws_event = lu.websocket_payload_to_event(ws_payload(BASE_OT, closed=True))
        rest_event = lu.rest_row_to_kline_event(
            rest_row(BASE_OT, close=2.5),
            symbol="BTCUSDT",
            interval="1m",
            now_ms=BASE_OT + ONE_MIN,
        )
        wh_event = lu.webhook_payload_to_event(
            normalized_payload(BASE_OT, closed=True, close=3.5),
            now_ms=BASE_OT + ONE_MIN,
        )
        ws_errors = lu._event_validation_errors(ws_event)
        rest_errors = lu._event_validation_errors(rest_event)
        wh_errors = lu._event_validation_errors(wh_event)
        self.assertEqual(ws_errors, [])
        self.assertEqual(rest_errors, [])
        self.assertEqual(wh_errors, [])

    def test_invalid_payload_rejected_by_all_three_sources(self):
        # Same OHLC defect (high below open) surfaces the same shared-validator
        # error for WebSocket, REST, and webhook events via _event_validation_errors.
        ws_bad = ws_payload(BASE_OT, closed=True, close=1.5)
        ws_bad["k"]["o"] = "5.0"
        ws_bad["k"]["h"] = "2.0"
        ws_event = lu.websocket_payload_to_event(ws_bad)

        rest_bad = rest_row(BASE_OT, close=1.5)
        rest_bad[1] = "5.0"  # open
        rest_bad[2] = "2.0"  # high
        rest_event = lu.rest_row_to_kline_event(
            rest_bad,
            symbol="BTCUSDT",
            interval="1m",
            now_ms=BASE_OT + ONE_MIN,
        )

        wh_bad = normalized_payload(BASE_OT, closed=True, close=1.5)
        wh_bad["open"] = "5.0"
        wh_bad["high"] = "2.0"
        wh_event = lu.webhook_payload_to_event(wh_bad, now_ms=BASE_OT + ONE_MIN)

        for event, label in (
            (ws_event, "websocket"),
            (rest_event, "rest"),
            (wh_event, "webhook"),
        ):
            errors = lu._event_validation_errors(event)
            self.assertTrue(
                any("OHLC invalid" in e for e in errors),
                f"{label} source did not surface shared OHLC validation error: {errors}",
            )

    @unittest.skipIf(pq is None, "pyarrow is required for Phase 8 flow tests")
    def test_unclosed_websocket_bar_skips_closed_buffer_and_current_dataset(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = lu.LiveUpdatePaths(repo_root=Path(tmp))
            state = lu.LiveUpdateState.create("1m", paths)
            result = lu.process_websocket_message(
                ws_payload(BASE_OT, closed=False),
                state,
                paths,
                received_at_utc="2026-06-26T00:00:00Z",
            )
            self.assertEqual(result.status, "open_buffered")
            self.assertFalse(result.is_closed)
            self.assertTrue(
                paths.buffer_jsonl("1m", "websocket_buffer", "2021-12-06").exists()
            )
            self.assertTrue(paths.latest_json("1m", "BTCUSDT").exists())
            self.assertFalse(
                paths.closed_buffer_jsonl("1m", "2021-12-06").exists()
            )
            self.assertFalse(paths.current_parquet_root("1m").exists())
            self.assertIsNone(
                state.symbols["BTCUSDT"].last_closed_open_time
            )

    @unittest.skipIf(pq is None, "pyarrow is required for Phase 8 flow tests")
    def test_closed_websocket_bar_flows_closed_buffer_merge_state(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = lu.LiveUpdatePaths(repo_root=Path(tmp))
            state = lu.LiveUpdateState.create("1m", paths)
            result = lu.process_websocket_message(
                ws_payload(BASE_OT, closed=True, close=4.5),
                state,
                paths,
                received_at_utc="2026-06-26T00:01:00Z",
            )
            self.assertEqual(result.status, "closed_merged")
            self.assertTrue(result.is_closed)
            self.assertTrue(
                paths.closed_buffer_jsonl("1m", "2021-12-06").exists()
            )
            self.assertIsNotNone(result.merge_result)
            rows = pq.ParquetFile(result.merge_result.target_path).read().to_pylist()
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["archive_source"], "live_websocket")
            self.assertEqual(
                state.symbols["BTCUSDT"].last_closed_open_time, BASE_OT
            )
            self.assertEqual(
                state.symbols["BTCUSDT"].last_flushed_open_time, BASE_OT
            )

    @unittest.skipIf(pq is None, "pyarrow is required for Phase 8 flow tests")
    def test_unclosed_webhook_bar_skips_closed_buffer_and_current_dataset(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = lu.LiveUpdatePaths(repo_root=Path(tmp))
            result = lu.process_webhook_payload(
                normalized_payload(BASE_OT, closed=False),
                paths,
                active_intervals=("1m",),
                now_ms=BASE_OT + 1000,
                received_at_utc="2026-06-26T00:00:00Z",
            )
            self.assertEqual(result.status, "accepted")
            self.assertFalse(result.is_closed)
            self.assertTrue(
                paths.buffer_jsonl("1m", "webhook_buffer", "2021-12-06").exists()
            )
            self.assertFalse(
                paths.closed_buffer_jsonl("1m", "2021-12-06").exists()
            )
            self.assertFalse(paths.current_parquet_root("1m").exists())

    @unittest.skipIf(pq is None, "pyarrow is required for Phase 8 flow tests")
    def test_closed_webhook_bar_flows_closed_buffer_merge_state(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = lu.LiveUpdatePaths(repo_root=Path(tmp))
            result = lu.process_webhook_payload(
                normalized_payload(BASE_OT, closed=True, close=6.5),
                paths,
                active_intervals=("1m",),
                now_ms=BASE_OT + ONE_MIN,
                received_at_utc="2026-06-26T00:01:00Z",
            )
            self.assertEqual(result.status, "merged")
            self.assertTrue(result.is_closed)
            self.assertTrue(
                paths.closed_buffer_jsonl("1m", "2021-12-06").exists()
            )
            self.assertIsNotNone(result.merge_result)
            rows = pq.ParquetFile(result.merge_result.target_path).read().to_pylist()
            self.assertEqual(rows[0]["archive_source"], "live_webhook")
            loaded = lu.load_live_update_state("1m", paths)
            self.assertEqual(
                loaded.symbols["BTCUSDT"].last_closed_open_time, BASE_OT
            )

    @unittest.skipIf(pq is None, "pyarrow is required for Phase 8 flow tests")
    def test_closed_rest_bar_flows_closed_buffer_merge_state(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = lu.LiveUpdatePaths(repo_root=Path(tmp))
            state = lu.LiveUpdateState.create("1m", paths)
            # Seed a last_closed_open_time one bar before BASE_OT so the plan
            # reports a one-bar gap ending at BASE_OT.
            state.symbol_state("BTCUSDT").last_closed_open_time = BASE_OT - ONE_MIN
            lu.save_live_update_state(state, paths)

            # now_ms is chosen so latest_closed_open_time == BASE_OT (one-bar gap).
            now_ms = BASE_OT + 2 * ONE_MIN
            plan = lu.plan_symbol_startup_backfill(
                interval="1m",
                symbol="BTCUSDT",
                state=state,
                paths=paths,
                now_ms=now_ms,
            )
            self.assertEqual(plan.status, "missing")
            self.assertEqual(plan.start_open_time, BASE_OT)
            self.assertEqual(plan.end_open_time, BASE_OT)

            fake_rows = [rest_row(BASE_OT, close=7.5)]
            from unittest.mock import MagicMock

            fake_fetch = MagicMock()
            fake_fetch.status = "ok"
            fake_fetch.rows = fake_rows
            fake_fetch.error = None

            with patch.object(lu, "fetch_rest_klines", return_value=fake_fetch):
                result = lu.run_rest_backfill_for_plan(
                    plan,
                    state,
                    paths,
                    now_ms=now_ms,
                    sleep_func=lambda _s: None,
                )
            self.assertEqual(result.status, "ok")
            self.assertEqual(result.closed_row_count, 1)
            self.assertEqual(result.merged_row_count, 1)
            self.assertTrue(
                paths.closed_buffer_jsonl("1m", "2021-12-06").exists()
            )
            self.assertEqual(
                state.symbols["BTCUSDT"].last_closed_open_time, BASE_OT
            )
            self.assertTrue(paths.current_parquet_root("1m").exists())


class CliModesStillWorkTests(unittest.TestCase):
    """Item 11: --interval all / --once / --run-startup-backfill-once / --describe-*."""

    def test_describe_layout_all_intervals(self):
        result = subprocess.run(
            [sys.executable, "scripts/live_update.py", "--interval", "all", "--describe-layout"],
            cwd=REPO_ROOT,
            text=True,
            capture_output=True,
            check=False,
            timeout=30,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        payload = json.loads(result.stdout)
        self.assertEqual(payload["active_intervals"], list(lu.SUPPORTED_INTERVALS))
        self.assertTrue(payload["intervals_are_api_safe"])

    def test_describe_websocket_connections(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = subprocess.run(
                [
                    sys.executable,
                    "scripts/live_update.py",
                    "--repo-root",
                    tmp,
                    "--interval",
                    "all",
                    "--symbols",
                    "BTCUSDT",
                    "--describe-websocket-connections",
                    "--ws-batch-size",
                    "3",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
                check=False,
                timeout=30,
            )
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            payload = json.loads(result.stdout)
            self.assertEqual(payload["stream_count"], 7)
            self.assertEqual(payload["connection_count"], 3)

    def test_describe_webhook_server(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = subprocess.run(
                [
                    sys.executable,
                    "scripts/live_update.py",
                    "--repo-root",
                    tmp,
                    "--interval",
                    "all",
                    "--describe-webhook-server",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
                check=False,
                timeout=30,
            )
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            payload = json.loads(result.stdout)
            self.assertTrue(payload["webhook_enabled"])
            self.assertEqual(payload["server"]["host"], "127.0.0.1")

    def test_plan_startup_backfill_without_network(self):
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
                    str(BASE_OT + 5 * ONE_MIN),
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
                check=False,
                timeout=30,
            )
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            payload = json.loads(result.stdout)
            self.assertEqual(payload["active_intervals"], ["1m"])
            self.assertEqual(payload["plans"][0]["plans"][0]["status"], "bootstrap_required")

    def test_run_startup_backfill_once_without_network(self):
        with tempfile.TemporaryDirectory() as tmp:
            # No seed, no current dataset, no symbols -> bootstrap_required, no REST.
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
                    "--run-startup-backfill-once",
                    "--now-ms",
                    str(BASE_OT + 5 * ONE_MIN),
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
                check=False,
                timeout=30,
            )
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            payload = json.loads(result.stdout)
            self.assertEqual(payload["active_intervals"], ["1m"])
            self.assertEqual(payload["results"][0]["status"], "bootstrap_required")

    @unittest.skipIf(pq is None, "pyarrow is required for Phase 8 once mode test")
    def test_once_mode_with_check_continuity_emits_summary(self):
        with tempfile.TemporaryDirectory() as tmp:
            # Pre-seed a current dataset so continuity has rows to check.
            paths = lu.LiveUpdatePaths(repo_root=Path(tmp))
            lu.merge_records_to_current_partition(
                [make_record(BASE_OT), make_record(BASE_OT + ONE_MIN)], paths
            )
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
                    "--once",
                    "--check-continuity",
                    "--disable-startup-backfill",
                    "--now-ms",
                    str(BASE_OT + 10 * ONE_MIN),
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
                check=False,
                timeout=60,
            )
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            # The once skeleton prints a startup-summary JSON block first, then
            # human-readable steps, then the continuity-check JSON block. Locate
            # the continuity block by its marker and parse just that JSON.
            self.assertIn("continuity_check", result.stdout)
            marker = '"continuity_check"'
            marker_idx = result.stdout.index(marker)
            json_start = result.stdout.rindex("{", 0, marker_idx)
            payload = json.JSONDecoder().raw_decode(result.stdout[json_start:])[0]
            cc = payload["continuity_check"]
            self.assertEqual(cc["overall_status"], "ok")
            self.assertEqual(cc["interval_results"]["1m"][0]["status"], "ok")


if __name__ == "__main__":
    unittest.main()
