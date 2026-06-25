"""Phase 1 tests for Binance UM Kline live-update primitives."""

from pathlib import Path
import io
import json
import subprocess
import sys
import tempfile
import threading
import unittest
import urllib.error
import urllib.request

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


def rest_row(open_time, *, close=1.5):
    return [
        open_time,
        "1.0",
        str(max(2.0, close)),
        "0.5",
        str(close),
        "10.0",
        open_time + lu.interval_milliseconds("1m") - 1,
        "15.0",
        3,
        "5.0",
        "7.5",
        "0",
    ]


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
        "close_time": open_time + lu.interval_milliseconds(interval) - 1,
        "quote_volume": "15.0",
        "trade_count": 3,
        "taker_buy_base_volume": "5.0",
        "taker_buy_quote_volume": "7.5",
    }
    if include_is_closed:
        payload["is_closed"] = closed
    return payload


class FakeHttpResponse:
    def __init__(self, payload):
        self.payload = payload

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def read(self):
        return json.dumps(self.payload).encode("utf-8")


def http_error(code, body):
    return urllib.error.HTTPError(
        url="https://example.test/fapi/v1/klines",
        code=code,
        msg="error",
        hdrs={},
        fp=io.BytesIO(body.encode("utf-8")),
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


class RestFetcherTests(unittest.TestCase):
    def test_rest_url_uses_required_klines_parameters(self):
        url = lu.rest_klines_url(
            base_url="https://fapi.binance.com",
            symbol="btcusdt",
            interval="1m",
            start_time=1000,
            end_time=2000,
            limit=500,
        )
        self.assertIn("/fapi/v1/klines?", url)
        self.assertIn("symbol=BTCUSDT", url)
        self.assertIn("interval=1m", url)
        self.assertIn("startTime=1000", url)
        self.assertIn("endTime=2000", url)
        self.assertIn("limit=500", url)

    def test_fetch_rest_klines_retries_429_then_succeeds(self):
        calls = []
        sleeps = []

        def opener(request, timeout):
            calls.append(request.full_url)
            if len(calls) == 1:
                raise http_error(429, '{"code":-1003,"msg":"rate limit"}')
            return FakeHttpResponse([rest_row(1638747600000)])

        result = lu.fetch_rest_klines(
            symbol="BTCUSDT",
            interval="1m",
            start_time=1638747600000,
            end_time=1638747600000,
            opener=opener,
            sleep_func=sleeps.append,
            backoff_base_seconds=2,
            max_retries=2,
        )

        self.assertEqual(result.status, "ok")
        self.assertEqual(len(result.rows), 1)
        self.assertEqual(len(calls), 2)
        self.assertEqual(sleeps, [2.0])

    def test_fetch_rest_klines_418_stops_requests(self):
        def opener(request, timeout):
            raise http_error(418, '{"code":-1003,"msg":"banned"}')

        with self.assertRaises(lu.RestStopRequests):
            lu.fetch_rest_klines(
                symbol="BTCUSDT",
                interval="1m",
                start_time=1,
                end_time=1,
                opener=opener,
                sleep_func=lambda _: None,
            )

    def test_fetch_rest_klines_invalid_symbol_is_nonfatal(self):
        def opener(request, timeout):
            raise http_error(400, '{"code":-1121,"msg":"Invalid symbol."}')

        result = lu.fetch_rest_klines(
            symbol="BADUSDT",
            interval="1m",
            start_time=1,
            end_time=1,
            opener=opener,
            sleep_func=lambda _: None,
        )

        self.assertEqual(result.status, "symbol_unavailable")

    def test_fetch_rest_klines_5xx_exhaustion_uses_backoff(self):
        sleeps = []

        def opener(request, timeout):
            raise http_error(500, '{"msg":"server error"}')

        result = lu.fetch_rest_klines(
            symbol="BTCUSDT",
            interval="1m",
            start_time=1,
            end_time=1,
            opener=opener,
            sleep_func=sleeps.append,
            max_retries=2,
            backoff_base_seconds=1,
        )

        self.assertEqual(result.status, "retry_exhausted")
        self.assertEqual(result.http_status, 500)
        self.assertEqual(sleeps, [1.0, 2.0])

    def test_fetch_rest_klines_timeout_exhaustion_uses_backoff(self):
        sleeps = []

        def opener(request, timeout):
            raise urllib.error.URLError("timed out")

        result = lu.fetch_rest_klines(
            symbol="BTCUSDT",
            interval="1m",
            start_time=1,
            end_time=1,
            opener=opener,
            sleep_func=sleeps.append,
            max_retries=1,
            backoff_base_seconds=3,
        )

        self.assertEqual(result.status, "retry_exhausted")
        self.assertEqual(sleeps, [3.0])

    def test_rest_row_normalization_identifies_unclosed_bar(self):
        row = rest_row(1638747600000)
        event = lu.rest_row_to_kline_event(
            row,
            symbol="btcusdt",
            interval="1m",
            now_ms=1638747659999,
            close_lag_ms=2000,
        )

        self.assertEqual(event.record.symbol, "BTCUSDT")
        self.assertEqual(event.record.archive_source, "live_rest")
        self.assertFalse(event.is_closed)


@unittest.skipIf(pq is None, "pyarrow is required for Phase 4 parquet tests")
class RestBackfillFlowTests(unittest.TestCase):
    def test_run_rest_backfill_for_plan_writes_buffers_merges_and_updates_state(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = lu.LiveUpdatePaths(repo_root=Path(tmp))
            state = lu.LiveUpdateState.create("1m", paths)
            plan = lu.MissingBarsPlan(
                symbol="BTCUSDT",
                interval="1m",
                last_closed_open_time=1638747480000,
                latest_closed_open_time=1638747660000,
                missing_bars=3,
                start_open_time=1638747540000,
                end_open_time=1638747660000,
                source="state",
                status="missing",
            )

            def opener(request, timeout):
                return FakeHttpResponse([
                    rest_row(1638747540000, close=1.5),
                    rest_row(1638747600000, close=2.5),
                    rest_row(1638747660000, close=3.5),
                ])

            result = lu.run_rest_backfill_for_plan(
                plan,
                state,
                paths,
                now_ms=1638747785000,
                rest_api_limit=1500,
                opener=opener,
                sleep_func=lambda _: None,
            )

            self.assertEqual(result.status, "ok")
            self.assertEqual(result.event_row_count, 3)
            self.assertEqual(result.closed_row_count, 3)
            self.assertEqual(result.merged_row_count, 3)
            self.assertEqual(state.symbols["BTCUSDT"].last_closed_open_time, 1638747660000)
            self.assertTrue(paths.buffer_jsonl("1m", "event_buffer", "2021-12-06").exists())
            self.assertTrue(paths.closed_buffer_jsonl("1m", "2021-12-06").exists())
            self.assertTrue(paths.latest_json("1m", "BTCUSDT").exists())
            self.assertTrue(result.merge_results[0].target_path.exists())
            self.assertTrue(paths.state_json("1m").exists())

    def test_unclosed_rest_bar_updates_event_and_latest_only(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = lu.LiveUpdatePaths(repo_root=Path(tmp))
            state = lu.LiveUpdateState.create("1m", paths)
            plan = lu.MissingBarsPlan(
                symbol="BTCUSDT",
                interval="1m",
                last_closed_open_time=1638747480000,
                latest_closed_open_time=1638747540000,
                missing_bars=1,
                start_open_time=1638747540000,
                end_open_time=1638747540000,
                source="state",
                status="missing",
            )

            def opener(request, timeout):
                return FakeHttpResponse([rest_row(1638747540000)])

            result = lu.run_rest_backfill_for_plan(
                plan,
                state,
                paths,
                now_ms=1638747541000,
                close_lag_ms=2000,
                opener=opener,
                sleep_func=lambda _: None,
            )

            self.assertEqual(result.closed_row_count, 0)
            self.assertEqual(result.latest_open_row_count, 1)
            self.assertTrue(paths.buffer_jsonl("1m", "event_buffer", "2021-12-06").exists())
            self.assertTrue(paths.latest_json("1m", "BTCUSDT").exists())
            self.assertFalse(paths.closed_buffer_jsonl("1m", "2021-12-06").exists())
            self.assertFalse(paths.current_parquet_root("1m").exists())
            self.assertIsNone(state.symbols.get("BTCUSDT"))

    def test_empty_rest_response_records_warning_and_stops(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = lu.LiveUpdatePaths(repo_root=Path(tmp))
            state = lu.LiveUpdateState.create("1m", paths)
            plan = lu.MissingBarsPlan(
                symbol="BTCUSDT",
                interval="1m",
                last_closed_open_time=1638747480000,
                latest_closed_open_time=1638747540000,
                missing_bars=1,
                start_open_time=1638747540000,
                end_open_time=1638747540000,
                source="state",
                status="missing",
            )

            result = lu.run_rest_backfill_for_plan(
                plan,
                state,
                paths,
                now_ms=1638747785000,
                opener=lambda request, timeout: FakeHttpResponse([]),
                sleep_func=lambda _: None,
            )

            self.assertEqual(result.status, "ok_with_warnings")
            self.assertIn("empty REST response", result.warnings[0])
            self.assertTrue(paths.runtime_log("1m", lu.utc_now()[:10], "warnings.log").exists())

    def test_run_startup_backfill_once_stops_on_418(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = lu.LiveUpdatePaths(repo_root=Path(tmp))
            state = lu.LiveUpdateState.create("1m", paths)
            state.symbol_state("BTCUSDT").last_closed_open_time = 1638747480000
            lu.save_live_update_state(state, paths)

            def opener(request, timeout):
                raise http_error(418, '{"code":-1003,"msg":"banned"}')

            results = lu.run_startup_backfill_once(
                ("1m",),
                ["BTCUSDT"],
                paths,
                now_ms=1638747785000,
                opener=opener,
                sleep_func=lambda _: None,
            )

            self.assertEqual(results[-1].status, "rest_stopped")
            self.assertTrue(results[-1].warnings)


class ScriptPhase4Tests(unittest.TestCase):
    def test_script_run_startup_backfill_once_requires_symbols(self):
        result = subprocess.run(
            [
                sys.executable,
                "scripts/live_update.py",
                "--interval",
                "1m",
                "--run-startup-backfill-once",
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


class WebSocketPlanningTests(unittest.TestCase):
    def test_stream_names_expand_symbols_and_intervals(self):
        self.assertEqual(
            lu.websocket_stream_name("BTCUSDT", "1m"),
            "btcusdt@kline_1m",
        )
        streams = lu.build_websocket_streams(["btcusdt"], lu.SUPPORTED_INTERVALS)
        self.assertEqual(len(streams), len(lu.SUPPORTED_INTERVALS))
        self.assertEqual(streams[0], "btcusdt@kline_1m")
        self.assertEqual(streams[-1], "btcusdt@kline_1d")

    def test_batches_and_combined_urls_are_deterministic(self):
        streams = (
            "btcusdt@kline_1m",
            "btcusdt@kline_3m",
            "ethusdt@kline_1m",
        )
        batches = lu.batch_websocket_streams(streams, ws_batch_size=2)
        self.assertEqual(
            batches,
            [("btcusdt@kline_1m", "btcusdt@kline_3m"), ("ethusdt@kline_1m",)],
        )
        url = lu.combined_stream_url(batches[0], base_url="wss://example.test/")
        self.assertEqual(
            url,
            "wss://example.test/market/stream?streams=btcusdt@kline_1m/btcusdt@kline_3m",
        )
        with self.assertRaises(lu.LiveUpdateCommandError):
            lu.batch_websocket_streams(streams, ws_batch_size=2, max_streams_per_connection=1)

    def test_connection_specs_include_urls_and_stream_counts(self):
        specs = lu.build_websocket_connection_specs(
            ["BTCUSDT"],
            lu.SUPPORTED_INTERVALS,
            ws_batch_size=3,
            base_url="wss://example.test",
        )

        self.assertEqual(len(specs), 3)
        self.assertEqual(sum(len(spec.streams) for spec in specs), 7)
        self.assertTrue(specs[0].url.startswith("wss://example.test/market/stream?streams="))


class WebSocketPayloadTests(unittest.TestCase):
    def test_combined_payload_parses_to_normalized_event(self):
        payload = {
            "stream": "btcusdt@kline_1m",
            "data": ws_payload(1638747600000, closed=True, close=2.5),
        }

        event = lu.websocket_payload_to_event(json.dumps(payload))

        self.assertEqual(event.stream, "btcusdt@kline_1m")
        self.assertTrue(event.is_closed)
        self.assertEqual(event.record.symbol, "BTCUSDT")
        self.assertEqual(event.record.archive_source, "live_websocket")
        self.assertEqual(event.record.close, 2.5)

    def test_raw_payload_derives_stream_name(self):
        event = lu.websocket_payload_to_event(ws_payload(1638747600000, interval="3m"))

        self.assertEqual(event.stream, "btcusdt@kline_3m")
        self.assertFalse(event.is_closed)
        self.assertEqual(event.record.interval, "3m")


@unittest.skipIf(pq is None, "pyarrow is required for Phase 5 parquet tests")
class WebSocketProcessingTests(unittest.TestCase):
    def test_open_websocket_bar_updates_buffer_latest_and_state_only(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = lu.LiveUpdatePaths(repo_root=Path(tmp))
            state = lu.LiveUpdateState.create("1m", paths)

            result = lu.process_websocket_message(
                ws_payload(1638747600000, closed=False),
                state,
                paths,
                received_at_utc="2026-06-26T00:00:00Z",
            )

            self.assertEqual(result.status, "open_buffered")
            self.assertTrue(paths.buffer_jsonl("1m", "websocket_buffer", "2021-12-06").exists())
            self.assertTrue(paths.latest_json("1m", "BTCUSDT").exists())
            self.assertFalse(paths.closed_buffer_jsonl("1m", "2021-12-06").exists())
            self.assertFalse(paths.current_parquet_root("1m").exists())
            self.assertEqual(
                state.websocket["last_message_at_utc"],
                "2026-06-26T00:00:00Z",
            )
            self.assertEqual(
                state.symbols["BTCUSDT"].last_ws_message_at_utc,
                "2026-06-26T00:00:00Z",
            )

    def test_closed_websocket_bar_writes_closed_buffer_merges_and_updates_state(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = lu.LiveUpdatePaths(repo_root=Path(tmp))
            state = lu.LiveUpdateState.create("1m", paths)

            result = lu.process_websocket_message(
                ws_payload(1638747600000, closed=True, close=4.5),
                state,
                paths,
                received_at_utc="2026-06-26T00:01:00Z",
            )

            self.assertEqual(result.status, "closed_merged")
            self.assertTrue(paths.buffer_jsonl("1m", "websocket_buffer", "2021-12-06").exists())
            self.assertTrue(paths.closed_buffer_jsonl("1m", "2021-12-06").exists())
            self.assertTrue(paths.latest_json("1m", "BTCUSDT").exists())
            self.assertIsNotNone(result.merge_result)
            rows = pq.ParquetFile(result.merge_result.target_path).read().to_pylist()
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["close"], 4.5)
            self.assertEqual(rows[0]["archive_source"], "live_websocket")
            self.assertEqual(state.symbols["BTCUSDT"].last_closed_open_time, 1638747600000)
            self.assertTrue(paths.state_json("1m").exists())


class WebSocketStaleAndReconnectTests(unittest.TestCase):
    def test_detect_stale_streams_uses_interval_multiplier(self):
        state = lu.LiveUpdateState.create("1m")
        state.symbol_state("BTCUSDT").last_ws_message_at_utc = "2021-12-06T00:00:00Z"
        stale = lu.detect_stale_streams(
            state,
            ["BTCUSDT", "ETHUSDT"],
            now_ms=1638748981000,
            ws_stale_multiplier=3,
        )

        self.assertEqual([item.symbol for item in stale], ["BTCUSDT", "ETHUSDT"])
        self.assertEqual(stale[0].stale_threshold_ms, 180000)

    def test_stale_rest_fallback_uses_phase4_plans_without_network_for_bootstrap(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = lu.LiveUpdatePaths(repo_root=Path(tmp))
            state = lu.LiveUpdateState.create("1m", paths)

            results = lu.run_stale_rest_fallback_once(
                state,
                ["BTCUSDT"],
                paths,
                now_ms=1638748981000,
            )

            self.assertEqual(len(results), 1)
            self.assertEqual(results[0].status, "bootstrap_required")
            self.assertEqual(results[0].fetched_row_count, 0)

    def test_reconnect_increments_state_and_persists(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = lu.LiveUpdatePaths(repo_root=Path(tmp))
            state = lu.LiveUpdateState.create("1m", paths)

            result = lu.handle_websocket_reconnect(
                state,
                ["BTCUSDT"],
                paths,
                now_ms=1638748981000,
                now_utc="2026-06-26T00:02:00Z",
            )

            self.assertEqual(result.status, "reconnected")
            self.assertEqual(result.reconnect_count, 1)
            self.assertEqual(state.websocket["last_reconnect_at_utc"], "2026-06-26T00:02:00Z")
            loaded = lu.load_live_update_state("1m", paths)
            self.assertEqual(loaded.websocket["reconnect_count"], 1)

    def test_rotation_reports_not_due_or_reconnects_when_due(self):
        state = lu.LiveUpdateState.create("1m")
        state.websocket["last_connected_at_utc"] = "2021-12-06T00:00:00Z"

        not_due = lu.plan_websocket_rotation(
            state,
            ["BTCUSDT"],
            now_ms=1638748981000,
            rotate_hours=23,
        )
        self.assertEqual(not_due.status, "not_due")

        with tempfile.TemporaryDirectory() as tmp:
            paths = lu.LiveUpdatePaths(repo_root=Path(tmp))
            due_state = lu.LiveUpdateState.create("1m", paths)
            due_state.websocket["last_connected_at_utc"] = "2021-12-05T00:00:00Z"
            due = lu.plan_websocket_rotation(
                due_state,
                ["BTCUSDT"],
                paths,
                now_ms=1638748981000,
                rotate_hours=23,
            )
            self.assertEqual(due.status, "reconnected")
            self.assertEqual(due.reconnect_count, 1)


class ScriptPhase5Tests(unittest.TestCase):
    def test_script_describe_websocket_connections_outputs_specs_without_socket(self):
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
            )

            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            payload = json.loads(result.stdout)
            self.assertEqual(payload["active_intervals"], list(lu.SUPPORTED_INTERVALS))
            self.assertEqual(payload["stream_count"], 7)
            self.assertEqual(payload["connection_count"], 3)
            self.assertIn("/market/stream?streams=", payload["connections"][0]["url"])


class WebhookPayloadTests(unittest.TestCase):
    def test_webhook_parses_raw_combined_and_normalized_payloads(self):
        raw = lu.webhook_payload_to_event(
            ws_payload(1638747600000, closed=True),
            now_ms=1638747785000,
        )
        combined = lu.webhook_payload_to_event(
            {
                "stream": "btcusdt@kline_1m",
                "data": ws_payload(1638747600000, closed=False),
            },
            now_ms=1638747785000,
        )
        normalized = lu.webhook_payload_to_event(
            normalized_payload(1638747600000, closed=True),
            now_ms=1638747785000,
        )

        self.assertEqual(raw.payload_format, "binance_raw")
        self.assertTrue(raw.is_closed)
        self.assertEqual(raw.record.archive_source, "live_webhook")
        self.assertEqual(combined.payload_format, "binance_combined")
        self.assertFalse(combined.is_closed)
        self.assertEqual(normalized.payload_format, "normalized")
        self.assertTrue(normalized.is_closed)

    def test_normalized_payload_without_is_closed_derives_from_close_time(self):
        event = lu.webhook_payload_to_event(
            normalized_payload(
                1638747600000,
                include_is_closed=False,
            ),
            now_ms=1638747785000,
            close_lag_ms=2000,
        )

        self.assertTrue(event.is_closed)


@unittest.skipIf(pq is None, "pyarrow is required for Phase 6 closed webhook tests")
class WebhookProcessingTests(unittest.TestCase):
    def test_open_webhook_bar_updates_webhook_buffer_and_latest_only(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = lu.LiveUpdatePaths(repo_root=Path(tmp))

            result = lu.process_webhook_payload(
                normalized_payload(1638747600000, closed=False),
                paths,
                active_intervals=("1m",),
                now_ms=1638747601000,
                received_at_utc="2026-06-26T00:00:00Z",
            )

            self.assertEqual(result.status, "accepted")
            self.assertFalse(result.is_closed)
            self.assertTrue(paths.buffer_jsonl("1m", "webhook_buffer", "2021-12-06").exists())
            self.assertTrue(paths.latest_json("1m", "BTCUSDT").exists())
            self.assertFalse(paths.closed_buffer_jsonl("1m", "2021-12-06").exists())
            self.assertFalse(paths.current_parquet_root("1m").exists())
            self.assertFalse(paths.state_json("1m").exists())

    def test_closed_webhook_bar_writes_closed_buffer_then_merges_and_updates_state(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = lu.LiveUpdatePaths(repo_root=Path(tmp))

            result = lu.process_webhook_payload(
                normalized_payload(1638747600000, closed=True, close=6.5),
                paths,
                active_intervals=("1m",),
                now_ms=1638747785000,
                received_at_utc="2026-06-26T00:01:00Z",
            )

            self.assertEqual(result.status, "merged")
            self.assertTrue(paths.buffer_jsonl("1m", "webhook_buffer", "2021-12-06").exists())
            self.assertTrue(paths.closed_buffer_jsonl("1m", "2021-12-06").exists())
            self.assertTrue(paths.latest_json("1m", "BTCUSDT").exists())
            self.assertIsNotNone(result.merge_result)
            rows = pq.ParquetFile(result.merge_result.target_path).read().to_pylist()
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["close"], 6.5)
            self.assertEqual(rows[0]["archive_source"], "live_webhook")
            loaded = lu.load_live_update_state("1m", paths)
            self.assertEqual(loaded.symbols["BTCUSDT"].last_closed_open_time, 1638747600000)
            closed_line = paths.closed_buffer_jsonl("1m", "2021-12-06").read_text(encoding="utf-8").splitlines()[0]
            self.assertEqual(json.loads(closed_line)["source"], "webhook")

    def test_invalid_webhook_payload_is_rejected_before_closed_buffer_or_merge(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = lu.LiveUpdatePaths(repo_root=Path(tmp))
            payload = normalized_payload(1638747600000, closed=True, close=3.0)
            payload["high"] = "2.0"

            result = lu.process_webhook_payload(
                payload,
                paths,
                active_intervals=("1m",),
                now_ms=1638747785000,
                received_at_utc="2026-06-26T00:02:00Z",
            )

            self.assertEqual(result.status, "rejected")
            self.assertIn("OHLC invalid", result.validation_errors[0])
            self.assertTrue(paths.buffer_jsonl("1m", "webhook_buffer", "2021-12-06").exists())
            self.assertTrue(paths.latest_json("1m", "BTCUSDT").exists())
            self.assertTrue(paths.rejects_jsonl("1m", "2021-12-06").exists())
            self.assertFalse(paths.closed_buffer_jsonl("1m", "2021-12-06").exists())
            self.assertFalse(paths.current_parquet_root("1m").exists())


class WebhookServerPrimitiveTests(unittest.TestCase):
    def test_health_payload_uses_loopback_defaults_and_active_intervals(self):
        paths = lu.LiveUpdatePaths(repo_root=Path("/repo"))
        config = lu.WebhookServerConfig(
            requested_interval="all",
            active_intervals=lu.SUPPORTED_INTERVALS,
            paths=paths,
        )

        self.assertEqual(config.host, "127.0.0.1")
        self.assertEqual(config.port, 8787)
        payload = lu.webhook_health_payload(config)
        self.assertEqual(payload["status"], "ok")
        self.assertEqual(payload["interval"], "all")
        self.assertEqual(payload["active_intervals"], list(lu.SUPPORTED_INTERVALS))

    def test_http_health_and_open_post_work_without_production_daemon(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = lu.LiveUpdatePaths(repo_root=Path(tmp))
            config = lu.WebhookServerConfig(
                requested_interval="1m",
                active_intervals=("1m",),
                paths=paths,
                port=0,
            )
            server = lu.build_webhook_server(config)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = server.server_address
                with urllib.request.urlopen(f"http://{host}:{port}/healthz") as response:
                    health = json.loads(response.read().decode("utf-8"))
                self.assertEqual(health["status"], "ok")

                body = json.dumps(normalized_payload(1638747600000, closed=False)).encode("utf-8")
                request = urllib.request.Request(
                    f"http://{host}:{port}/webhook/kline",
                    data=body,
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                with urllib.request.urlopen(request) as response:
                    result = json.loads(response.read().decode("utf-8"))
                self.assertEqual(result["status"], "accepted")
                self.assertFalse(result["is_closed"])
                self.assertTrue(paths.buffer_jsonl("1m", "webhook_buffer", "2021-12-06").exists())
            finally:
                server.shutdown()
                thread.join(timeout=5)
                server.server_close()

    def test_http_post_rejects_payload_over_size_limit(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = lu.LiveUpdatePaths(repo_root=Path(tmp))
            config = lu.WebhookServerConfig(
                requested_interval="1m",
                active_intervals=("1m",),
                paths=paths,
                port=0,
                max_body_bytes=8,
            )
            server = lu.build_webhook_server(config)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = server.server_address
                request = urllib.request.Request(
                    f"http://{host}:{port}/webhook/kline",
                    data=b'{"too":"large"}',
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                with self.assertRaises(urllib.error.HTTPError) as ctx:
                    urllib.request.urlopen(request)
                self.assertEqual(ctx.exception.code, 413)
            finally:
                server.shutdown()
                thread.join(timeout=5)
                server.server_close()


class ScriptPhase6Tests(unittest.TestCase):
    def test_script_describe_webhook_server_outputs_health_without_daemon(self):
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
            )

            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            payload = json.loads(result.stdout)
            self.assertTrue(payload["webhook_enabled"])
            self.assertEqual(payload["server"]["host"], "127.0.0.1")
            self.assertEqual(payload["server"]["port"], 8787)
            self.assertEqual(payload["healthz"]["active_intervals"], list(lu.SUPPORTED_INTERVALS))
            self.assertEqual(payload["endpoints"]["kline"], "POST /webhook/kline")


if __name__ == "__main__":
    unittest.main()
