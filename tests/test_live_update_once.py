"""--once semantics tests.

``--once`` must run one complete live-update cycle (ensure current symbols from
seed, run the startup / REST gap repair once, write closed_buffer, merge into
current parquet, update state only after a successful merge) and exit -- not
just print an orchestration skeleton. It shares the core flow with
``--run-startup-backfill-once``.

All tests use temp dirs and mock REST / exchangeInfo; none write the repo's real
``local_data`` or leave parquet/jsonl/state artifacts behind.
"""

import io
import json
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest.mock import MagicMock, patch

from datahub import live_update as lu

try:
    import pyarrow.parquet as pq
except ImportError:  # pragma: no cover
    pq = None


BASE_OT = 1638747600000  # 2021-12-06T00:00:00Z
ONE_MIN = lu.interval_milliseconds("1m")


def make_record(open_time, *, interval="1m", symbol="ETHUSDT", close=1.5):
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


def rest_row(open_time, *, interval="1m", close=1.5):
    interval_ms = lu.interval_milliseconds(interval)
    return [
        open_time, "1.0", str(max(2.0, close)), "0.5", str(close), "10.0",
        open_time + interval_ms - 1, "15.0", 3, "5.0", "7.5", "0",
    ]


def make_paths(tmp):
    return lu.LiveUpdatePaths(
        repo_root=Path(tmp),
        seed_dataset_root=Path("seed"),
        current_dataset_root=Path("current"),
    )


def write_seed_symbol(tmp, *, symbol="ETHUSDT", open_times=(BASE_OT,)):
    writer = lu.LiveUpdatePaths(repo_root=Path(tmp), current_dataset_root=Path("seed"))
    lu.merge_records_to_current_partition(
        [make_record(ot, symbol=symbol) for ot in open_times], writer
    )


def gap_fetch(now_ms, start_open):
    latest = lu.calculate_latest_closed_open_time("1m", now_ms)
    rows = [rest_row(ot) for ot in range(start_open, latest + ONE_MIN, ONE_MIN)]
    fake = MagicMock()
    fake.status = "ok"
    fake.rows = rows
    fake.error = None
    return fake, latest


def run_once(tmp, symbols, now_ms, *, extra=None, fetch=None):
    """Invoke main() with --once in-process, return parsed once_update payload."""
    argv = [
        "--repo-root", tmp,
        "--seed-dataset-root", "seed",
        "--current-dataset-root", "current",
        "--interval", "1m",
        "--symbols", *symbols,
        "--once",
        "--now-ms", str(now_ms),
    ]
    if extra:
        argv += extra
    out = io.StringIO()
    if fetch is not None:
        cm = patch.object(lu, "fetch_rest_klines", return_value=fetch)
    else:
        cm = patch.object(lu, "fetch_rest_klines", side_effect=AssertionError("no REST"))
    with cm, redirect_stdout(out):
        rc = lu.main(argv + ["--rest-backoff-base-seconds", "0"])
    assert rc == 0, out.getvalue()
    text = out.getvalue()
    marker = '"once_update"'
    idx = text.index(marker)
    start = text.rindex("{", 0, idx)
    payload = json.JSONDecoder().raw_decode(text[start:])[0]
    return payload["once_update"], text


class OnceRequiresSymbolsTests(unittest.TestCase):
    def test_once_without_symbols_fails_with_helpful_message(self):
        with tempfile.TemporaryDirectory() as tmp:
            err = io.StringIO()
            with self.assertRaises(SystemExit) as ctx, redirect_stderr(err):
                lu.main(
                    ["--repo-root", tmp, "--interval", "1m", "--once",
                     "--now-ms", str(BASE_OT + 5 * ONE_MIN)]
                )
            self.assertNotEqual(ctx.exception.code, 0)
            message = err.getvalue()
            self.assertIn("--symbols", message)
            self.assertIn("all", message)
            self.assertIn("BTCUSDT", message)


@unittest.skipIf(pq is None, "pyarrow is required for --once flow tests")
class OnceOneShotUpdateTests(unittest.TestCase):
    def test_once_runs_one_shot_update_not_skeleton(self):
        with tempfile.TemporaryDirectory() as tmp:
            write_seed_symbol(tmp, open_times=(BASE_OT,))
            now_ms = BASE_OT + 4 * ONE_MIN
            fetch, _ = gap_fetch(now_ms, BASE_OT + ONE_MIN)
            payload, text = run_once(tmp, ["ETHUSDT"], now_ms, fetch=fetch)
            self.assertEqual(payload["mode"], "once")
            self.assertEqual(payload["symbols"], ["ETHUSDT"])
            self.assertIn("results", payload)
            # Real flow markers, not the old skeleton.
            self.assertNotIn("[skeleton]", text)

    def test_once_seed_exists_current_missing_initializes_then_backfills(self):
        with tempfile.TemporaryDirectory() as tmp:
            write_seed_symbol(tmp, open_times=(BASE_OT,))
            paths = make_paths(tmp)
            self.assertFalse(paths.current_parquet_root("1m").exists())
            now_ms = BASE_OT + 4 * ONE_MIN
            fetch, latest = gap_fetch(now_ms, BASE_OT + ONE_MIN)
            payload, _ = run_once(tmp, ["ETHUSDT"], now_ms, fetch=fetch)
            result = payload["results"][0]
            self.assertNotEqual(result["status"], "bootstrap_required")
            self.assertGreaterEqual(result["merged_row_count"], 1)
            self.assertEqual(
                lu.max_open_time_from_current_dataset("1m", "ETHUSDT", paths),
                latest,
            )

    def test_once_current_exists_with_lag_fetches_merges_updates_state(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = make_paths(tmp)
            # Current already has one bar; seed also present but current wins.
            write_seed_symbol(tmp, open_times=(BASE_OT,))
            lu.merge_records_to_current_partition(
                [make_record(BASE_OT, symbol="ETHUSDT")], paths
            )
            now_ms = BASE_OT + 4 * ONE_MIN
            fetch, latest = gap_fetch(now_ms, BASE_OT + ONE_MIN)
            payload, _ = run_once(tmp, ["ETHUSDT"], now_ms, fetch=fetch)
            result = payload["results"][0]
            self.assertIn(result["status"], ("ok", "ok_with_warnings"))
            self.assertGreaterEqual(result["merged_row_count"], 1)
            state = lu.load_live_update_state("1m", paths)
            self.assertEqual(
                state.symbols["ETHUSDT"].last_closed_open_time, latest
            )

    def test_once_seed_missing_is_bootstrap_required_no_rest(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = make_paths(tmp)
            now_ms = BASE_OT + 4 * ONE_MIN
            # fetch=None -> any REST call raises AssertionError.
            payload, _ = run_once(tmp, ["ETHUSDT"], now_ms, fetch=None)
            result = payload["results"][0]
            self.assertEqual(result["status"], "bootstrap_required")
            # No fake current dataset written.
            self.assertFalse(
                (paths.current_parquet_root("1m") / "symbol=ETHUSDT").exists()
            )

    def test_once_all_token_never_reaches_rest_ws_state_or_parquet(self):
        with tempfile.TemporaryDirectory() as tmp:
            write_seed_symbol(tmp, symbol="ETHUSDT", open_times=(BASE_OT,))
            paths = make_paths(tmp)
            now_ms = BASE_OT + 4 * ONE_MIN
            fetch, latest = gap_fetch(now_ms, BASE_OT + ONE_MIN)
            exchange_info = MagicMock()
            exchange_info.read.return_value = json.dumps(
                {"symbols": [
                    {"symbol": "ETHUSDT", "status": "TRADING",
                     "contractType": "PERPETUAL", "quoteAsset": "USDT"},
                ]}
            ).encode("utf-8")
            exchange_info.__enter__.return_value = exchange_info

            out = io.StringIO()
            with patch("urllib.request.urlopen", return_value=exchange_info), \
                    patch.object(lu, "fetch_rest_klines", return_value=fetch), \
                    redirect_stdout(out):
                rc = lu.main([
                    "--repo-root", tmp,
                    "--seed-dataset-root", "seed",
                    "--current-dataset-root", "current",
                    "--interval", "1m", "--once",
                    "--symbols", "all",
                    "--now-ms", str(now_ms),
                    "--rest-backoff-base-seconds", "0",
                ])
            self.assertEqual(rc, 0, out.getvalue())
            text = out.getvalue()
            marker = '"once_update"'
            payload = json.JSONDecoder().raw_decode(
                text[text.rindex("{", 0, text.index(marker)):]
            )[0]["once_update"]
            self.assertEqual(payload["symbols"], ["ETHUSDT"])
            self.assertNotIn("all", payload["symbols"])
            self.assertNotIn("all@", text)
            # No "all" symbol partition or state anywhere.
            current_root = paths.current_parquet_root("1m")
            self.assertFalse((current_root / "symbol=ALL").exists())
            self.assertFalse((current_root / "symbol=all").exists())
            state = lu.load_live_update_state("1m", paths)
            self.assertNotIn("ALL", state.symbols)
            self.assertNotIn("all", state.symbols)


@unittest.skipIf(pq is None, "pyarrow is required for --once parity tests")
class OnceRunStartupParityTests(unittest.TestCase):
    def _run_backfill_once(self, tmp, now_ms, fetch):
        out = io.StringIO()
        with patch.object(lu, "fetch_rest_klines", return_value=fetch), \
                redirect_stdout(out):
            rc = lu.main([
                "--repo-root", tmp,
                "--seed-dataset-root", "seed",
                "--current-dataset-root", "current",
                "--interval", "1m",
                "--symbols", "ETHUSDT",
                "--run-startup-backfill-once",
                "--now-ms", str(now_ms),
                "--rest-backoff-base-seconds", "0",
            ])
        assert rc == 0, out.getvalue()
        return json.loads(out.getvalue())

    def test_once_and_run_startup_backfill_once_match(self):
        now_ms = BASE_OT + 4 * ONE_MIN
        with tempfile.TemporaryDirectory() as tmp_a:
            write_seed_symbol(tmp_a, open_times=(BASE_OT,))
            fetch_a, _ = gap_fetch(now_ms, BASE_OT + ONE_MIN)
            once_payload, _ = run_once(tmp_a, ["ETHUSDT"], now_ms, fetch=fetch_a)

        with tempfile.TemporaryDirectory() as tmp_b:
            write_seed_symbol(tmp_b, open_times=(BASE_OT,))
            fetch_b, _ = gap_fetch(now_ms, BASE_OT + ONE_MIN)
            backfill_payload = self._run_backfill_once(tmp_b, now_ms, fetch_b)

        once_results = once_payload["results"][0]
        backfill_results = backfill_payload["results"][0]
        for field in ("status", "merged_row_count", "closed_row_count", "symbol"):
            self.assertEqual(once_results[field], backfill_results[field])


if __name__ == "__main__":
    unittest.main()
