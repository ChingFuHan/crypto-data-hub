"""Symbols parsing / resolver / ``--symbols all`` hardening tests.

Covers:

* A. ``--symbols`` parsing: space-separated, quoted, comma-separated, casing,
  de-duplication while preserving order.
* B. ``--symbols all`` resolves via the Binance USD-M Futures
  ``/fapi/v1/exchangeInfo`` endpoint (never spot ``/api/v3/exchangeInfo``),
  keeps only TRADING + PERPETUAL + USDT, honours ``--max-symbols``, and never
  lets the ``all`` token reach a kline REST URL, WebSocket stream, or any
  state / parquet / buffer record.
* C. Data-writing / heavy modes fail clearly when ``--symbols`` is missing.
* D. The existing ``--symbols "BTCUSDT ETHUSDT"`` usage still works.

All exchangeInfo access is mocked; no test touches the network or the repo's
real ``local_data`` directory.
"""

import io
import json
import tempfile
import unittest
from contextlib import redirect_stderr
from pathlib import Path
from unittest.mock import MagicMock, patch

from datahub.live_update import (
    ALL_SYMBOLS_TOKEN,
    LiveUpdateCommandError,
    SYMBOLS_REQUIRED_MESSAGE,
    build_parser,
    build_websocket_streams,
    fetch_um_perpetual_usdt_symbols,
    main,
    parse_symbols_arg,
    rest_klines_url,
    resolve_symbols,
    websocket_stream_name,
)


EXCHANGE_INFO_PAYLOAD = {
    "symbols": [
        {"symbol": "BTCUSDT", "status": "TRADING", "contractType": "PERPETUAL", "quoteAsset": "USDT"},
        {"symbol": "ETHUSDT", "status": "TRADING", "contractType": "PERPETUAL", "quoteAsset": "USDT"},
        {"symbol": "SOLUSDT", "status": "TRADING", "contractType": "PERPETUAL", "quoteAsset": "USDT"},
        # Excluded: spot-style quote asset.
        {"symbol": "LTCBTC", "status": "TRADING", "contractType": "PERPETUAL", "quoteAsset": "BTC"},
        # Excluded: not trading.
        {"symbol": "AAAUSDT", "status": "BREAK", "contractType": "PERPETUAL", "quoteAsset": "USDT"},
        # Excluded: dated future, not a perpetual.
        {"symbol": "BBBUSDT", "status": "TRADING", "contractType": "CURRENT_QUARTER", "quoteAsset": "USDT"},
    ]
}


def _mock_urlopen(payload):
    mock_response = MagicMock()
    mock_response.read.return_value = json.dumps(payload).encode("utf-8")
    mock_response.__enter__.return_value = mock_response
    return mock_response


# ---------------------------------------------------------------------------
# A. symbols parsing
# ---------------------------------------------------------------------------
class SymbolsParsingTests(unittest.TestCase):
    def test_space_separated_tokens(self):
        # argparse nargs="*" yields a list of tokens.
        self.assertEqual(
            parse_symbols_arg(["BTCUSDT", "ETHUSDT"]),
            ["BTCUSDT", "ETHUSDT"],
        )

    def test_single_quoted_string(self):
        self.assertEqual(
            parse_symbols_arg(["BTCUSDT ETHUSDT"]),
            ["BTCUSDT", "ETHUSDT"],
        )

    def test_comma_separated(self):
        self.assertEqual(
            parse_symbols_arg(["BTCUSDT,ETHUSDT"]),
            ["BTCUSDT", "ETHUSDT"],
        )

    def test_comma_and_space_mixed_tokens(self):
        self.assertEqual(
            parse_symbols_arg(["BTCUSDT,ETHUSDT", "SOLUSDT"]),
            ["BTCUSDT", "ETHUSDT", "SOLUSDT"],
        )

    def test_normalization_uppercases(self):
        self.assertEqual(
            parse_symbols_arg(["btcusdt", "ethusdt"]),
            ["BTCUSDT", "ETHUSDT"],
        )

    def test_dedup_preserves_order(self):
        self.assertEqual(
            parse_symbols_arg(["BTCUSDT", "BTCUSDT", "ETHUSDT"]),
            ["BTCUSDT", "ETHUSDT"],
        )

    def test_all_returns_sentinel(self):
        self.assertEqual(parse_symbols_arg(["all"]), [ALL_SYMBOLS_TOKEN])
        self.assertEqual(parse_symbols_arg(["ALL"]), [ALL_SYMBOLS_TOKEN])
        self.assertEqual(parse_symbols_arg("all"), [ALL_SYMBOLS_TOKEN])

    def test_empty_and_none(self):
        self.assertEqual(parse_symbols_arg(None), [])
        self.assertEqual(parse_symbols_arg(""), [])
        self.assertEqual(parse_symbols_arg([]), [])

    def test_parser_accepts_space_separated_symbols(self):
        parser = build_parser()
        args = parser.parse_args(["--symbols", "BTCUSDT", "ETHUSDT"])
        self.assertEqual(args.symbols, ["BTCUSDT", "ETHUSDT"])
        self.assertEqual(parse_symbols_arg(args.symbols), ["BTCUSDT", "ETHUSDT"])

    def test_parser_accepts_all(self):
        parser = build_parser()
        args = parser.parse_args(["--symbols", "all"])
        self.assertEqual(parse_symbols_arg(args.symbols), [ALL_SYMBOLS_TOKEN])


# ---------------------------------------------------------------------------
# B. --symbols all
# ---------------------------------------------------------------------------
class SymbolsAllResolverTests(unittest.TestCase):
    @patch("urllib.request.urlopen")
    def test_resolver_uses_futures_exchange_info(self, mock_urlopen):
        mock_urlopen.return_value = _mock_urlopen(EXCHANGE_INFO_PAYLOAD)
        symbols = resolve_symbols(["all"], "", 0)
        self.assertEqual(symbols, ["BTCUSDT", "ETHUSDT", "SOLUSDT"])

        request_obj = mock_urlopen.call_args[0][0]
        self.assertEqual(
            request_obj.full_url,
            "https://fapi.binance.com/fapi/v1/exchangeInfo",
        )
        # Never the spot endpoint.
        self.assertNotIn("/api/v3/exchangeInfo", request_obj.full_url)
        self.assertIn("/fapi/", request_obj.full_url)

    @patch("urllib.request.urlopen")
    def test_resolver_filters_status_contract_quote(self, mock_urlopen):
        mock_urlopen.return_value = _mock_urlopen(EXCHANGE_INFO_PAYLOAD)
        symbols = fetch_um_perpetual_usdt_symbols()
        # LTCBTC (BTC quote), AAAUSDT (BREAK), BBBUSDT (dated) all excluded.
        self.assertEqual(symbols, ["BTCUSDT", "ETHUSDT", "SOLUSDT"])

    @patch("urllib.request.urlopen")
    def test_max_symbols_truncates_after_resolution(self, mock_urlopen):
        mock_urlopen.return_value = _mock_urlopen(EXCHANGE_INFO_PAYLOAD)
        symbols = resolve_symbols(["all"], "", 2)
        self.assertEqual(symbols, ["BTCUSDT", "ETHUSDT"])

    @patch("urllib.request.urlopen")
    def test_resolved_symbols_never_contain_all_token(self, mock_urlopen):
        mock_urlopen.return_value = _mock_urlopen(EXCHANGE_INFO_PAYLOAD)
        symbols = resolve_symbols(["all"], "", 0)
        self.assertNotIn("all", symbols)
        self.assertNotIn("ALL", symbols)

    def test_all_rejected_by_kline_rest_url(self):
        with self.assertRaises(LiveUpdateCommandError):
            rest_klines_url(
                base_url="https://fapi.binance.com",
                symbol="all",
                interval="1m",
                start_time=None,
                end_time=None,
                limit=10,
            )

    def test_all_rejected_by_websocket_stream_name(self):
        with self.assertRaises(LiveUpdateCommandError):
            websocket_stream_name("all", "1m")

    def test_all_never_appears_in_built_stream_names(self):
        # Resolved concrete symbols build clean stream names; "all" appears nowhere.
        streams = build_websocket_streams(["BTCUSDT", "ETHUSDT"], ("1m",))
        self.assertEqual(streams, ("btcusdt@kline_1m", "ethusdt@kline_1m"))
        self.assertFalse(any("all@" in stream for stream in streams))


# ---------------------------------------------------------------------------
# C. missing --symbols must fail for data-writing / heavy modes
# ---------------------------------------------------------------------------
class MissingSymbolsTests(unittest.TestCase):
    def _assert_fails_without_symbols(self, mode_flag):
        with tempfile.TemporaryDirectory() as tmp:
            stderr = io.StringIO()
            with self.assertRaises(SystemExit) as ctx, redirect_stderr(stderr):
                main(
                    [
                        "--repo-root",
                        tmp,
                        "--interval",
                        "1m",
                        mode_flag,
                        "--now-ms",
                        "1700000000000",
                    ]
                )
            self.assertNotEqual(ctx.exception.code, 0)
            message = stderr.getvalue()
            self.assertIn("--symbols", message)
            self.assertIn("all", message)
            self.assertIn("BTCUSDT", message)

    def test_run_startup_backfill_once_requires_symbols(self):
        self._assert_fails_without_symbols("--run-startup-backfill-once")

    def test_once_requires_symbols(self):
        self._assert_fails_without_symbols("--once")

    def test_default_run_requires_symbols(self):
        # No mode flag -> default daemon path must also refuse the whole market.
        with tempfile.TemporaryDirectory() as tmp:
            stderr = io.StringIO()
            with self.assertRaises(SystemExit), redirect_stderr(stderr):
                main(["--repo-root", tmp, "--interval", "1m", "--now-ms", "1700000000000"])
            self.assertIn("--symbols", stderr.getvalue())

    def test_message_constant_mentions_examples(self):
        self.assertIn("--symbols", SYMBOLS_REQUIRED_MESSAGE)
        self.assertIn("all", SYMBOLS_REQUIRED_MESSAGE)
        self.assertIn("BTCUSDT", SYMBOLS_REQUIRED_MESSAGE)


# ---------------------------------------------------------------------------
# D. regression: existing quoted usage still works
# ---------------------------------------------------------------------------
class RegressionTests(unittest.TestCase):
    def test_quoted_symbols_still_resolve(self):
        # Equivalent to: --symbols "BTCUSDT ETHUSDT"
        self.assertEqual(
            resolve_symbols(["BTCUSDT ETHUSDT"], "", 0),
            ["BTCUSDT", "ETHUSDT"],
        )

    def test_string_arg_still_supported(self):
        # resolve_symbols historically accepted a raw string.
        self.assertEqual(
            resolve_symbols("btcusdt, ethusdt  solusdt", "", 0),
            ["BTCUSDT", "ETHUSDT", "SOLUSDT"],
        )

    def test_symbols_file_still_supported(self):
        with tempfile.NamedTemporaryFile("w", delete=False) as f:
            f.write("# comment\n btcusdt \nethusdt\nethusdt\n")
            filepath = f.name
        try:
            self.assertEqual(
                resolve_symbols(None, filepath, 0),
                ["BTCUSDT", "ETHUSDT"],
            )
        finally:
            Path(filepath).unlink()


if __name__ == "__main__":
    unittest.main()
