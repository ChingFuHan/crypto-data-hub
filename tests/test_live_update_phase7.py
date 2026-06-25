import argparse
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch, MagicMock

from datahub.live_update import (
    build_parser,
    resolve_symbols,
    LiveUpdateCommandError,
)

class TestLiveUpdatePhase7(unittest.TestCase):
    def test_build_parser_defaults(self):
        parser = build_parser()
        args = parser.parse_args([])
        self.assertEqual(args.interval, "all")
        self.assertEqual(args.symbols, "")
        self.assertEqual(args.max_symbols, 0)
        self.assertEqual(args.ws_batch_size, 100)
        self.assertEqual(args.max_streams_per_connection, 1024)
        self.assertFalse(args.disable_webhook)
        self.assertFalse(args.disable_websocket)
        self.assertFalse(args.disable_rest_fallback)
        self.assertFalse(args.disable_startup_backfill)
        self.assertFalse(args.once)

    def test_resolve_symbols_from_arg(self):
        symbols = resolve_symbols(
            symbols_arg="btcUsdt, ethusdt  SOLUSDT",
            symbols_file_arg="",
            max_symbols=0,
        )
        self.assertEqual(symbols, ["BTCUSDT", "ETHUSDT", "SOLUSDT"])

    def test_resolve_symbols_with_max(self):
        symbols = resolve_symbols(
            symbols_arg="BTCUSDT ETHUSDT SOLUSDT",
            symbols_file_arg="",
            max_symbols=2,
        )
        self.assertEqual(symbols, ["BTCUSDT", "ETHUSDT"])

    def test_resolve_symbols_from_file(self):
        with tempfile.NamedTemporaryFile("w", delete=False) as f:
            f.write("# some comment\n")
            f.write(" btcusdt \n")
            f.write("ethusdt\n")
            filepath = f.name

        try:
            symbols = resolve_symbols(
                symbols_arg="",
                symbols_file_arg=filepath,
                max_symbols=0,
            )
            self.assertEqual(symbols, ["BTCUSDT", "ETHUSDT"])
        finally:
            Path(filepath).unlink()

    @patch("urllib.request.urlopen")
    def test_resolve_symbols_from_exchange_info(self, mock_urlopen):
        mock_response = MagicMock()
        mock_response.read.return_value = json.dumps({
            "symbols": [
                {"symbol": "BTCUSDT", "status": "TRADING", "contractType": "PERPETUAL", "quoteAsset": "USDT"},
                {"symbol": "ETHUSDT", "status": "TRADING", "contractType": "PERPETUAL", "quoteAsset": "USDT"},
                {"symbol": "LTCBTC", "status": "TRADING", "contractType": "PERPETUAL", "quoteAsset": "BTC"},
                {"symbol": "SOLUSDT", "status": "BREAK", "contractType": "PERPETUAL", "quoteAsset": "USDT"},
                {"symbol": "DOGEUSDT", "status": "TRADING", "contractType": "CURRENT_MONTH", "quoteAsset": "USDT"}
            ]
        }).encode("utf-8")
        mock_response.__enter__.return_value = mock_response
        mock_urlopen.return_value = mock_response

        symbols = resolve_symbols(
            symbols_arg="",
            symbols_file_arg="",
            max_symbols=0,
        )
        # Should only include TRADING, PERPETUAL, USDT and be sorted alphabetically
        self.assertEqual(symbols, ["BTCUSDT", "ETHUSDT"])
        
        # Verify it uses the futures endpoint
        call_args = mock_urlopen.call_args
        self.assertIsNotNone(call_args)
        request_obj = call_args[0][0]
        self.assertEqual(request_obj.full_url, "https://fapi.binance.com/fapi/v1/exchangeInfo")
