"""Tests for the Binance UM Kline Parquet materialization."""

import io
import json
from pathlib import Path
import tempfile
import unittest
import zipfile
from datetime import datetime, timedelta

from datahub.materialization import binance_um_klines_parquet as mat
from datahub.validation.binance_um_klines_parquet import validate_parquet_manifest
from datahub.validation.cli import build_parser, run as cli_run

# A canonical no-header 1D row at open_time = 2020-01-01 00:00:00 UTC.
# open,high,low,close,volume,close_time,quote_volume,count,taker_buy_vol,taker_buy_quote,ignore
OPEN_TIME_20200101 = 1577836800000
# 12 columns: open_time,open,high,low,close,volume,close_time,quote_volume,count,
#             taker_buy_volume,taker_buy_quote_volume,ignore
ROW_20200101 = "1577836800000,1.0,2.0,0.5,1.5,10.0,1577923199999,15.0,3,5.0,7.5,0"
HEADER = (
    "open_time,open,high,low,close,volume,close_time,quote_volume,count,"
    "taker_buy_volume,taker_buy_quote_volume,ignore"
)
FOUR_H_MS = 14_400_000


def ms_utc(year, month, day, hour=0):
    return int(
        (datetime(year, month, day, hour) - datetime(1970, 1, 1)).total_seconds()
        * 1000
    )


def six_rows_for_taipei_date_20200101():
    # Taipei 2020-01-01 contains UTC opens:
    # 2019-12-31 16:00/20:00 and 2020-01-01 00:00/04:00/08:00/12:00.
    opens = [
        ms_utc(2019, 12, 31, 16),
        ms_utc(2019, 12, 31, 20),
        ms_utc(2020, 1, 1, 0),
        ms_utc(2020, 1, 1, 4),
        ms_utc(2020, 1, 1, 8),
        ms_utc(2020, 1, 1, 12),
    ]
    return [row_for_interval(ms, "4h") for ms in opens]


ONE_H_MS = 3_600_000


def twentyfour_rows_for_taipei_date_20200101():
    # Taipei 2020-01-01 spans UTC 2019-12-31 16:00 .. 2020-01-01 15:00, i.e. the
    # 24 hour-aligned opens 00:00..23:00 Taipei. This is the legal daily maximum
    # for the 1h interval.
    base = ms_utc(2019, 12, 31, 16)
    opens = [base + h * ONE_H_MS for h in range(24)]
    return [row_for_interval(ms, "1h") for ms in opens]


FIFTEEN_M_MS = 900_000


def ninetysix_rows_for_taipei_date_20200101():
    # Taipei 2020-01-01 spans UTC 2019-12-31 16:00 .. 2020-01-01 15:45, i.e. the
    # 96 quarter-hour-aligned opens 00:00..23:45 Taipei. This is the legal daily
    # maximum for the 15m interval (24h / 15m = 96).
    base = ms_utc(2019, 12, 31, 16)
    opens = [base + q * FIFTEEN_M_MS for q in range(96)]
    return [row_for_interval(ms, "15m") for ms in opens]


FIVE_M_MS = 300_000


def twoeightyeight_rows_for_taipei_date_20200101():
    # Taipei 2020-01-01 spans UTC 2019-12-31 16:00 .. 2020-01-01 15:55, i.e. the
    # 288 five-minute-aligned opens 00:00..23:55 Taipei. This is the legal daily
    # maximum for the 5m interval (24h / 5m = 288).
    base = ms_utc(2019, 12, 31, 16)
    opens = [base + q * FIVE_M_MS for q in range(288)]
    return [row_for_interval(ms, "5m") for ms in opens]


THREE_M_MS = 180_000


def foureighty_rows_for_taipei_date_20200101():
    # Taipei 2020-01-01 spans UTC 2019-12-31 16:00 .. 2020-01-01 15:57, i.e. the
    # 480 three-minute-aligned opens 00:00..23:57 Taipei. This is the legal daily
    # maximum for the 3m interval (24h / 3m = 480).
    base = ms_utc(2019, 12, 31, 16)
    opens = [base + q * THREE_M_MS for q in range(480)]
    return [row_for_interval(ms, "3m") for ms in opens]


def csv_bytes(rows, *, header=False):
    lines = []
    if header:
        lines.append(HEADER)
    lines.extend(rows)
    return ("\n".join(lines) + "\n").encode("utf-8")


def zip_bytes(member_name, data):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(member_name, data)
    return buf.getvalue()


def row_for_interval(open_time, interval):
    delta = mat.interval_milliseconds(interval) - 1
    close_time = open_time + delta
    return (
        f"{open_time},1.0,2.0,0.5,1.5,10.0,{close_time},"
        "15.0,3,5.0,7.5,0"
    )


def write_archive(raw_root, source, symbol, period, rows, *, header=False, interval="1d"):
    name = f"{symbol}-{interval}-{period}"
    directory = Path(raw_root) / source / symbol
    directory.mkdir(parents=True, exist_ok=True)
    zip_path = directory / f"{name}.zip"
    zip_path.write_bytes(zip_bytes(f"{name}.csv", csv_bytes(rows, header=header)))
    return zip_path


def build_env(tmp, archive_specs, *, discovered_symbol_count=None, interval="1d"):
    """Create raw layout + run manifest + files.jsonl from archive_specs.

    Each spec: (source, symbol, period, rows[, header]).
    Returns (manifest_path, raw_root, output_root).
    """
    base = Path(tmp) / "local_data" / "binance_um_klines" / f"interval={interval}"
    raw_root = base / "raw"
    manifests = base / "manifests"
    manifests.mkdir(parents=True, exist_ok=True)
    output_root = base / "parquet"

    file_records = []
    symbols = set()
    for spec in archive_specs:
        source, symbol, period, rows = spec[0], spec[1], spec[2], spec[3]
        header = spec[4] if len(spec) > 4 else False
        zip_path = write_archive(
            raw_root, source, symbol, period, rows, header=header, interval=interval
        )
        symbols.add(symbol)
        file_records.append(
            {
                "symbol": symbol,
                "archive_package_source": source,
                "archive_period": period,
                "local_zip_path": str(zip_path),
                "zip_name": zip_path.name,
                "download_status": "downloaded",
                "checksum_status": "passed",
                "skip_reason": None,
            }
        )

    files_jsonl = manifests / "files.jsonl"
    files_jsonl.write_text(
        "".join(json.dumps(r) + "\n" for r in file_records), encoding="utf-8"
    )

    manifest_path = manifests / "manifest.json"
    manifest = {
        "interval": interval,
        "dataset_id": "market.binance.um.klines",
        "symbol_count": discovered_symbol_count or len(symbols),
        "discovered_symbol_count": discovered_symbol_count or len(symbols),
        "file_manifest": str(files_jsonl),
    }
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    return manifest_path, raw_root, output_root


def make_config(manifest_path, raw_root, output_root, **overrides):
    interval = overrides.pop("interval", "1d")
    params = dict(
        interval=interval,
        raw_root=Path(raw_root),
        manifest=Path(manifest_path),
        output_root=Path(output_root),
        symbols=None,
        all_symbols=True,
        resume=False,
        overwrite=False,
        workers=1,
        strict=False,
    )
    params.update(overrides)
    return mat.RunConfig(**params)


# --------------------------------------------------------------------------- #
# Parsing / schema / timestamp (cases 1-7)
# --------------------------------------------------------------------------- #


class ParsingTest(unittest.TestCase):
    def test_case01_no_header_csv(self):
        rows = mat.parse_kline_csv(csv_bytes([ROW_20200101], header=False))
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][0], "1577836800000")

    def test_case02_header_csv(self):
        rows = mat.parse_kline_csv(csv_bytes([ROW_20200101], header=True))
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][0], "1577836800000")

    def test_case03_schema_conversion_types(self):
        cells = mat.parse_kline_csv(csv_bytes([ROW_20200101]))[0]
        rec = mat.build_record(
            cells,
            symbol="BTCUSDT",
            interval="1d",
            archive_source="monthly",
            archive_period="2020-01",
            source_archive="x.zip",
        )
        self.assertIsInstance(rec.open, float)
        self.assertIsInstance(rec.trade_count, int)
        self.assertEqual(rec.trade_count, 3)
        self.assertEqual(rec.taker_buy_base_volume, 5.0)
        self.assertEqual(rec.taker_buy_quote_volume, 7.5)
        self.assertEqual(rec.interval, "1d")

    def test_case04_timestamp_conversion(self):
        cells = mat.parse_kline_csv(csv_bytes([ROW_20200101]))[0]
        rec = mat.build_record(
            cells, symbol="BTCUSDT", interval="1d",
            archive_source="monthly", archive_period="2020-01",
            source_archive="x.zip",
        )
        self.assertEqual(rec.open_time, OPEN_TIME_20200101)
        self.assertEqual(rec.open_time_utc.isoformat(), "2020-01-01T00:00:00")
        self.assertEqual(rec.open_time_taipei.isoformat(), "2020-01-01T08:00:00")

    def test_case05_date_from_taipei(self):
        # 2020-01-01 23:00 UTC -> 2020-01-02 07:00 Taipei -> date 2020-01-02.
        ms = OPEN_TIME_20200101 + 23 * 3600 * 1000
        row = f"{ms},1.0,2.0,0.5,1.5,10.0,1577923199999,15.0,3,5.0,7.5,0"
        cells = mat.parse_kline_csv(csv_bytes([row]))[0]
        rec = mat.build_record(
            cells, symbol="BTCUSDT", interval="1d",
            archive_source="monthly", archive_period="2020-01",
            source_archive="x.zip",
        )
        self.assertEqual(rec.date, "2020-01-02")
        self.assertEqual(rec.year, 2020)
        self.assertEqual(rec.month, 1)

    def test_case06_ignore_column_not_in_parquet(self):
        self.assertNotIn("ignore", mat.PHYSICAL_COLUMNS)
        self.assertNotIn("ignore", mat.LOGICAL_COLUMNS)
        # symbol/year are logical (Hive) but not physical.
        self.assertNotIn("symbol", mat.PHYSICAL_COLUMNS)
        self.assertNotIn("year", mat.PHYSICAL_COLUMNS)
        self.assertIn("symbol", mat.LOGICAL_COLUMNS)
        self.assertIn("year", mat.LOGICAL_COLUMNS)

    def test_case07_required_fields_non_null(self):
        cells = mat.parse_kline_csv(csv_bytes([ROW_20200101]))[0]
        rec = mat.build_record(
            cells, symbol="BTCUSDT", interval="1d",
            archive_source="monthly", archive_period="2020-01",
            source_archive="x.zip",
        )
        for col in (
            "symbol", "interval", "open_time", "open_time_utc",
            "open_time_taipei", "date", "open", "high", "low", "close",
            "volume", "close_time", "quote_volume", "trade_count",
            "taker_buy_base_volume", "taker_buy_quote_volume",
        ):
            self.assertIsNotNone(getattr(rec, col), col)

    def test_case07b_4h_header_and_no_header_csv(self):
        row = row_for_interval(OPEN_TIME_20200101, "4h")
        for header in (False, True):
            rows = mat.parse_kline_csv(csv_bytes([row], header=header))
            rec = mat.build_record(
                rows[0],
                symbol="BTCUSDT",
                interval="4h",
                archive_source="monthly",
                archive_period="2020-01",
                source_archive="x.zip",
            )
            self.assertEqual(rec.interval, "4h")
            self.assertEqual(rec.close_time, OPEN_TIME_20200101 + FOUR_H_MS - 1)

    def test_case07c_1h_header_and_no_header_csv(self):
        row = row_for_interval(OPEN_TIME_20200101, "1h")
        for header in (False, True):
            rows = mat.parse_kline_csv(csv_bytes([row], header=header))
            rec = mat.build_record(
                rows[0],
                symbol="BTCUSDT",
                interval="1h",
                archive_source="monthly",
                archive_period="2020-01",
                source_archive="x.zip",
            )
            self.assertEqual(rec.interval, "1h")
            self.assertEqual(rec.close_time, OPEN_TIME_20200101 + ONE_H_MS - 1)

    def test_case07d_15m_header_and_no_header_csv(self):
        row = row_for_interval(OPEN_TIME_20200101, "15m")
        for header in (False, True):
            rows = mat.parse_kline_csv(csv_bytes([row], header=header))
            self.assertEqual(len(rows), 1)
            rec = mat.build_record(
                rows[0],
                symbol="BTCUSDT",
                interval="15m",
                archive_source="monthly",
                archive_period="2020-01",
                source_archive="x.zip",
            )
            self.assertEqual(rec.interval, "15m")
            self.assertEqual(rec.open_time, OPEN_TIME_20200101)
            self.assertEqual(rec.close_time, OPEN_TIME_20200101 + FIFTEEN_M_MS - 1)

    def test_case07e_5m_header_and_no_header_csv(self):
        row = row_for_interval(OPEN_TIME_20200101, "5m")
        for header in (False, True):
            rows = mat.parse_kline_csv(csv_bytes([row], header=header))
            self.assertEqual(len(rows), 1)
            rec = mat.build_record(
                rows[0],
                symbol="BTCUSDT",
                interval="5m",
                archive_source="monthly",
                archive_period="2020-01",
                source_archive="x.zip",
            )
            self.assertEqual(rec.interval, "5m")
            self.assertEqual(rec.open_time, OPEN_TIME_20200101)
            self.assertEqual(rec.close_time, OPEN_TIME_20200101 + FIVE_M_MS - 1)

    def test_case07f_3m_header_and_no_header_csv(self):
        row = row_for_interval(OPEN_TIME_20200101, "3m")
        for header in (False, True):
            rows = mat.parse_kline_csv(csv_bytes([row], header=header))
            self.assertEqual(len(rows), 1)
            rec = mat.build_record(
                rows[0],
                symbol="BTCUSDT",
                interval="3m",
                archive_source="monthly",
                archive_period="2020-01",
                source_archive="x.zip",
            )
            self.assertEqual(rec.interval, "3m")
            self.assertEqual(rec.open_time, OPEN_TIME_20200101)
            self.assertEqual(rec.close_time, OPEN_TIME_20200101 + THREE_M_MS - 1)


# --------------------------------------------------------------------------- #
# Dedup / conflict / OHLC (cases 8-12)
# --------------------------------------------------------------------------- #


class NormalizeTest(unittest.TestCase):
    def _specs(self, rows_by_archive):
        out = []
        for (source, period), zip_path in rows_by_archive.items():
            out.append(
                mat.ArchiveSpec(
                    archive_source=source, archive_period=period,
                    local_zip_path=str(zip_path), zip_name=Path(zip_path).name,
                )
            )
        return out

    def test_case08_exact_duplicate_dedup(self):
        with tempfile.TemporaryDirectory() as tmp:
            raw = Path(tmp) / "raw"
            m = write_archive(raw, "monthly", "AAAUSDT", "2020-01", [ROW_20200101])
            d = write_archive(raw, "daily", "AAAUSDT", "2020-01-01", [ROW_20200101])
            specs = self._specs({("monthly", "2020-01"): m,
                                 ("daily", "2020-01-01"): d})
            res = mat.normalize_symbol("AAAUSDT", specs, interval="1d", strict=False)
            self.assertEqual(len(res.rows), 1)
            self.assertEqual(res.duplicate_count, 1)
            self.assertEqual(res.conflict_count, 0)

    def test_case09_duplicate_date_detected(self):
        # Two distinct open_times on the same Taipei calendar date.
        with tempfile.TemporaryDirectory() as tmp:
            raw = Path(tmp) / "raw"
            ms2 = OPEN_TIME_20200101 + 3600 * 1000  # +1h, same Taipei date
            row2 = f"{ms2},1.0,2.0,0.5,1.5,10.0,1577923199999,15.0,3,5.0,7.5,0"
            m = write_archive(raw, "monthly", "AAAUSDT", "2020-01",
                              [ROW_20200101, row2])
            specs = self._specs({("monthly", "2020-01"): m})
            res = mat.normalize_symbol("AAAUSDT", specs, interval="1d", strict=False)
            self.assertEqual(len(res.rows), 2)
            self.assertEqual(res.duplicate_date_count, 1)

    def test_case10_conflict_daily_wins_and_recorded(self):
        with tempfile.TemporaryDirectory() as tmp:
            raw = Path(tmp) / "raw"
            monthly_row = ROW_20200101  # close 1.5
            daily_row = "1577836800000,1.0,2.0,0.5,1.9,10.0,1577923199999,15.0,3,5.0,7.5,0"
            m = write_archive(raw, "monthly", "AAAUSDT", "2020-01", [monthly_row])
            d = write_archive(raw, "daily", "AAAUSDT", "2020-01-01", [daily_row])
            specs = self._specs({("monthly", "2020-01"): m,
                                 ("daily", "2020-01-01"): d})
            res = mat.normalize_symbol("AAAUSDT", specs, interval="1d", strict=False)
            self.assertEqual(len(res.rows), 1)
            self.assertEqual(res.conflict_count, 1)
            self.assertEqual(res.rows[0].close, 1.9)  # daily won
            self.assertEqual(res.rows[0].archive_source, "daily")

    def test_case11_strict_conflict_fails(self):
        with tempfile.TemporaryDirectory() as tmp:
            raw = Path(tmp) / "raw"
            daily_row = "1577836800000,1.0,2.0,0.5,1.9,10.0,1577923199999,15.0,3,5.0,7.5,0"
            m = write_archive(raw, "monthly", "AAAUSDT", "2020-01", [ROW_20200101])
            d = write_archive(raw, "daily", "AAAUSDT", "2020-01-01", [daily_row])
            specs = self._specs({("monthly", "2020-01"): m,
                                 ("daily", "2020-01-01"): d})
            with self.assertRaises(mat.StrictModeError):
                mat.normalize_symbol("AAAUSDT", specs, interval="1d", strict=True)

    def test_case12_invalid_ohlc_detected(self):
        # high < low.
        bad = "1577836800000,1.0,0.4,0.5,0.45,10.0,1577923199999,15.0,3,5.0,7.5,0"
        cells = mat.parse_kline_csv(csv_bytes([bad]))[0]
        rec = mat.build_record(
            cells, symbol="BTCUSDT", interval="1d",
            archive_source="monthly", archive_period="2020-01",
            source_archive="x.zip",
        )
        violations = mat.find_ohlc_violations([rec])
        self.assertEqual(len(violations), 1)

    def test_case12b_4h_time_rules_detect_alignment_and_close_time(self):
        aligned = mat.build_record(
            mat.parse_kline_csv(csv_bytes([row_for_interval(OPEN_TIME_20200101, "4h")]))[0],
            symbol="BTCUSDT", interval="4h",
            archive_source="monthly", archive_period="2020-01",
            source_archive="x.zip",
        )
        self.assertEqual(mat.find_time_rule_violations([aligned], interval="4h"), [])

        bad_open = mat.build_record(
            mat.parse_kline_csv(csv_bytes([row_for_interval(OPEN_TIME_20200101 + 1, "4h")]))[0],
            symbol="BTCUSDT", interval="4h",
            archive_source="monthly", archive_period="2020-01",
            source_archive="x.zip",
        )
        bad_close_row = (
            f"{OPEN_TIME_20200101},1.0,2.0,0.5,1.5,10.0,"
            f"{OPEN_TIME_20200101 + FOUR_H_MS},15.0,3,5.0,7.5,0"
        )
        bad_close = mat.build_record(
            mat.parse_kline_csv(csv_bytes([bad_close_row]))[0],
            symbol="BTCUSDT", interval="4h",
            archive_source="monthly", archive_period="2020-01",
            source_archive="x.zip",
        )
        self.assertEqual(
            len(mat.find_time_rule_violations([bad_open, bad_close], interval="4h")),
            2,
        )

    def test_case12c_4h_same_date_limit_allows_six_rejects_seven(self):
        with tempfile.TemporaryDirectory() as tmp:
            raw = Path(tmp) / "raw"
            rows = six_rows_for_taipei_date_20200101()
            archive = write_archive(
                raw, "monthly", "AAAUSDT", "2020-01", rows, interval="4h"
            )
            specs = self._specs({("monthly", "2020-01"): archive})
            res = mat.normalize_symbol("AAAUSDT", specs, interval="4h", strict=False)
            self.assertEqual(res.max_rows_per_date, 6)
            self.assertEqual(res.rows_per_date_violation_count, 0)

            seven = rows + [row_for_interval(ms_utc(2020, 1, 1, 13), "4h")]
            archive = write_archive(
                raw, "monthly", "BBBUSDT", "2020-01", seven, interval="4h"
            )
            specs = self._specs({("monthly", "2020-01"): archive})
            res = mat.normalize_symbol("BBBUSDT", specs, interval="4h", strict=False)
            self.assertEqual(res.rows_per_date_violation_count, 1)
            with self.assertRaises(mat.StrictModeError):
                mat.normalize_symbol("BBBUSDT", specs, interval="4h", strict=True)

    def test_case12d_1h_time_rules_detect_alignment_and_close_time(self):
        aligned = mat.build_record(
            mat.parse_kline_csv(csv_bytes([row_for_interval(OPEN_TIME_20200101, "1h")]))[0],
            symbol="BTCUSDT", interval="1h",
            archive_source="monthly", archive_period="2020-01",
            source_archive="x.zip",
        )
        self.assertEqual(mat.find_time_rule_violations([aligned], interval="1h"), [])

        bad_open = mat.build_record(
            mat.parse_kline_csv(csv_bytes([row_for_interval(OPEN_TIME_20200101 + 1, "1h")]))[0],
            symbol="BTCUSDT", interval="1h",
            archive_source="monthly", archive_period="2020-01",
            source_archive="x.zip",
        )
        bad_close_row = (
            f"{OPEN_TIME_20200101},1.0,2.0,0.5,1.5,10.0,"
            f"{OPEN_TIME_20200101 + ONE_H_MS},15.0,3,5.0,7.5,0"
        )
        bad_close = mat.build_record(
            mat.parse_kline_csv(csv_bytes([bad_close_row]))[0],
            symbol="BTCUSDT", interval="1h",
            archive_source="monthly", archive_period="2020-01",
            source_archive="x.zip",
        )
        self.assertEqual(
            len(mat.find_time_rule_violations([bad_open, bad_close], interval="1h")),
            2,
        )

    def test_case12e_1h_same_date_limit_allows_24_rejects_25(self):
        with tempfile.TemporaryDirectory() as tmp:
            raw = Path(tmp) / "raw"
            rows = twentyfour_rows_for_taipei_date_20200101()
            archive = write_archive(
                raw, "monthly", "AAAUSDT", "2020-01", rows, interval="1h"
            )
            specs = self._specs({("monthly", "2020-01"): archive})
            res = mat.normalize_symbol("AAAUSDT", specs, interval="1h", strict=False)
            self.assertEqual(res.max_rows_per_date, 24)
            self.assertEqual(res.rows_per_date_violation_count, 0)

            # A 25th distinct open_time still on Taipei 2020-01-01 (16:30 UTC ->
            # 00:30 Taipei) is one over the daily limit.
            extra = row_for_interval(ms_utc(2019, 12, 31, 16) + 1_800_000, "1h")
            twentyfive = rows + [extra]
            archive = write_archive(
                raw, "monthly", "BBBUSDT", "2020-01", twentyfive, interval="1h"
            )
            specs = self._specs({("monthly", "2020-01"): archive})
            res = mat.normalize_symbol("BBBUSDT", specs, interval="1h", strict=False)
            self.assertEqual(res.max_rows_per_date, 25)
            self.assertEqual(res.rows_per_date_violation_count, 1)
            with self.assertRaises(mat.StrictModeError):
                mat.normalize_symbol("BBBUSDT", specs, interval="1h", strict=True)

    def test_case12f_15m_time_rules_detect_alignment_and_close_time(self):
        aligned = mat.build_record(
            mat.parse_kline_csv(csv_bytes([row_for_interval(OPEN_TIME_20200101, "15m")]))[0],
            symbol="BTCUSDT", interval="15m",
            archive_source="monthly", archive_period="2020-01",
            source_archive="x.zip",
        )
        self.assertEqual(mat.find_time_rule_violations([aligned], interval="15m"), [])

        bad_open = mat.build_record(
            mat.parse_kline_csv(csv_bytes([row_for_interval(OPEN_TIME_20200101 + 1, "15m")]))[0],
            symbol="BTCUSDT", interval="15m",
            archive_source="monthly", archive_period="2020-01",
            source_archive="x.zip",
        )
        bad_close_row = (
            f"{OPEN_TIME_20200101},1.0,2.0,0.5,1.5,10.0,"
            f"{OPEN_TIME_20200101 + FIFTEEN_M_MS},15.0,3,5.0,7.5,0"
        )
        bad_close = mat.build_record(
            mat.parse_kline_csv(csv_bytes([bad_close_row]))[0],
            symbol="BTCUSDT", interval="15m",
            archive_source="monthly", archive_period="2020-01",
            source_archive="x.zip",
        )
        self.assertEqual(
            len(mat.find_time_rule_violations([bad_open, bad_close], interval="15m")),
            2,
        )

    def test_case12g_15m_same_date_limit_allows_96_rejects_97(self):
        with tempfile.TemporaryDirectory() as tmp:
            raw = Path(tmp) / "raw"
            rows = ninetysix_rows_for_taipei_date_20200101()
            archive = write_archive(
                raw, "monthly", "AAAUSDT", "2020-01", rows, interval="15m"
            )
            specs = self._specs({("monthly", "2020-01"): archive})
            res = mat.normalize_symbol("AAAUSDT", specs, interval="15m", strict=False)
            self.assertEqual(res.max_rows_per_date, 96)
            self.assertEqual(res.rows_per_date_violation_count, 0)

            # A 97th distinct open_time still on Taipei 2020-01-01 (16:07:30 UTC ->
            # 00:07:30 Taipei) is one over the daily limit.
            extra = row_for_interval(ms_utc(2019, 12, 31, 16) + 450_000, "15m")
            ninetyseven = rows + [extra]
            archive = write_archive(
                raw, "monthly", "BBBUSDT", "2020-01", ninetyseven, interval="15m"
            )
            specs = self._specs({("monthly", "2020-01"): archive})
            res = mat.normalize_symbol("BBBUSDT", specs, interval="15m", strict=False)
            self.assertEqual(res.max_rows_per_date, 97)
            self.assertEqual(res.rows_per_date_violation_count, 1)
            with self.assertRaises(mat.StrictModeError):
                mat.normalize_symbol("BBBUSDT", specs, interval="15m", strict=True)

    def test_case12h_5m_time_rules_detect_alignment_and_close_time(self):
        aligned = mat.build_record(
            mat.parse_kline_csv(csv_bytes([row_for_interval(OPEN_TIME_20200101, "5m")]))[0],
            symbol="BTCUSDT", interval="5m",
            archive_source="monthly", archive_period="2020-01",
            source_archive="x.zip",
        )
        self.assertEqual(mat.find_time_rule_violations([aligned], interval="5m"), [])

        bad_open = mat.build_record(
            mat.parse_kline_csv(csv_bytes([row_for_interval(OPEN_TIME_20200101 + 1, "5m")]))[0],
            symbol="BTCUSDT", interval="5m",
            archive_source="monthly", archive_period="2020-01",
            source_archive="x.zip",
        )
        bad_close_row = (
            f"{OPEN_TIME_20200101},1.0,2.0,0.5,1.5,10.0,"
            f"{OPEN_TIME_20200101 + FIVE_M_MS},15.0,3,5.0,7.5,0"
        )
        bad_close = mat.build_record(
            mat.parse_kline_csv(csv_bytes([bad_close_row]))[0],
            symbol="BTCUSDT", interval="5m",
            archive_source="monthly", archive_period="2020-01",
            source_archive="x.zip",
        )
        self.assertEqual(
            len(mat.find_time_rule_violations([bad_open, bad_close], interval="5m")),
            2,
        )

    def test_case12i_5m_same_date_limit_allows_288_rejects_289(self):
        with tempfile.TemporaryDirectory() as tmp:
            raw = Path(tmp) / "raw"
            rows = twoeightyeight_rows_for_taipei_date_20200101()
            archive = write_archive(
                raw, "monthly", "AAAUSDT", "2020-01", rows, interval="5m"
            )
            specs = self._specs({("monthly", "2020-01"): archive})
            res = mat.normalize_symbol("AAAUSDT", specs, interval="5m", strict=False)
            self.assertEqual(res.max_rows_per_date, 288)
            self.assertEqual(res.rows_per_date_violation_count, 0)

            # A 289th distinct open_time still on Taipei 2020-01-01 (16:02:30 UTC ->
            # 00:02:30 Taipei) is one over the daily limit.
            extra = row_for_interval(ms_utc(2019, 12, 31, 16) + 150_000, "5m")
            twoeightynine = rows + [extra]
            archive = write_archive(
                raw, "monthly", "BBBUSDT", "2020-01", twoeightynine, interval="5m"
            )
            specs = self._specs({("monthly", "2020-01"): archive})
            res = mat.normalize_symbol("BBBUSDT", specs, interval="5m", strict=False)
            self.assertEqual(res.max_rows_per_date, 289)
            self.assertEqual(res.rows_per_date_violation_count, 1)
            with self.assertRaises(mat.StrictModeError):
                mat.normalize_symbol("BBBUSDT", specs, interval="5m", strict=True)

    def test_case12j_3m_time_rules_detect_alignment_and_close_time(self):
        aligned = mat.build_record(
            mat.parse_kline_csv(csv_bytes([row_for_interval(OPEN_TIME_20200101, "3m")]))[0],
            symbol="BTCUSDT", interval="3m",
            archive_source="monthly", archive_period="2020-01",
            source_archive="x.zip",
        )
        self.assertEqual(mat.find_time_rule_violations([aligned], interval="3m"), [])

        bad_open = mat.build_record(
            mat.parse_kline_csv(csv_bytes([row_for_interval(OPEN_TIME_20200101 + 1, "3m")]))[0],
            symbol="BTCUSDT", interval="3m",
            archive_source="monthly", archive_period="2020-01",
            source_archive="x.zip",
        )
        bad_close_row = (
            f"{OPEN_TIME_20200101},1.0,2.0,0.5,1.5,10.0,"
            f"{OPEN_TIME_20200101 + THREE_M_MS},15.0,3,5.0,7.5,0"
        )
        bad_close = mat.build_record(
            mat.parse_kline_csv(csv_bytes([bad_close_row]))[0],
            symbol="BTCUSDT", interval="3m",
            archive_source="monthly", archive_period="2020-01",
            source_archive="x.zip",
        )
        self.assertEqual(
            len(mat.find_time_rule_violations([bad_open, bad_close], interval="3m")),
            2,
        )

    def test_case12k_3m_same_date_limit_allows_480_rejects_481(self):
        with tempfile.TemporaryDirectory() as tmp:
            raw = Path(tmp) / "raw"
            rows = foureighty_rows_for_taipei_date_20200101()
            archive = write_archive(
                raw, "monthly", "AAAUSDT", "2020-01", rows, interval="3m"
            )
            specs = self._specs({("monthly", "2020-01"): archive})
            res = mat.normalize_symbol("AAAUSDT", specs, interval="3m", strict=False)
            self.assertEqual(res.max_rows_per_date, 480)
            self.assertEqual(res.rows_per_date_violation_count, 0)

            # A 481st distinct open_time still on Taipei 2020-01-01 (16:01:30 UTC ->
            # 00:01:30 Taipei) is one over the daily limit.
            extra = row_for_interval(ms_utc(2019, 12, 31, 16) + 90_000, "3m")
            foureightyone = rows + [extra]
            archive = write_archive(
                raw, "monthly", "BBBUSDT", "2020-01", foureightyone, interval="3m"
            )
            specs = self._specs({("monthly", "2020-01"): archive})
            res = mat.normalize_symbol("BBBUSDT", specs, interval="3m", strict=False)
            self.assertEqual(res.max_rows_per_date, 481)
            self.assertEqual(res.rows_per_date_violation_count, 1)
            with self.assertRaises(mat.StrictModeError):
                mat.normalize_symbol("BBBUSDT", specs, interval="3m", strict=True)


# --------------------------------------------------------------------------- #
# End-to-end: resume / DuckDB / validation / counts (cases 13-17)
# --------------------------------------------------------------------------- #


class EndToEndTest(unittest.TestCase):
    def _two_month_specs(self, tmp):
        # BTCUSDT: monthly 2020-01 (1 row) + monthly 2020-02 (1 row, Feb).
        feb_ms = 1580515200000  # 2020-02-01 00:00 UTC
        feb_row = f"{feb_ms},1.0,2.0,0.5,1.5,10.0,1580601599999,15.0,3,5.0,7.5,0"
        return build_env(
            tmp,
            [
                ("monthly", "BTCUSDT", "2020-01", [ROW_20200101]),
                ("monthly", "BTCUSDT", "2020-02", [feb_row]),
            ],
        )

    def _four_h_specs(self, tmp):
        return build_env(
            tmp,
            [
                (
                    "monthly",
                    "BTCUSDT",
                    "2020-01",
                    six_rows_for_taipei_date_20200101(),
                )
            ],
            interval="4h",
        )

    def _one_h_specs(self, tmp):
        return build_env(
            tmp,
            [
                (
                    "monthly",
                    "BTCUSDT",
                    "2020-01",
                    twentyfour_rows_for_taipei_date_20200101(),
                )
            ],
            interval="1h",
        )

    def _fifteen_m_specs(self, tmp):
        return build_env(
            tmp,
            [
                (
                    "monthly",
                    "BTCUSDT",
                    "2020-01",
                    ninetysix_rows_for_taipei_date_20200101(),
                )
            ],
            interval="15m",
        )

    def _five_m_specs(self, tmp):
        return build_env(
            tmp,
            [
                (
                    "monthly",
                    "BTCUSDT",
                    "2020-01",
                    twoeightyeight_rows_for_taipei_date_20200101(),
                )
            ],
            interval="5m",
        )

    def _three_m_specs(self, tmp):
        return build_env(
            tmp,
            [
                (
                    "monthly",
                    "BTCUSDT",
                    "2020-01",
                    foureighty_rows_for_taipei_date_20200101(),
                )
            ],
            interval="3m",
        )

    def test_case13_resume_skips_completed_symbol(self):
        with tempfile.TemporaryDirectory() as tmp:
            manifest_p, raw, out = self._two_month_specs(tmp)
            mat.run(make_config(manifest_p, raw, out))
            # Second pass with resume should skip the symbol.
            res = mat.process_symbol(
                "BTCUSDT",
                mat.load_work_units(manifest_p, raw)[0]["BTCUSDT"],
                interval="1d", output_root=out, strict=False,
                resume=True, overwrite=False,
            )
            self.assertEqual(res.status, "skipped")

    def test_case14_duckdb_reads_parquet(self):
        import duckdb
        with tempfile.TemporaryDirectory() as tmp:
            manifest_p, raw, out = self._two_month_specs(tmp)
            mat.run(make_config(manifest_p, raw, out))
            glob = str(out / "**" / "*.parquet")
            n = duckdb.sql(
                f"SELECT COUNT(*) FROM read_parquet('{glob}', "
                f"hive_partitioning=true)"
            ).fetchone()[0]
            self.assertEqual(n, 2)
            # Hive columns exposed.
            cols = {
                r[0]
                for r in duckdb.sql(
                    f"DESCRIBE SELECT * FROM read_parquet('{glob}', "
                    f"hive_partitioning=true)"
                ).fetchall()
            }
            self.assertIn("symbol", cols)
            self.assertIn("year", cols)

    def test_case15_validation_target_is_explicit_only(self):
        # Clone-safe: the parquet target never defaults to local_data; it must
        # be invoked explicitly with --manifest.
        from datahub.validation.errors import ValidationCommandError
        parser = build_parser()
        args = parser.parse_args(["--target", "binance-um-klines-parquet"])
        with self.assertRaises(ValidationCommandError):
            cli_run(args)

    def test_case16_explicit_manifest_validation_passes(self):
        with tempfile.TemporaryDirectory() as tmp:
            manifest_p, raw, out = self._two_month_specs(tmp)
            mat.run(make_config(manifest_p, raw, out))
            mat_manifest = out / "manifests" / "materialization_manifest.json"
            report = validate_parquet_manifest(mat_manifest, "1d", ".")
            self.assertFalse(
                report.has_failures,
                "\n".join(
                    f"{c.rule_id}: {c.message}"
                    for c in report.error_summary
                ),
            )

    def test_case17_manifest_counts_match_actual(self):
        import duckdb
        with tempfile.TemporaryDirectory() as tmp:
            manifest_p, raw, out = self._two_month_specs(tmp)
            manifest = mat.run(make_config(manifest_p, raw, out))
            actual_files = len(list(Path(out).rglob("*.parquet")))
            glob = str(out / "**" / "*.parquet")
            actual_rows = duckdb.sql(
                f"SELECT COUNT(*) FROM read_parquet('{glob}', "
                f"hive_partitioning=true)"
            ).fetchone()[0]
            self.assertEqual(manifest["file_count"], actual_files)
            self.assertEqual(manifest["row_count"], actual_rows)
            self.assertEqual(manifest["generated_csv_file_count"], 0)
            # Two months -> two year=2020 files would collide; both are 2020 so
            # they share one part file. Confirm scope + no csv.
            self.assertEqual(
                len(list(Path(out).rglob("*.csv"))), 0
            )

    def test_case18_duckdb_reads_and_validates_4h(self):
        import duckdb
        with tempfile.TemporaryDirectory() as tmp:
            manifest_p, raw, out = self._four_h_specs(tmp)
            manifest = mat.run(make_config(manifest_p, raw, out, interval="4h"))
            self.assertEqual(
                manifest["materialized_dataset_id"],
                "market.binance.um.klines.4h.parquet",
            )
            self.assertEqual(manifest["interval"], "4h")
            self.assertEqual(manifest["output_scope"], "FULL_OUTPUT")
            self.assertEqual(manifest["symbol_count"], 1)
            self.assertEqual(manifest["generated_csv_file_count"], 0)

            glob = str(out / "**" / "*.parquet")
            n, max_per_date = duckdb.sql(
                f"SELECT COUNT(*), MAX(n) FROM ("
                f"SELECT symbol, date, COUNT(*) AS n "
                f"FROM read_parquet('{glob}', hive_partitioning=true) "
                f"GROUP BY symbol, date)"
            ).fetchone()
            self.assertEqual(n, 1)
            self.assertEqual(max_per_date, 6)

            mat_manifest = out / "manifests" / "materialization_manifest.json"
            report = validate_parquet_manifest(mat_manifest, "4h", ".")
            self.assertFalse(report.has_failures, report.render())

    def test_case18b_duckdb_reads_and_validates_1h(self):
        import duckdb
        with tempfile.TemporaryDirectory() as tmp:
            manifest_p, raw, out = self._one_h_specs(tmp)
            manifest = mat.run(make_config(manifest_p, raw, out, interval="1h"))
            self.assertEqual(
                manifest["materialized_dataset_id"],
                "market.binance.um.klines.1h.parquet",
            )
            self.assertEqual(manifest["interval"], "1h")
            self.assertEqual(manifest["output_scope"], "FULL_OUTPUT")
            self.assertEqual(manifest["symbol_count"], 1)
            self.assertEqual(manifest["row_count"], 24)
            self.assertEqual(manifest["generated_csv_file_count"], 0)

            glob = str(out / "**" / "*.parquet")
            n, max_per_date = duckdb.sql(
                f"SELECT COUNT(*), MAX(n) FROM ("
                f"SELECT symbol, date, COUNT(*) AS n "
                f"FROM read_parquet('{glob}', hive_partitioning=true) "
                f"GROUP BY symbol, date)"
            ).fetchone()
            self.assertEqual(n, 1)
            self.assertEqual(max_per_date, 24)

            # open_time alignment + close_time rule hold for every 1h bar.
            bad = duckdb.sql(
                f"SELECT COUNT(*) FROM read_parquet('{glob}', "
                f"hive_partitioning=true) "
                f"WHERE open_time % {ONE_H_MS} != 0 "
                f"OR close_time != open_time + {ONE_H_MS - 1}"
            ).fetchone()[0]
            self.assertEqual(bad, 0)

            mat_manifest = out / "manifests" / "materialization_manifest.json"
            report = validate_parquet_manifest(mat_manifest, "1h", ".")
            self.assertFalse(report.has_failures, report.render())

    def test_case18c_invalid_ohlc_bar_quarantined_from_output(self):
        import duckdb
        good = row_for_interval(OPEN_TIME_20200101, "1h")
        bad_open = OPEN_TIME_20200101 + ONE_H_MS
        # Corrupt source bar: open(36054) and close(39550) above high(35999).
        bad = (
            f"{bad_open},36054.1,35999.4,35900.0,39550.0,4.0,"
            f"{bad_open + ONE_H_MS - 1},1.0,3,1.0,1.0,0"
        )
        with tempfile.TemporaryDirectory() as tmp:
            manifest_p, raw, out = build_env(
                tmp, [("monthly", "BTCUSDT", "2020-01", [good, bad])], interval="1h"
            )
            manifest = mat.run(make_config(manifest_p, raw, out, interval="1h"))
            # Bad bar is quarantined out of the query layer.
            self.assertEqual(manifest["row_count"], 1)
            self.assertEqual(manifest["failed_symbol_count"], 0)
            glob = str(out / "**" / "*.parquet")
            remaining_bad = duckdb.sql(
                f"SELECT COUNT(*) FROM read_parquet('{glob}', "
                f"hive_partitioning=true) "
                f"WHERE NOT (high>=open AND high>=close AND high>=low "
                f"AND low<=open AND low<=close)"
            ).fetchone()[0]
            self.assertEqual(remaining_bad, 0)
            # Report discloses the quarantined bar.
            dq = json.loads(
                (out / "reports" / "data_quality_report.json").read_text()
            )
            self.assertEqual(dq["ohlc_violation_count"], 1)
            self.assertEqual(dq["quarantined_bar_count"], 1)
            # Explicit validation passes on the cleaned layer.
            mat_manifest = out / "manifests" / "materialization_manifest.json"
            report = validate_parquet_manifest(mat_manifest, "1h", ".")
            self.assertFalse(report.has_failures, report.render())

        # --strict fails the symbol instead of quarantining.
        with tempfile.TemporaryDirectory() as tmp:
            manifest_p, raw, out = build_env(
                tmp, [("monthly", "BTCUSDT", "2020-01", [good, bad])], interval="1h"
            )
            manifest = mat.run(
                make_config(manifest_p, raw, out, interval="1h", strict=True)
            )
            self.assertEqual(manifest["failed_symbol_count"], 1)

    def test_case18d_duckdb_reads_and_validates_15m(self):
        import duckdb
        with tempfile.TemporaryDirectory() as tmp:
            manifest_p, raw, out = self._fifteen_m_specs(tmp)
            manifest = mat.run(make_config(manifest_p, raw, out, interval="15m"))
            self.assertEqual(
                manifest["materialized_dataset_id"],
                "market.binance.um.klines.15m.parquet",
            )
            self.assertEqual(manifest["interval"], "15m")
            self.assertEqual(manifest["output_scope"], "FULL_OUTPUT")
            self.assertEqual(manifest["symbol_count"], 1)
            self.assertEqual(manifest["row_count"], 96)
            self.assertEqual(manifest["generated_csv_file_count"], 0)

            glob = str(out / "**" / "*.parquet")
            n, max_per_date = duckdb.sql(
                f"SELECT COUNT(*), MAX(n) FROM ("
                f"SELECT symbol, date, COUNT(*) AS n "
                f"FROM read_parquet('{glob}', hive_partitioning=true) "
                f"GROUP BY symbol, date)"
            ).fetchone()
            self.assertEqual(n, 1)
            self.assertEqual(max_per_date, 96)

            # open_time alignment + close_time rule hold for every 15m bar.
            bad = duckdb.sql(
                f"SELECT COUNT(*) FROM read_parquet('{glob}', "
                f"hive_partitioning=true) "
                f"WHERE open_time % {FIFTEEN_M_MS} != 0 "
                f"OR close_time != open_time + {FIFTEEN_M_MS - 1}"
            ).fetchone()[0]
            self.assertEqual(bad, 0)

            mat_manifest = out / "manifests" / "materialization_manifest.json"
            report = validate_parquet_manifest(mat_manifest, "15m", ".")
            self.assertFalse(report.has_failures, report.render())

    def test_case18e_duckdb_reads_and_validates_5m(self):
        import duckdb
        with tempfile.TemporaryDirectory() as tmp:
            manifest_p, raw, out = self._five_m_specs(tmp)
            manifest = mat.run(make_config(manifest_p, raw, out, interval="5m"))
            self.assertEqual(
                manifest["materialized_dataset_id"],
                "market.binance.um.klines.5m.parquet",
            )
            self.assertEqual(manifest["interval"], "5m")
            self.assertEqual(manifest["output_scope"], "FULL_OUTPUT")
            self.assertEqual(manifest["symbol_count"], 1)
            self.assertEqual(manifest["row_count"], 288)
            self.assertEqual(manifest["generated_csv_file_count"], 0)

            glob = str(out / "**" / "*.parquet")
            n, max_per_date = duckdb.sql(
                f"SELECT COUNT(*), MAX(n) FROM ("
                f"SELECT symbol, date, COUNT(*) AS n "
                f"FROM read_parquet('{glob}', hive_partitioning=true) "
                f"GROUP BY symbol, date)"
            ).fetchone()
            self.assertEqual(n, 1)
            self.assertEqual(max_per_date, 288)

            # open_time alignment + close_time rule hold for every 5m bar.
            bad = duckdb.sql(
                f"SELECT COUNT(*) FROM read_parquet('{glob}', "
                f"hive_partitioning=true) "
                f"WHERE open_time % {FIVE_M_MS} != 0 "
                f"OR close_time != open_time + {FIVE_M_MS - 1}"
            ).fetchone()[0]
            self.assertEqual(bad, 0)

            mat_manifest = out / "manifests" / "materialization_manifest.json"
            report = validate_parquet_manifest(mat_manifest, "5m", ".")
            self.assertFalse(report.has_failures, report.render())

    def test_case18f_duckdb_reads_and_validates_3m(self):
        import duckdb
        with tempfile.TemporaryDirectory() as tmp:
            manifest_p, raw, out = self._three_m_specs(tmp)
            manifest = mat.run(make_config(manifest_p, raw, out, interval="3m"))
            self.assertEqual(
                manifest["materialized_dataset_id"],
                "market.binance.um.klines.3m.parquet",
            )
            self.assertEqual(manifest["interval"], "3m")
            self.assertEqual(manifest["output_scope"], "FULL_OUTPUT")
            self.assertEqual(manifest["symbol_count"], 1)
            self.assertEqual(manifest["row_count"], 480)
            self.assertEqual(manifest["generated_csv_file_count"], 0)

            glob = str(out / "**" / "*.parquet")
            n, max_per_date = duckdb.sql(
                f"SELECT COUNT(*), MAX(n) FROM ("
                f"SELECT symbol, date, COUNT(*) AS n "
                f"FROM read_parquet('{glob}', hive_partitioning=true) "
                f"GROUP BY symbol, date)"
            ).fetchone()
            self.assertEqual(n, 1)
            self.assertEqual(max_per_date, 480)

            # open_time alignment + close_time rule hold for every 3m bar.
            bad = duckdb.sql(
                f"SELECT COUNT(*) FROM read_parquet('{glob}', "
                f"hive_partitioning=true) "
                f"WHERE open_time % {THREE_M_MS} != 0 "
                f"OR close_time != open_time + {THREE_M_MS - 1}"
            ).fetchone()[0]
            self.assertEqual(bad, 0)

            mat_manifest = out / "manifests" / "materialization_manifest.json"
            report = validate_parquet_manifest(mat_manifest, "3m", ".")
            self.assertFalse(report.has_failures, report.render())

    def test_case19_default_paths_are_interval_aware(self):
        self.assertIn("interval=4h", mat.default_raw_root("4h"))
        self.assertIn("interval=4h", mat.default_manifest("4h"))
        self.assertIn("interval=4h", mat.default_output_root("4h"))
        self.assertIn("interval=1h", mat.default_raw_root("1h"))
        self.assertIn("interval=1h", mat.default_manifest("1h"))
        self.assertIn("interval=1h", mat.default_output_root("1h"))
        self.assertIn("interval=15m", mat.default_raw_root("15m"))
        self.assertIn("interval=15m", mat.default_manifest("15m"))
        self.assertIn("interval=15m", mat.default_output_root("15m"))
        self.assertIn("interval=5m", mat.default_raw_root("5m"))
        self.assertIn("interval=5m", mat.default_manifest("5m"))
        self.assertIn("interval=5m", mat.default_output_root("5m"))
        self.assertIn("interval=3m", mat.default_raw_root("3m"))
        self.assertIn("interval=3m", mat.default_manifest("3m"))
        self.assertIn("interval=3m", mat.default_output_root("3m"))

    def test_full_vs_sample_scope(self):
        with tempfile.TemporaryDirectory() as tmp:
            manifest_p, raw, out = self._two_month_specs(tmp)
            # all symbols -> FULL_OUTPUT (single symbol universe).
            full = mat.run(make_config(manifest_p, raw, out))
            self.assertEqual(full["output_scope"], "FULL_OUTPUT")
            # subset -> SAMPLE_OUTPUT.
            sample = mat.run(
                make_config(
                    manifest_p, raw, out,
                    all_symbols=False, symbols=["BTCUSDT"], overwrite=True,
                )
            )
            self.assertEqual(sample["output_scope"], "SAMPLE_OUTPUT")


if __name__ == "__main__":
    unittest.main()
