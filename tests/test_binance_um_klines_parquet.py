"""Tests for the Binance UM 1D Kline Parquet materialization (Phase 6)."""

import io
import json
from pathlib import Path
import tempfile
import unittest
import zipfile

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


def write_archive(raw_root, source, symbol, period, rows, *, header=False):
    name = f"{symbol}-1d-{period}"
    directory = Path(raw_root) / source / symbol
    directory.mkdir(parents=True, exist_ok=True)
    zip_path = directory / f"{name}.zip"
    zip_path.write_bytes(zip_bytes(f"{name}.csv", csv_bytes(rows, header=header)))
    return zip_path


def build_env(tmp, archive_specs, *, discovered_symbol_count=None):
    """Create raw layout + run manifest + files.jsonl from archive_specs.

    Each spec: (source, symbol, period, rows[, header]).
    Returns (manifest_path, raw_root, output_root).
    """
    base = Path(tmp) / "local_data" / "binance_um_klines" / "interval=1d"
    raw_root = base / "raw"
    manifests = base / "manifests"
    manifests.mkdir(parents=True, exist_ok=True)
    output_root = base / "parquet"

    file_records = []
    symbols = set()
    for spec in archive_specs:
        source, symbol, period, rows = spec[0], spec[1], spec[2], spec[3]
        header = spec[4] if len(spec) > 4 else False
        zip_path = write_archive(raw_root, source, symbol, period, rows, header=header)
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
        "interval": "1d",
        "dataset_id": "market.binance.um.klines",
        "symbol_count": discovered_symbol_count or len(symbols),
        "discovered_symbol_count": discovered_symbol_count or len(symbols),
        "file_manifest": str(files_jsonl),
    }
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    return manifest_path, raw_root, output_root


def make_config(manifest_path, raw_root, output_root, **overrides):
    params = dict(
        interval="1d",
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
