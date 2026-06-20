"""Materialize Binance USD-M Futures Kline raw archives into Parquet.

Layer model
-----------
* ``raw`` zip archive  — immutable **source** layer (Phase 5 output).
* ``parquet``          — **query / materialized** layer produced here.
* DuckDB               — standard query engine over the Parquet layer.

CSV exists only transiently inside the per-zip parsing flow. No persistent CSV
is ever written under the materialized tree (``generated_csv_file_count == 0``).

Date policy
-----------
``date`` is the **Asia/Taipei calendar date** derived from ``open_time``. Both
``open_time_utc`` and ``open_time_taipei`` are materialized as naive timestamps
(UTC wall-clock and UTC+8 wall-clock respectively) so that
``CAST(open_time_taipei AS DATE)`` is independent of the DuckDB session
timezone and always equals ``date``.

Partition layout
----------------
Hive-style ``symbol=<S>/year=<Y>/part-000.parquet``. Physical parquet files
*omit* the ``symbol`` and ``year`` columns; DuckDB exposes them from the Hive
path via ``hive_partitioning = true``. This avoids duplicate/ambiguous columns
when querying. The logical DuckDB schema therefore re-exposes ``symbol`` and
``year`` on top of the physical columns.

Dedup / conflict policy (key = symbol + interval + open_time)
-------------------------------------------------------------
* monthly and daily agree on a bar           -> keep one row (duplicate)
* monthly and daily disagree on a bar         -> daily wins, record conflict
* same source duplicated and consistent       -> keep one row (duplicate)
* same source duplicated and inconsistent      -> keep daily-preferred/first,
                                                  record data-quality issue
``--strict`` turns any conflict or data-quality issue into a symbol failure.

Quarantine policy
-----------------
Bars that fail the OHLC ordering/non-negativity rules or the interval time rules
(open-time alignment, close-time delta) are corrupt at the source. They are
**quarantined** — excluded from the materialized Parquet query layer and
disclosed in ``reports/data_quality_report.json`` — so the DuckDB query layer is
guaranteed clean. ``--strict`` fails the symbol instead of quarantining. Coarser
intervals (1d/4h) contain no such bars, so quarantine is a no-op for them.
"""

from __future__ import annotations

import argparse
import concurrent.futures
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
import io
import json
from pathlib import Path
import shutil
import subprocess
import sys
from typing import Any, Iterable
import zipfile

try:  # pragma: no cover - exercised indirectly
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError as exc:  # pragma: no cover
    pa = None
    pq = None
    _PYARROW_IMPORT_ERROR = exc
else:
    _PYARROW_IMPORT_ERROR = None

# --------------------------------------------------------------------------- #
# Constants
# --------------------------------------------------------------------------- #

RAW_DATASET_ID = "market.binance.um.klines"
CODE_VERSION = "v0.12.0"
QUERY_ENGINE = "duckdb"
OUTPUT_FORMAT = "parquet"

ALLOWED_INTERVALS: tuple[str, ...] = ("1d", "4h", "1h", "15m", "5m", "3m")
INTERVAL_MILLISECONDS = {
    "1d": 86_400_000,
    "4h": 14_400_000,
    "1h": 3_600_000,
    "15m": 900_000,
    "5m": 300_000,
    "3m": 180_000,
}
ROWS_PER_SYMBOL_DATE_LIMIT = {
    "1d": 1,
    "4h": 6,
    "1h": 24,
    "15m": 96,
    "5m": 288,
    "3m": 480,
}
PRIMARY_KEY: tuple[str, ...] = ("symbol", "interval", "open_time")

FULL_OUTPUT = "FULL_OUTPUT"
SAMPLE_OUTPUT = "SAMPLE_OUTPUT"

TAIPEI_OFFSET = timedelta(hours=8)
EPOCH = datetime(1970, 1, 1)

PROGRESS_EVERY = 25

# Raw Binance Kline CSV columns are interpreted **positionally**. Newer archive
# files carry a header (``open_time,open,...,count,taker_buy_volume,...``) whose
# names differ from our canonical names; older files carry no header at all.
# Position is the stable contract, not the header text.
#   0 open_time              6 close_time
#   1 open                   7 quote_volume   (header: quote_volume)
#   2 high                   8 trade_count    (header: count)
#   3 low                    9 taker_buy_base_volume  (header: taker_buy_volume)
#   4 close                 10 taker_buy_quote_volume
#   5 volume                11 ignore  (dropped)
N_RAW_COLUMNS = 12
IDX_OPEN_TIME = 0


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #


class MaterializationError(RuntimeError):
    """Base error for the materialization pipeline."""


class MaterializationCommandError(MaterializationError):
    """Invalid CLI arguments or invocation state."""


class CsvParseError(MaterializationError):
    """A raw Kline CSV row could not be parsed."""


class StrictModeError(MaterializationError):
    """Raised in --strict mode when a conflict or data-quality issue is found."""


class DependencyError(MaterializationError):
    """A required third-party dependency is missing."""


# --------------------------------------------------------------------------- #
# Small helpers
# --------------------------------------------------------------------------- #


def utc_now() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def pretty_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n"


def progress_log(message: str) -> None:
    """Emit a progress line to stderr (never stdout)."""
    print(message, file=sys.stderr, flush=True)


def git_commit() -> str:
    try:
        out = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            check=True,
        )
        return out.stdout.strip()
    except (subprocess.CalledProcessError, OSError):
        return "unknown"


def materialized_dataset_id(interval: str) -> str:
    return f"{RAW_DATASET_ID}.{interval}.parquet"


def default_raw_root(interval: str) -> str:
    return f"local_data/binance_um_klines/interval={interval}/raw"


def default_manifest(interval: str) -> str:
    return f"local_data/binance_um_klines/interval={interval}/manifests/manifest.json"


def default_output_root(interval: str) -> str:
    return f"local_data/binance_um_klines/interval={interval}/parquet"


def require_pyarrow() -> None:
    if pa is None:
        raise DependencyError(
            "pyarrow is required to write Parquet but is not installed. "
            "Install with: pip install pyarrow"
        )


def validate_interval(interval: str) -> str:
    if interval not in ALLOWED_INTERVALS:
        raise MaterializationCommandError(
            f"unsupported interval: {interval!r}; "
            f"supported intervals: {' '.join(ALLOWED_INTERVALS)}"
        )
    return interval


def interval_milliseconds(interval: str) -> int:
    validate_interval(interval)
    return INTERVAL_MILLISECONDS[interval]


def rows_per_symbol_date_limit(interval: str) -> int:
    validate_interval(interval)
    return ROWS_PER_SYMBOL_DATE_LIMIT[interval]


# --------------------------------------------------------------------------- #
# CSV parsing
# --------------------------------------------------------------------------- #


def _is_header_row(cells: list[str]) -> bool:
    """A header row has a non-integer first cell (data rows start with ms epoch)."""
    if not cells:
        return True
    first = cells[IDX_OPEN_TIME].strip()
    if not first:
        return True
    try:
        int(first)
    except ValueError:
        return True
    return False


def parse_kline_csv(data: bytes) -> list[list[str]]:
    """Parse raw Kline CSV bytes into a list of 12-column string rows.

    Supports both header and header-less CSV. Blank lines are skipped. Rows with
    fewer than 11 fields are rejected (we require through taker_buy_quote_volume).
    The trailing ``ignore`` column is preserved here and dropped during
    normalization.
    """
    rows: list[list[str]] = []
    text = data.decode("utf-8")
    for lineno, raw_line in enumerate(text.splitlines()):
        line = raw_line.strip()
        if not line:
            continue
        cells = [c.strip() for c in line.split(",")]
        if lineno == 0 and _is_header_row(cells):
            continue
        if _is_header_row(cells):
            # Defensive: a header somewhere other than line 0 (rare). Skip it.
            continue
        if len(cells) < N_RAW_COLUMNS - 1:
            raise CsvParseError(
                f"expected >= {N_RAW_COLUMNS - 1} columns, got {len(cells)}: {line!r}"
            )
        rows.append(cells)
    return rows


def _to_float(value: str, field_name: str) -> float:
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise CsvParseError(f"non-numeric {field_name}: {value!r}") from exc


def _to_int(value: str, field_name: str) -> int:
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise CsvParseError(f"non-integer {field_name}: {value!r}") from exc


# --------------------------------------------------------------------------- #
# Record model
# --------------------------------------------------------------------------- #


@dataclass
class KlineRecord:
    """One normalized Kline bar (physical + partition fields)."""

    symbol: str
    interval: str
    open_time: int
    open_time_utc: datetime
    open_time_taipei: datetime
    date: str
    year: int
    month: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    close_time: int
    quote_volume: float
    trade_count: int
    taker_buy_base_volume: float
    taker_buy_quote_volume: float
    source_archive: str
    archive_source: str
    archive_period: str

    def value_signature(self) -> tuple:
        """Numeric identity used to decide equal-vs-conflict for the same key."""
        return (
            self.open,
            self.high,
            self.low,
            self.close,
            self.volume,
            self.close_time,
            self.quote_volume,
            self.trade_count,
            self.taker_buy_base_volume,
            self.taker_buy_quote_volume,
        )


def build_record(
    cells: list[str],
    *,
    symbol: str,
    interval: str,
    archive_source: str,
    archive_period: str,
    source_archive: str,
) -> KlineRecord:
    open_time = _to_int(cells[0], "open_time")
    dt_utc = EPOCH + timedelta(milliseconds=open_time)
    dt_taipei = dt_utc + TAIPEI_OFFSET
    return KlineRecord(
        symbol=symbol,
        interval=interval,
        open_time=open_time,
        open_time_utc=dt_utc,
        open_time_taipei=dt_taipei,
        date=dt_taipei.strftime("%Y-%m-%d"),
        year=dt_taipei.year,
        month=dt_taipei.month,
        open=_to_float(cells[1], "open"),
        high=_to_float(cells[2], "high"),
        low=_to_float(cells[3], "low"),
        close=_to_float(cells[4], "close"),
        volume=_to_float(cells[5], "volume"),
        close_time=_to_int(cells[6], "close_time"),
        quote_volume=_to_float(cells[7], "quote_volume"),
        trade_count=_to_int(cells[8], "trade_count"),
        taker_buy_base_volume=_to_float(cells[9], "taker_buy_base_volume"),
        taker_buy_quote_volume=_to_float(cells[10], "taker_buy_quote_volume"),
        source_archive=source_archive,
        archive_source=archive_source,
        archive_period=archive_period,
    )


# --------------------------------------------------------------------------- #
# Per-symbol normalization (dedup / conflict / sort)
# --------------------------------------------------------------------------- #


@dataclass
class ArchiveSpec:
    """One source zip belonging to a symbol."""

    archive_source: str  # "monthly" | "daily"
    archive_period: str  # "YYYY-MM" | "YYYY-MM-DD"
    local_zip_path: str
    zip_name: str


@dataclass
class SymbolResult:
    symbol: str
    rows: list[KlineRecord] = field(default_factory=list)
    duplicate_count: int = 0
    conflict_count: int = 0
    quality_issue_count: int = 0
    duplicate_samples: list[dict] = field(default_factory=list)
    conflict_samples: list[dict] = field(default_factory=list)
    quality_samples: list[dict] = field(default_factory=list)
    duplicate_date_count: int = 0
    rows_per_date_violation_count: int = 0
    rows_per_date_samples: list[dict] = field(default_factory=list)
    max_rows_per_date: int = 0
    archive_sources: set[str] = field(default_factory=set)


# daily wins over monthly on conflict.
_SOURCE_RANK = {"daily": 2, "monthly": 1}


def read_archive_records(
    spec: ArchiveSpec, *, symbol: str, interval: str
) -> list[KlineRecord]:
    """Read one zip and return its normalized records."""
    zip_path = Path(spec.local_zip_path)
    with zipfile.ZipFile(zip_path) as zf:
        names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        if not names:
            raise CsvParseError(f"no CSV member in archive: {zip_path}")
        data = zf.read(names[0])
    rows = parse_kline_csv(data)
    return [
        build_record(
            cells,
            symbol=symbol,
            interval=interval,
            archive_source=spec.archive_source,
            archive_period=spec.archive_period,
            source_archive=spec.local_zip_path,
        )
        for cells in rows
    ]


def normalize_symbol(
    symbol: str,
    specs: Iterable[ArchiveSpec],
    *,
    interval: str,
    strict: bool,
    sample_cap: int = 20,
) -> SymbolResult:
    """Read every archive for a symbol, dedup by key, resolve conflicts, sort."""
    result = SymbolResult(symbol=symbol)
    by_key: dict[int, KlineRecord] = {}

    # Process monthly before daily so daily is the later writer; conflict policy
    # is enforced explicitly via source rank regardless of iteration order.
    ordered = sorted(
        specs, key=lambda s: (_SOURCE_RANK.get(s.archive_source, 0), s.archive_period)
    )
    for spec in ordered:
        result.archive_sources.add(spec.archive_source)
        for rec in read_archive_records(spec, symbol=symbol, interval=interval):
            key = rec.open_time
            prev = by_key.get(key)
            if prev is None:
                by_key[key] = rec
                continue
            if prev.value_signature() == rec.value_signature():
                # Identical bar from a second archive: deduplicate.
                result.duplicate_count += 1
                _record_sample(
                    result.duplicate_samples,
                    sample_cap,
                    {
                        "symbol": symbol,
                        "open_time": key,
                        "kept_source": prev.archive_source,
                        "dropped_source": rec.archive_source,
                    },
                )
                continue
            # Values disagree for the same key.
            cross_source = prev.archive_source != rec.archive_source
            if cross_source:
                # monthly vs daily conflict -> daily wins.
                winner, loser = _pick_daily(prev, rec)
                by_key[key] = winner
                result.conflict_count += 1
                _record_sample(
                    result.conflict_samples,
                    sample_cap,
                    {
                        "symbol": symbol,
                        "open_time": key,
                        "winner_source": winner.archive_source,
                        "loser_source": loser.archive_source,
                        "winner_close": winner.close,
                        "loser_close": loser.close,
                    },
                )
                if strict:
                    raise StrictModeError(
                        f"strict: monthly/daily conflict for {symbol} "
                        f"open_time={key}"
                    )
            else:
                # Same-source duplicate but inconsistent -> data-quality issue.
                result.quality_issue_count += 1
                _record_sample(
                    result.quality_samples,
                    sample_cap,
                    {
                        "symbol": symbol,
                        "open_time": key,
                        "source": rec.archive_source,
                        "kept_close": prev.close,
                        "other_close": rec.close,
                    },
                )
                if strict:
                    raise StrictModeError(
                        f"strict: inconsistent same-source duplicate for "
                        f"{symbol} open_time={key}"
                    )
                # Keep first-seen deterministically.

    result.rows = [by_key[k] for k in sorted(by_key)]

    date_counts: dict[str, int] = {}
    for rec in result.rows:
        date_counts[rec.date] = date_counts.get(rec.date, 0) + 1
    result.max_rows_per_date = max(date_counts.values(), default=0)

    per_date_limit = rows_per_symbol_date_limit(interval)
    for date, count in sorted(date_counts.items()):
        if count <= per_date_limit:
            continue
        result.rows_per_date_violation_count += 1
        if interval == "1d":
            result.duplicate_date_count += count - per_date_limit
        _record_sample(
            result.rows_per_date_samples,
            sample_cap,
            {
                "symbol": symbol,
                "date": date,
                "row_count": count,
                "limit": per_date_limit,
                "interval": interval,
            },
        )
        if strict:
            raise StrictModeError(
                f"strict: {count} rows for {symbol} date={date}; "
                f"limit={per_date_limit} for interval={interval}"
            )
    if result.rows_per_date_violation_count:
        result.quality_issue_count += result.rows_per_date_violation_count

    return result


def _pick_daily(a: KlineRecord, b: KlineRecord) -> tuple[KlineRecord, KlineRecord]:
    if _SOURCE_RANK.get(a.archive_source, 0) >= _SOURCE_RANK.get(b.archive_source, 0):
        return a, b
    return b, a


def _record_sample(bucket: list[dict], cap: int, item: dict) -> None:
    if len(bucket) < cap:
        bucket.append(item)


# --------------------------------------------------------------------------- #
# OHLC quality validation (in-process, mirrors the validation target rules)
# --------------------------------------------------------------------------- #


def is_ohlc_valid(r: KlineRecord) -> bool:
    return (
        r.high >= r.low
        and r.high >= r.open
        and r.high >= r.close
        and r.low <= r.open
        and r.low <= r.close
        and r.volume >= 0
        and r.quote_volume >= 0
        and r.trade_count >= 0
    )


def find_ohlc_violations(rows: Iterable[KlineRecord]) -> list[dict]:
    bad: list[dict] = []
    for r in rows:
        if not is_ohlc_valid(r):
            bad.append(
                {
                    "symbol": r.symbol,
                    "open_time": r.open_time,
                    "date": r.date,
                    "open": r.open,
                    "high": r.high,
                    "low": r.low,
                    "close": r.close,
                }
            )
    return bad


def find_time_rule_violations(
    rows: Iterable[KlineRecord], *, interval: str
) -> list[dict]:
    bad: list[dict] = []
    ms = interval_milliseconds(interval)
    expected_delta = ms - 1
    for r in rows:
        aligned = r.open_time % ms == 0
        close_ok = r.close_time == r.open_time + expected_delta
        if aligned and close_ok:
            continue
        bad.append(
            {
                "symbol": r.symbol,
                "interval": r.interval,
                "open_time": r.open_time,
                "close_time": r.close_time,
                "date": r.date,
                "open_time_mod": r.open_time % ms,
                "expected_close_time": r.open_time + expected_delta,
            }
        )
    return bad


# --------------------------------------------------------------------------- #
# Parquet writing
# --------------------------------------------------------------------------- #

# Physical column order written to each parquet file. ``symbol`` and ``year``
# are intentionally absent; the Hive path supplies them.
PHYSICAL_COLUMNS: tuple[str, ...] = (
    "interval",
    "open_time",
    "open_time_utc",
    "open_time_taipei",
    "date",
    "month",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "close_time",
    "quote_volume",
    "trade_count",
    "taker_buy_base_volume",
    "taker_buy_quote_volume",
    "source_archive",
    "archive_source",
    "archive_period",
)

# Logical schema (what DuckDB exposes) = physical + Hive partition columns.
LOGICAL_COLUMNS: tuple[str, ...] = ("symbol", "year") + PHYSICAL_COLUMNS


def _parquet_schema():
    require_pyarrow()
    return pa.schema(
        [
            ("interval", pa.string()),
            ("open_time", pa.int64()),
            ("open_time_utc", pa.timestamp("us")),
            ("open_time_taipei", pa.timestamp("us")),
            ("date", pa.string()),
            ("month", pa.int32()),
            ("open", pa.float64()),
            ("high", pa.float64()),
            ("low", pa.float64()),
            ("close", pa.float64()),
            ("volume", pa.float64()),
            ("close_time", pa.int64()),
            ("quote_volume", pa.float64()),
            ("trade_count", pa.int64()),
            ("taker_buy_base_volume", pa.float64()),
            ("taker_buy_quote_volume", pa.float64()),
            ("source_archive", pa.string()),
            ("archive_source", pa.string()),
            ("archive_period", pa.string()),
        ]
    )


def _table_for_year(rows: list[KlineRecord]):
    schema = _parquet_schema()
    columns = {
        "interval": [r.interval for r in rows],
        "open_time": [r.open_time for r in rows],
        "open_time_utc": [r.open_time_utc for r in rows],
        "open_time_taipei": [r.open_time_taipei for r in rows],
        "date": [r.date for r in rows],
        "month": [r.month for r in rows],
        "open": [r.open for r in rows],
        "high": [r.high for r in rows],
        "low": [r.low for r in rows],
        "close": [r.close for r in rows],
        "volume": [r.volume for r in rows],
        "close_time": [r.close_time for r in rows],
        "quote_volume": [r.quote_volume for r in rows],
        "trade_count": [r.trade_count for r in rows],
        "taker_buy_base_volume": [r.taker_buy_base_volume for r in rows],
        "taker_buy_quote_volume": [r.taker_buy_quote_volume for r in rows],
        "source_archive": [r.source_archive for r in rows],
        "archive_source": [r.archive_source for r in rows],
        "archive_period": [r.archive_period for r in rows],
    }
    return pa.table(columns, schema=schema)


@dataclass
class SymbolWriteResult:
    symbol: str
    row_count: int
    file_count: int
    files: list[str]
    date_min: str | None
    date_max: str | None
    year_min: int | None
    year_max: int | None
    duplicate_count: int
    conflict_count: int
    quality_issue_count: int
    duplicate_date_count: int
    rows_per_date_violation_count: int
    max_rows_per_date: int
    ohlc_violation_count: int
    time_rule_violation_count: int
    archive_sources: list[str]
    status: str = "ok"  # "ok" | "failed" | "skipped"
    error: str | None = None
    conflict_samples: list[dict] = field(default_factory=list)
    duplicate_samples: list[dict] = field(default_factory=list)
    quality_samples: list[dict] = field(default_factory=list)
    rows_per_date_samples: list[dict] = field(default_factory=list)
    ohlc_samples: list[dict] = field(default_factory=list)
    time_rule_samples: list[dict] = field(default_factory=list)

    def to_sidecar(self) -> dict:
        return {
            "symbol": self.symbol,
            "row_count": self.row_count,
            "file_count": self.file_count,
            "files": self.files,
            "date_min": self.date_min,
            "date_max": self.date_max,
            "year_min": self.year_min,
            "year_max": self.year_max,
            "duplicate_count": self.duplicate_count,
            "conflict_count": self.conflict_count,
            "quality_issue_count": self.quality_issue_count,
            "duplicate_date_count": self.duplicate_date_count,
            "rows_per_date_violation_count": self.rows_per_date_violation_count,
            "max_rows_per_date": self.max_rows_per_date,
            "ohlc_violation_count": self.ohlc_violation_count,
            "time_rule_violation_count": self.time_rule_violation_count,
            "archive_sources": self.archive_sources,
            "status": self.status,
        }


def symbol_partition_dir(output_root: Path, symbol: str) -> Path:
    return output_root / f"symbol={symbol}"


def symbol_sidecar_path(output_root: Path, symbol: str) -> Path:
    return output_root / "manifests" / "symbols" / f"{symbol}.json"


def write_symbol_parquet(
    result: SymbolResult, output_root: Path, *, interval: str
) -> SymbolWriteResult:
    """Write one symbol's rows to Hive-partitioned parquet (year per file)."""
    require_pyarrow()
    symbol = result.symbol
    part_dir = symbol_partition_dir(output_root, symbol)
    if part_dir.exists():
        shutil.rmtree(part_dir)

    rows = result.rows
    # Detect corrupt bars on the full input (for disclosure counts), then
    # quarantine them OUT of the materialized query layer so the Parquet/DuckDB
    # layer is guaranteed clean. Strict mode still fails on any such bar; the
    # data-quality report discloses every quarantined bar. Coarser intervals
    # (1d/4h) contain none, so this is a no-op for them.
    ohlc_bad = find_ohlc_violations(rows)
    time_bad = find_time_rule_violations(rows, interval=interval)
    drop_keys = {(d["symbol"], d["open_time"]) for d in ohlc_bad}
    drop_keys |= {(d["symbol"], d["open_time"]) for d in time_bad}
    if drop_keys:
        rows = [r for r in rows if (r.symbol, r.open_time) not in drop_keys]
    by_year: dict[int, list[KlineRecord]] = {}
    for rec in rows:
        by_year.setdefault(rec.year, []).append(rec)

    files: list[str] = []
    for year in sorted(by_year):
        year_dir = part_dir / f"year={year}"
        year_dir.mkdir(parents=True, exist_ok=True)
        out = year_dir / "part-000.parquet"
        table = _table_for_year(by_year[year])
        pq.write_table(table, out)
        files.append(str(out))

    dates = [r.date for r in rows]
    years = sorted(by_year)
    return SymbolWriteResult(
        symbol=symbol,
        row_count=len(rows),
        file_count=len(files),
        files=sorted(files),
        date_min=min(dates) if dates else None,
        date_max=max(dates) if dates else None,
        year_min=years[0] if years else None,
        year_max=years[-1] if years else None,
        duplicate_count=result.duplicate_count,
        conflict_count=result.conflict_count,
        quality_issue_count=result.quality_issue_count,
        duplicate_date_count=result.duplicate_date_count,
        rows_per_date_violation_count=result.rows_per_date_violation_count,
        max_rows_per_date=result.max_rows_per_date,
        ohlc_violation_count=len(ohlc_bad),
        time_rule_violation_count=len(time_bad),
        archive_sources=sorted(result.archive_sources),
        conflict_samples=result.conflict_samples,
        duplicate_samples=result.duplicate_samples,
        quality_samples=result.quality_samples,
        rows_per_date_samples=result.rows_per_date_samples,
        ohlc_samples=ohlc_bad[:20],
        time_rule_samples=time_bad[:20],
    )


# --------------------------------------------------------------------------- #
# Manifest / work discovery
# --------------------------------------------------------------------------- #


def load_work_units(
    manifest_path: Path, raw_root: Path
) -> tuple[dict[str, list[ArchiveSpec]], int]:
    """Read the run manifest + per-file manifest into per-symbol archive specs.

    Returns (symbol -> [ArchiveSpec], raw_discovered_symbol_count).
    Only ``downloaded`` + checksum-``passed`` records are materialized.
    """
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    raw_discovered = int(
        manifest.get("discovered_symbol_count")
        or manifest.get("symbol_count")
        or 0
    )
    file_manifest = manifest.get("file_manifest")
    if not file_manifest:
        raise MaterializationCommandError(
            f"run manifest has no file_manifest pointer: {manifest_path}"
        )
    fm_path = Path(file_manifest)
    if not fm_path.exists():
        raise MaterializationCommandError(f"file manifest not found: {fm_path}")

    units: dict[str, list[ArchiveSpec]] = {}
    verified_statuses = {"passed", "skipped_existing_verified"}
    with fm_path.open(encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            rec = json.loads(line)
            if rec.get("download_status") not in ("downloaded", "skipped_existing_verified"):
                continue
            if rec.get("checksum_status") not in verified_statuses:
                continue
            symbol = rec["symbol"]
            local_zip = rec.get("local_zip_path")
            zip_name = rec.get("zip_name") or (
                Path(local_zip).name if local_zip else None
            )
            source = rec.get("archive_package_source")
            period = rec.get("archive_period")
            if not local_zip:
                local_zip = str(
                    raw_root / source / symbol / zip_name
                )
            units.setdefault(symbol, []).append(
                ArchiveSpec(
                    archive_source=source,
                    archive_period=period,
                    local_zip_path=local_zip,
                    zip_name=zip_name,
                )
            )
    return units, raw_discovered


# --------------------------------------------------------------------------- #
# Resume
# --------------------------------------------------------------------------- #


def load_sidecar(output_root: Path, symbol: str) -> dict | None:
    path = symbol_sidecar_path(output_root, symbol)
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def sidecar_is_complete(sidecar: dict) -> bool:
    """A symbol is resumable iff its sidecar is ok and all its files exist."""
    if not sidecar or sidecar.get("status") != "ok":
        return False
    files = sidecar.get("files") or []
    if not files:
        return sidecar.get("row_count", 0) == 0
    return all(Path(f).exists() for f in files)


# --------------------------------------------------------------------------- #
# Per-symbol pipeline
# --------------------------------------------------------------------------- #


def process_symbol(
    symbol: str,
    specs: list[ArchiveSpec],
    *,
    interval: str,
    output_root: Path,
    strict: bool,
    resume: bool,
    overwrite: bool,
) -> SymbolWriteResult:
    sidecar = None if overwrite else load_sidecar(output_root, symbol)
    if resume and sidecar and sidecar_is_complete(sidecar):
        return SymbolWriteResult(
            symbol=symbol,
            row_count=sidecar.get("row_count", 0),
            file_count=sidecar.get("file_count", 0),
            files=sidecar.get("files", []),
            date_min=sidecar.get("date_min"),
            date_max=sidecar.get("date_max"),
            year_min=sidecar.get("year_min"),
            year_max=sidecar.get("year_max"),
            duplicate_count=sidecar.get("duplicate_count", 0),
            conflict_count=sidecar.get("conflict_count", 0),
            quality_issue_count=sidecar.get("quality_issue_count", 0),
            duplicate_date_count=sidecar.get("duplicate_date_count", 0),
            rows_per_date_violation_count=sidecar.get(
                "rows_per_date_violation_count", 0
            ),
            max_rows_per_date=sidecar.get("max_rows_per_date", 0),
            ohlc_violation_count=sidecar.get("ohlc_violation_count", 0),
            time_rule_violation_count=sidecar.get("time_rule_violation_count", 0),
            archive_sources=sidecar.get("archive_sources", []),
            status="skipped",
        )

    try:
        normalized = normalize_symbol(
            symbol, specs, interval=interval, strict=strict
        )
        write_result = write_symbol_parquet(
            normalized, output_root, interval=interval
        )
        if strict and write_result.ohlc_violation_count:
            raise StrictModeError(
                f"strict: {write_result.ohlc_violation_count} OHLC violations "
                f"for {symbol}"
            )
        if strict and write_result.time_rule_violation_count:
            raise StrictModeError(
                f"strict: {write_result.time_rule_violation_count} time-rule "
                f"violations for {symbol}"
            )
        _write_sidecar(output_root, write_result)
        return write_result
    except MaterializationError as exc:
        # Remove any partial output for this symbol.
        part_dir = symbol_partition_dir(output_root, symbol)
        if part_dir.exists():
            shutil.rmtree(part_dir)
        sc = symbol_sidecar_path(output_root, symbol)
        if sc.exists():
            sc.unlink()
        return SymbolWriteResult(
            symbol=symbol,
            row_count=0,
            file_count=0,
            files=[],
            date_min=None,
            date_max=None,
            year_min=None,
            year_max=None,
            duplicate_count=0,
            conflict_count=0,
            quality_issue_count=0,
            duplicate_date_count=0,
            rows_per_date_violation_count=0,
            max_rows_per_date=0,
            ohlc_violation_count=0,
            time_rule_violation_count=0,
            archive_sources=[],
            status="failed",
            error=str(exc),
        )


def _write_sidecar(output_root: Path, result: SymbolWriteResult) -> None:
    path = symbol_sidecar_path(output_root, result.symbol)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(pretty_json(result.to_sidecar()), encoding="utf-8")


# --------------------------------------------------------------------------- #
# Run orchestration
# --------------------------------------------------------------------------- #


@dataclass
class RunConfig:
    interval: str
    raw_root: Path
    manifest: Path
    output_root: Path
    symbols: list[str] | None
    all_symbols: bool
    resume: bool
    overwrite: bool
    workers: int
    strict: bool


def run(config: RunConfig) -> dict:
    require_pyarrow()
    validate_interval(config.interval)

    units, raw_discovered = load_work_units(config.manifest, config.raw_root)
    raw_universe = set(units)

    if config.all_symbols:
        selected = sorted(units)
    else:
        selected = sorted(config.symbols or [])
        missing = [s for s in selected if s not in units]
        if missing:
            raise MaterializationCommandError(
                f"symbols not present in manifest: {' '.join(missing)}"
            )

    if not selected:
        raise MaterializationCommandError(
            "no symbols selected; pass --all or --symbols"
        )

    progress_log(
        f"materialize interval={config.interval} symbols={len(selected)} "
        f"raw_universe={len(raw_universe)} workers={config.workers} "
        f"strict={config.strict} resume={config.resume} overwrite={config.overwrite}"
    )

    config.output_root.mkdir(parents=True, exist_ok=True)

    results: list[SymbolWriteResult] = []
    done = 0

    def _task(sym: str) -> SymbolWriteResult:
        return process_symbol(
            sym,
            units[sym],
            interval=config.interval,
            output_root=config.output_root,
            strict=config.strict,
            resume=config.resume,
            overwrite=config.overwrite,
        )

    if config.workers <= 1:
        iterator = (_task(s) for s in selected)
    else:
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=config.workers)
        iterator = executor.map(_task, selected)

    for res in iterator:
        results.append(res)
        done += 1
        if done % PROGRESS_EVERY == 0 or done == len(selected):
            ok = sum(1 for r in results if r.status == "ok")
            skipped = sum(1 for r in results if r.status == "skipped")
            failed = sum(1 for r in results if r.status == "failed")
            progress_log(
                f"  progress {done}/{len(selected)} "
                f"ok={ok} skipped={skipped} failed={failed}"
            )

    if config.workers > 1:
        executor.shutdown()

    manifest = _build_manifest(config, results, raw_universe, raw_discovered)
    _write_reports(config, results, manifest)
    return manifest


def _build_manifest(
    config: RunConfig,
    results: list[SymbolWriteResult],
    raw_universe: set[str],
    raw_discovered: int,
) -> dict:
    ok_results = [r for r in results if r.status in ("ok", "skipped")]
    failed = [r for r in results if r.status == "failed"]
    materialized_symbols = {r.symbol for r in ok_results if r.row_count > 0}

    row_count = sum(r.row_count for r in ok_results)
    # Count actual parquet files on disk (authoritative for the manifest).
    actual_file_count = len(list(config.output_root.rglob("*.parquet")))

    dates_min = [r.date_min for r in ok_results if r.date_min]
    dates_max = [r.date_max for r in ok_results if r.date_max]
    duplicate_count = sum(r.duplicate_count for r in ok_results)
    conflict_count = sum(r.conflict_count for r in ok_results)

    is_full = (
        config.all_symbols
        and not failed
        and materialized_symbols == raw_universe
        and len(materialized_symbols) > 0
    )
    output_scope = FULL_OUTPUT if is_full else SAMPLE_OUTPUT

    return {
        "materialized_dataset_id": materialized_dataset_id(config.interval),
        "interval": config.interval,
        "input_manifest": str(config.manifest),
        "output_root": str(config.output_root),
        "raw_discovered_symbol_count": raw_discovered,
        "symbol_count": len(materialized_symbols),
        "row_count": row_count,
        "file_count": actual_file_count,
        "date_min": min(dates_min) if dates_min else None,
        "date_max": max(dates_max) if dates_max else None,
        "duplicate_count": duplicate_count,
        "conflict_count": conflict_count,
        "failed_symbol_count": len(failed),
        "failed_symbols": sorted(r.symbol for r in failed),
        "generated_csv_file_count": 0,
        "query_engine": QUERY_ENGINE,
        "output_format": OUTPUT_FORMAT,
        "output_scope": output_scope,
        "git_commit": git_commit(),
        "code_version": CODE_VERSION,
        "created_at_utc": utc_now(),
    }


def _write_reports(
    config: RunConfig, results: list[SymbolWriteResult], manifest: dict
) -> None:
    out = config.output_root
    manifest_path = out / "manifests" / "materialization_manifest.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(pretty_json(manifest), encoding="utf-8")

    reports_dir = out / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)

    coverage = {
        "materialized_dataset_id": materialized_dataset_id(config.interval),
        "interval": config.interval,
        "output_scope": manifest["output_scope"],
        "raw_discovered_symbol_count": manifest["raw_discovered_symbol_count"],
        "symbol_count": manifest["symbol_count"],
        "row_count": manifest["row_count"],
        "file_count": manifest["file_count"],
        "date_min": manifest["date_min"],
        "date_max": manifest["date_max"],
        "failed_symbol_count": manifest["failed_symbol_count"],
        "failed_symbols": manifest["failed_symbols"],
        "per_symbol": [
            {
                "symbol": r.symbol,
                "row_count": r.row_count,
                "file_count": r.file_count,
                "date_min": r.date_min,
                "date_max": r.date_max,
                "status": r.status,
                "archive_sources": r.archive_sources,
                "max_rows_per_date": r.max_rows_per_date,
            }
            for r in sorted(results, key=lambda x: x.symbol)
        ],
        "generated_at_utc": manifest["created_at_utc"],
    }
    (reports_dir / "coverage_report.json").write_text(
        pretty_json(coverage), encoding="utf-8"
    )

    duplicates = {
        "duplicate_count": manifest["duplicate_count"],
        "description": (
            "Bars seen in more than one archive (or twice in one archive) with "
            "identical values; deduplicated by (symbol, interval, open_time)."
        ),
        "samples": _collect(results, "duplicate_samples"),
        "per_symbol": _per_symbol_counts(results, "duplicate_count"),
    }
    (reports_dir / "duplicate_report.json").write_text(
        pretty_json(duplicates), encoding="utf-8"
    )

    conflicts = {
        "conflict_count": manifest["conflict_count"],
        "policy": "daily wins over monthly on value disagreement",
        "samples": _collect(results, "conflict_samples"),
        "per_symbol": _per_symbol_counts(results, "conflict_count"),
    }
    (reports_dir / "conflict_report.json").write_text(
        pretty_json(conflicts), encoding="utf-8"
    )

    quality_issue_count = sum(
        r.quality_issue_count for r in results if r.status in ("ok", "skipped")
    )
    duplicate_date_count = sum(
        r.duplicate_date_count for r in results if r.status in ("ok", "skipped")
    )
    rows_per_date_violation_count = sum(
        r.rows_per_date_violation_count
        for r in results
        if r.status in ("ok", "skipped")
    )
    ohlc_violation_count = sum(
        r.ohlc_violation_count for r in results if r.status in ("ok", "skipped")
    )
    time_rule_violation_count = sum(
        r.time_rule_violation_count for r in results if r.status in ("ok", "skipped")
    )
    data_quality = {
        "quality_issue_count": quality_issue_count,
        "duplicate_date_count": duplicate_date_count,
        "rows_per_date_limit": rows_per_symbol_date_limit(config.interval),
        "rows_per_date_violation_count": rows_per_date_violation_count,
        "ohlc_violation_count": ohlc_violation_count,
        "time_rule_violation_count": time_rule_violation_count,
        "quarantine_policy": (
            "OHLC-invalid and time-rule-invalid bars are excluded from the "
            "materialized Parquet query layer and disclosed here; the counts "
            "above are the number of bars quarantined. --strict fails the symbol "
            "instead of quarantining."
        ),
        "quarantined_bar_count": ohlc_violation_count + time_rule_violation_count,
        "failed_symbol_count": manifest["failed_symbol_count"],
        "failed_symbols": [
            {"symbol": r.symbol, "error": r.error}
            for r in results
            if r.status == "failed"
        ],
        "rules": [
            "required identity/time/OHLCV fields are non-null",
            "high >= low, high >= open, high >= close",
            "low <= open, low <= close",
            "volume >= 0, quote_volume >= 0, trade_count >= 0",
            "date derived from open_time_taipei calendar date",
            f"open_time aligned to interval={config.interval}",
            "close_time = open_time + interval_ms - 1",
            (
                "rows per (symbol, date) <= "
                f"{rows_per_symbol_date_limit(config.interval)}"
            ),
        ],
        "same_source_inconsistency_samples": _collect(results, "quality_samples"),
        "rows_per_date_samples": _collect(results, "rows_per_date_samples"),
        "ohlc_violation_samples": _collect(results, "ohlc_samples"),
        "time_rule_violation_samples": _collect(results, "time_rule_samples"),
    }
    (reports_dir / "data_quality_report.json").write_text(
        pretty_json(data_quality), encoding="utf-8"
    )


def _collect(results: list[SymbolWriteResult], attr: str, cap: int = 200) -> list:
    out: list = []
    for r in results:
        for item in getattr(r, attr, []) or []:
            if len(out) >= cap:
                return out
            out.append(item)
    return out


def _per_symbol_counts(results: list[SymbolWriteResult], attr: str) -> list[dict]:
    return [
        {"symbol": r.symbol, attr: getattr(r, attr)}
        for r in sorted(results, key=lambda x: x.symbol)
        if getattr(r, attr)
    ]


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m datahub.materialization.binance_um_klines_parquet",
        description=(
            "Materialize Binance USD-M Kline raw zip archives into a "
            "DuckDB-queryable Hive-partitioned Parquet dataset."
        ),
    )
    parser.add_argument("--interval", default="1d", choices=ALLOWED_INTERVALS)
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--all",
        dest="all_symbols",
        action="store_true",
        help="materialize every verified symbol in the manifest",
    )
    group.add_argument(
        "--symbols",
        nargs="+",
        help="materialize only these symbols (SAMPLE_OUTPUT)",
    )
    parser.add_argument(
        "--raw-root",
        help="raw archive root (default: local_data/.../interval=<INTERVAL>/raw)",
    )
    parser.add_argument(
        "--manifest",
        help="raw run manifest (default: local_data/.../interval=<INTERVAL>/manifests/manifest.json)",
    )
    parser.add_argument(
        "--output-root",
        help="parquet output root (default: local_data/.../interval=<INTERVAL>/parquet)",
    )
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--strict", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        require_pyarrow()
    except DependencyError as exc:
        print(f"dependency error: {exc}", file=sys.stderr)
        return 3

    if not args.all_symbols and not args.symbols:
        print(
            "argument error: pass --all or --symbols",
            file=sys.stderr,
        )
        return 2

    config = RunConfig(
        interval=args.interval,
        raw_root=Path(args.raw_root or default_raw_root(args.interval)),
        manifest=Path(args.manifest or default_manifest(args.interval)),
        output_root=Path(args.output_root or default_output_root(args.interval)),
        symbols=args.symbols,
        all_symbols=args.all_symbols,
        resume=args.resume,
        overwrite=args.overwrite,
        workers=max(1, args.workers),
        strict=args.strict,
    )

    try:
        manifest = run(config)
    except MaterializationCommandError as exc:
        print(f"command error: {exc}", file=sys.stderr)
        return 2
    except DependencyError as exc:
        print(f"dependency error: {exc}", file=sys.stderr)
        return 3
    except MaterializationError as exc:
        print(f"materialization error: {exc}", file=sys.stderr)
        return 1

    progress_log(
        "done: scope={output_scope} symbols={symbol_count} rows={row_count} "
        "files={file_count} duplicates={duplicate_count} "
        "conflicts={conflict_count} failed={failed_symbol_count}".format(**manifest)
    )
    print(pretty_json(manifest), end="")

    if manifest["failed_symbol_count"] > 0:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
