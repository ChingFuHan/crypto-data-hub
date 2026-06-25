"""Phase 1 primitives for Binance UM Kline live update.

This module intentionally stops at the Phase 1 boundary from ``LIVE_UPDATE.md``:
data structures, normalized ``KlineRecord`` values, supported interval helpers,
and deterministic path construction/parsing. Runtime behavior such as current
dataset initialization, REST fallback, WebSocket handling, webhook serving, and
Parquet merging belongs to later phases.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timedelta
import json
from pathlib import Path
from typing import Any


DATASET_ID = "market.binance.um.klines.live_update"
SCHEMA_VERSION = 1
DATASET_VERSION = "current-v1"

SUPPORTED_INTERVALS: tuple[str, ...] = ("1m", "3m", "5m", "15m", "1h", "4h", "1d")
CLI_INTERVALS: tuple[str, ...] = ("all",) + SUPPORTED_INTERVALS
INTERVAL_MILLISECONDS: dict[str, int] = {
    "1m": 60_000,
    "3m": 180_000,
    "5m": 300_000,
    "15m": 900_000,
    "1h": 3_600_000,
    "4h": 14_400_000,
    "1d": 86_400_000,
}

LIVE_RUNTIME_DATASET = "binance_um_klines"
DEFAULT_SEED_DATASET_ROOT = Path("local_data/binance_um_klines")
DEFAULT_CURRENT_DATASET_ROOT = Path("local_data/binance_um_klines_current")
DEFAULT_LIVE_ROOT = Path("local_data/live_update") / LIVE_RUNTIME_DATASET

TAIPEI_OFFSET = timedelta(hours=8)
EPOCH = datetime(1970, 1, 1)


class LiveUpdateCommandError(ValueError):
    """Raised for invalid live-update CLI/path/interval input."""


def validate_interval(interval: str) -> str:
    """Validate a Binance API interval.

    ``all`` is a CLI expansion token, not a Binance interval, and is rejected
    here so downstream REST/WebSocket code cannot pass it to Binance by mistake.
    """
    if interval not in SUPPORTED_INTERVALS:
        raise LiveUpdateCommandError(
            f"unsupported interval: {interval!r}; "
            f"supported intervals: {' '.join(SUPPORTED_INTERVALS)}"
        )
    return interval


def expand_intervals(interval: str) -> tuple[str, ...]:
    """Expand a CLI interval value into concrete Binance intervals."""
    if interval == "all":
        return SUPPORTED_INTERVALS
    return (validate_interval(interval),)


def interval_milliseconds(interval: str) -> int:
    """Return the interval duration in milliseconds."""
    return INTERVAL_MILLISECONDS[validate_interval(interval)]


def datetime_fields(open_time: int) -> dict[str, Any]:
    """Derive UTC/Taipei timestamp fields from a Binance millisecond open time."""
    dt_utc = EPOCH + timedelta(milliseconds=int(open_time))
    dt_taipei = dt_utc + TAIPEI_OFFSET
    return {
        "open_time_utc": dt_utc.isoformat(),
        "open_time_taipei": dt_taipei.isoformat(),
        "date": dt_taipei.strftime("%Y-%m-%d"),
        "year": dt_taipei.year,
        "month": dt_taipei.month,
    }


@dataclass(frozen=True)
class PartitionKey:
    """Hive partition identity for one live-update write target."""

    interval: str
    symbol: str
    year: int
    month: int

    def as_tuple(self) -> tuple[str, str, int, int]:
        return (self.interval, self.symbol, self.year, self.month)


@dataclass(frozen=True)
class RecordKey:
    """Primary key for one Kline row."""

    symbol: str
    interval: str
    open_time: int

    def as_tuple(self) -> tuple[str, str, int]:
        return (self.symbol, self.interval, self.open_time)


@dataclass(frozen=True)
class KlineRecord:
    """One normalized Kline bar for live-update buffers and current dataset rows."""

    symbol: str
    interval: str
    open_time: int
    open_time_utc: str
    open_time_taipei: str
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
    schema_version: int = SCHEMA_VERSION
    dataset_version: str = DATASET_VERSION

    @classmethod
    def build(
        cls,
        *,
        symbol: str,
        interval: str,
        open_time: int,
        open: float,
        high: float,
        low: float,
        close: float,
        volume: float,
        close_time: int,
        quote_volume: float,
        trade_count: int,
        taker_buy_base_volume: float,
        taker_buy_quote_volume: float,
        source_archive: str,
        archive_source: str,
        archive_period: str,
        schema_version: int = SCHEMA_VERSION,
        dataset_version: str = DATASET_VERSION,
    ) -> "KlineRecord":
        validate_interval(interval)
        fields = datetime_fields(int(open_time))
        return cls(
            symbol=symbol.upper(),
            interval=interval,
            open_time=int(open_time),
            open_time_utc=fields["open_time_utc"],
            open_time_taipei=fields["open_time_taipei"],
            date=fields["date"],
            year=fields["year"],
            month=fields["month"],
            open=float(open),
            high=float(high),
            low=float(low),
            close=float(close),
            volume=float(volume),
            close_time=int(close_time),
            quote_volume=float(quote_volume),
            trade_count=int(trade_count),
            taker_buy_base_volume=float(taker_buy_base_volume),
            taker_buy_quote_volume=float(taker_buy_quote_volume),
            source_archive=source_archive,
            archive_source=archive_source,
            archive_period=archive_period,
            schema_version=int(schema_version),
            dataset_version=dataset_version,
        )

    def record_key(self) -> RecordKey:
        return RecordKey(self.symbol, self.interval, self.open_time)

    def partition_key(self) -> PartitionKey:
        return PartitionKey(self.interval, self.symbol, self.year, self.month)

    def logical_dict(self) -> dict[str, Any]:
        """Return the complete record used by JSON logs, latest, and state."""
        return {
            "symbol": self.symbol,
            "interval": self.interval,
            "open_time": self.open_time,
            "open_time_utc": self.open_time_utc,
            "open_time_taipei": self.open_time_taipei,
            "date": self.date,
            "year": self.year,
            "month": self.month,
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "volume": self.volume,
            "close_time": self.close_time,
            "quote_volume": self.quote_volume,
            "trade_count": self.trade_count,
            "taker_buy_base_volume": self.taker_buy_base_volume,
            "taker_buy_quote_volume": self.taker_buy_quote_volume,
            "source_archive": self.source_archive,
            "archive_source": self.archive_source,
            "archive_period": self.archive_period,
            "schema_version": self.schema_version,
            "dataset_version": self.dataset_version,
        }

    def physical_dict(self) -> dict[str, Any]:
        """Return physical Parquet columns.

        The current dataset uses Hive paths for ``symbol`` and ``year``, matching
        the existing materialized kline layout, so those partition columns are
        omitted from the physical row.
        """
        row = self.logical_dict()
        row.pop("symbol")
        row.pop("year")
        return row


@dataclass(frozen=True)
class LiveUpdatePaths:
    """Deterministic path resolver for the live-update data layout."""

    repo_root: Path = Path(".")
    seed_dataset_root: Path = DEFAULT_SEED_DATASET_ROOT
    current_dataset_root: Path = DEFAULT_CURRENT_DATASET_ROOT
    live_root: Path = DEFAULT_LIVE_ROOT

    def _rooted(self, path: Path) -> Path:
        if path.is_absolute():
            return path
        return self.repo_root / path

    def seed_parquet_root(self, interval: str) -> Path:
        validate_interval(interval)
        return self._rooted(self.seed_dataset_root) / f"interval={interval}" / "parquet"

    def current_interval_root(self, interval: str) -> Path:
        validate_interval(interval)
        return self._rooted(self.current_dataset_root) / f"interval={interval}"

    def current_parquet_root(self, interval: str) -> Path:
        return self.current_interval_root(interval) / "parquet"

    def current_initialized_marker(self, interval: str) -> Path:
        return self.current_interval_root(interval) / "_current_dataset_initialized.json"

    def runtime_interval_root(self, interval: str) -> Path:
        validate_interval(interval)
        return self._rooted(self.live_root) / f"interval={interval}"

    def buffer_jsonl(self, interval: str, buffer_name: str, date: str) -> Path:
        if buffer_name not in {"event_buffer", "websocket_buffer", "webhook_buffer"}:
            raise LiveUpdateCommandError(f"unsupported event buffer: {buffer_name!r}")
        return self.runtime_interval_root(interval) / buffer_name / f"date={date}" / "events.jsonl"

    def closed_buffer_jsonl(self, interval: str, date: str) -> Path:
        return self.runtime_interval_root(interval) / "closed_buffer" / f"date={date}" / "closed.jsonl"

    def latest_json(self, interval: str, symbol: str) -> Path:
        return self.runtime_interval_root(interval) / "latest" / f"symbol={symbol.upper()}.json"

    def rejects_jsonl(self, interval: str, date: str) -> Path:
        return self.runtime_interval_root(interval) / "rejects" / f"date={date}" / "rejects.jsonl"

    def state_json(self, interval: str) -> Path:
        return self.runtime_interval_root(interval) / "state" / "live_update_state.json"

    def runtime_log(self, interval: str, date: str, name: str = "runtime.log") -> Path:
        if name not in {"runtime.log", "warnings.log"}:
            raise LiveUpdateCommandError(f"unsupported log file: {name!r}")
        return self.runtime_interval_root(interval) / "logs" / f"date={date}" / name

    def current_partition_file(self, key: PartitionKey) -> Path:
        validate_interval(key.interval)
        return (
            self.current_parquet_root(key.interval)
            / f"symbol={key.symbol.upper()}"
            / f"year={key.year}"
            / f"month={key.month:02d}"
            / "part-000.parquet"
        )

    def describe_interval(self, interval: str) -> dict[str, str]:
        """Return the Phase 1 path layout for one concrete interval."""
        validate_interval(interval)
        sample_date = "YYYY-MM-DD"
        return {
            "seed_parquet_root": str(self.seed_parquet_root(interval)),
            "current_parquet_root": str(self.current_parquet_root(interval)),
            "current_initialized_marker": str(self.current_initialized_marker(interval)),
            "runtime_interval_root": str(self.runtime_interval_root(interval)),
            "event_buffer": str(self.buffer_jsonl(interval, "event_buffer", sample_date)),
            "websocket_buffer": str(self.buffer_jsonl(interval, "websocket_buffer", sample_date)),
            "webhook_buffer": str(self.buffer_jsonl(interval, "webhook_buffer", sample_date)),
            "closed_buffer": str(self.closed_buffer_jsonl(interval, sample_date)),
            "latest": str(self.latest_json(interval, "BTCUSDT")),
            "rejects": str(self.rejects_jsonl(interval, sample_date)),
            "state": str(self.state_json(interval)),
            "runtime_log": str(self.runtime_log(interval, sample_date)),
        }


def parse_interval_segment(segment: str) -> str:
    """Parse a Hive-style ``interval=<INTERVAL>`` path segment."""
    prefix = "interval="
    if not segment.startswith(prefix):
        raise LiveUpdateCommandError(f"expected interval=<INTERVAL>, got {segment!r}")
    return validate_interval(segment[len(prefix):])


def parse_symbol_segment(segment: str) -> str:
    """Parse a Hive-style ``symbol=<SYMBOL>`` path segment."""
    prefix = "symbol="
    if not segment.startswith(prefix) or not segment[len(prefix):]:
        raise LiveUpdateCommandError(f"expected symbol=<SYMBOL>, got {segment!r}")
    return segment[len(prefix):].upper()


def parse_date_segment(segment: str) -> str:
    """Parse a Hive-style ``date=YYYY-MM-DD`` path segment."""
    prefix = "date="
    if not segment.startswith(prefix):
        raise LiveUpdateCommandError(f"expected date=YYYY-MM-DD, got {segment!r}")
    value = segment[len(prefix):]
    try:
        datetime.strptime(value, "%Y-%m-%d")
    except ValueError as exc:
        raise LiveUpdateCommandError(f"invalid date segment: {segment!r}") from exc
    return value


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python scripts/live_update.py",
        description="Phase 1 live-update layout and record primitives.",
    )
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--interval", default="all", choices=CLI_INTERVALS)
    parser.add_argument("--current-dataset-root", default=str(DEFAULT_CURRENT_DATASET_ROOT))
    parser.add_argument("--seed-dataset-root", default=str(DEFAULT_SEED_DATASET_ROOT))
    parser.add_argument(
        "--describe-layout",
        action="store_true",
        help="print Phase 1 path layout JSON and exit",
    )
    return parser


def run(args: argparse.Namespace) -> int:
    intervals = expand_intervals(args.interval)
    paths = LiveUpdatePaths(
        repo_root=Path(args.repo_root),
        seed_dataset_root=Path(args.seed_dataset_root),
        current_dataset_root=Path(args.current_dataset_root),
    )
    if args.describe_layout:
        payload = {
            "schema_version": SCHEMA_VERSION,
            "dataset_version": DATASET_VERSION,
            "requested_interval": args.interval,
            "active_intervals": list(intervals),
            "intervals_are_api_safe": "all" not in intervals,
            "paths": {
                interval: paths.describe_interval(interval) for interval in intervals
            },
        }
        print(json.dumps(payload, indent=2, sort_keys=True))
        return 0
    print(
        "Phase 1 scaffold ready: "
        f"active_intervals={','.join(intervals)}; "
        "runtime phases are not implemented yet."
    )
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return run(args)
    except LiveUpdateCommandError as exc:
        parser.error(str(exc))
        return 2


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
