"""Phase 1-8 primitives for Binance UM Kline live update.

This module intentionally stops at the Phase 8 boundary from ``LIVE_UPDATE.md``:
data structures, normalized ``KlineRecord`` values, supported interval helpers,
deterministic path construction/parsing, current dataset initialization, and
atomic per-partition Parquet merge, state management, and startup-backfill gap
planning, REST fallback, testable WebSocket manager primitives, webhook
server primitives, CLI modes, and continuity checks / acceptance validation.
Full long-running production orchestration belongs to later hardening.
"""

from __future__ import annotations

import argparse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import os
import shutil
import socket
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
from typing import Any

try:  # pragma: no cover - exercised through require_pyarrow
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError as exc:  # pragma: no cover
    pa = None
    pq = None
    _PYARROW_IMPORT_ERROR = exc
else:
    _PYARROW_IMPORT_ERROR = None


DATASET_ID = "market.binance.um.klines.live_update"
SCHEMA_VERSION = 1
DATASET_VERSION = "current-v1"
BINANCE_REST_BASE_URL = "https://fapi.binance.com"
REST_KLINES_PATH = "/fapi/v1/klines"
REST_SOURCE_ARCHIVE = "live_rest:/fapi/v1/klines"
REST_ARCHIVE_SOURCE = "live_rest"
BINANCE_WS_BASE_URL = "wss://fstream.binance.com"
WS_SOURCE_ARCHIVE = "live_websocket:kline"
WS_ARCHIVE_SOURCE = "live_websocket"
WEBHOOK_SOURCE_ARCHIVE = "live_webhook:kline"
WEBHOOK_ARCHIVE_SOURCE = "live_webhook"
DEFAULT_WEBHOOK_HOST = "127.0.0.1"
DEFAULT_WEBHOOK_PORT = 8787
DEFAULT_WEBHOOK_MAX_BODY_BYTES = 1_048_576

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

CURRENT_PHYSICAL_COLUMNS: tuple[str, ...] = (
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
    "schema_version",
    "dataset_version",
)


class LiveUpdateCommandError(ValueError):
    """Raised for invalid live-update CLI/path/interval input."""


class LiveUpdateDependencyError(RuntimeError):
    """Raised when an optional runtime dependency is missing."""


class RestStopRequests(RuntimeError):
    """Raised when REST requests must stop, e.g. HTTP 418."""


def require_pyarrow() -> None:
    if pa is None:
        raise LiveUpdateDependencyError(
            "pyarrow is required for parquet writing. Install it with: "
            ".venv/bin/python -m pip install pyarrow"
        ) from _PYARROW_IMPORT_ERROR


def utc_now() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def pretty_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n"


def write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(pretty_json(payload), encoding="utf-8")
    os.replace(tmp, path)


def jsonl_line(record: dict[str, Any]) -> str:
    return json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n"


def append_jsonl(path: Path, record: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        fh.write(jsonl_line(record))


def write_warning_log(
    paths: "LiveUpdatePaths",
    interval: str,
    message: str,
    *,
    date: str | None = None,
    level: str = "warning",
) -> None:
    log_date = date or utc_now()[:10]
    record = {
        "logged_at_utc": utc_now(),
        "level": level,
        "message": message,
    }
    append_jsonl(paths.runtime_log(interval, log_date, "warnings.log"), record)


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

    def as_dict(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "interval": self.interval,
            "open_time": self.open_time,
        }


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


@dataclass(frozen=True)
class CurrentDatasetInitResult:
    """Result of ensuring one interval's current dataset exists."""

    interval: str
    status: str  # "initialized" | "already_initialized" | "bootstrap_required"
    seed_root: Path
    current_root: Path
    marker_path: Path
    message: str | None = None

    def to_dict(self) -> dict[str, Any]:
        payload = {
            "interval": self.interval,
            "status": self.status,
            "seed_root": str(self.seed_root),
            "current_root": str(self.current_root),
            "marker_path": str(self.marker_path),
        }
        if self.message:
            payload["message"] = self.message
        return payload


@dataclass(frozen=True)
class ParquetMergeResult:
    """Metadata returned after an atomic current-dataset partition merge."""

    partition_key: PartitionKey
    target_path: Path
    input_row_count: int
    existing_row_count: int
    output_row_count: int
    duplicate_replaced_count: int
    min_open_time: int | None
    max_open_time: int | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "partition_key": {
                "interval": self.partition_key.interval,
                "symbol": self.partition_key.symbol,
                "year": self.partition_key.year,
                "month": self.partition_key.month,
            },
            "target_path": str(self.target_path),
            "input_row_count": self.input_row_count,
            "existing_row_count": self.existing_row_count,
            "output_row_count": self.output_row_count,
            "duplicate_replaced_count": self.duplicate_replaced_count,
            "min_open_time": self.min_open_time,
            "max_open_time": self.max_open_time,
        }


@dataclass
class SymbolState:
    """Per-symbol state used for resume and startup gap calculation."""

    last_buffered_open_time: int | None = None
    last_flushed_open_time: int | None = None
    last_closed_open_time: int | None = None
    last_closed_at_utc: str | None = None
    last_ws_message_at_utc: str | None = None
    merged_bar_count: int = 0
    last_target_path: str | None = None

    @classmethod
    def from_dict(cls, value: dict[str, Any] | None) -> "SymbolState":
        if not value:
            return cls()
        return cls(
            last_buffered_open_time=_optional_int(value.get("last_buffered_open_time")),
            last_flushed_open_time=_optional_int(value.get("last_flushed_open_time")),
            last_closed_open_time=_optional_int(value.get("last_closed_open_time")),
            last_closed_at_utc=value.get("last_closed_at_utc"),
            last_ws_message_at_utc=value.get("last_ws_message_at_utc"),
            merged_bar_count=int(value.get("merged_bar_count") or 0),
            last_target_path=value.get("last_target_path"),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "last_buffered_open_time": self.last_buffered_open_time,
            "last_flushed_open_time": self.last_flushed_open_time,
            "last_closed_open_time": self.last_closed_open_time,
            "last_closed_at_utc": self.last_closed_at_utc,
            "last_ws_message_at_utc": self.last_ws_message_at_utc,
            "merged_bar_count": self.merged_bar_count,
            "last_target_path": self.last_target_path,
        }


@dataclass
class LiveUpdateState:
    """Interval-level live-update state file."""

    interval: str
    current_dataset_root: str
    created_at_utc: str
    updated_at_utc: str
    websocket: dict[str, Any]
    symbols: dict[str, SymbolState]
    dataset: str = DATASET_ID
    schema_version: int = SCHEMA_VERSION
    dataset_version: str = DATASET_VERSION

    @classmethod
    def create(
        cls,
        interval: str,
        paths: LiveUpdatePaths | None = None,
        *,
        now_utc: str | None = None,
    ) -> "LiveUpdateState":
        validate_interval(interval)
        resolver = paths or LiveUpdatePaths()
        timestamp = now_utc or utc_now()
        return cls(
            interval=interval,
            current_dataset_root=str(resolver.current_parquet_root(interval)),
            created_at_utc=timestamp,
            updated_at_utc=timestamp,
            websocket={
                "enabled": True,
                "last_connected_at_utc": None,
                "last_message_at_utc": None,
                "last_reconnect_at_utc": None,
                "reconnect_count": 0,
            },
            symbols={},
        )

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "LiveUpdateState":
        interval = validate_interval(str(value["interval"]))
        symbols = {
            symbol.upper(): SymbolState.from_dict(symbol_state)
            for symbol, symbol_state in (value.get("symbols") or {}).items()
        }
        return cls(
            dataset=str(value.get("dataset") or DATASET_ID),
            schema_version=int(value.get("schema_version") or SCHEMA_VERSION),
            dataset_version=str(value.get("dataset_version") or DATASET_VERSION),
            interval=interval,
            created_at_utc=str(value["created_at_utc"]),
            updated_at_utc=str(value["updated_at_utc"]),
            current_dataset_root=str(value["current_dataset_root"]),
            websocket=dict(value.get("websocket") or {}),
            symbols=symbols,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "dataset": self.dataset,
            "schema_version": self.schema_version,
            "dataset_version": self.dataset_version,
            "interval": self.interval,
            "created_at_utc": self.created_at_utc,
            "updated_at_utc": self.updated_at_utc,
            "current_dataset_root": self.current_dataset_root,
            "websocket": self.websocket,
            "symbols": {
                symbol: self.symbols[symbol].to_dict()
                for symbol in sorted(self.symbols)
            },
        }

    def symbol_state(self, symbol: str) -> SymbolState:
        normalized = symbol.upper()
        if normalized not in self.symbols:
            self.symbols[normalized] = SymbolState()
        return self.symbols[normalized]


@dataclass(frozen=True)
class MissingBarsPlan:
    """Calculated missing-bar range for a symbol + interval."""

    symbol: str
    interval: str
    last_closed_open_time: int | None
    latest_closed_open_time: int
    missing_bars: int
    start_open_time: int | None
    end_open_time: int | None
    source: str
    status: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "interval": self.interval,
            "last_closed_open_time": self.last_closed_open_time,
            "latest_closed_open_time": self.latest_closed_open_time,
            "missing_bars": self.missing_bars,
            "start_open_time": self.start_open_time,
            "end_open_time": self.end_open_time,
            "source": self.source,
            "status": self.status,
        }


@dataclass(frozen=True)
class StartupBackfillPlan:
    """Startup backfill orchestration skeleton output.

    Phase 3 only calculates what would need backfilling. It intentionally does
    not call REST, write buffers, enqueue partitions, or update flush state.
    """

    interval: str
    init_result: CurrentDatasetInitResult
    plans: list[MissingBarsPlan]

    def to_dict(self) -> dict[str, Any]:
        return {
            "interval": self.interval,
            "init_result": self.init_result.to_dict(),
            "plans": [plan.to_dict() for plan in self.plans],
        }


@dataclass(frozen=True)
class WebSocketKlineEvent:
    """Normalized WebSocket Kline event."""

    record: KlineRecord
    is_closed: bool
    raw_payload: dict[str, Any]
    stream: str | None = None


@dataclass(frozen=True)
class WebSocketConnectionSpec:
    """One combined-stream WebSocket connection specification."""

    streams: tuple[str, ...]
    url: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "stream_count": len(self.streams),
            "streams": list(self.streams),
            "url": self.url,
        }


@dataclass(frozen=True)
class WebSocketProcessResult:
    """Outcome of processing one WebSocket kline payload."""

    status: str
    symbol: str
    interval: str
    open_time: int
    is_closed: bool
    merge_result: ParquetMergeResult | None = None
    validation_errors: list[str] | None = None

    def to_dict(self) -> dict[str, Any]:
        payload = {
            "status": self.status,
            "symbol": self.symbol,
            "interval": self.interval,
            "open_time": self.open_time,
            "is_closed": self.is_closed,
        }
        if self.validation_errors:
            payload["validation_errors"] = self.validation_errors
        if self.merge_result:
            payload["merge_result"] = self.merge_result.to_dict()
        return payload


@dataclass(frozen=True)
class StaleStream:
    """One stale symbol + interval detected from WebSocket state."""

    symbol: str
    interval: str
    last_ws_message_at_utc: str | None
    stale_threshold_ms: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "interval": self.interval,
            "last_ws_message_at_utc": self.last_ws_message_at_utc,
            "stale_threshold_ms": self.stale_threshold_ms,
        }


@dataclass(frozen=True)
class WebSocketReconnectResult:
    """Reconnect/rotate skeleton result."""

    status: str
    reconnect_count: int
    rest_results: list[RestBackfillResult]

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "reconnect_count": self.reconnect_count,
            "rest_results": [result.to_dict() for result in self.rest_results],
        }


@dataclass(frozen=True)
class WebhookKlineEvent:
    """Normalized webhook Kline event."""

    record: KlineRecord
    is_closed: bool
    raw_payload: dict[str, Any]
    payload_format: str


@dataclass(frozen=True)
class WebhookProcessResult:
    """Outcome of processing one webhook payload."""

    status: str
    symbol: str
    interval: str
    open_time: int
    is_closed: bool
    validation_errors: list[str]
    merge_result: ParquetMergeResult | None = None

    def to_dict(self) -> dict[str, Any]:
        payload = {
            "status": self.status,
            "symbol": self.symbol,
            "interval": self.interval,
            "open_time": self.open_time,
            "is_closed": self.is_closed,
        }
        if self.validation_errors:
            payload["errors"] = self.validation_errors
        if self.merge_result:
            payload["merge_result"] = self.merge_result.to_dict()
        return payload


@dataclass(frozen=True)
class WebhookServerConfig:
    """Configuration for the Phase 6 webhook HTTP server primitive."""

    requested_interval: str
    active_intervals: tuple[str, ...]
    paths: LiveUpdatePaths
    host: str = DEFAULT_WEBHOOK_HOST
    port: int = DEFAULT_WEBHOOK_PORT
    max_body_bytes: int = DEFAULT_WEBHOOK_MAX_BODY_BYTES
    close_lag_ms: int = 2000

    def to_dict(self) -> dict[str, Any]:
        return {
            "host": self.host,
            "port": self.port,
            "requested_interval": self.requested_interval,
            "active_intervals": list(self.active_intervals),
            "live_root": str(self.paths._rooted(self.paths.live_root)),
            "current_dataset_root": str(self.paths._rooted(self.paths.current_dataset_root)),
            "max_body_bytes": self.max_body_bytes,
            "close_lag_ms": self.close_lag_ms,
        }


@dataclass(frozen=True)
class RestKlineEvent:
    """Normalized REST Kline row plus closed/open status."""

    record: KlineRecord
    is_closed: bool
    raw_row: list[Any]


@dataclass(frozen=True)
class RestFetchResult:
    """Result of one REST /fapi/v1/klines request."""

    status: str
    rows: list[list[Any]]
    url: str
    http_status: int | None = None
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        payload = {
            "status": self.status,
            "row_count": len(self.rows),
            "url": self.url,
            "http_status": self.http_status,
        }
        if self.error:
            payload["error"] = self.error
        return payload


@dataclass(frozen=True)
class RestBackfillResult:
    """Outcome for one symbol interval REST backfill/gap-repair run."""

    symbol: str
    interval: str
    status: str
    requested_start_open_time: int | None
    requested_end_open_time: int | None
    fetched_row_count: int
    event_row_count: int
    closed_row_count: int
    merged_row_count: int
    latest_open_row_count: int
    merge_results: list[ParquetMergeResult]
    warnings: list[str]

    def to_dict(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "interval": self.interval,
            "status": self.status,
            "requested_start_open_time": self.requested_start_open_time,
            "requested_end_open_time": self.requested_end_open_time,
            "fetched_row_count": self.fetched_row_count,
            "event_row_count": self.event_row_count,
            "closed_row_count": self.closed_row_count,
            "merged_row_count": self.merged_row_count,
            "latest_open_row_count": self.latest_open_row_count,
            "merge_results": [result.to_dict() for result in self.merge_results],
            "warnings": self.warnings,
        }


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None
    return int(value)


def current_dataset_has_parquet(current_root: Path) -> bool:
    return current_root.exists() and any(current_root.rglob("*.parquet"))


def ensure_current_dataset(
    interval: str,
    paths: LiveUpdatePaths | None = None,
    *,
    initialized_at_utc: str | None = None,
) -> CurrentDatasetInitResult:
    """Initialize current historical dataset for one interval if needed.

    Existing current parquet is never overwritten. Missing seed parquet is
    reported as ``bootstrap_required`` so callers can continue other intervals.
    """
    validate_interval(interval)
    resolver = paths or LiveUpdatePaths()
    seed_root = resolver.seed_parquet_root(interval)
    current_root = resolver.current_parquet_root(interval)
    marker = resolver.current_initialized_marker(interval)

    if marker.exists() and current_dataset_has_parquet(current_root):
        return CurrentDatasetInitResult(
            interval=interval,
            status="already_initialized",
            seed_root=seed_root,
            current_root=current_root,
            marker_path=marker,
        )
    if current_dataset_has_parquet(current_root):
        return CurrentDatasetInitResult(
            interval=interval,
            status="already_initialized",
            seed_root=seed_root,
            current_root=current_root,
            marker_path=marker,
            message="current parquet exists; leaving it untouched",
        )
    if not seed_root.exists() or not current_dataset_has_parquet(seed_root):
        return CurrentDatasetInitResult(
            interval=interval,
            status="bootstrap_required",
            seed_root=seed_root,
            current_root=current_root,
            marker_path=marker,
            message="historical seed parquet is missing",
        )

    current_root.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(seed_root, current_root, dirs_exist_ok=current_root.exists())
    marker_payload = {
        "initialized_at_utc": initialized_at_utc or utc_now(),
        "seed_root": str(seed_root),
        "current_root": str(current_root),
        "interval": interval,
        "method": "copy",
        "schema_version": SCHEMA_VERSION,
        "dataset_version": DATASET_VERSION,
    }
    write_json_atomic(marker, marker_payload)
    return CurrentDatasetInitResult(
        interval=interval,
        status="initialized",
        seed_root=seed_root,
        current_root=current_root,
        marker_path=marker,
    )


def ensure_current_datasets(
    intervals: tuple[str, ...],
    paths: LiveUpdatePaths | None = None,
) -> list[CurrentDatasetInitResult]:
    """Initialize each requested interval independently."""
    resolver = paths or LiveUpdatePaths()
    return [ensure_current_dataset(interval, resolver) for interval in intervals]


# Status values for one explicitly requested symbol's current dataset.
CURRENT_SYMBOL_ALREADY_AVAILABLE = "already_available"
CURRENT_SYMBOL_INITIALIZED_FROM_SEED = "initialized_current_symbol_from_seed"
CURRENT_SYMBOL_BOOTSTRAP_REQUIRED = "bootstrap_required"


@dataclass(frozen=True)
class CurrentSymbolInitResult:
    """Result of ensuring one explicitly requested symbol exists in current.

    Distinguishes a genuine historical-seed gap (``bootstrap_required``) from a
    *partial current dataset symbol missing* situation -- the seed has the
    symbol but the current dataset does not -- which is repaired by copying the
    seed symbol into the current dataset.
    """

    interval: str
    symbol: str
    status: str
    seed_symbol_root: Path
    current_symbol_root: Path
    message: str | None = None

    def to_dict(self) -> dict[str, Any]:
        payload = {
            "interval": self.interval,
            "symbol": self.symbol,
            "status": self.status,
            "seed_symbol_root": str(self.seed_symbol_root),
            "current_symbol_root": str(self.current_symbol_root),
        }
        if self.message:
            payload["message"] = self.message
        return payload


def _atomic_copy_symbol_dir(src: Path, dst: Path) -> None:
    """Copy a seed symbol directory into the current dataset atomically.

    Copies into a sibling temp directory first, then renames it onto the target
    so a failed copy never leaves a half-written directory that later code could
    mistake for a complete current symbol. Never overwrites an existing target
    and never touches the source.
    """
    dst.parent.mkdir(parents=True, exist_ok=True)
    tmp = dst.parent / (dst.name + f".tmp-{os.getpid()}")
    if tmp.exists():
        shutil.rmtree(tmp)
    try:
        shutil.copytree(src, tmp)
        os.replace(tmp, dst)
    except BaseException:
        if tmp.exists():
            shutil.rmtree(tmp, ignore_errors=True)
        raise


def ensure_current_symbol_from_seed(
    interval: str,
    symbol: str,
    paths: LiveUpdatePaths | None = None,
    *,
    copied_at_utc: str | None = None,
) -> CurrentSymbolInitResult:
    """Ensure one explicitly requested symbol exists in the current dataset.

    * current symbol already present -> ``already_available`` (never overwritten)
    * current missing but seed present -> copy seed symbol into current and
      report ``initialized_current_symbol_from_seed``
    * seed symbol also missing -> ``bootstrap_required`` (do not synthesize a
      full history from zero)

    Only the named symbol is touched. The seed dataset is never modified.
    """
    validate_interval(interval)
    resolver = paths or LiveUpdatePaths()
    normalized = symbol.upper()
    seed_symbol_root = resolver.seed_parquet_root(interval) / f"symbol={normalized}"
    current_symbol_root = resolver.current_parquet_root(interval) / f"symbol={normalized}"

    if current_symbol_root.exists():
        return CurrentSymbolInitResult(
            interval=interval,
            symbol=normalized,
            status=CURRENT_SYMBOL_ALREADY_AVAILABLE,
            seed_symbol_root=seed_symbol_root,
            current_symbol_root=current_symbol_root,
        )
    if not current_dataset_has_parquet(seed_symbol_root):
        return CurrentSymbolInitResult(
            interval=interval,
            symbol=normalized,
            status=CURRENT_SYMBOL_BOOTSTRAP_REQUIRED,
            seed_symbol_root=seed_symbol_root,
            current_symbol_root=current_symbol_root,
            message="historical seed parquet is missing for this symbol",
        )

    _atomic_copy_symbol_dir(seed_symbol_root, current_symbol_root)
    return CurrentSymbolInitResult(
        interval=interval,
        symbol=normalized,
        status=CURRENT_SYMBOL_INITIALIZED_FROM_SEED,
        seed_symbol_root=seed_symbol_root,
        current_symbol_root=current_symbol_root,
        message="copied seed symbol into current dataset",
    )


def ensure_current_symbols_from_seed(
    interval: str,
    symbols: list[str],
    paths: LiveUpdatePaths | None = None,
) -> list[CurrentSymbolInitResult]:
    """Repair partial current dataset for each explicitly requested symbol.

    Only the given symbols are considered -- this never copies a whole interval
    or expands to the whole market.
    """
    resolver = paths or LiveUpdatePaths()
    results: list[CurrentSymbolInitResult] = []
    for symbol in symbols:
        normalized = symbol.strip().upper()
        if not normalized:
            continue
        results.append(ensure_current_symbol_from_seed(interval, normalized, resolver))
    return results


def _current_parquet_schema():
    require_pyarrow()
    return pa.schema(
        [
            ("interval", pa.string()),
            ("open_time", pa.int64()),
            ("open_time_utc", pa.string()),
            ("open_time_taipei", pa.string()),
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
            ("schema_version", pa.int32()),
            ("dataset_version", pa.string()),
        ]
    )


def _normalize_timestamp_value(value: Any) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def _normalize_physical_row(
    row: dict[str, Any],
    key: PartitionKey,
) -> dict[str, Any]:
    """Coerce existing physical parquet rows to the current Phase 2 schema."""
    return {
        "interval": str(row.get("interval", key.interval)),
        "open_time": int(row["open_time"]),
        "open_time_utc": _normalize_timestamp_value(row["open_time_utc"]),
        "open_time_taipei": _normalize_timestamp_value(row["open_time_taipei"]),
        "date": str(row["date"]),
        "month": int(row.get("month", key.month)),
        "open": float(row["open"]),
        "high": float(row["high"]),
        "low": float(row["low"]),
        "close": float(row["close"]),
        "volume": float(row["volume"]),
        "close_time": int(row["close_time"]),
        "quote_volume": float(row["quote_volume"]),
        "trade_count": int(row["trade_count"]),
        "taker_buy_base_volume": float(row["taker_buy_base_volume"]),
        "taker_buy_quote_volume": float(row["taker_buy_quote_volume"]),
        "source_archive": str(row["source_archive"]),
        "archive_source": str(row["archive_source"]),
        "archive_period": str(row["archive_period"]),
        "schema_version": int(row.get("schema_version") or SCHEMA_VERSION),
        "dataset_version": str(row.get("dataset_version") or DATASET_VERSION),
    }


def _table_from_physical_rows(rows: list[dict[str, Any]]):
    schema = _current_parquet_schema()
    columns = {
        name: [row[name] for row in rows]
        for name in CURRENT_PHYSICAL_COLUMNS
    }
    return pa.table(columns, schema=schema)


def read_current_partition_rows(path: Path, key: PartitionKey) -> list[dict[str, Any]]:
    require_pyarrow()
    if not path.exists():
        return []
    table = pq.ParquetFile(path).read()
    return [_normalize_physical_row(row, key) for row in table.to_pylist()]


def merge_records_to_current_partition(
    records: list[KlineRecord],
    paths: LiveUpdatePaths | None = None,
) -> ParquetMergeResult:
    """Atomically merge closed Kline records into their current parquet partition.

    All records must share one partition key. The caller owns queueing and state
    updates in later phases; this function only performs the partition merge and
    reports the target path that can be used after a successful flush.
    """
    if not records:
        raise LiveUpdateCommandError("cannot merge an empty record batch")
    key = records[0].partition_key()
    for rec in records:
        if rec.partition_key() != key:
            raise LiveUpdateCommandError(
                "all records in one parquet merge must share a partition key"
            )

    resolver = paths or LiveUpdatePaths()
    target = resolver.current_partition_file(key)
    existing_rows = read_current_partition_rows(target, key)
    by_key: dict[tuple[str, str, int], dict[str, Any]] = {}
    duplicate_replaced = 0

    for row in existing_rows:
        row_key = (key.symbol.upper(), str(row["interval"]), int(row["open_time"]))
        by_key[row_key] = row

    for rec in records:
        rec_key = rec.record_key().as_tuple()
        if rec_key in by_key:
            duplicate_replaced += 1
        by_key[rec_key] = rec.physical_dict()

    merged_rows = [by_key[k] for k in sorted(by_key, key=lambda item: item[2])]
    target.parent.mkdir(parents=True, exist_ok=True)
    table = _table_from_physical_rows(merged_rows)
    tmp = target.with_name(target.name + ".tmp")
    pq.write_table(table, tmp)
    os.replace(tmp, target)

    open_times = [int(row["open_time"]) for row in merged_rows]
    return ParquetMergeResult(
        partition_key=key,
        target_path=target,
        input_row_count=len(records),
        existing_row_count=len(existing_rows),
        output_row_count=len(merged_rows),
        duplicate_replaced_count=duplicate_replaced,
        min_open_time=min(open_times) if open_times else None,
        max_open_time=max(open_times) if open_times else None,
    )


def load_live_update_state(
    interval: str,
    paths: LiveUpdatePaths | None = None,
    *,
    create_if_missing: bool = True,
) -> LiveUpdateState | None:
    """Load interval state, optionally returning a default state if absent."""
    validate_interval(interval)
    resolver = paths or LiveUpdatePaths()
    path = resolver.state_json(interval)
    if not path.exists():
        if not create_if_missing:
            return None
        return LiveUpdateState.create(interval, resolver)
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise LiveUpdateCommandError(f"state is not readable JSON: {path}") from exc
    state = LiveUpdateState.from_dict(payload)
    if state.interval != interval:
        raise LiveUpdateCommandError(
            f"state interval mismatch: expected {interval}, got {state.interval}"
        )
    return state


def save_live_update_state(
    state: LiveUpdateState,
    paths: LiveUpdatePaths | None = None,
    *,
    now_utc: str | None = None,
) -> Path:
    """Atomically save interval state."""
    resolver = paths or LiveUpdatePaths()
    state.updated_at_utc = now_utc or utc_now()
    path = resolver.state_json(state.interval)
    write_json_atomic(path, state.to_dict())
    return path


def mark_symbol_buffered(
    state: LiveUpdateState,
    symbol: str,
    open_time: int,
    *,
    now_utc: str | None = None,
) -> None:
    """Record that a closed bar has reached closed_buffer or a write queue."""
    symbol_state = state.symbol_state(symbol)
    open_time = int(open_time)
    current = symbol_state.last_buffered_open_time
    if current is None or open_time > current:
        symbol_state.last_buffered_open_time = open_time
    state.updated_at_utc = now_utc or utc_now()


def apply_flush_result_to_state(
    state: LiveUpdateState,
    symbol: str,
    merge_result: ParquetMergeResult,
    *,
    closed_at_utc: str | None = None,
    now_utc: str | None = None,
) -> None:
    """Update closed/flushed state after a successful Phase 2 parquet merge."""
    if merge_result.max_open_time is None:
        return
    if merge_result.partition_key.interval != state.interval:
        raise LiveUpdateCommandError(
            "merge result interval does not match state interval"
        )
    normalized_symbol = symbol.upper()
    if merge_result.partition_key.symbol.upper() != normalized_symbol:
        raise LiveUpdateCommandError(
            "merge result symbol does not match state symbol"
        )
    symbol_state = state.symbol_state(normalized_symbol)
    max_open = int(merge_result.max_open_time)
    symbol_state.last_flushed_open_time = max_open
    symbol_state.last_closed_open_time = max_open
    symbol_state.last_closed_at_utc = closed_at_utc or now_utc or utc_now()
    symbol_state.merged_bar_count += int(merge_result.input_row_count)
    symbol_state.last_target_path = str(merge_result.target_path)
    state.updated_at_utc = now_utc or utc_now()


def state_last_closed_open_time(
    state: LiveUpdateState | None,
    symbol: str,
) -> int | None:
    if state is None:
        return None
    symbol_state = state.symbols.get(symbol.upper())
    if symbol_state is None:
        return None
    return symbol_state.last_closed_open_time


def max_open_time_from_current_dataset(
    interval: str,
    symbol: str,
    paths: LiveUpdatePaths | None = None,
) -> int | None:
    """Read the current dataset and return max open_time for one symbol."""
    validate_interval(interval)
    require_pyarrow()
    resolver = paths or LiveUpdatePaths()
    symbol_root = resolver.current_parquet_root(interval) / f"symbol={symbol.upper()}"
    if not symbol_root.exists():
        return None
    max_open: int | None = None
    for path in sorted(symbol_root.rglob("*.parquet")):
        table = pq.ParquetFile(path).read(columns=["open_time"])
        values = table.column("open_time").to_pylist()
        if not values:
            continue
        file_max = max(int(value) for value in values)
        if max_open is None or file_max > max_open:
            max_open = file_max
    return max_open


def resolve_last_closed_open_time(
    state: LiveUpdateState | None,
    interval: str,
    symbol: str,
    paths: LiveUpdatePaths | None = None,
) -> tuple[int | None, str]:
    """Resolve startup source: state first, then current dataset max open_time."""
    from_state = state_last_closed_open_time(state, symbol)
    if from_state is not None:
        return from_state, "state"
    from_current = max_open_time_from_current_dataset(interval, symbol, paths)
    if from_current is not None:
        return from_current, "current_dataset"
    return None, "bootstrap_required"


def calculate_latest_closed_open_time(
    interval: str,
    now_ms: int,
    *,
    close_lag_ms: int = 2000,
) -> int:
    interval_ms = interval_milliseconds(interval)
    safe_now_ms = int(now_ms) - int(close_lag_ms)
    return (safe_now_ms // interval_ms) * interval_ms - interval_ms


def calculate_missing_bars(
    *,
    last_closed_open_time: int,
    latest_closed_open_time: int,
    interval: str,
) -> tuple[int, int | None, int | None]:
    interval_ms = interval_milliseconds(interval)
    missing = max(
        0,
        (int(latest_closed_open_time) - int(last_closed_open_time)) // interval_ms,
    )
    if missing == 0:
        return 0, None, None
    return (
        missing,
        int(last_closed_open_time) + interval_ms,
        int(latest_closed_open_time),
    )


def plan_symbol_startup_backfill(
    *,
    interval: str,
    symbol: str,
    state: LiveUpdateState | None,
    paths: LiveUpdatePaths | None,
    now_ms: int,
    close_lag_ms: int = 2000,
) -> MissingBarsPlan:
    latest_closed = calculate_latest_closed_open_time(
        interval,
        now_ms,
        close_lag_ms=close_lag_ms,
    )
    last_closed, source = resolve_last_closed_open_time(
        state,
        interval,
        symbol,
        paths,
    )
    if last_closed is None:
        return MissingBarsPlan(
            symbol=symbol.upper(),
            interval=interval,
            last_closed_open_time=None,
            latest_closed_open_time=latest_closed,
            missing_bars=0,
            start_open_time=None,
            end_open_time=None,
            source=source,
            status="bootstrap_required",
        )
    missing, start, end = calculate_missing_bars(
        last_closed_open_time=last_closed,
        latest_closed_open_time=latest_closed,
        interval=interval,
    )
    return MissingBarsPlan(
        symbol=symbol.upper(),
        interval=interval,
        last_closed_open_time=last_closed,
        latest_closed_open_time=latest_closed,
        missing_bars=missing,
        start_open_time=start,
        end_open_time=end,
        source=source,
        status="missing" if missing > 0 else "up_to_date",
    )


def plan_startup_backfill(
    intervals: tuple[str, ...],
    symbols: list[str],
    paths: LiveUpdatePaths | None = None,
    *,
    now_ms: int,
    close_lag_ms: int = 2000,
) -> list[StartupBackfillPlan]:
    """Phase 3 startup-backfill orchestration skeleton.

    This initializes/checks current datasets, loads state, and calculates gap
    ranges. It intentionally does not call REST or mutate last_closed state.
    """
    resolver = paths or LiveUpdatePaths()
    plans: list[StartupBackfillPlan] = []
    for interval in intervals:
        init_result = ensure_current_dataset(interval, resolver)
        state = load_live_update_state(interval, resolver)
        symbol_plans = [
            plan_symbol_startup_backfill(
                interval=interval,
                symbol=symbol,
                state=state,
                paths=resolver,
                now_ms=now_ms,
                close_lag_ms=close_lag_ms,
            )
            for symbol in symbols
        ]
        plans.append(
            StartupBackfillPlan(
                interval=interval,
                init_result=init_result,
                plans=symbol_plans,
            )
        )
    return plans


def rest_klines_url(
    *,
    base_url: str,
    symbol: str,
    interval: str,
    start_time: int | None,
    end_time: int | None,
    limit: int,
) -> str:
    validate_interval(interval)
    if symbol.lower() == "all":
        raise LiveUpdateCommandError(
            "symbol 'all' is a CLI expansion token and must be resolved to "
            "concrete symbols before calling /fapi/v1/klines"
        )
    params: dict[str, Any] = {
        "symbol": symbol.upper(),
        "interval": interval,
        "limit": int(limit),
    }
    if start_time is not None:
        params["startTime"] = int(start_time)
    if end_time is not None:
        params["endTime"] = int(end_time)
    query = urllib.parse.urlencode(params)
    return base_url.rstrip("/") + REST_KLINES_PATH + "?" + query


def fetch_rest_klines(
    *,
    symbol: str,
    interval: str,
    start_time: int | None,
    end_time: int | None,
    limit: int = 1500,
    base_url: str = BINANCE_REST_BASE_URL,
    timeout: float = 15,
    max_retries: int = 5,
    backoff_base_seconds: float = 1,
    backoff_max_seconds: float = 60,
    opener: Any | None = None,
    sleep_func: Any | None = None,
) -> RestFetchResult:
    """Fetch Binance USD-M Futures Klines with Phase 4 backoff semantics."""
    url = rest_klines_url(
        base_url=base_url,
        symbol=symbol,
        interval=interval,
        start_time=start_time,
        end_time=end_time,
        limit=limit,
    )
    open_func = opener or urllib.request.urlopen
    sleeper = sleep_func or time.sleep
    attempts = 0
    while True:
        request = urllib.request.Request(url, headers={"User-Agent": "crypto-data-hub-live-update/0.1"})
        try:
            with open_func(request, timeout=timeout) as response:
                data = response.read()
            rows = json.loads(data.decode("utf-8"))
            if not isinstance(rows, list):
                return RestFetchResult("error", [], url, error="REST response is not a list")
            return RestFetchResult("ok", rows, url, http_status=200)
        except urllib.error.HTTPError as exc:
            body = _read_http_error_body(exc)
            if exc.code == 418:
                raise RestStopRequests(f"HTTP 418 from Binance REST: {body}")
            if exc.code == 429 or 500 <= exc.code <= 599:
                if attempts >= max_retries:
                    return RestFetchResult(
                        "retry_exhausted",
                        [],
                        url,
                        http_status=exc.code,
                        error=body or str(exc),
                    )
                _sleep_backoff(sleeper, attempts, backoff_base_seconds, backoff_max_seconds)
                attempts += 1
                continue
            if exc.code in (400, 404) and _looks_invalid_symbol(body):
                return RestFetchResult(
                    "symbol_unavailable",
                    [],
                    url,
                    http_status=exc.code,
                    error=body or str(exc),
                )
            return RestFetchResult(
                "error",
                [],
                url,
                http_status=exc.code,
                error=body or str(exc),
            )
        except (urllib.error.URLError, TimeoutError, socket.timeout, OSError) as exc:
            if attempts >= max_retries:
                return RestFetchResult(
                    "retry_exhausted",
                    [],
                    url,
                    error=str(exc),
                )
            _sleep_backoff(sleeper, attempts, backoff_base_seconds, backoff_max_seconds)
            attempts += 1


def _read_http_error_body(exc: urllib.error.HTTPError) -> str:
    try:
        raw = exc.read()
    except Exception:
        return ""
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError:
        return repr(raw)


def _looks_invalid_symbol(body: str) -> bool:
    lowered = body.lower()
    return "-1121" in body or "invalid symbol" in lowered or "symbol" in lowered


def _sleep_backoff(
    sleep_func: Any,
    attempt: int,
    base_seconds: float,
    max_seconds: float,
) -> None:
    delay = min(float(max_seconds), float(base_seconds) * (2 ** int(attempt)))
    sleep_func(delay)


def validate_live_kline_record(record: KlineRecord, *, is_closed: bool) -> list[str]:
    """Return shared live-update Kline validation errors.

    This is intentionally small and source-agnostic so REST, WebSocket, and
    webhook payloads go through the same record-level gate before any closed bar
    can update the current historical dataset.
    """
    errors: list[str] = []
    interval_ms = interval_milliseconds(record.interval)
    if not record.symbol:
        errors.append("symbol is required")
    if record.open_time < 0:
        errors.append("open_time must be non-negative")
    if record.open_time % interval_ms != 0:
        errors.append("open_time is not aligned to interval")
    expected_close_time = record.open_time + interval_ms - 1
    if record.close_time != expected_close_time:
        errors.append(
            f"close_time mismatch: expected {expected_close_time}, got {record.close_time}"
        )
    if record.high < max(record.open, record.close, record.low):
        errors.append("OHLC invalid: high is below open/close/low")
    if record.low > min(record.open, record.close, record.high):
        errors.append("OHLC invalid: low is above open/close/high")
    if record.volume < 0:
        errors.append("volume must be non-negative")
    if record.quote_volume < 0:
        errors.append("quote_volume must be non-negative")
    if record.trade_count < 0:
        errors.append("trade_count must be non-negative")
    if record.taker_buy_base_volume < 0:
        errors.append("taker_buy_base_volume must be non-negative")
    if record.taker_buy_quote_volume < 0:
        errors.append("taker_buy_quote_volume must be non-negative")
    if is_closed and record.close_time < record.open_time:
        errors.append("closed Kline close_time must be >= open_time")
    return errors


def _event_validation_errors(event: Any) -> list[str]:
    return validate_live_kline_record(event.record, is_closed=bool(event.is_closed))


def rest_row_to_kline_event(
    row: list[Any],
    *,
    symbol: str,
    interval: str,
    now_ms: int,
    close_lag_ms: int = 2000,
) -> RestKlineEvent:
    if len(row) < 11:
        raise LiveUpdateCommandError(f"REST kline row has fewer than 11 fields: {row!r}")
    open_time = int(row[0])
    close_time = int(row[6])
    fields = datetime_fields(open_time)
    record = KlineRecord.build(
        symbol=symbol,
        interval=interval,
        open_time=open_time,
        open=float(row[1]),
        high=float(row[2]),
        low=float(row[3]),
        close=float(row[4]),
        volume=float(row[5]),
        close_time=close_time,
        quote_volume=float(row[7]),
        trade_count=int(row[8]),
        taker_buy_base_volume=float(row[9]),
        taker_buy_quote_volume=float(row[10]),
        source_archive=REST_SOURCE_ARCHIVE,
        archive_source=REST_ARCHIVE_SOURCE,
        archive_period=fields["date"],
    )
    return RestKlineEvent(
        record=record,
        is_closed=close_time <= int(now_ms) - int(close_lag_ms),
        raw_row=row,
    )


def write_event_buffer(
    paths: LiveUpdatePaths,
    event: RestKlineEvent,
    *,
    received_at_utc: str,
    validation_errors: list[str] | None = None,
) -> None:
    record = event.record
    append_jsonl(
        paths.buffer_jsonl(record.interval, "event_buffer", record.date),
        {
            "received_at_utc": received_at_utc,
            "source": "rest_fallback",
            "record_key": record.record_key().as_dict(),
            "validation_errors": validation_errors or [],
            "payload": event.raw_row,
        },
    )


def write_closed_buffer(
    paths: LiveUpdatePaths,
    event: Any,
    *,
    closed_at_utc: str,
    source: str = REST_ARCHIVE_SOURCE,
) -> None:
    record = event.record
    append_jsonl(
        paths.closed_buffer_jsonl(record.interval, record.date),
        {
            "closed_at_utc": closed_at_utc,
            "source": source,
            "schema_version": SCHEMA_VERSION,
            "record": record.logical_dict(),
        },
    )


def write_latest(
    paths: LiveUpdatePaths,
    event: Any,
    *,
    updated_at_utc: str,
    source: str = REST_ARCHIVE_SOURCE,
    validation_errors: list[str] | None = None,
) -> None:
    record = event.record
    latest_record = record.logical_dict()
    latest_record["is_closed"] = event.is_closed
    write_json_atomic(
        paths.latest_json(record.interval, record.symbol),
        {
            "updated_at_utc": updated_at_utc,
            "source": source,
            "record": latest_record,
            "validation_errors": validation_errors or [],
        },
    )


def write_reject(
    paths: LiveUpdatePaths,
    *,
    interval: str,
    source: str,
    errors: list[str],
    payload: Any,
    rejected_at_utc: str,
    record: KlineRecord | None = None,
) -> None:
    reject_date = record.date if record else rejected_at_utc[:10]
    append_jsonl(
        paths.rejects_jsonl(interval, reject_date),
        {
            "rejected_at_utc": rejected_at_utc,
            "source": source,
            "errors": errors,
            "record": record.logical_dict() if record else None,
            "payload": payload,
        },
    )


def _merge_closed_records_by_partition(
    records: list[KlineRecord],
    paths: LiveUpdatePaths,
) -> list[ParquetMergeResult]:
    by_partition: dict[PartitionKey, list[KlineRecord]] = {}
    for record in records:
        by_partition.setdefault(record.partition_key(), []).append(record)
    results: list[ParquetMergeResult] = []
    for key in sorted(by_partition, key=lambda k: k.as_tuple()):
        ordered = sorted(by_partition[key], key=lambda rec: rec.open_time)
        results.append(merge_records_to_current_partition(ordered, paths))
    return results


def run_rest_backfill_for_plan(
    plan: MissingBarsPlan,
    state: LiveUpdateState,
    paths: LiveUpdatePaths | None = None,
    *,
    now_ms: int,
    close_lag_ms: int = 2000,
    rest_api_limit: int = 1500,
    base_url: str = BINANCE_REST_BASE_URL,
    timeout: float = 15,
    max_retries: int = 5,
    backoff_base_seconds: float = 1,
    backoff_max_seconds: float = 60,
    opener: Any | None = None,
    sleep_func: Any | None = None,
) -> RestBackfillResult:
    """Execute REST backfill for one Phase 3 gap plan."""
    resolver = paths or LiveUpdatePaths()
    if plan.status != "missing" or plan.start_open_time is None or plan.end_open_time is None:
        return RestBackfillResult(
            symbol=plan.symbol,
            interval=plan.interval,
            status=plan.status,
            requested_start_open_time=plan.start_open_time,
            requested_end_open_time=plan.end_open_time,
            fetched_row_count=0,
            event_row_count=0,
            closed_row_count=0,
            merged_row_count=0,
            latest_open_row_count=0,
            merge_results=[],
            warnings=[],
        )

    warnings: list[str] = []
    closed_records: list[KlineRecord] = []
    fetched_count = 0
    event_count = 0
    latest_open_count = 0
    cursor = int(plan.start_open_time)
    interval_ms = interval_milliseconds(plan.interval)

    while cursor <= int(plan.end_open_time):
        fetch = fetch_rest_klines(
            symbol=plan.symbol,
            interval=plan.interval,
            start_time=cursor,
            end_time=plan.end_open_time,
            limit=rest_api_limit,
            base_url=base_url,
            timeout=timeout,
            max_retries=max_retries,
            backoff_base_seconds=backoff_base_seconds,
            backoff_max_seconds=backoff_max_seconds,
            opener=opener,
            sleep_func=sleep_func,
        )
        if fetch.status == "symbol_unavailable":
            warning = f"symbol unavailable for REST backfill: {plan.symbol}"
            warnings.append(warning)
            write_warning_log(resolver, plan.interval, warning)
            return _rest_result_from_counts(plan, "symbol_unavailable", fetched_count, event_count, closed_records, [], latest_open_count, warnings)
        if fetch.status != "ok":
            warning = f"REST fetch failed for {plan.symbol} {plan.interval}: {fetch.status} {fetch.error or ''}".strip()
            warnings.append(warning)
            write_warning_log(resolver, plan.interval, warning)
            return _rest_result_from_counts(plan, fetch.status, fetched_count, event_count, closed_records, [], latest_open_count, warnings)
        if not fetch.rows:
            warning = f"empty REST response for {plan.symbol} {plan.interval} at {cursor}"
            warnings.append(warning)
            write_warning_log(resolver, plan.interval, warning)
            break

        fetched_count += len(fetch.rows)
        events: list[RestKlineEvent] = [
            rest_row_to_kline_event(
                row,
                symbol=plan.symbol,
                interval=plan.interval,
                now_ms=now_ms,
                close_lag_ms=close_lag_ms,
            )
            for row in fetch.rows
        ]
        for event in events:
            timestamp = utc_now()
            validation_errors = _event_validation_errors(event)
            write_event_buffer(
                resolver,
                event,
                received_at_utc=timestamp,
                validation_errors=validation_errors,
            )
            write_latest(
                resolver,
                event,
                updated_at_utc=timestamp,
                validation_errors=validation_errors,
            )
            event_count += 1
            if validation_errors:
                warning = (
                    f"REST validation failed for {event.record.symbol} "
                    f"{event.record.interval} {event.record.open_time}: "
                    + "; ".join(validation_errors)
                )
                warnings.append(warning)
                write_reject(
                    resolver,
                    interval=event.record.interval,
                    source="rest_fallback",
                    errors=validation_errors,
                    payload=event.raw_row,
                    rejected_at_utc=timestamp,
                    record=event.record,
                )
                continue
            if event.is_closed:
                write_closed_buffer(resolver, event, closed_at_utc=timestamp)
                mark_symbol_buffered(state, event.record.symbol, event.record.open_time, now_utc=timestamp)
                closed_records.append(event.record)
            else:
                latest_open_count += 1

        last_open = max(event.record.open_time for event in events)
        next_cursor = last_open + interval_ms
        if next_cursor <= cursor:
            warning = f"REST cursor did not advance for {plan.symbol} {plan.interval}"
            warnings.append(warning)
            write_warning_log(resolver, plan.interval, warning)
            break
        cursor = next_cursor

    merge_results = _merge_closed_records_by_partition(closed_records, resolver)
    for merge_result in merge_results:
        apply_flush_result_to_state(
            state,
            merge_result.partition_key.symbol,
            merge_result,
            closed_at_utc=utc_now(),
        )
    if merge_results:
        save_live_update_state(state, resolver)
    return _rest_result_from_counts(
        plan,
        "ok" if not warnings else "ok_with_warnings",
        fetched_count,
        event_count,
        closed_records,
        merge_results,
        latest_open_count,
        warnings,
    )


def _rest_result_from_counts(
    plan: MissingBarsPlan,
    status: str,
    fetched_count: int,
    event_count: int,
    closed_records: list[KlineRecord],
    merge_results: list[ParquetMergeResult],
    latest_open_count: int,
    warnings: list[str],
) -> RestBackfillResult:
    return RestBackfillResult(
        symbol=plan.symbol,
        interval=plan.interval,
        status=status,
        requested_start_open_time=plan.start_open_time,
        requested_end_open_time=plan.end_open_time,
        fetched_row_count=fetched_count,
        event_row_count=event_count,
        closed_row_count=len(closed_records),
        merged_row_count=sum(result.input_row_count for result in merge_results),
        latest_open_row_count=latest_open_count,
        merge_results=merge_results,
        warnings=warnings,
    )


def run_startup_backfill_once(
    intervals: tuple[str, ...],
    symbols: list[str],
    paths: LiveUpdatePaths | None = None,
    *,
    now_ms: int,
    close_lag_ms: int = 2000,
    rest_api_limit: int = 1500,
    base_url: str = BINANCE_REST_BASE_URL,
    timeout: float = 15,
    max_retries: int = 5,
    backoff_base_seconds: float = 1,
    backoff_max_seconds: float = 60,
    opener: Any | None = None,
    sleep_func: Any | None = None,
) -> list[RestBackfillResult]:
    """Run Phase 4 startup backfill once; no long-running runtime is started."""
    resolver = paths or LiveUpdatePaths()
    # Repair partial current dataset for the explicitly requested symbols before
    # planning: if the seed has a symbol the current dataset is missing, copy it
    # in so gap planning resolves to a real max_open_time instead of falsely
    # reporting bootstrap_required. Seed-missing symbols stay bootstrap_required.
    for interval in intervals:
        ensure_current_symbols_from_seed(interval, symbols, resolver)
    startup_plans = plan_startup_backfill(
        intervals,
        symbols,
        resolver,
        now_ms=now_ms,
        close_lag_ms=close_lag_ms,
    )
    results: list[RestBackfillResult] = []
    for interval_plan in startup_plans:
        state = load_live_update_state(interval_plan.interval, resolver)
        assert state is not None
        try:
            for symbol_plan in interval_plan.plans:
                results.append(
                    run_rest_backfill_for_plan(
                        symbol_plan,
                        state,
                        resolver,
                        now_ms=now_ms,
                        close_lag_ms=close_lag_ms,
                        rest_api_limit=rest_api_limit,
                        base_url=base_url,
                        timeout=timeout,
                        max_retries=max_retries,
                        backoff_base_seconds=backoff_base_seconds,
                        backoff_max_seconds=backoff_max_seconds,
                        opener=opener,
                        sleep_func=sleep_func,
                    )
                )
        except RestStopRequests as exc:
            write_warning_log(resolver, interval_plan.interval, str(exc), level="critical")
            results.append(
                RestBackfillResult(
                    symbol="*",
                    interval=interval_plan.interval,
                    status="rest_stopped",
                    requested_start_open_time=None,
                    requested_end_open_time=None,
                    fetched_row_count=0,
                    event_row_count=0,
                    closed_row_count=0,
                    merged_row_count=0,
                    latest_open_row_count=0,
                    merge_results=[],
                    warnings=[str(exc)],
                )
            )
            break
    return results


def run_gap_repair_once(
    intervals: tuple[str, ...],
    symbols: list[str],
    paths: LiveUpdatePaths | None = None,
    **kwargs: Any,
) -> list[RestBackfillResult]:
    """Callable Phase 4 gap repair skeleton.

    Gap repair uses the same state-driven one-shot REST backfill path. Scheduling
    and long-running managers belong to later phases.
    """
    return run_startup_backfill_once(intervals, symbols, paths, **kwargs)


def websocket_stream_name(symbol: str, interval: str) -> str:
    validate_interval(interval)
    if symbol.lower() == "all":
        raise LiveUpdateCommandError(
            "symbol 'all' is a CLI expansion token and must be resolved to "
            "concrete symbols before building a WebSocket stream name"
        )
    return f"{symbol.lower()}@kline_{interval}"


def build_websocket_streams(
    symbols: list[str],
    intervals: tuple[str, ...],
) -> tuple[str, ...]:
    streams: list[str] = []
    for symbol in symbols:
        normalized = symbol.strip().upper()
        if not normalized:
            continue
        for interval in intervals:
            streams.append(websocket_stream_name(normalized, interval))
    return tuple(streams)


def batch_websocket_streams(
    streams: tuple[str, ...],
    *,
    ws_batch_size: int = 100,
    max_streams_per_connection: int = 1024,
) -> list[tuple[str, ...]]:
    if ws_batch_size <= 0:
        raise LiveUpdateCommandError("ws_batch_size must be positive")
    if max_streams_per_connection <= 0:
        raise LiveUpdateCommandError("max_streams_per_connection must be positive")
    if ws_batch_size > max_streams_per_connection:
        raise LiveUpdateCommandError(
            "ws_batch_size must not exceed max_streams_per_connection"
        )
    return [
        tuple(streams[i:i + ws_batch_size])
        for i in range(0, len(streams), ws_batch_size)
    ]


def combined_stream_url(
    streams: tuple[str, ...],
    *,
    base_url: str = BINANCE_WS_BASE_URL,
) -> str:
    if not streams:
        raise LiveUpdateCommandError("cannot build combined stream URL with no streams")
    joined = "/".join(streams)
    return base_url.rstrip("/") + "/market/stream?streams=" + joined


def build_websocket_connection_specs(
    symbols: list[str],
    intervals: tuple[str, ...],
    *,
    ws_batch_size: int = 100,
    max_streams_per_connection: int = 1024,
    base_url: str = BINANCE_WS_BASE_URL,
) -> list[WebSocketConnectionSpec]:
    streams = build_websocket_streams(symbols, intervals)
    batches = batch_websocket_streams(
        streams,
        ws_batch_size=ws_batch_size,
        max_streams_per_connection=max_streams_per_connection,
    )
    return [
        WebSocketConnectionSpec(
            streams=batch,
            url=combined_stream_url(batch, base_url=base_url),
        )
        for batch in batches
    ]


def unwrap_websocket_payload(payload: str | dict[str, Any]) -> tuple[str | None, dict[str, Any]]:
    if isinstance(payload, str):
        payload = json.loads(payload)
    if not isinstance(payload, dict):
        raise LiveUpdateCommandError("WebSocket payload must be a JSON object")
    if "data" in payload and "stream" in payload:
        data = payload["data"]
        if not isinstance(data, dict):
            raise LiveUpdateCommandError("combined WebSocket data must be an object")
        return str(payload["stream"]), data
    return None, payload


def _kline_record_from_binance_payload(
    data: dict[str, Any],
    *,
    source_archive: str,
    archive_source: str,
) -> KlineRecord:
    if data.get("e") != "kline" or not isinstance(data.get("k"), dict):
        raise LiveUpdateCommandError("payload is not a Binance kline event")
    kline = data["k"]
    symbol = str(kline.get("s") or data.get("s") or "").upper()
    interval = str(kline.get("i") or "")
    if not symbol:
        raise LiveUpdateCommandError("Binance kline payload has no symbol")
    validate_interval(interval)
    open_time = int(kline["t"])
    fields = datetime_fields(open_time)
    return KlineRecord.build(
        symbol=symbol,
        interval=interval,
        open_time=open_time,
        open=float(kline["o"]),
        high=float(kline["h"]),
        low=float(kline["l"]),
        close=float(kline["c"]),
        volume=float(kline["v"]),
        close_time=int(kline["T"]),
        quote_volume=float(kline["q"]),
        trade_count=int(kline["n"]),
        taker_buy_base_volume=float(kline["V"]),
        taker_buy_quote_volume=float(kline["Q"]),
        source_archive=source_archive,
        archive_source=archive_source,
        archive_period=fields["date"],
    )


def _binance_payload_is_closed(data: dict[str, Any]) -> bool:
    if not isinstance(data.get("k"), dict):
        raise LiveUpdateCommandError("payload is not a Binance kline event")
    return bool(data["k"].get("x"))


def websocket_payload_to_event(payload: str | dict[str, Any]) -> WebSocketKlineEvent:
    stream, data = unwrap_websocket_payload(payload)
    record = _kline_record_from_binance_payload(
        data,
        source_archive=WS_SOURCE_ARCHIVE,
        archive_source=WS_ARCHIVE_SOURCE,
    )
    if stream is None:
        stream = websocket_stream_name(record.symbol, record.interval)
    return WebSocketKlineEvent(
        record=record,
        is_closed=_binance_payload_is_closed(data),
        raw_payload=data,
        stream=stream,
    )


def write_websocket_buffer(
    paths: LiveUpdatePaths,
    event: WebSocketKlineEvent,
    *,
    received_at_utc: str,
    validation_errors: list[str] | None = None,
) -> None:
    record = event.record
    append_jsonl(
        paths.buffer_jsonl(record.interval, "websocket_buffer", record.date),
        {
            "received_at_utc": received_at_utc,
            "source": "websocket",
            "stream": event.stream,
            "record_key": record.record_key().as_dict(),
            "validation_errors": validation_errors or [],
            "payload": event.raw_payload,
        },
    )


def process_websocket_kline_event(
    event: WebSocketKlineEvent,
    state: LiveUpdateState,
    paths: LiveUpdatePaths | None = None,
    *,
    received_at_utc: str | None = None,
) -> WebSocketProcessResult:
    resolver = paths or LiveUpdatePaths()
    timestamp = received_at_utc or utc_now()
    record = event.record
    if record.interval != state.interval:
        raise LiveUpdateCommandError(
            f"WebSocket event interval {record.interval} does not match state {state.interval}"
        )

    validation_errors = _event_validation_errors(event)
    write_websocket_buffer(
        resolver,
        event,
        received_at_utc=timestamp,
        validation_errors=validation_errors,
    )
    write_latest(
        resolver,
        event,
        updated_at_utc=timestamp,
        source="websocket",
        validation_errors=validation_errors,
    )
    symbol_state = state.symbol_state(record.symbol)
    symbol_state.last_ws_message_at_utc = timestamp
    state.websocket["last_message_at_utc"] = timestamp
    state.updated_at_utc = timestamp

    if validation_errors:
        write_reject(
            resolver,
            interval=record.interval,
            source="websocket",
            errors=validation_errors,
            payload=event.raw_payload,
            rejected_at_utc=timestamp,
            record=record,
        )
        save_live_update_state(state, resolver, now_utc=timestamp)
        return WebSocketProcessResult(
            status="rejected",
            symbol=record.symbol,
            interval=record.interval,
            open_time=record.open_time,
            is_closed=event.is_closed,
            validation_errors=validation_errors,
        )

    if not event.is_closed:
        save_live_update_state(state, resolver, now_utc=timestamp)
        return WebSocketProcessResult(
            status="open_buffered",
            symbol=record.symbol,
            interval=record.interval,
            open_time=record.open_time,
            is_closed=False,
        )

    write_closed_buffer(resolver, event, closed_at_utc=timestamp, source="websocket")
    mark_symbol_buffered(state, record.symbol, record.open_time, now_utc=timestamp)
    merge_result = merge_records_to_current_partition([record], resolver)
    apply_flush_result_to_state(
        state,
        record.symbol,
        merge_result,
        closed_at_utc=timestamp,
        now_utc=timestamp,
    )
    save_live_update_state(state, resolver, now_utc=timestamp)
    return WebSocketProcessResult(
        status="closed_merged",
        symbol=record.symbol,
        interval=record.interval,
        open_time=record.open_time,
        is_closed=True,
        merge_result=merge_result,
    )


def process_websocket_message(
    payload: str | dict[str, Any],
    state: LiveUpdateState,
    paths: LiveUpdatePaths | None = None,
    *,
    received_at_utc: str | None = None,
) -> WebSocketProcessResult:
    event = websocket_payload_to_event(payload)
    return process_websocket_kline_event(
        event,
        state,
        paths,
        received_at_utc=received_at_utc,
    )


def _json_object_payload(payload: bytes | str | dict[str, Any], *, label: str) -> dict[str, Any]:
    if isinstance(payload, bytes):
        payload = payload.decode("utf-8")
    if isinstance(payload, str):
        payload = json.loads(payload)
    if not isinstance(payload, dict):
        raise LiveUpdateCommandError(f"{label} payload must be a JSON object")
    return payload


def _optional_bool(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"true", "1", "yes"}:
            return True
        if lowered in {"false", "0", "no"}:
            return False
    return bool(value)


def _normalized_payload_to_webhook_event(
    payload: dict[str, Any],
    *,
    now_ms: int,
    close_lag_ms: int = 2000,
) -> WebhookKlineEvent:
    required = (
        "symbol",
        "interval",
        "open_time",
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
    )
    missing = [field for field in required if field not in payload]
    if missing:
        raise LiveUpdateCommandError(
            "normalized webhook payload missing fields: " + ", ".join(missing)
        )
    open_time = int(payload["open_time"])
    close_time = int(payload["close_time"])
    fields = datetime_fields(open_time)
    record = KlineRecord.build(
        symbol=str(payload["symbol"]),
        interval=str(payload["interval"]),
        open_time=open_time,
        open=float(payload["open"]),
        high=float(payload["high"]),
        low=float(payload["low"]),
        close=float(payload["close"]),
        volume=float(payload["volume"]),
        close_time=close_time,
        quote_volume=float(payload["quote_volume"]),
        trade_count=int(payload["trade_count"]),
        taker_buy_base_volume=float(payload["taker_buy_base_volume"]),
        taker_buy_quote_volume=float(payload["taker_buy_quote_volume"]),
        source_archive=WEBHOOK_SOURCE_ARCHIVE,
        archive_source=WEBHOOK_ARCHIVE_SOURCE,
        archive_period=fields["date"],
    )
    provided_is_closed = _optional_bool(payload.get("is_closed"))
    is_closed = (
        close_time <= int(now_ms) - int(close_lag_ms)
        if provided_is_closed is None
        else provided_is_closed
    )
    return WebhookKlineEvent(
        record=record,
        is_closed=is_closed,
        raw_payload=payload,
        payload_format="normalized",
    )


def webhook_payload_to_event(
    payload: bytes | str | dict[str, Any],
    *,
    now_ms: int,
    close_lag_ms: int = 2000,
) -> WebhookKlineEvent:
    data = _json_object_payload(payload, label="webhook")
    if "data" in data and "stream" in data:
        wrapped = data["data"]
        if not isinstance(wrapped, dict):
            raise LiveUpdateCommandError("combined webhook data must be an object")
        record = _kline_record_from_binance_payload(
            wrapped,
            source_archive=WEBHOOK_SOURCE_ARCHIVE,
            archive_source=WEBHOOK_ARCHIVE_SOURCE,
        )
        return WebhookKlineEvent(
            record=record,
            is_closed=_binance_payload_is_closed(wrapped),
            raw_payload=data,
            payload_format="binance_combined",
        )
    if data.get("e") == "kline" and isinstance(data.get("k"), dict):
        record = _kline_record_from_binance_payload(
            data,
            source_archive=WEBHOOK_SOURCE_ARCHIVE,
            archive_source=WEBHOOK_ARCHIVE_SOURCE,
        )
        return WebhookKlineEvent(
            record=record,
            is_closed=_binance_payload_is_closed(data),
            raw_payload=data,
            payload_format="binance_raw",
        )
    return _normalized_payload_to_webhook_event(
        data,
        now_ms=now_ms,
        close_lag_ms=close_lag_ms,
    )


def write_webhook_buffer(
    paths: LiveUpdatePaths,
    event: WebhookKlineEvent,
    *,
    received_at_utc: str,
    validation_errors: list[str] | None = None,
) -> None:
    record = event.record
    append_jsonl(
        paths.buffer_jsonl(record.interval, "webhook_buffer", record.date),
        {
            "received_at_utc": received_at_utc,
            "source": "webhook",
            "payload_format": event.payload_format,
            "record_key": record.record_key().as_dict(),
            "validation_errors": validation_errors or [],
            "payload": event.raw_payload,
        },
    )


def process_webhook_kline_event(
    event: WebhookKlineEvent,
    state: LiveUpdateState,
    paths: LiveUpdatePaths | None = None,
    *,
    received_at_utc: str | None = None,
) -> WebhookProcessResult:
    resolver = paths or LiveUpdatePaths()
    timestamp = received_at_utc or utc_now()
    record = event.record
    if record.interval != state.interval:
        raise LiveUpdateCommandError(
            f"webhook event interval {record.interval} does not match state {state.interval}"
        )

    validation_errors = _event_validation_errors(event)
    write_webhook_buffer(
        resolver,
        event,
        received_at_utc=timestamp,
        validation_errors=validation_errors,
    )
    write_latest(
        resolver,
        event,
        updated_at_utc=timestamp,
        source="webhook",
        validation_errors=validation_errors,
    )

    if validation_errors:
        write_reject(
            resolver,
            interval=record.interval,
            source="webhook",
            errors=validation_errors,
            payload=event.raw_payload,
            rejected_at_utc=timestamp,
            record=record,
        )
        return WebhookProcessResult(
            status="rejected",
            symbol=record.symbol,
            interval=record.interval,
            open_time=record.open_time,
            is_closed=event.is_closed,
            validation_errors=validation_errors,
        )

    if not event.is_closed:
        return WebhookProcessResult(
            status="accepted",
            symbol=record.symbol,
            interval=record.interval,
            open_time=record.open_time,
            is_closed=False,
            validation_errors=[],
        )

    write_closed_buffer(resolver, event, closed_at_utc=timestamp, source="webhook")
    mark_symbol_buffered(state, record.symbol, record.open_time, now_utc=timestamp)
    merge_result = merge_records_to_current_partition([record], resolver)
    apply_flush_result_to_state(
        state,
        record.symbol,
        merge_result,
        closed_at_utc=timestamp,
        now_utc=timestamp,
    )
    save_live_update_state(state, resolver, now_utc=timestamp)
    return WebhookProcessResult(
        status="merged",
        symbol=record.symbol,
        interval=record.interval,
        open_time=record.open_time,
        is_closed=True,
        validation_errors=[],
        merge_result=merge_result,
    )


def process_webhook_payload(
    payload: bytes | str | dict[str, Any],
    paths: LiveUpdatePaths | None = None,
    *,
    active_intervals: tuple[str, ...] | None = None,
    now_ms: int | None = None,
    close_lag_ms: int = 2000,
    received_at_utc: str | None = None,
) -> WebhookProcessResult:
    resolver = paths or LiveUpdatePaths()
    timestamp_ms = (
        int(datetime.now(timezone.utc).timestamp() * 1000)
        if now_ms is None
        else int(now_ms)
    )
    event = webhook_payload_to_event(
        payload,
        now_ms=timestamp_ms,
        close_lag_ms=close_lag_ms,
    )
    if active_intervals is not None and event.record.interval not in active_intervals:
        raise LiveUpdateCommandError(
            f"webhook interval {event.record.interval} is not active"
        )
    state = load_live_update_state(event.record.interval, resolver)
    if state is None:
        state = LiveUpdateState.create(event.record.interval, resolver)
    return process_webhook_kline_event(
        event,
        state,
        resolver,
        received_at_utc=received_at_utc,
    )


def _utc_iso_to_ms(value: str | None) -> int | None:
    if not value:
        return None
    normalized = value.replace("Z", "+00:00")
    dt = datetime.fromisoformat(normalized)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def detect_stale_streams(
    state: LiveUpdateState,
    symbols: list[str],
    *,
    now_ms: int,
    ws_stale_multiplier: int = 3,
) -> list[StaleStream]:
    stale: list[StaleStream] = []
    threshold = interval_milliseconds(state.interval) * int(ws_stale_multiplier)
    for symbol in symbols:
        normalized = symbol.upper()
        symbol_state = state.symbols.get(normalized)
        last_ws = symbol_state.last_ws_message_at_utc if symbol_state else None
        last_ms = _utc_iso_to_ms(last_ws)
        if last_ms is None or int(now_ms) - last_ms > threshold:
            stale.append(
                StaleStream(
                    symbol=normalized,
                    interval=state.interval,
                    last_ws_message_at_utc=last_ws,
                    stale_threshold_ms=threshold,
                )
            )
    return stale


def run_stale_rest_fallback_once(
    state: LiveUpdateState,
    symbols: list[str],
    paths: LiveUpdatePaths | None = None,
    *,
    now_ms: int,
    ws_stale_multiplier: int = 3,
    **rest_kwargs: Any,
) -> list[RestBackfillResult]:
    resolver = paths or LiveUpdatePaths()
    stale = detect_stale_streams(
        state,
        symbols,
        now_ms=now_ms,
        ws_stale_multiplier=ws_stale_multiplier,
    )
    results: list[RestBackfillResult] = []
    for stale_stream in stale:
        plan = plan_symbol_startup_backfill(
            interval=state.interval,
            symbol=stale_stream.symbol,
            state=state,
            paths=resolver,
            now_ms=now_ms,
            close_lag_ms=int(rest_kwargs.get("close_lag_ms", 2000)),
        )
        results.append(
            run_rest_backfill_for_plan(
                plan,
                state,
                resolver,
                now_ms=now_ms,
                **rest_kwargs,
            )
        )
    return results


def handle_websocket_reconnect(
    state: LiveUpdateState,
    symbols: list[str],
    paths: LiveUpdatePaths | None = None,
    *,
    now_ms: int,
    now_utc: str | None = None,
    **rest_kwargs: Any,
) -> WebSocketReconnectResult:
    timestamp = now_utc or utc_now()
    state.websocket["last_reconnect_at_utc"] = timestamp
    state.websocket["reconnect_count"] = int(state.websocket.get("reconnect_count") or 0) + 1
    state.updated_at_utc = timestamp
    results = run_stale_rest_fallback_once(
        state,
        symbols,
        paths,
        now_ms=now_ms,
        **rest_kwargs,
    )
    save_live_update_state(state, paths, now_utc=timestamp)
    return WebSocketReconnectResult(
        status="reconnected",
        reconnect_count=int(state.websocket["reconnect_count"]),
        rest_results=results,
    )


def plan_websocket_rotation(
    state: LiveUpdateState,
    symbols: list[str],
    paths: LiveUpdatePaths | None = None,
    *,
    now_ms: int,
    rotate_hours: int = 23,
    last_connected_at_utc: str | None = None,
    **rest_kwargs: Any,
) -> WebSocketReconnectResult:
    last_connected = last_connected_at_utc or state.websocket.get("last_connected_at_utc")
    last_connected_ms = _utc_iso_to_ms(last_connected)
    rotate_ms = int(rotate_hours) * 3_600_000
    if last_connected_ms is not None and int(now_ms) - last_connected_ms < rotate_ms:
        return WebSocketReconnectResult(
            status="not_due",
            reconnect_count=int(state.websocket.get("reconnect_count") or 0),
            rest_results=[],
        )
    return handle_websocket_reconnect(
        state,
        symbols,
        paths,
        now_ms=now_ms,
        **rest_kwargs,
    )


@dataclass(frozen=True)
class ContinuityCheckResult:
    """Phase 8 continuity check outcome for one symbol + interval.

    Reports row count, open_time range, duplicate / missing / misaligned /
    close_time-mismatch counts, the latest closed open_time derived from the
    clock, the lag in bars, and an overall status. ``status`` is ``ok`` when
    the series is continuous, ``gap_detected`` when any duplicate, missing,
    misaligned, or close_time-mismatch is found, and ``empty`` when the
    current dataset has no rows for this symbol + interval.
    """

    symbol: str
    interval: str
    rows: int
    min_open_time: int | None
    max_open_time: int | None
    duplicate_count: int
    missing_count: int
    latest_closed_open_time: int
    lag_bars: int | None
    status: str
    misaligned_count: int = 0
    close_time_mismatch_count: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "interval": self.interval,
            "rows": self.rows,
            "min_open_time": self.min_open_time,
            "max_open_time": self.max_open_time,
            "duplicate_count": self.duplicate_count,
            "missing_count": self.missing_count,
            "latest_closed_open_time": self.latest_closed_open_time,
            "lag_bars": self.lag_bars,
            "status": self.status,
            "misaligned_count": self.misaligned_count,
            "close_time_mismatch_count": self.close_time_mismatch_count,
        }


def read_current_symbol_open_times(
    interval: str,
    symbol: str,
    paths: LiveUpdatePaths | None = None,
) -> list[tuple[int, int]]:
    """Read ``(open_time, close_time)`` rows for one symbol from the current dataset.

    Returns rows sorted by ``open_time`` ascending. Partitions that cannot be
    read are skipped so a single corrupt file does not abort the whole check.
    """
    validate_interval(interval)
    require_pyarrow()
    resolver = paths or LiveUpdatePaths()
    symbol_root = resolver.current_parquet_root(interval) / f"symbol={symbol.upper()}"
    if not symbol_root.exists():
        return []
    rows: list[tuple[int, int]] = []
    for path in sorted(symbol_root.rglob("*.parquet")):
        try:
            table = pq.ParquetFile(path).read(columns=["open_time", "close_time"])
        except (OSError, pa.ArrowInvalid):  # pragma: no cover - defensive
            continue
        for ot, ct in zip(
            table.column("open_time").to_pylist(),
            table.column("close_time").to_pylist(),
        ):
            rows.append((int(ot), int(ct)))
    rows.sort(key=lambda item: item[0])
    return rows


def check_continuity_for_symbol(
    interval: str,
    symbol: str,
    paths: LiveUpdatePaths | None = None,
    *,
    now_ms: int,
    close_lag_ms: int = 2000,
) -> ContinuityCheckResult:
    """Check current-dataset continuity for one symbol + interval.

    Verifies open_time interval alignment, duplicate open_time, missing
    open_time gaps, ``close_time == open_time + interval_ms - 1``, and reports
    the lag between the latest stored open_time and the latest closed open_time
    derived from the clock.
    """
    validate_interval(interval)
    interval_ms = interval_milliseconds(interval)
    rows = read_current_symbol_open_times(interval, symbol, paths)
    latest_closed = calculate_latest_closed_open_time(
        interval,
        now_ms,
        close_lag_ms=close_lag_ms,
    )
    normalized_symbol = symbol.upper()

    if not rows:
        return ContinuityCheckResult(
            symbol=normalized_symbol,
            interval=interval,
            rows=0,
            min_open_time=None,
            max_open_time=None,
            duplicate_count=0,
            missing_count=0,
            latest_closed_open_time=latest_closed,
            lag_bars=None,
            status="empty",
        )

    open_times = [ot for ot, _ in rows]
    min_open = open_times[0]
    max_open = open_times[-1]

    duplicate_count = len(open_times) - len(set(open_times))

    misaligned_count = sum(1 for ot in open_times if ot % interval_ms != 0)

    close_time_mismatch_count = sum(
        1 for ot, ct in rows if ct != ot + interval_ms - 1
    )

    missing_count = 0
    unique_sorted = sorted(set(open_times))
    for prev, curr in zip(unique_sorted, unique_sorted[1:]):
        step = (curr - prev) // interval_ms
        if step > 1:
            missing_count += step - 1

    lag_bars = max(0, (latest_closed - max_open) // interval_ms)

    has_gap = (
        duplicate_count > 0
        or missing_count > 0
        or misaligned_count > 0
        or close_time_mismatch_count > 0
    )
    status = "gap_detected" if has_gap else "ok"

    return ContinuityCheckResult(
        symbol=normalized_symbol,
        interval=interval,
        rows=len(rows),
        min_open_time=min_open,
        max_open_time=max_open,
        duplicate_count=duplicate_count,
        missing_count=missing_count,
        latest_closed_open_time=latest_closed,
        lag_bars=lag_bars,
        status=status,
        misaligned_count=misaligned_count,
        close_time_mismatch_count=close_time_mismatch_count,
    )


def discover_current_dataset_symbols(
    interval: str,
    paths: LiveUpdatePaths | None = None,
) -> list[str]:
    """Return symbols present in the current dataset for one interval.

    Used by ``--check-continuity`` when no ``--symbols`` are provided so the
    acceptance check stays network-free and clone-safe.
    """
    validate_interval(interval)
    resolver = paths or LiveUpdatePaths()
    symbol_root = resolver.current_parquet_root(interval)
    if not symbol_root.exists():
        return []
    symbols: list[str] = []
    for entry in sorted(symbol_root.iterdir()):
        if not entry.is_dir() or not entry.name.startswith("symbol="):
            continue
        symbol = entry.name[len("symbol="):].upper()
        if symbol:
            symbols.append(symbol)
    return symbols


def run_continuity_check(
    intervals: tuple[str, ...],
    symbols: list[str],
    paths: LiveUpdatePaths | None = None,
    *,
    now_ms: int,
    close_lag_ms: int = 2000,
) -> dict[str, list[ContinuityCheckResult]]:
    """Run continuity checks for every interval + symbol pair.

    Returns a mapping ``interval -> [ContinuityCheckResult, ...]`` ordered by
    the requested intervals and sorted symbols.
    """
    resolver = paths or LiveUpdatePaths()
    results: dict[str, list[ContinuityCheckResult]] = {}
    for interval in intervals:
        interval_symbols = list(symbols)
        if not interval_symbols:
            interval_symbols = discover_current_dataset_symbols(interval, resolver)
        interval_symbols = sorted(set(s.upper() for s in interval_symbols))
        results[interval] = [
            check_continuity_for_symbol(
                interval,
                symbol,
                resolver,
                now_ms=now_ms,
                close_lag_ms=close_lag_ms,
            )
            for symbol in interval_symbols
        ]
    return results


def continuity_summary_payload(
    results: dict[str, list[ContinuityCheckResult]],
    *,
    requested_interval: str,
    active_intervals: tuple[str, ...],
    symbols: list[str],
    now_ms: int,
) -> dict[str, Any]:
    """Build the ``--check-continuity`` summary payload."""
    flat = [item for items in results.values() for item in items]
    statuses = {item.status for item in flat}
    if not flat or all(item.status == "empty" for item in flat):
        overall = "empty"
    elif "gap_detected" in statuses:
        overall = "gap_detected"
    else:
        overall = "ok"
    # Symbols reflect what was actually checked: explicit symbols plus any
    # discovered from the current dataset (so a network-free --check-continuity
    # still reports the symbols it inspected).
    reported_symbols = set(s.upper() for s in symbols)
    reported_symbols.update(item.symbol for item in flat)
    return {
        "schema_version": SCHEMA_VERSION,
        "dataset_version": DATASET_VERSION,
        "requested_interval": requested_interval,
        "active_intervals": list(active_intervals),
        "symbols": sorted(reported_symbols),
        "now_ms": int(now_ms),
        "overall_status": overall,
        "interval_results": {
            interval: [item.to_dict() for item in items]
            for interval, items in results.items()
        },
    }


def webhook_health_payload(config: WebhookServerConfig) -> dict[str, Any]:
    return {
        "status": "ok",
        "interval": config.requested_interval,
        "active_intervals": list(config.active_intervals),
        "live_root": str(config.paths._rooted(config.paths.live_root)),
        "current_dataset_root": str(config.paths._rooted(config.paths.current_dataset_root)),
    }


class LiveUpdateWebhookRequestHandler(BaseHTTPRequestHandler):
    """Minimal Phase 6 webhook HTTP handler.

    It is exposed as a primitive for tests and later runtime wiring. This phase
    intentionally does not start a production daemon from the default CLI path.
    """

    server_version = "crypto-data-hub-live-update-webhook/0.1"

    def log_message(self, format: str, *args: Any) -> None:  # pragma: no cover
        return

    @property
    def webhook_config(self) -> WebhookServerConfig:
        return self.server.webhook_config  # type: ignore[attr-defined]

    def _write_json_response(self, status_code: int, payload: dict[str, Any]) -> None:
        body = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:
        if self.path != "/healthz":
            self._write_json_response(404, {"status": "not_found"})
            return
        self._write_json_response(200, webhook_health_payload(self.webhook_config))

    def do_POST(self) -> None:
        if self.path != "/webhook/kline":
            self._write_json_response(404, {"status": "not_found"})
            return
        config = self.webhook_config
        raw_length = self.headers.get("Content-Length")
        if raw_length is None:
            self._write_json_response(
                411,
                {"status": "rejected", "errors": ["Content-Length is required"]},
            )
            return
        try:
            length = int(raw_length)
        except ValueError:
            self._write_json_response(
                400,
                {"status": "rejected", "errors": ["invalid Content-Length"]},
            )
            return
        if length > config.max_body_bytes:
            self._write_json_response(
                413,
                {
                    "status": "rejected",
                    "errors": [
                        f"payload too large: {length} bytes > {config.max_body_bytes}"
                    ],
                },
            )
            return
        body = self.rfile.read(length)
        try:
            result = process_webhook_payload(
                body,
                config.paths,
                active_intervals=config.active_intervals,
                close_lag_ms=config.close_lag_ms,
            )
        except (KeyError, TypeError, ValueError, json.JSONDecodeError, LiveUpdateCommandError) as exc:
            self._write_json_response(
                400,
                {"status": "rejected", "errors": [str(exc)]},
            )
            return
        status_code = 422 if result.status == "rejected" else 200
        self._write_json_response(status_code, result.to_dict())


def build_webhook_server(config: WebhookServerConfig) -> ThreadingHTTPServer:
    if config.max_body_bytes <= 0:
        raise LiveUpdateCommandError("webhook max body bytes must be positive")
    server = ThreadingHTTPServer(
        (config.host, int(config.port)),
        LiveUpdateWebhookRequestHandler,
    )
    server.webhook_config = config  # type: ignore[attr-defined]
    return server


ALL_SYMBOLS_TOKEN = "all"
REST_EXCHANGE_INFO_PATH = "/fapi/v1/exchangeInfo"
SYMBOLS_REQUIRED_MESSAGE = (
    "no symbols provided. Please provide --symbols BTCUSDT ETHUSDT "
    "or --symbols all (Binance USD-M Futures USDT perpetuals)."
)


def parse_symbols_arg(raw: str | list[str] | None) -> list[str]:
    """Normalize raw ``--symbols`` CLI input into a list or the ``["all"]`` sentinel.

    Accepts argparse ``nargs`` results (a list of tokens), a single string, or
    ``None``. Each token is split on commas and whitespace so all of these are
    equivalent::

        --symbols BTCUSDT ETHUSDT
        --symbols "BTCUSDT ETHUSDT"
        --symbols BTCUSDT,ETHUSDT

    Symbols are upper-cased and de-duplicated while preserving first-seen order.
    The special token ``all`` (any case) is a CLI expansion sentinel: it returns
    ``["all"]`` so callers must resolve it to concrete symbols via
    ``/fapi/v1/exchangeInfo`` before any REST / WebSocket / state / parquet use.
    """
    if raw is None:
        tokens: list[str] = []
    elif isinstance(raw, str):
        tokens = [raw]
    else:
        tokens = list(raw)

    parsed: list[str] = []
    seen: set[str] = set()
    for token in tokens:
        for chunk in str(token).replace(",", " ").split():
            normalized = chunk.strip().upper()
            if not normalized:
                continue
            if normalized == ALL_SYMBOLS_TOKEN.upper():
                # ``all`` is only ever a whole-market expansion, never mixed with
                # concrete symbols. Resolve it on its own.
                return [ALL_SYMBOLS_TOKEN]
            if normalized not in seen:
                seen.add(normalized)
                parsed.append(normalized)
    return parsed


def fetch_um_perpetual_usdt_symbols(
    base_url: str = BINANCE_REST_BASE_URL,
    timeout: float = 15.0,
) -> list[str]:
    """Resolve ``--symbols all`` to currently tradable USD-M USDT perpetuals.

    Uses the Binance USD-M Futures ``/fapi/v1/exchangeInfo`` endpoint (never the
    spot ``/api/v3/exchangeInfo``) and keeps only symbols where
    ``status == "TRADING"``, ``contractType == "PERPETUAL"`` and
    ``quoteAsset == "USDT"``. The result is sorted for deterministic truncation.
    """
    url = base_url.rstrip("/") + REST_EXCHANGE_INFO_PATH
    req = urllib.request.Request(url, headers={"User-Agent": "crypto-data-hub/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except urllib.error.URLError as exc:
        raise LiveUpdateCommandError(f"failed to fetch exchangeInfo: {exc}") from exc
    except json.JSONDecodeError as exc:
        raise LiveUpdateCommandError(f"invalid exchangeInfo JSON: {exc}") from exc

    symbols: list[str] = []
    for s in payload.get("symbols", []):
        if (
            s.get("status") == "TRADING"
            and s.get("contractType") == "PERPETUAL"
            and s.get("quoteAsset") == "USDT"
        ):
            symbols.append(str(s["symbol"]).upper())
    symbols.sort()
    return symbols


def _read_symbols_file(symbols_file_arg: str) -> list[str]:
    path = Path(symbols_file_arg)
    if not path.is_file():
        raise LiveUpdateCommandError(f"symbols file not found: {symbols_file_arg}")
    symbols: list[str] = []
    seen: set[str] = set()
    for line in path.read_text("utf-8").splitlines():
        line = line.strip().upper()
        if line and not line.startswith("#") and line not in seen:
            seen.add(line)
            symbols.append(line)
    return symbols


def resolve_symbols(
    symbols_arg: str | list[str] | None,
    symbols_file_arg: str,
    max_symbols: int,
    base_url: str = BINANCE_REST_BASE_URL,
    timeout: float = 15.0,
) -> list[str]:
    """Resolve CLI symbol input into a concrete, normalized symbol list.

    Resolution order: ``--symbols`` (with ``all`` expanding via exchangeInfo),
    then ``--symbols-file``. Missing input returns ``[]`` -- it never silently
    expands to the whole market; callers decide whether an empty result is fatal.
    ``--max-symbols`` truncates the final list (smoke-test aid, not a universe).
    """
    parsed = parse_symbols_arg(symbols_arg)

    if parsed == [ALL_SYMBOLS_TOKEN]:
        symbols = fetch_um_perpetual_usdt_symbols(base_url, timeout)
    elif parsed:
        symbols = parsed
    elif symbols_file_arg:
        symbols = _read_symbols_file(symbols_file_arg)
    else:
        return []

    if max_symbols > 0:
        return symbols[:max_symbols]
    return symbols


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python scripts/live_update.py",
        description="Phase 1-8 live-update layout, state, REST, WebSocket, webhook tools, CLI modes, and continuity checks.",
    )
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--interval", default="all", choices=CLI_INTERVALS)
    parser.add_argument(
        "--symbols",
        nargs="*",
        default=None,
        metavar="SYMBOL",
        help=(
            "symbols to run, e.g. --symbols BTCUSDT ETHUSDT, "
            "--symbols \"BTCUSDT ETHUSDT\", --symbols BTCUSDT,ETHUSDT, "
            "or --symbols all for all Binance USD-M USDT perpetuals"
        ),
    )
    parser.add_argument("--symbols-file", default="")
    parser.add_argument("--max-symbols", type=int, default=0)
    parser.add_argument("--lookback-bars", type=int, default=3)
    parser.add_argument("--poll-seconds", type=int, default=65)
    parser.add_argument("--request-delay", type=float, default=0.02)
    parser.add_argument("--close-lag-ms", type=int, default=2000)
    parser.add_argument("--now-ms", type=int, default=None)
    parser.add_argument("--binance-rest-base-url", default=BINANCE_REST_BASE_URL)
    parser.add_argument("--http-timeout", type=float, default=15)
    parser.add_argument("--binance-ws-base-url", default=BINANCE_WS_BASE_URL)
    parser.add_argument("--webhook-host", default=DEFAULT_WEBHOOK_HOST)
    parser.add_argument("--webhook-port", type=int, default=DEFAULT_WEBHOOK_PORT)
    parser.add_argument("--webhook-max-body-bytes", type=int, default=DEFAULT_WEBHOOK_MAX_BODY_BYTES)

    parser.add_argument("--disable-webhook", action="store_true")
    parser.add_argument("--disable-websocket", action="store_true")
    parser.add_argument("--disable-rest-fallback", action="store_true")
    parser.add_argument("--disable-startup-backfill", action="store_true")
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--strict", action="store_true")
    parser.add_argument("--quiet-http", action="store_true")

    parser.add_argument("--ws-batch-size", type=int, default=100)
    parser.add_argument("--max-streams-per-connection", type=int, default=1024)
    parser.add_argument("--max-total-streams", type=int, default=0)
    parser.add_argument("--startup-batch-size", type=int, default=5)
    parser.add_argument("--startup-batch-delay", type=float, default=1)
    parser.add_argument("--ws-stale-multiplier", type=int, default=3)
    parser.add_argument("--ws-reconnect-max-retries", type=int, default=10)
    parser.add_argument("--ws-reconnect-backoff-seconds", type=float, default=5)
    parser.add_argument("--ws-connection-rotate-hours", type=int, default=23)

    parser.add_argument("--rest-api-limit", type=int, default=1500)
    parser.add_argument("--rest-max-retries", type=int, default=5)
    parser.add_argument("--rest-backoff-base-seconds", type=float, default=1)
    parser.add_argument("--rest-backoff-max-seconds", type=float, default=60)

    parser.add_argument("--gap-repair-seconds", type=int, default=300)
    parser.add_argument("--flush-seconds", type=int, default=10)
    parser.add_argument("--flush-max-rows", type=int, default=1000)
    parser.add_argument("--buffer-retention-days", type=int, default=30)
    parser.add_argument("--closed-buffer-retention-days", type=int, default=0)
    parser.add_argument("--compress-old-buffers", action="store_true")
    parser.add_argument("--check-continuity", action="store_true")

    parser.add_argument("--current-dataset-root", default=str(DEFAULT_CURRENT_DATASET_ROOT))
    parser.add_argument("--seed-dataset-root", default=str(DEFAULT_SEED_DATASET_ROOT))
    parser.add_argument(
        "--describe-layout",
        action="store_true",
        help="print Phase 1 path layout JSON and exit",
    )
    parser.add_argument(
        "--initialize-current-dataset",
        action="store_true",
        help="initialize current historical dataset for the requested interval(s) and exit",
    )
    parser.add_argument(
        "--plan-startup-backfill",
        action="store_true",
        help="calculate Phase 3 startup-backfill gaps without calling REST",
    )
    parser.add_argument(
        "--run-startup-backfill-once",
        action="store_true",
        help="run Phase 4 REST startup backfill once and exit",
    )
    parser.add_argument(
        "--describe-websocket-connections",
        action="store_true",
        help="print Phase 5 combined stream connection specs and exit",
    )
    parser.add_argument(
        "--describe-webhook-server",
        action="store_true",
        help="print Phase 6 webhook server config and health payload, then exit",
    )
    return parser


def run_once_mode(
    args: argparse.Namespace,
    intervals: tuple[str, ...],
    symbols: list[str],
    paths: LiveUpdatePaths,
    now_ms: int,
) -> int:
    """Run one complete live-update cycle and exit.

    ``--once`` is the user-facing shorthand for a one-shot update. It shares the
    exact core flow as ``--run-startup-backfill-once``: ensure current symbols
    from seed, run the startup / REST gap repair once (writing closed_buffer,
    merging into current parquet, and updating state only after a successful
    merge), then exit. Seed-missing symbols stay ``bootstrap_required`` -- no
    REST and no zero-history rebuild.
    """
    results_backfill: list[RestBackfillResult] = []
    if not args.disable_startup_backfill:
        results_backfill = run_startup_backfill_once(
            intervals,
            symbols,
            paths,
            now_ms=now_ms,
            close_lag_ms=args.close_lag_ms,
            rest_api_limit=args.rest_api_limit,
            base_url=args.binance_rest_base_url,
            timeout=args.http_timeout,
            max_retries=args.rest_max_retries,
            backoff_base_seconds=args.rest_backoff_base_seconds,
            backoff_max_seconds=args.rest_backoff_max_seconds,
        )

    payload = {
        "schema_version": SCHEMA_VERSION,
        "dataset_version": DATASET_VERSION,
        "mode": "once",
        "requested_interval": args.interval,
        "active_intervals": list(intervals),
        "symbols": symbols,
        "startup_backfill_enabled": not args.disable_startup_backfill,
        "results": [result.to_dict() for result in results_backfill],
    }
    print(pretty_json({"once_update": payload}), end="")

    if args.check_continuity:
        results = run_continuity_check(
            intervals,
            symbols,
            paths,
            now_ms=now_ms,
            close_lag_ms=args.close_lag_ms,
        )
        payload_cc = continuity_summary_payload(
            results,
            requested_interval=args.interval,
            active_intervals=intervals,
            symbols=symbols,
            now_ms=now_ms,
        )
        print(pretty_json({"continuity_check": payload_cc}), end="")
    return 0


def run_orchestration_skeleton(
    args: argparse.Namespace,
    intervals: tuple[str, ...],
    symbols: list[str],
    paths: LiveUpdatePaths,
    now_ms: int,
) -> int:
    print("5. initialize current dataset for each interval")
    ensure_current_datasets(intervals, paths)

    print("6. startup backfill for each symbol + interval")
    if not args.disable_startup_backfill:
        run_startup_backfill_once(
            intervals,
            symbols,
            paths,
            now_ms=now_ms,
            close_lag_ms=args.close_lag_ms,
            rest_api_limit=args.rest_api_limit,
            base_url=args.binance_rest_base_url,
            timeout=args.http_timeout,
            max_retries=args.rest_max_retries,
            backoff_base_seconds=args.rest_backoff_base_seconds,
            backoff_max_seconds=args.rest_backoff_max_seconds,
        )
    else:
        print("WARNING: startup backfill is disabled. Data might be incomplete.")

    print("7. start partition writers (skeleton)")
    print("8. start webhook server (skeleton)")
    if args.disable_webhook:
        print("webhook disabled.")

    print("9. start WebSocket manager (skeleton)")
    if args.disable_websocket:
        print("websocket disabled.")

    print("10. start REST fallback manager (skeleton)")
    if args.disable_rest_fallback:
        print("WARNING: REST fallback disabled. Data gaps might occur.")

    print("11. start retention manager (skeleton)")
    print("12. handle shutdown signals (skeleton)")

    import threading
    import signal

    stop_event = threading.Event()
    def sig_handler(signum: int, frame: Any) -> None:
        print(f"Received signal {signum}, stopping...")
        stop_event.set()

    signal.signal(signal.SIGINT, sig_handler)
    signal.signal(signal.SIGTERM, sig_handler)

    try:
        while not stop_event.is_set():
            time.sleep(0.1)
    except KeyboardInterrupt:
        stop_event.set()

    print("stop event set")
    print("stop WebSocket manager")
    print("stop REST fallback manager")
    print("stop webhook server")
    print("forced flush all partition queues")
    print("flush successful, update state")
    print("shutdown webhook server")
    print("server_close")
    print("flush pending writes")
    print("stopped message")
    return 0


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
    if args.initialize_current_dataset:
        init_symbols = resolve_symbols(
            args.symbols,
            args.symbols_file,
            args.max_symbols,
            base_url=args.binance_rest_base_url,
            timeout=args.http_timeout,
        )
        if init_symbols:
            # Symbol-scoped initialization: only repair the explicitly requested
            # symbols, never copy a whole interval implicitly.
            symbol_results = {
                interval: [
                    result.to_dict()
                    for result in ensure_current_symbols_from_seed(
                        interval, init_symbols, paths
                    )
                ]
                for interval in intervals
            }
            payload = {
                "schema_version": SCHEMA_VERSION,
                "dataset_version": DATASET_VERSION,
                "requested_interval": args.interval,
                "active_intervals": list(intervals),
                "symbols": init_symbols,
                "symbol_results": symbol_results,
            }
            print(pretty_json(payload), end="")
            return 0
        results = ensure_current_datasets(intervals, paths)
        payload = {
            "schema_version": SCHEMA_VERSION,
            "dataset_version": DATASET_VERSION,
            "requested_interval": args.interval,
            "active_intervals": list(intervals),
            "results": [result.to_dict() for result in results],
        }
        print(pretty_json(payload), end="")
        return 0

    if args.check_continuity and not args.once:
        # Continuity check is network-free: use --symbols / --symbols-file if
        # provided, otherwise discover symbols from the current dataset so the
        # acceptance check stays clone-safe. When combined with --once, the
        # once flow runs the continuity check at its final step instead.
        continuity_symbols = parse_symbols_arg(args.symbols)
        if continuity_symbols == [ALL_SYMBOLS_TOKEN]:
            # Continuity is network-free: ``all`` falls back to dataset discovery
            # rather than calling exchangeInfo.
            continuity_symbols = []
        if not continuity_symbols and args.symbols_file:
            sf_path = Path(args.symbols_file)
            if sf_path.is_file():
                continuity_symbols = _read_symbols_file(args.symbols_file)
        now_ms_cc = args.now_ms
        if now_ms_cc is None:
            now_ms_cc = int(datetime.now(timezone.utc).timestamp() * 1000)
        results = run_continuity_check(
            intervals,
            continuity_symbols,
            paths,
            now_ms=now_ms_cc,
            close_lag_ms=args.close_lag_ms,
        )
        payload_cc = continuity_summary_payload(
            results,
            requested_interval=args.interval,
            active_intervals=intervals,
            symbols=continuity_symbols,
            now_ms=now_ms_cc,
        )
        print(pretty_json(payload_cc), end="")
        return 0

    # All other commands require symbol resolution
    symbols = resolve_symbols(
        args.symbols,
        args.symbols_file,
        args.max_symbols,
        base_url=args.binance_rest_base_url,
        timeout=args.http_timeout,
    )

    now_ms = args.now_ms
    if now_ms is None:
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

    if args.plan_startup_backfill:
        if not symbols:
            raise LiveUpdateCommandError(SYMBOLS_REQUIRED_MESSAGE)
        plans = plan_startup_backfill(
            intervals,
            symbols,
            paths,
            now_ms=now_ms,
            close_lag_ms=args.close_lag_ms,
        )
        payload = {
            "schema_version": SCHEMA_VERSION,
            "dataset_version": DATASET_VERSION,
            "requested_interval": args.interval,
            "active_intervals": list(intervals),
            "plans": [plan.to_dict() for plan in plans],
        }
        print(pretty_json(payload), end="")
        return 0
    if args.run_startup_backfill_once:
        if not symbols:
            raise LiveUpdateCommandError(SYMBOLS_REQUIRED_MESSAGE)
        results_backfill = run_startup_backfill_once(
            intervals,
            symbols,
            paths,
            now_ms=now_ms,
            close_lag_ms=args.close_lag_ms,
            rest_api_limit=args.rest_api_limit,
            base_url=args.binance_rest_base_url,
            timeout=args.http_timeout,
            max_retries=args.rest_max_retries,
            backoff_base_seconds=args.rest_backoff_base_seconds,
            backoff_max_seconds=args.rest_backoff_max_seconds,
        )
        payload_backfill = {
            "schema_version": SCHEMA_VERSION,
            "dataset_version": DATASET_VERSION,
            "requested_interval": args.interval,
            "active_intervals": list(intervals),
            "results": [result.to_dict() for result in results_backfill],
        }
        print(pretty_json(payload_backfill), end="")
        return 0
    if args.describe_websocket_connections:
        if not symbols:
            raise LiveUpdateCommandError(SYMBOLS_REQUIRED_MESSAGE)
        specs = build_websocket_connection_specs(
            symbols,
            intervals,
            ws_batch_size=args.ws_batch_size,
            max_streams_per_connection=args.max_streams_per_connection,
            base_url=args.binance_ws_base_url,
        )
        payload_ws = {
            "schema_version": SCHEMA_VERSION,
            "dataset_version": DATASET_VERSION,
            "requested_interval": args.interval,
            "active_intervals": list(intervals),
            "symbols": symbols,
            "connection_count": len(specs),
            "stream_count": sum(len(spec.streams) for spec in specs),
            "connections": [spec.to_dict() for spec in specs],
        }
        print(pretty_json(payload_ws), end="")
        return 0
    if args.describe_webhook_server:
        config = WebhookServerConfig(
            requested_interval=args.interval,
            active_intervals=intervals,
            paths=paths,
            host=args.webhook_host,
            port=args.webhook_port,
            max_body_bytes=args.webhook_max_body_bytes,
            close_lag_ms=args.close_lag_ms,
        )
        payload_wh = {
            "schema_version": SCHEMA_VERSION,
            "dataset_version": DATASET_VERSION,
            "webhook_enabled": not args.disable_webhook,
            "server": config.to_dict(),
            "healthz": webhook_health_payload(config),
            "endpoints": {
                "healthz": "GET /healthz",
                "kline": "POST /webhook/kline",
            },
        }
        print(pretty_json(payload_wh), end="")
        return 0

    # Live update default run and --once both write data / open many streams, so
    # a missing --symbols must fail loudly rather than silently spanning the whole
    # market.
    if not symbols:
        raise LiveUpdateCommandError(SYMBOLS_REQUIRED_MESSAGE)

    # Live update default run
    print("1. parse CLI")
    print("2. expand intervals")
    print("3. load symbols")
    stream_count = len(symbols) * len(intervals)
    if args.max_total_streams > 0 and stream_count > args.max_total_streams:
        raise LiveUpdateCommandError(f"stream_count ({stream_count}) exceeds max_total_streams ({args.max_total_streams})")

    import math
    connection_count = math.ceil(stream_count / args.ws_batch_size) if args.ws_batch_size > 0 else 0

    summary = {
        "symbols_count": len(symbols),
        "intervals_count": len(intervals),
        "stream_count": stream_count,
        "connection_count": connection_count,
        "ws_batch_size": args.ws_batch_size,
        "max_streams_per_connection": args.max_streams_per_connection,
        "estimated_partition_count": len(symbols) * len(intervals),
        "startup_backfill_enabled": not args.disable_startup_backfill,
        "rest_fallback_enabled": not args.disable_rest_fallback,
        "websocket_enabled": not args.disable_websocket,
        "webhook_enabled": not args.disable_webhook,
    }
    print("4. print startup summary")
    print(pretty_json({"startup_summary": summary}), end="")

    if args.once:
        return run_once_mode(args, intervals, symbols, paths, now_ms)

    return run_orchestration_skeleton(args, intervals, symbols, paths, now_ms)


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
