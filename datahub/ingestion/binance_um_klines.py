"""Binance USD-M Futures Kline historical ingestion pipeline.

Parameterized by Kline ``interval`` (1d / 4h / 1h / 15m / 5m / 1m). The first
production interval is ``1d`` but no interval is hard-coded into the pipeline.

Terminology — kept strictly separate throughout this module:

* **Kline interval** — the trading-data period of each row (``1d`` ... ``1m``).
* **Archive package source** — how Binance Data Vision packages files on disk:
  ``monthly`` archive packages (historical base) and ``daily`` archive packages
  (recent delta). ``monthly`` / ``daily`` are *not* Kline intervals.

Large market data is written under ``local_data/`` only and is never committed.

Memory model
------------
Discovery and download are **streaming and bounded-memory**: the full archive
inventory (hundreds of thousands of records for ``--archive-source both``) is
never held in memory at once. Records are written to ``archive_files.jsonl`` /
``files.jsonl`` incrementally, only aggregate counters are retained, and at most
``--workers`` symbols are in flight at any moment. S3 listings are paginated and
parsed one page at a time.
"""

from __future__ import annotations

import argparse
import calendar
import concurrent.futures
from dataclasses import dataclass, field
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sys
import time
from typing import Any, Callable, Iterable, Iterator
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET

# --------------------------------------------------------------------------- #
# Constants
# --------------------------------------------------------------------------- #

DATASET_ID = "market.binance.um.klines"
DATASET_VERSION = "v0.1.0"
CODE_VERSION = "v0.6.0"

# Allowed Kline intervals are defined once here. Adding an interval is a single
# edit to this tuple; nothing else hard-codes ``1d``.
ALLOWED_INTERVALS: tuple[str, ...] = ("1d", "4h", "1h", "15m", "5m", "1m")

# Archive package sources (NOT Kline intervals).
ARCHIVE_SOURCES: tuple[str, ...] = ("monthly", "daily")

DATA_VISION_BASE = "https://data.binance.vision"
S3_LIST_ENDPOINT = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision"
S3_NAMESPACE = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}
ARCHIVE_PREFIX = "data/futures/um/{source}/klines"
USER_AGENT = "crypto-data-hub-klines/0.6.0"

DEFAULT_LOCAL_ROOT = Path("local_data/binance_um_klines")

# Kline CSV columns as published in each archive zip (no header row in older
# files; newer files may carry an `open_time,...` header — both are documented
# in docs/research_agent_klines_access.md).
KLINE_COLUMNS: tuple[str, ...] = (
    "open_time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "close_time",
    "quote_asset_volume",
    "count",
    "taker_buy_volume",
    "taker_buy_quote_volume",
    "ignore",
)

# Primary key for normalized rows.
PRIMARY_KEY: tuple[str, ...] = ("symbol", "interval", "open_time")

DAILY_REQUIRED_DELTA = "required_delta"
DAILY_SKIPPED_BY_DEFAULT = "skipped_by_default"
DAILY_INCLUDED_FULL = "included_by_explicit_full_daily_history"

DOWNLOAD = "download"
SKIP = "skip"

# Checksum status enum (per task spec).
CHK_PASSED = "passed"
CHK_FAILED = "failed"
CHK_MISSING_CHECKSUM = "missing_checksum"
CHK_MISSING_ZIP = "missing_zip"
CHK_SKIPPED_EXISTING = "skipped_existing_verified"
CHK_DOWNLOAD_FAILED = "download_failed"
CHK_NOT_ATTEMPTED = "not_attempted"

# Download status enum (internal companion to checksum status).
DL_DOWNLOADED = "downloaded"
DL_SKIPPED_EXISTING = "skipped_existing_verified"
DL_SKIPPED_OVERLAP = "skipped_daily_overlap"
DL_FAILED = "failed"
DL_NOT_ATTEMPTED = "not_attempted"

# Cap on how many gap descriptions to keep in the coverage summary (the full set
# is always written incrementally to reports/missing_files.jsonl).
KNOWN_GAPS_CAP = 100

# Progress log cadence (symbols) — keeps discovery progress visible without
# emitting one line per symbol.
PROGRESS_EVERY = 25


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #


class KlinesCommandError(ValueError):
    """Raised for invalid CLI use (exit code 2)."""


class KlinesError(RuntimeError):
    """Raised when ingestion fails loud (exit code 1)."""


class ObjectNotFound(Exception):
    """Raised by a backend when an archive object does not exist (HTTP 404)."""


class TransientDownloadError(Exception):
    """Raised by a backend for a retryable network/transport failure."""


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


def compact_timestamp(value: str) -> str:
    return value.replace("-", "").replace(":", "").replace("+00:00", "Z")


def pretty_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n"


def jsonl_line(record: dict[str, Any]) -> str:
    return json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n"


def jsonl(records: Iterable[dict[str, Any]]) -> str:
    return "".join(jsonl_line(record) for record in records)


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def write_bytes_atomic(path: Path, data: bytes, tmp_dir: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_dir.mkdir(parents=True, exist_ok=True)
    tmp = tmp_dir / (path.name + ".part")
    tmp.write_bytes(data)
    tmp.replace(path)


def progress_log(message: str) -> None:
    """Emit a memory-friendly progress line to stderr (never stdout)."""
    print(message, file=sys.stderr, flush=True)


def validate_interval(interval: str) -> str:
    if interval not in ALLOWED_INTERVALS:
        raise KlinesCommandError(
            f"unsupported interval: {interval!r}; "
            f"allowed intervals: {' '.join(ALLOWED_INTERVALS)}"
        )
    return interval


def parse_checksum_text(text: str) -> str:
    """Binance `.CHECKSUM` files hold ``<sha256>  <filename>``."""
    token = text.strip().split()
    if not token:
        raise KlinesError("empty checksum file")
    return token[0].strip().lower()


def verify_bytes(zip_bytes: bytes, checksum_text: str) -> tuple[bool, str, str]:
    expected = parse_checksum_text(checksum_text)
    actual = sha256_hex(zip_bytes)
    return actual == expected, expected, actual


def parse_archive_period(zip_name: str, symbol: str, interval: str) -> str:
    prefix = f"{symbol}-{interval}-"
    if not (zip_name.startswith(prefix) and zip_name.endswith(".zip")):
        raise KlinesError(f"unexpected archive file name: {zip_name}")
    return zip_name[len(prefix):-len(".zip")]


def period_bounds(period: str) -> tuple[str, str]:
    """Return (start_date, end_date) as ``YYYY-MM-DD`` for a monthly/daily period."""
    parts = period.split("-")
    if len(parts) == 2:  # monthly YYYY-MM
        year, month = int(parts[0]), int(parts[1])
        last = calendar.monthrange(year, month)[1]
        return f"{period}-01", f"{period}-{last:02d}"
    return period, period  # daily YYYY-MM-DD


def month_of(period: str) -> str:
    return period[:7]  # YYYY-MM


def bounded_map(
    func: Callable[[Any], Any], items: Iterable[Any], workers: int
) -> Iterator[tuple[Any, Any]]:
    """Yield ``(item, func(item))`` as each completes, keeping at most ``workers``
    tasks in flight so the caller can consume and discard results incrementally.

    Unlike ``Executor.map``, this never submits the whole work-list up front and
    never buffers all results — essential for bounded memory over ~10^5 items.
    Exceptions from ``func`` propagate to the caller (fail loud)."""
    workers = max(1, workers)
    iterator = iter(items)
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
        pending: dict[concurrent.futures.Future, Any] = {}
        for _ in range(workers):
            try:
                item = next(iterator)
            except StopIteration:
                break
            pending[pool.submit(func, item)] = item
        while pending:
            done, _ = concurrent.futures.wait(
                pending, return_when=concurrent.futures.FIRST_COMPLETED
            )
            for future in done:
                item = pending.pop(future)
                result = future.result()  # re-raises on failure
                yield item, result
                try:
                    nxt = next(iterator)
                except StopIteration:
                    continue
                pending[pool.submit(func, nxt)] = nxt


# --------------------------------------------------------------------------- #
# Archive backends
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class ObjectEntry:
    key: str
    size: int


class ArchiveBackend:
    """Read access to the Binance Data Vision archive (or a local emulation)."""

    def iter_objects(self, prefix: str) -> Iterator[tuple[ObjectEntry | None, str | None]]:
        """Yield ``(object_entry, common_prefix)`` for an S3-style listing, one
        item at a time, transparently across pages. Exactly one element of each
        tuple is non-None."""
        raise NotImplementedError

    def list_objects(self, prefix: str) -> tuple[list[ObjectEntry], list[str]]:
        contents: list[ObjectEntry] = []
        prefixes: list[str] = []
        for entry, common in self.iter_objects(prefix):
            if entry is not None:
                contents.append(entry)
            if common is not None:
                prefixes.append(common)
        return contents, prefixes

    def download(self, key: str) -> bytes:
        """Return the bytes of one archive object. Raise ObjectNotFound / Transient."""
        raise NotImplementedError


class HttpArchiveBackend(ArchiveBackend):
    """Real backend backed by the public Binance Data Vision S3 bucket.

    Listings are paginated; each page's XML is parsed and discarded before the
    next page is fetched, so no single huge XML tree or object list is retained.
    """

    def __init__(self, timeout: int = 30, retries: int = 3) -> None:
        self.timeout = timeout
        self.retries = max(0, retries)

    def _get(self, url: str) -> bytes:
        request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                if response.status != 200:
                    raise TransientDownloadError(f"HTTP {response.status} for {url}")
                return response.read()
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                raise ObjectNotFound(url) from exc
            raise TransientDownloadError(f"HTTP {exc.code} for {url}") from exc
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            raise TransientDownloadError(f"{type(exc).__name__}: {exc}") from exc

    def _get_with_retries(self, url: str) -> bytes:
        attempt = 0
        while True:
            try:
                return self._get(url)
            except TransientDownloadError:
                if attempt >= self.retries:
                    raise
                attempt += 1
                time.sleep(min(2.0**attempt, 8.0))

    def iter_objects(self, prefix: str) -> Iterator[tuple[ObjectEntry | None, str | None]]:
        marker = ""
        while True:
            query = urllib.parse.urlencode(
                {"delimiter": "/", "prefix": prefix, "marker": marker}
            )
            xml_bytes = self._get_with_retries(f"{S3_LIST_ENDPOINT}?{query}")
            root = ET.fromstring(xml_bytes)
            last_key = ""
            for node in root.findall("s3:Contents", S3_NAMESPACE):
                key = node.findtext("s3:Key", default="", namespaces=S3_NAMESPACE)
                size = node.findtext("s3:Size", default="0", namespaces=S3_NAMESPACE)
                last_key = key
                yield ObjectEntry(key, int(size)), None
            for node in root.findall("s3:CommonPrefixes", S3_NAMESPACE):
                yield None, node.findtext(
                    "s3:Prefix", default="", namespaces=S3_NAMESPACE
                )
            truncated = root.findtext(
                "s3:IsTruncated", default="false", namespaces=S3_NAMESPACE
            )
            next_marker = root.findtext(
                "s3:NextMarker", default="", namespaces=S3_NAMESPACE
            )
            root.clear()  # release the page's XML tree before the next fetch
            if truncated != "true":
                break
            marker = next_marker or last_key
            if not marker:
                break

    def download(self, key: str) -> bytes:
        url = f"{DATA_VISION_BASE}/{urllib.parse.quote(key)}"
        return self._get(url)


class LocalArchiveBackend(ArchiveBackend):
    """Filesystem emulation of the archive, rooted at a directory that mirrors
    ``data/futures/um/<source>/klines/<SYMBOL>/<INTERVAL>/``.

    Enables fully offline, deterministic discovery / download / verify in tests
    and offline runs (selected via ``--archive-root``)."""

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)

    def iter_objects(self, prefix: str) -> Iterator[tuple[ObjectEntry | None, str | None]]:
        base = self.root / prefix
        if base.is_dir():
            for child in sorted(base.iterdir(), key=lambda p: p.name):
                if child.is_dir():
                    yield None, f"{prefix}{child.name}/"
                else:
                    yield ObjectEntry(f"{prefix}{child.name}", child.stat().st_size), None

    def download(self, key: str) -> bytes:
        path = self.root / key
        if not path.exists():
            raise ObjectNotFound(key)
        return path.read_bytes()


def download_with_retries(
    backend: ArchiveBackend,
    key: str,
    retries: int,
    sleep: Callable[[float], None] = time.sleep,
) -> tuple[bytes, int]:
    """Download one object, retrying transient errors. Returns (bytes, retry_count).

    ``ObjectNotFound`` is never retried (the object genuinely does not exist)."""
    attempt = 0
    while True:
        try:
            return backend.download(key), attempt
        except ObjectNotFound:
            raise
        except TransientDownloadError:
            if attempt >= retries:
                raise
            attempt += 1
            sleep(min(2.0**attempt, 8.0))


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class Config:
    interval: str
    local_root: Path
    archive_source: str  # monthly | daily | both
    include_full_daily_history: bool
    resume: bool
    dry_run: bool
    workers: int
    timeout: int
    retries: int
    symbols_file: Path | None
    max_symbols: int | None

    @property
    def use_monthly(self) -> bool:
        return self.archive_source in ("monthly", "both")

    @property
    def use_daily(self) -> bool:
        return self.archive_source in ("daily", "both")

    @property
    def interval_root(self) -> Path:
        return self.local_root / f"interval={self.interval}"

    @property
    def variant_id(self) -> str:
        return f"{DATASET_ID}.{self.interval}"

    @property
    def catalog_dir(self) -> Path:
        return self.interval_root / "catalog"

    @property
    def manifest_dir(self) -> Path:
        return self.interval_root / "manifests"

    @property
    def reports_dir(self) -> Path:
        return self.interval_root / "reports"

    @property
    def tmp_dir(self) -> Path:
        return self.interval_root / "tmp"

    def raw_zip_path(self, source: str, symbol: str, zip_name: str) -> Path:
        return self.interval_root / "raw" / source / symbol / zip_name

    def checksum_path(self, source: str, symbol: str, checksum_name: str) -> Path:
        return self.interval_root / "checksums" / source / symbol / checksum_name


# --------------------------------------------------------------------------- #
# Symbol + per-symbol file discovery (each bounded in memory)
# --------------------------------------------------------------------------- #


def discover_symbols(backend: ArchiveBackend, source: str, interval: str) -> list[str]:
    prefix = ARCHIVE_PREFIX.format(source=source) + "/"
    symbols = []
    for _, common in backend.iter_objects(prefix):
        if common:
            symbols.append(common.rstrip("/").rsplit("/", 1)[-1])
    return symbols


def resolve_symbols(config: Config, backend: ArchiveBackend) -> list[str]:
    if config.symbols_file is not None:
        symbols = read_symbols_file(config.symbols_file)
    else:
        found: set[str] = set()
        if config.use_monthly:
            found.update(discover_symbols(backend, "monthly", config.interval))
        if config.use_daily:
            found.update(discover_symbols(backend, "daily", config.interval))
        symbols = sorted(found)
    symbols = sorted(set(symbols))
    if config.max_symbols is not None:
        symbols = symbols[: config.max_symbols]
    return symbols


def read_symbols_file(path: Path) -> list[str]:
    text = path.read_text(encoding="utf-8")
    stripped = text.strip()
    if stripped.startswith("["):
        data = json.loads(stripped)
        if not isinstance(data, list):
            raise KlinesCommandError("symbols file JSON must be an array")
        return [str(item) for item in data]
    symbols = []
    for line in text.splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            symbols.append(line)
    return symbols


def list_symbol_archive_files(
    config: Config,
    backend: ArchiveBackend,
    source: str,
    symbol: str,
    discovered_at: str,
) -> list[dict[str, Any]]:
    """List one symbol's archive files for one source. Memory is bounded by this
    symbol's file count (streamed page by page, no global accumulation)."""
    prefix = f"{ARCHIVE_PREFIX.format(source=source)}/{symbol}/{config.interval}/"
    records: list[dict[str, Any]] = []
    checksum_keys: set[str] = set()
    pending_zip: list[ObjectEntry] = []
    for entry, _ in backend.iter_objects(prefix):
        if entry is None:
            continue
        if entry.key.endswith(".zip.CHECKSUM"):
            checksum_keys.add(entry.key)
        elif entry.key.endswith(".zip"):
            pending_zip.append(entry)
    for entry in pending_zip:
        zip_name = entry.key.rsplit("/", 1)[-1]
        checksum_key = entry.key + ".CHECKSUM"
        period = parse_archive_period(zip_name, symbol, config.interval)
        records.append(
            {
                "symbol": symbol,
                "interval": config.interval,
                "archive_package_source": source,
                "archive_period": period,
                "zip_name": zip_name,
                "checksum_name": zip_name + ".CHECKSUM",
                "source_path": entry.key,
                "source_checksum_path": checksum_key,
                "local_zip_path": str(config.raw_zip_path(source, symbol, zip_name)),
                "local_checksum_path": str(
                    config.checksum_path(source, symbol, zip_name + ".CHECKSUM")
                ),
                "remote_size": entry.size,
                "has_remote_checksum": checksum_key in checksum_keys,
                "discovered_at": discovered_at,
            }
        )
    return records


def classify_and_decide(record: dict[str, Any], monthly_months: set[str], config: Config) -> None:
    """Attach daily overlap classification and the download decision to a record."""
    source = record["archive_package_source"]
    if source == "monthly":
        record["covered_by_monthly"] = None
        record["daily_status"] = None
        record["download_decision"] = DOWNLOAD
        return
    covered = month_of(record["archive_period"]) in monthly_months
    record["covered_by_monthly"] = covered
    if not covered:
        record["daily_status"] = DAILY_REQUIRED_DELTA
        record["download_decision"] = DOWNLOAD
    elif config.include_full_daily_history:
        record["daily_status"] = DAILY_INCLUDED_FULL
        record["download_decision"] = DOWNLOAD
    else:
        record["daily_status"] = DAILY_SKIPPED_BY_DEFAULT
        record["download_decision"] = SKIP


def symbol_records(
    config: Config, backend: ArchiveBackend, symbol: str, discovered_at: str
) -> list[dict[str, Any]]:
    """All archive records for one symbol (monthly + classified daily)."""
    monthly_records: list[dict[str, Any]] = []
    daily_records: list[dict[str, Any]] = []
    if config.use_monthly:
        monthly_records = list_symbol_archive_files(
            config, backend, "monthly", symbol, discovered_at
        )
    if config.use_daily:
        daily_records = list_symbol_archive_files(
            config, backend, "daily", symbol, discovered_at
        )
    monthly_months = {month_of(rec["archive_period"]) for rec in monthly_records}
    for rec in monthly_records + daily_records:
        classify_and_decide(rec, monthly_months, config)
    return monthly_records + daily_records


# --------------------------------------------------------------------------- #
# Streaming discovery (bounded memory)
# --------------------------------------------------------------------------- #


@dataclass
class DiscoveryAggregate:
    total_file_count: int = 0
    monthly_file_count: int = 0
    daily_file_count: int = 0
    monthly_symbol_count: int = 0
    daily_symbol_count: int = 0
    daily_required_delta_count: int = 0
    daily_skipped_by_default_count: int = 0
    daily_included_full_history_count: int = 0
    planned_download_count: int = 0
    date_min: str | None = None
    date_max: str | None = None

    def add_record(self, record: dict[str, Any]) -> None:
        self.total_file_count += 1
        source = record["archive_package_source"]
        if source == "monthly":
            self.monthly_file_count += 1
        else:
            self.daily_file_count += 1
            status = record.get("daily_status")
            if status == DAILY_REQUIRED_DELTA:
                self.daily_required_delta_count += 1
            elif status == DAILY_SKIPPED_BY_DEFAULT:
                self.daily_skipped_by_default_count += 1
            elif status == DAILY_INCLUDED_FULL:
                self.daily_included_full_history_count += 1
        if record.get("download_decision") == DOWNLOAD:
            self.planned_download_count += 1
        start, end = period_bounds(record["archive_period"])
        if self.date_min is None or start < self.date_min:
            self.date_min = start
        if self.date_max is None or end > self.date_max:
            self.date_max = end


def stream_discover(
    config: Config,
    backend: ArchiveBackend,
    discovered_at: str,
    sink: Callable[[dict[str, Any]], None] | None,
    progress: Callable[[str], None] | None = None,
) -> tuple[DiscoveryAggregate, list[str]]:
    """Discover the full archive inventory, calling ``sink`` once per record.

    Only aggregate counters and the (bounded) symbol list are retained; the
    record stream is never accumulated. At most ``workers`` symbols are listed
    concurrently. Listing failures propagate as ``KlinesError`` (fail loud)."""
    try:
        symbols = resolve_symbols(config, backend)
    except (TransientDownloadError, ObjectNotFound, OSError) as exc:
        raise KlinesError(f"discovery failed resolving symbols: {exc}") from exc

    if progress:
        progress(
            f"[discover {config.archive_source}] interval={config.interval} "
            f"symbols={len(symbols)} (monthly={'y' if config.use_monthly else 'n'} "
            f"daily={'y' if config.use_daily else 'n'})"
        )

    aggregate = DiscoveryAggregate()
    processed = 0

    def worker(symbol: str) -> list[dict[str, Any]]:
        try:
            return symbol_records(config, backend, symbol, discovered_at)
        except (TransientDownloadError, ObjectNotFound, OSError) as exc:
            raise KlinesError(
                f"discovery failed listing symbol {symbol!r}: {exc}"
            ) from exc

    for symbol, records in bounded_map(worker, symbols, config.workers):
        had_monthly = had_daily = False
        for record in records:
            if record["archive_package_source"] == "monthly":
                had_monthly = True
            else:
                had_daily = True
            aggregate.add_record(record)
            if sink is not None:
                sink(record)
        if had_monthly:
            aggregate.monthly_symbol_count += 1
        if had_daily:
            aggregate.daily_symbol_count += 1
        processed += 1
        if progress and (processed % PROGRESS_EVERY == 0 or processed == len(symbols)):
            progress(
                f"[discover {config.archive_source}] {processed}/{len(symbols)} symbols "
                f"files={aggregate.total_file_count} last={symbol}"
            )
    return aggregate, symbols


def discover(
    config: Config, backend: ArchiveBackend, discovered_at: str
) -> tuple[list[dict[str, Any]], list[str]]:
    """In-memory discovery helper (small inputs / tests). Collects the streamed
    records into a sorted list."""
    records: list[dict[str, Any]] = []
    _, symbols = stream_discover(config, backend, discovered_at, records.append)
    records.sort(key=_record_sort_key)
    return records, symbols


def _record_sort_key(record: dict[str, Any]) -> tuple[str, str, str]:
    return (
        record["symbol"],
        record["archive_package_source"],
        record["archive_period"],
    )


# --------------------------------------------------------------------------- #
# Download / verify (per-record; bounded memory)
# --------------------------------------------------------------------------- #


def process_record(
    config: Config, backend: ArchiveBackend, record: dict[str, Any]
) -> dict[str, Any]:
    record.setdefault("source_checksum_path", record["source_path"] + ".CHECKSUM")
    record.setdefault("retry_count", 0)
    record.setdefault("skip_reason", None)
    record.setdefault("file_size", None)

    if record["download_decision"] == SKIP:
        record["download_status"] = DL_SKIPPED_OVERLAP
        record["checksum_status"] = CHK_NOT_ATTEMPTED
        record["skip_reason"] = record.get("daily_status")
        return record

    zip_path = Path(record["local_zip_path"])
    checksum_path = Path(record["local_checksum_path"])

    # Resume / skip already-verified files (idempotent re-verification).
    if zip_path.exists() and checksum_path.exists():
        try:
            ok, _, _ = verify_bytes(
                zip_path.read_bytes(),
                checksum_path.read_text(encoding="utf-8"),
            )
        except (OSError, KlinesError):
            ok = False
        if ok:
            record["download_status"] = DL_SKIPPED_EXISTING
            record["checksum_status"] = CHK_SKIPPED_EXISTING
            record["file_size"] = zip_path.stat().st_size
            record["skip_reason"] = "already_verified"
            return record

    # Download checksum first, then the zip.
    try:
        checksum_bytes, c_retry = download_with_retries(
            backend, record["source_checksum_path"], config.retries
        )
    except ObjectNotFound:
        record["download_status"] = DL_FAILED
        record["checksum_status"] = CHK_MISSING_CHECKSUM
        return record
    except TransientDownloadError:
        record["download_status"] = DL_FAILED
        record["checksum_status"] = CHK_DOWNLOAD_FAILED
        record["retry_count"] = config.retries
        return record

    try:
        zip_bytes, z_retry = download_with_retries(
            backend, record["source_path"], config.retries
        )
    except ObjectNotFound:
        record["download_status"] = DL_FAILED
        record["checksum_status"] = CHK_MISSING_ZIP
        record["retry_count"] = c_retry
        return record
    except TransientDownloadError:
        record["download_status"] = DL_FAILED
        record["checksum_status"] = CHK_DOWNLOAD_FAILED
        record["retry_count"] = config.retries
        return record

    record["retry_count"] = c_retry + z_retry
    ok, expected, actual = verify_bytes(zip_bytes, checksum_bytes.decode("utf-8"))
    write_bytes_atomic(checksum_path, checksum_bytes, config.tmp_dir)
    write_bytes_atomic(zip_path, zip_bytes, config.tmp_dir)
    record["file_size"] = len(zip_bytes)
    record["download_status"] = DL_DOWNLOADED
    if ok:
        record["checksum_status"] = CHK_PASSED
    else:
        record["checksum_status"] = CHK_FAILED
        record["expected_checksum"] = expected
        record["actual_checksum"] = actual
    return record


def verify_record(record: dict[str, Any]) -> dict[str, Any]:
    """Re-verify a single record's local zip against its stored checksum."""
    zip_path = Path(record["local_zip_path"])
    checksum_path = Path(record["local_checksum_path"])
    if not zip_path.exists():
        if record.get("download_decision") == SKIP:
            record.setdefault("download_status", DL_SKIPPED_OVERLAP)
            record.setdefault("checksum_status", CHK_NOT_ATTEMPTED)
            return record
        record["checksum_status"] = CHK_MISSING_ZIP
        record["download_status"] = DL_NOT_ATTEMPTED
        return record
    if not checksum_path.exists():
        record["checksum_status"] = CHK_MISSING_CHECKSUM
        return record
    ok, _, _ = verify_bytes(
        zip_path.read_bytes(), checksum_path.read_text(encoding="utf-8")
    )
    record["checksum_status"] = CHK_PASSED if ok else CHK_FAILED
    record["download_status"] = record.get("download_status") or DL_SKIPPED_EXISTING
    record["file_size"] = zip_path.stat().st_size
    return record


# --------------------------------------------------------------------------- #
# Field projections
# --------------------------------------------------------------------------- #

CATALOG_FIELDS = (
    "symbol",
    "interval",
    "archive_package_source",
    "archive_period",
    "zip_name",
    "checksum_name",
    "source_path",
    "source_checksum_path",
    "local_zip_path",
    "local_checksum_path",
    "covered_by_monthly",
    "daily_status",
    "download_decision",
    "discovered_at",
)

FILE_MANIFEST_FIELDS = (
    "symbol",
    "interval",
    "archive_package_source",
    "archive_period",
    "zip_name",
    "checksum_name",
    "source_path",
    "local_zip_path",
    "local_checksum_path",
    "checksum_status",
    "download_status",
    "file_size",
    "retry_count",
    "skip_reason",
)


def _project(record: dict[str, Any], fields: tuple[str, ...]) -> dict[str, Any]:
    return {key: record.get(key) for key in fields}


def daily_delta_policy(config: Config) -> dict[str, Any]:
    return {
        "monthly_is_historical_base": True,
        "daily_is_recent_delta": True,
        "default_skips_daily_when_covered_by_monthly": True,
        "include_full_daily_history": config.include_full_daily_history,
        "prefer_monthly_for_canonical_coverage": True,
    }


# --------------------------------------------------------------------------- #
# Catalog writers (jsonl streamed; small json files written last)
# --------------------------------------------------------------------------- #


def discovery_summary_from(
    config: Config,
    aggregate: DiscoveryAggregate,
    symbols: list[str],
    discovered_at: str,
) -> dict[str, Any]:
    return {
        "interval": config.interval,
        "dataset_id": DATASET_ID,
        "dataset_variant_id": config.variant_id,
        "archive_source": config.archive_source,
        "include_full_daily_history": config.include_full_daily_history,
        "discovered_symbol_count": len(symbols),
        "monthly_archive_symbol_count": aggregate.monthly_symbol_count,
        "daily_archive_symbol_count": aggregate.daily_symbol_count,
        "total_archive_file_count": aggregate.total_file_count,
        "monthly_archive_file_count": aggregate.monthly_file_count,
        "daily_archive_file_count": aggregate.daily_file_count,
        "daily_required_delta_count": aggregate.daily_required_delta_count,
        "daily_skipped_by_default_count": aggregate.daily_skipped_by_default_count,
        "daily_included_full_history_count": aggregate.daily_included_full_history_count,
        "planned_download_count": aggregate.planned_download_count,
        "date_min": aggregate.date_min,
        "date_max": aggregate.date_max,
        "daily_delta_policy": daily_delta_policy(config),
        "discovered_at": discovered_at,
    }


def write_symbols_json(config: Config, symbols: list[str], discovered_at: str) -> None:
    write_text(
        config.catalog_dir / "symbols.json",
        pretty_json(
            {
                "interval": config.interval,
                "dataset_id": DATASET_ID,
                "dataset_variant_id": config.variant_id,
                "archive_source": config.archive_source,
                "symbol_count": len(symbols),
                "symbols": symbols,
                "discovered_at": discovered_at,
            }
        ),
    )


def load_symbols(config: Config) -> list[str]:
    path = config.catalog_dir / "symbols.json"
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8")).get("symbols", [])
    return []


# --------------------------------------------------------------------------- #
# Processing aggregate (download / verify / report)
# --------------------------------------------------------------------------- #


@dataclass
class ProcessAggregate:
    file_count: int = 0
    downloaded: int = 0
    skipped_existing: int = 0
    skipped_overlap: int = 0
    verified: int = 0
    checksum_failed: int = 0
    missing: int = 0
    failed: int = 0
    total_bytes: int = 0
    date_min: str | None = None
    date_max: str | None = None
    sources: set[str] = field(default_factory=set)
    known_gaps: list[str] = field(default_factory=list)

    def add(self, record: dict[str, Any]) -> None:
        self.file_count += 1
        self.sources.add(record["archive_package_source"])
        ds = record.get("download_status")
        cs = record.get("checksum_status")
        if ds == DL_DOWNLOADED:
            self.downloaded += 1
        elif ds == DL_SKIPPED_EXISTING:
            self.skipped_existing += 1
        elif ds == DL_SKIPPED_OVERLAP:
            self.skipped_overlap += 1
        elif ds == DL_FAILED:
            self.failed += 1
        if cs in (CHK_PASSED, CHK_SKIPPED_EXISTING):
            self.verified += 1
        if cs == CHK_FAILED:
            self.checksum_failed += 1
        if cs in (CHK_MISSING_ZIP, CHK_MISSING_CHECKSUM):
            self.missing += 1
        self.total_bytes += record.get("file_size") or 0
        start, end = period_bounds(record["archive_period"])
        if self.date_min is None or start < self.date_min:
            self.date_min = start
        if self.date_max is None or end > self.date_max:
            self.date_max = end
        if cs in (CHK_FAILED, CHK_MISSING_ZIP, CHK_MISSING_CHECKSUM, CHK_DOWNLOAD_FAILED):
            if len(self.known_gaps) < KNOWN_GAPS_CAP:
                self.known_gaps.append(
                    f"{record['symbol']} {record['archive_period']} "
                    f"({record['archive_package_source']}): {cs}"
                )

    @property
    def skipped(self) -> int:
        return self.skipped_existing + self.skipped_overlap

    def is_gap(self, record: dict[str, Any]) -> bool:
        return record.get("checksum_status") in (
            CHK_FAILED, CHK_MISSING_ZIP, CHK_MISSING_CHECKSUM, CHK_DOWNLOAD_FAILED,
        ) or record.get("download_status") == DL_FAILED


def load_jsonl(path: Path) -> list[dict[str, Any]]:
    """Read a whole jsonl file into a list. Use only for small files / tests;
    the pipeline itself streams via ``iter_catalog`` / ``iter_file_manifest``."""
    if not path.exists():
        return []
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def iter_catalog(config: Config) -> Iterator[dict[str, Any]]:
    path = config.catalog_dir / "archive_files.jsonl"
    if not path.exists():
        return
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                record = json.loads(line)
                record.setdefault(
                    "source_checksum_path", record["source_path"] + ".CHECKSUM"
                )
                yield record


def iter_file_manifest(config: Config) -> Iterator[dict[str, Any]]:
    path = config.manifest_dir / "files.jsonl"
    if not path.exists():
        return
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                yield json.loads(line)


def stream_process(
    config: Config,
    backend: ArchiveBackend,
    *,
    mode: str,
    progress: Callable[[str], None] | None = None,
) -> ProcessAggregate:
    """Process the catalog (``mode='download'``) or re-verify (``mode='verify'``),
    writing files.jsonl + missing/checksum-failure reports incrementally and
    retaining only aggregate counters."""
    aggregate = ProcessAggregate()
    config.manifest_dir.mkdir(parents=True, exist_ok=True)
    config.reports_dir.mkdir(parents=True, exist_ok=True)
    files_path = config.manifest_dir / "files.jsonl"
    missing_path = config.reports_dir / "missing_files.jsonl"
    failures_path = config.reports_dir / "checksum_failures.jsonl"

    def handle(record: dict[str, Any]) -> dict[str, Any]:
        if mode == "download":
            return process_record(config, backend, record)
        return verify_record(record)

    with files_path.open("w", encoding="utf-8") as files_out, \
            missing_path.open("w", encoding="utf-8") as missing_out, \
            failures_path.open("w", encoding="utf-8") as failures_out:
        for _, record in bounded_map(handle, iter_catalog(config), config.workers):
            aggregate.add(record)
            projected = _project(record, FILE_MANIFEST_FIELDS)
            files_out.write(jsonl_line(projected))
            if record.get("checksum_status") == CHK_FAILED:
                failures_out.write(jsonl_line(projected))
            if aggregate.is_gap(record):
                missing_out.write(jsonl_line(projected))
            if progress and aggregate.file_count % 500 == 0:
                progress(
                    f"[{mode}] files={aggregate.file_count} "
                    f"verified={aggregate.verified} failed={aggregate.checksum_failed}"
                )
    return aggregate


def stream_recount(config: Config) -> ProcessAggregate:
    """Recompute the processing aggregate + reports from an existing files.jsonl."""
    aggregate = ProcessAggregate()
    config.reports_dir.mkdir(parents=True, exist_ok=True)
    missing_path = config.reports_dir / "missing_files.jsonl"
    failures_path = config.reports_dir / "checksum_failures.jsonl"
    with missing_path.open("w", encoding="utf-8") as missing_out, \
            failures_path.open("w", encoding="utf-8") as failures_out:
        for record in iter_file_manifest(config):
            aggregate.add(record)
            if record.get("checksum_status") == CHK_FAILED:
                failures_out.write(jsonl_line(record))
            if aggregate.is_gap(record):
                missing_out.write(jsonl_line(record))
    return aggregate


# --------------------------------------------------------------------------- #
# Manifest + reports (built from aggregates)
# --------------------------------------------------------------------------- #


def build_manifest(
    config: Config,
    aggregate: ProcessAggregate,
    symbol_count: int,
    run_id: str,
    generated_at: str,
) -> dict[str, Any]:
    return {
        "dataset_id": DATASET_ID,
        "dataset_variant_id": config.variant_id,
        "dataset_version": DATASET_VERSION,
        "code_version": CODE_VERSION,
        "interval": config.interval,
        "local_root": str(config.interval_root),
        "run_id": run_id,
        "generated_at": generated_at,
        "primary_key": list(PRIMARY_KEY),
        "symbol_count": symbol_count,
        "file_count": aggregate.file_count,
        "downloaded_count": aggregate.downloaded,
        "verified_count": aggregate.verified,
        "skipped_count": aggregate.skipped,
        "failed_count": aggregate.failed,
        "checksum_failed_count": aggregate.checksum_failed,
        "missing_count": aggregate.missing,
        "skipped_daily_overlap_count": aggregate.skipped_overlap,
        "total_bytes": aggregate.total_bytes,
        "date_min": aggregate.date_min,
        "date_max": aggregate.date_max,
        "archive_package_sources": sorted(aggregate.sources),
        "include_full_daily_history": config.include_full_daily_history,
        "daily_delta_policy": daily_delta_policy(config),
        "file_manifest": str(config.manifest_dir / "files.jsonl"),
        "coverage_report": str(config.reports_dir / "coverage_summary.json"),
        "research_access": str(config.catalog_dir / "research_access.json"),
        "validation_summary": None,
    }


def write_manifest_json(config: Config, manifest: dict[str, Any]) -> None:
    write_text(config.manifest_dir / "manifest.json", pretty_json(manifest))


def build_coverage_summary(
    config: Config,
    manifest: dict[str, Any],
    aggregate: ProcessAggregate,
    discovery_summary: dict[str, Any],
) -> dict[str, Any]:
    gaps = list(aggregate.known_gaps)
    if aggregate.missing + aggregate.checksum_failed + aggregate.failed > len(gaps):
        gaps.append(
            f"... ({aggregate.missing + aggregate.checksum_failed + aggregate.failed} "
            f"total gap files; see reports/missing_files.jsonl)"
        )
    return {
        "interval": config.interval,
        "dataset_id": DATASET_ID,
        "dataset_variant_id": config.variant_id,
        "discovered_symbol_count": discovery_summary.get(
            "discovered_symbol_count", manifest["symbol_count"]
        ),
        "monthly_archive_symbol_count": discovery_summary.get(
            "monthly_archive_symbol_count", 0
        ),
        "daily_archive_symbol_count": discovery_summary.get(
            "daily_archive_symbol_count", 0
        ),
        "total_archive_file_count": manifest["file_count"],
        "verified_file_count": aggregate.verified,
        "failed_file_count": aggregate.failed,
        "checksum_failed_count": aggregate.checksum_failed,
        "missing_count": aggregate.missing,
        "skipped_daily_overlap_count": aggregate.skipped_overlap,
        "date_min": manifest["date_min"],
        "date_max": manifest["date_max"],
        "known_gaps": gaps,
    }


def write_reports(
    config: Config,
    manifest: dict[str, Any],
    aggregate: ProcessAggregate,
    discovery_summary: dict[str, Any],
) -> dict[str, Any]:
    coverage = build_coverage_summary(config, manifest, aggregate, discovery_summary)
    write_text(config.reports_dir / "coverage_summary.json", pretty_json(coverage))
    write_text(
        config.reports_dir / "run_summary.md", render_run_summary(manifest, coverage)
    )
    return coverage


def render_run_summary(manifest: dict[str, Any], coverage: dict[str, Any]) -> str:
    lines = [
        f"# Binance USD-M Klines Run Summary — interval={manifest['interval']}",
        "",
        f"- dataset_id: `{manifest['dataset_id']}`",
        f"- dataset_variant_id: `{manifest['dataset_variant_id']}`",
        f"- run_id: `{manifest['run_id']}`",
        f"- generated_at: {manifest['generated_at']}",
        f"- local_root: `{manifest['local_root']}`",
        f"- primary_key: {' + '.join(manifest['primary_key'])}",
        "",
        "## Counts",
        "",
        f"- symbol_count: {manifest['symbol_count']}",
        f"- file_count: {manifest['file_count']}",
        f"- downloaded_count: {manifest['downloaded_count']}",
        f"- verified_count: {manifest['verified_count']}",
        f"- skipped_count: {manifest['skipped_count']}",
        f"- skipped_daily_overlap_count: {manifest['skipped_daily_overlap_count']}",
        f"- failed_count: {manifest['failed_count']}",
        f"- checksum_failed_count: {manifest['checksum_failed_count']}",
        f"- missing_count: {manifest['missing_count']}",
        f"- total_bytes: {manifest['total_bytes']}",
        f"- date_min: {manifest['date_min']}",
        f"- date_max: {manifest['date_max']}",
        "",
        f"Archive package sources: {', '.join(manifest['archive_package_sources']) or 'none'}",
        f"include_full_daily_history: {manifest['include_full_daily_history']}",
        "",
        f"Known gaps: {len(coverage['known_gaps'])}",
        "",
    ]
    return "\n".join(lines)


def write_research_access(
    config: Config, manifest: dict[str, Any], generated_at: str
) -> None:
    research = {
        "dataset_id": DATASET_ID,
        "dataset_variant_id": config.variant_id,
        "interval": config.interval,
        "supported_intervals": list(ALLOWED_INTERVALS),
        "first_production_interval": "1d",
        "primary_key": list(PRIMARY_KEY),
        "local_root": str(config.interval_root),
        "manifest_path": str(config.manifest_dir / "manifest.json"),
        "file_manifest_path": str(config.manifest_dir / "files.jsonl"),
        "catalog_path": str(config.catalog_dir / "archive_files.jsonl"),
        "coverage_report_path": str(config.reports_dir / "coverage_summary.json"),
        "raw_layout": {
            "monthly": str(config.interval_root / "raw" / "monthly" / "<SYMBOL>"),
            "daily": str(config.interval_root / "raw" / "daily" / "<SYMBOL>"),
            "checksums": str(config.interval_root / "checksums" / "<SOURCE>" / "<SYMBOL>"),
        },
        "schema": {
            "columns": list(KLINE_COLUMNS),
            "open_time_unit": "milliseconds since epoch (UTC)",
            "header_row": "absent in older archives; may be present in newer files",
        },
        "kline_interval_vs_archive_package_source": (
            "Kline interval (1d/4h/1h/15m/5m/1m) is the row period; archive "
            "package source (monthly/daily) is only how Binance packages files."
        ),
        "locate_files_by_symbol": (
            "Filter file_manifest by `symbol`; raw zips live at "
            "raw/<source>/<symbol>/<symbol>-<interval>-<period>.zip"
        ),
        "locate_files_by_interval": (
            "Each interval has its own local_root: "
            "local_data/binance_um_klines/interval=<INTERVAL>/"
        ),
        "check_coverage": str(config.reports_dir / "coverage_summary.json"),
        "check_checksum_status": (
            "Read file_manifest `checksum_status`; only `passed` / "
            "`skipped_existing_verified` are verified."
        ),
        "warning": "local_data/ is NOT committed to Git. Files exist only on the machine that ran the pipeline.",
        "current_universe_warning": (
            "Do not assume the current active universe equals the historical "
            "universe; the archive includes delisted symbols."
        ),
        "generated_at": generated_at,
        "summary_counts": {
            "symbol_count": manifest["symbol_count"],
            "file_count": manifest["file_count"],
            "verified_count": manifest["verified_count"],
            "checksum_failed_count": manifest["checksum_failed_count"],
        },
    }
    write_text(config.catalog_dir / "research_access.json", pretty_json(research))


# --------------------------------------------------------------------------- #
# Orchestration
# --------------------------------------------------------------------------- #


@dataclass
class RunResult:
    config: Config
    symbols: list[str]
    manifest: dict[str, Any] | None
    coverage: dict[str, Any] | None
    discovery_summary: dict[str, Any]


def load_discovery_summary(config: Config) -> dict[str, Any]:
    path = config.catalog_dir / "discovery_summary.json"
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    return {}


def run_pipeline(
    config: Config,
    backend: ArchiveBackend,
    phases: set[str],
    now: str | None = None,
    progress: Callable[[str], None] | None = None,
) -> RunResult:
    now = now or utc_now()
    run_id = f"{config.interval}-{compact_timestamp(now)}"
    need_fresh_discovery = "discover" in phases or "all" in phases

    # ---- dry-run: discover (no writes), print plan, return ----
    if config.dry_run:
        aggregate, symbols = stream_discover(config, backend, now, None, progress)
        summary = discovery_summary_from(config, aggregate, symbols, now)
        return RunResult(config, symbols, None, None, summary)

    # ---- discovery: stream records to catalog jsonl incrementally ----
    if need_fresh_discovery:
        config.catalog_dir.mkdir(parents=True, exist_ok=True)
        catalog_path = config.catalog_dir / "archive_files.jsonl"
        with catalog_path.open("w", encoding="utf-8") as handle:
            def sink(record: dict[str, Any]) -> None:
                handle.write(jsonl_line(_project(record, CATALOG_FIELDS)))
            aggregate, symbols = stream_discover(config, backend, now, sink, progress)
        write_symbols_json(config, symbols, now)
        summary = discovery_summary_from(config, aggregate, symbols, now)
        write_text(
            config.catalog_dir / "discovery_summary.json", pretty_json(summary)
        )
    else:
        symbols = load_symbols(config)
        summary = load_discovery_summary(config)
        if not (config.catalog_dir / "archive_files.jsonl").exists():
            # No catalog on disk; produce one so downstream phases have input.
            return run_pipeline(config, backend, phases | {"discover"}, now, progress)

    if phases == {"discover"}:
        return RunResult(config, symbols, None, None, summary)

    do_download = "download" in phases or "all" in phases
    do_verify = "verify" in phases or "all" in phases
    do_report = "report" in phases or "all" in phases or do_download or do_verify

    if do_download:
        aggregate_p = stream_process(config, backend, mode="download", progress=progress)
    elif do_verify:
        aggregate_p = stream_process(config, backend, mode="verify", progress=progress)
    else:  # report-only
        aggregate_p = stream_recount(config)

    manifest = build_manifest(config, aggregate_p, len(symbols), run_id, now)
    write_manifest_json(config, manifest)
    coverage = None
    if do_report:
        coverage = write_reports(config, manifest, aggregate_p, summary)
        write_research_access(config, manifest, now)
    return RunResult(config, symbols, manifest, coverage, summary)


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m datahub.ingestion.binance_um_klines",
        description="Binance USD-M Futures Kline historical ingestion pipeline.",
    )
    parser.add_argument(
        "--interval",
        default="1d",
        help=f"Kline interval (default 1d). Allowed: {' '.join(ALLOWED_INTERVALS)}",
    )
    parser.add_argument("--discover", action="store_true", help="discover archive files")
    parser.add_argument("--download", action="store_true", help="download + verify files")
    parser.add_argument("--verify", action="store_true", help="re-verify local files")
    parser.add_argument("--report", action="store_true", help="write coverage reports")
    parser.add_argument("--all", action="store_true", help="discover + download + verify + report")
    parser.add_argument("--resume", action="store_true", help="skip already-verified files")
    parser.add_argument("--dry-run", action="store_true", help="discover + plan only; no downloads, no writes")
    parser.add_argument("--workers", type=int, default=8, help="concurrent workers (default 8)")
    parser.add_argument("--timeout", type=int, default=30, help="per-request timeout seconds (default 30)")
    parser.add_argument("--retries", type=int, default=3, help="transient retry count (default 3)")
    parser.add_argument(
        "--archive-source",
        choices=("monthly", "daily", "both"),
        default="both",
        help="archive package source (default both: monthly base + daily delta)",
    )
    parser.add_argument("--symbols-file", help="path to a symbols allow-list (txt or json array)")
    parser.add_argument("--max-symbols", type=int, help="limit number of symbols (testing / sampling)")
    parser.add_argument(
        "--local-root",
        default=str(DEFAULT_LOCAL_ROOT),
        help="local_data root (default local_data/binance_um_klines)",
    )
    parser.add_argument(
        "--include-full-daily-history",
        action="store_true",
        help="download full historical daily archive even where monthly covers it",
    )
    parser.add_argument(
        "--archive-root",
        help="offline/testing: read archive from a local directory tree instead of HTTP",
    )
    parser.add_argument(
        "--quiet", action="store_true", help="suppress progress logs on stderr"
    )
    return parser


def config_from_args(args: argparse.Namespace) -> Config:
    interval = validate_interval(args.interval)
    return Config(
        interval=interval,
        local_root=Path(args.local_root),
        archive_source=args.archive_source,
        include_full_daily_history=args.include_full_daily_history,
        resume=args.resume,
        dry_run=args.dry_run,
        workers=max(1, args.workers),
        timeout=max(1, args.timeout),
        retries=max(0, args.retries),
        symbols_file=Path(args.symbols_file) if args.symbols_file else None,
        max_symbols=args.max_symbols,
    )


def phases_from_args(args: argparse.Namespace) -> set[str]:
    phases = set()
    if args.all:
        phases.add("all")
    if args.discover:
        phases.add("discover")
    if args.download:
        phases.add("download")
    if args.verify:
        phases.add("verify")
    if args.report:
        phases.add("report")
    return phases


def backend_from_args(args: argparse.Namespace, config: Config) -> ArchiveBackend:
    if args.archive_root:
        return LocalArchiveBackend(args.archive_root)
    return HttpArchiveBackend(timeout=config.timeout, retries=config.retries)


def run_cli(args: argparse.Namespace) -> int:
    config = config_from_args(args)
    phases = phases_from_args(args)
    if not phases and not args.dry_run:
        raise KlinesCommandError(
            "choose at least one of --discover, --download, --verify, --report, --all (or --dry-run)"
        )
    backend = backend_from_args(args, config)
    if not phases and args.dry_run:
        phases = {"discover"}
    progress = None if args.quiet else progress_log

    result = run_pipeline(config, backend, phases, progress=progress)
    print_result(result)

    if result.manifest and result.manifest["checksum_failed_count"] > 0:
        raise KlinesError(
            f"checksum verification failed for "
            f"{result.manifest['checksum_failed_count']} file(s)"
        )
    return 0


def print_result(result: RunResult) -> None:
    config = result.config
    summary = result.discovery_summary
    print(f"interval={config.interval}")
    print(f"dataset_id={DATASET_ID}")
    print(f"dataset_variant_id={config.variant_id}")
    print(f"archive_source={config.archive_source}")
    print(f"local_root={config.interval_root}")
    print(f"discovered_symbol_count={summary.get('discovered_symbol_count')}")
    print(f"total_archive_file_count={summary.get('total_archive_file_count')}")
    print(f"planned_download_count={summary.get('planned_download_count')}")
    print(
        f"daily_skipped_by_default_count={summary.get('daily_skipped_by_default_count')}"
    )
    if config.dry_run:
        print("dry_run=true")
        return
    if result.manifest:
        manifest = result.manifest
        for key in (
            "downloaded_count",
            "verified_count",
            "skipped_count",
            "skipped_daily_overlap_count",
            "failed_count",
            "checksum_failed_count",
            "missing_count",
            "total_bytes",
            "date_min",
            "date_max",
        ):
            print(f"{key}={manifest[key]}")
        print(f"manifest={config.manifest_dir / 'manifest.json'}")


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    try:
        args = parser.parse_args(argv)
        return run_cli(args)
    except KlinesCommandError as exc:
        print(f"klines command error: {exc}", file=sys.stderr)
        return 2
    except KlinesError as exc:
        print(f"klines ingestion failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
