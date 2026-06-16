"""Binance USD-M Futures Kline historical ingestion pipeline.

Parameterized by Kline ``interval`` (1d / 4h / 1h / 15m / 5m / 1m). The first
production interval is ``1d`` but no interval is hard-coded into the pipeline.

Terminology — kept strictly separate throughout this module:

* **Kline interval** — the trading-data period of each row (``1d`` ... ``1m``).
* **Archive package source** — how Binance Data Vision packages files on disk:
  ``monthly`` archive packages (historical base) and ``daily`` archive packages
  (recent delta). ``monthly`` / ``daily`` are *not* Kline intervals.

Large market data is written under ``local_data/`` only and is never committed.
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
from typing import Any, Callable
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


def jsonl(records: list[dict[str, Any]]) -> str:
    return "".join(
        json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n"
        for record in records
    )


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


# --------------------------------------------------------------------------- #
# Archive backends
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class ObjectEntry:
    key: str
    size: int


class ArchiveBackend:
    """Read access to the Binance Data Vision archive (or a local emulation)."""

    def list_objects(self, prefix: str) -> tuple[list[ObjectEntry], list[str]]:
        """Return (contents, common_prefixes) for an S3-style listing."""
        raise NotImplementedError

    def download(self, key: str) -> bytes:
        """Return the bytes of one archive object. Raise ObjectNotFound / Transient."""
        raise NotImplementedError


class HttpArchiveBackend(ArchiveBackend):
    """Real backend backed by the public Binance Data Vision S3 bucket."""

    def __init__(self, timeout: int = 30) -> None:
        self.timeout = timeout

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

    def list_objects(self, prefix: str) -> tuple[list[ObjectEntry], list[str]]:
        contents: list[ObjectEntry] = []
        prefixes: list[str] = []
        marker = ""
        while True:
            query = urllib.parse.urlencode(
                {"delimiter": "/", "prefix": prefix, "marker": marker}
            )
            xml_bytes = self._get(f"{S3_LIST_ENDPOINT}?{query}")
            root = ET.fromstring(xml_bytes)
            for node in root.findall("s3:Contents", S3_NAMESPACE):
                key = node.findtext("s3:Key", default="", namespaces=S3_NAMESPACE)
                size = node.findtext("s3:Size", default="0", namespaces=S3_NAMESPACE)
                contents.append(ObjectEntry(key, int(size)))
            for node in root.findall("s3:CommonPrefixes", S3_NAMESPACE):
                prefixes.append(
                    node.findtext("s3:Prefix", default="", namespaces=S3_NAMESPACE)
                )
            truncated = root.findtext(
                "s3:IsTruncated", default="false", namespaces=S3_NAMESPACE
            )
            if truncated != "true":
                break
            next_marker = root.findtext(
                "s3:NextMarker", default="", namespaces=S3_NAMESPACE
            )
            if not next_marker and contents:
                next_marker = contents[-1].key
            if not next_marker:
                break
            marker = next_marker
        return contents, prefixes

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

    def list_objects(self, prefix: str) -> tuple[list[ObjectEntry], list[str]]:
        base = self.root / prefix
        contents: list[ObjectEntry] = []
        prefixes: list[str] = []
        if base.is_dir():
            for child in sorted(base.iterdir(), key=lambda p: p.name):
                if child.is_dir():
                    prefixes.append(f"{prefix}{child.name}/")
                else:
                    contents.append(
                        ObjectEntry(f"{prefix}{child.name}", child.stat().st_size)
                    )
        return contents, prefixes

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
# Discovery
# --------------------------------------------------------------------------- #


def discover_symbols(backend: ArchiveBackend, source: str, interval: str) -> list[str]:
    prefix = ARCHIVE_PREFIX.format(source=source) + "/"
    _, common = backend.list_objects(prefix)
    return [cp.rstrip("/").rsplit("/", 1)[-1] for cp in common]


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
    prefix = f"{ARCHIVE_PREFIX.format(source=source)}/{symbol}/{config.interval}/"
    contents, _ = backend.list_objects(prefix)
    sizes = {entry.key: entry.size for entry in contents}
    records = []
    for entry in contents:
        if not entry.key.endswith(".zip"):
            continue
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
                "local_zip_path": str(
                    config.raw_zip_path(source, symbol, zip_name)
                ),
                "local_checksum_path": str(
                    config.checksum_path(source, symbol, zip_name + ".CHECKSUM")
                ),
                "remote_size": entry.size,
                "has_remote_checksum": checksum_key in sizes,
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
    # daily archive package
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


def discover(
    config: Config, backend: ArchiveBackend, discovered_at: str
) -> tuple[list[dict[str, Any]], list[str]]:
    symbols = resolve_symbols(config, backend)

    def for_symbol(symbol: str) -> list[dict[str, Any]]:
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
        monthly_months = {
            month_of(rec["archive_period"]) for rec in monthly_records
        }
        for rec in monthly_records + daily_records:
            classify_and_decide(rec, monthly_months, config)
        return monthly_records + daily_records

    records: list[dict[str, Any]] = []
    if symbols:
        max_workers = max(1, min(config.workers, len(symbols)))
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as pool:
            for sym_records in pool.map(for_symbol, symbols):
                records.extend(sym_records)
    records.sort(key=_record_sort_key)
    return records, symbols


def _record_sort_key(record: dict[str, Any]) -> tuple[str, str, str]:
    return (
        record["symbol"],
        record["archive_package_source"],
        record["archive_period"],
    )


# --------------------------------------------------------------------------- #
# Download / verify
# --------------------------------------------------------------------------- #


def process_record(
    config: Config, backend: ArchiveBackend, record: dict[str, Any]
) -> dict[str, Any]:
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


def process_records(
    config: Config, backend: ArchiveBackend, records: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    max_workers = max(1, min(config.workers, max(1, len(records))))
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as pool:
        results = list(
            pool.map(lambda rec: process_record(config, backend, rec), records)
        )
    results.sort(key=_record_sort_key)
    return results


def verify_records(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Re-verify any locally present zip against its stored checksum."""
    for record in records:
        zip_path = Path(record["local_zip_path"])
        checksum_path = Path(record["local_checksum_path"])
        if not zip_path.exists():
            if record["download_decision"] == SKIP:
                continue
            record["checksum_status"] = CHK_MISSING_ZIP
            record["download_status"] = DL_NOT_ATTEMPTED
            continue
        if not checksum_path.exists():
            record["checksum_status"] = CHK_MISSING_CHECKSUM
            continue
        ok, _, _ = verify_bytes(
            zip_path.read_bytes(), checksum_path.read_text(encoding="utf-8")
        )
        record["checksum_status"] = CHK_PASSED if ok else CHK_FAILED
        record["file_size"] = zip_path.stat().st_size
    return records


# --------------------------------------------------------------------------- #
# Catalog / manifest / report writers
# --------------------------------------------------------------------------- #

CATALOG_FIELDS = (
    "symbol",
    "interval",
    "archive_package_source",
    "archive_period",
    "zip_name",
    "checksum_name",
    "source_path",
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


def write_catalog(
    config: Config,
    records: list[dict[str, Any]],
    symbols: list[str],
    discovered_at: str,
) -> None:
    write_text(
        config.catalog_dir / "archive_files.jsonl",
        jsonl([_project(rec, CATALOG_FIELDS) for rec in records]),
    )
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
    write_text(
        config.catalog_dir / "discovery_summary.json",
        pretty_json(build_discovery_summary(config, records, symbols, discovered_at)),
    )


def build_discovery_summary(
    config: Config,
    records: list[dict[str, Any]],
    symbols: list[str],
    discovered_at: str,
) -> dict[str, Any]:
    monthly = [r for r in records if r["archive_package_source"] == "monthly"]
    daily = [r for r in records if r["archive_package_source"] == "daily"]
    return {
        "interval": config.interval,
        "dataset_id": DATASET_ID,
        "dataset_variant_id": config.variant_id,
        "archive_source": config.archive_source,
        "include_full_daily_history": config.include_full_daily_history,
        "discovered_symbol_count": len(symbols),
        "monthly_archive_symbol_count": len({r["symbol"] for r in monthly}),
        "daily_archive_symbol_count": len({r["symbol"] for r in daily}),
        "total_archive_file_count": len(records),
        "monthly_archive_file_count": len(monthly),
        "daily_archive_file_count": len(daily),
        "daily_required_delta_count": sum(
            1 for r in daily if r["daily_status"] == DAILY_REQUIRED_DELTA
        ),
        "daily_skipped_by_default_count": sum(
            1 for r in daily if r["daily_status"] == DAILY_SKIPPED_BY_DEFAULT
        ),
        "daily_included_full_history_count": sum(
            1 for r in daily if r["daily_status"] == DAILY_INCLUDED_FULL
        ),
        "planned_download_count": sum(
            1 for r in records if r["download_decision"] == DOWNLOAD
        ),
        "daily_delta_policy": daily_delta_policy(config),
        "discovered_at": discovered_at,
    }


def _date_range(records: list[dict[str, Any]]) -> tuple[str | None, str | None]:
    starts, ends = [], []
    for record in records:
        start, end = period_bounds(record["archive_period"])
        starts.append(start)
        ends.append(end)
    return (min(starts) if starts else None, max(ends) if ends else None)


def status_counts(records: list[dict[str, Any]]) -> dict[str, int]:
    downloaded = sum(1 for r in records if r.get("download_status") == DL_DOWNLOADED)
    skipped_existing = sum(
        1 for r in records if r.get("download_status") == DL_SKIPPED_EXISTING
    )
    verified = sum(
        1
        for r in records
        if r.get("checksum_status") in (CHK_PASSED, CHK_SKIPPED_EXISTING)
    )
    checksum_failed = sum(
        1 for r in records if r.get("checksum_status") == CHK_FAILED
    )
    missing = sum(
        1
        for r in records
        if r.get("checksum_status") in (CHK_MISSING_ZIP, CHK_MISSING_CHECKSUM)
    )
    failed = sum(1 for r in records if r.get("download_status") == DL_FAILED)
    skipped_overlap = sum(
        1 for r in records if r.get("download_status") == DL_SKIPPED_OVERLAP
    )
    return {
        "downloaded_count": downloaded,
        "skipped_existing_verified_count": skipped_existing,
        "verified_count": verified,
        "checksum_failed_count": checksum_failed,
        "missing_count": missing,
        "failed_count": failed,
        "skipped_count": skipped_existing + skipped_overlap,
        "skipped_daily_overlap_count": skipped_overlap,
        "total_bytes": sum(r.get("file_size") or 0 for r in records),
    }


def build_manifest(
    config: Config,
    records: list[dict[str, Any]],
    symbols: list[str],
    run_id: str,
    generated_at: str,
    validation_summary: dict[str, Any] | None,
) -> dict[str, Any]:
    counts = status_counts(records)
    date_min, date_max = _date_range(records)
    sources = sorted({r["archive_package_source"] for r in records})
    manifest = {
        "dataset_id": DATASET_ID,
        "dataset_variant_id": config.variant_id,
        "dataset_version": DATASET_VERSION,
        "code_version": CODE_VERSION,
        "interval": config.interval,
        "local_root": str(config.interval_root),
        "run_id": run_id,
        "generated_at": generated_at,
        "primary_key": list(PRIMARY_KEY),
        "symbol_count": len(symbols),
        "file_count": len(records),
        "downloaded_count": counts["downloaded_count"],
        "verified_count": counts["verified_count"],
        "skipped_count": counts["skipped_count"],
        "failed_count": counts["failed_count"],
        "checksum_failed_count": counts["checksum_failed_count"],
        "missing_count": counts["missing_count"],
        "skipped_daily_overlap_count": counts["skipped_daily_overlap_count"],
        "total_bytes": counts["total_bytes"],
        "date_min": date_min,
        "date_max": date_max,
        "archive_package_sources": sources,
        "include_full_daily_history": config.include_full_daily_history,
        "daily_delta_policy": daily_delta_policy(config),
        "file_manifest": str(config.manifest_dir / "files.jsonl"),
        "coverage_report": str(config.reports_dir / "coverage_summary.json"),
        "research_access": str(config.catalog_dir / "research_access.json"),
        "validation_summary": validation_summary,
    }
    return manifest


def write_manifests(
    config: Config,
    records: list[dict[str, Any]],
    manifest: dict[str, Any],
) -> None:
    write_text(
        config.manifest_dir / "files.jsonl",
        jsonl([_project(rec, FILE_MANIFEST_FIELDS) for rec in records]),
    )
    write_text(config.manifest_dir / "manifest.json", pretty_json(manifest))


def build_coverage_summary(
    config: Config, records: list[dict[str, Any]], symbols: list[str]
) -> dict[str, Any]:
    monthly = [r for r in records if r["archive_package_source"] == "monthly"]
    daily = [r for r in records if r["archive_package_source"] == "daily"]
    counts = status_counts(records)
    date_min, date_max = _date_range(records)
    known_gaps = [
        f"{r['symbol']} {r['archive_period']} ({r['archive_package_source']}): "
        f"{r.get('checksum_status')}"
        for r in records
        if r.get("checksum_status")
        in (CHK_FAILED, CHK_MISSING_ZIP, CHK_MISSING_CHECKSUM, CHK_DOWNLOAD_FAILED)
    ]
    return {
        "interval": config.interval,
        "dataset_id": DATASET_ID,
        "dataset_variant_id": config.variant_id,
        "discovered_symbol_count": len(symbols),
        "monthly_archive_symbol_count": len({r["symbol"] for r in monthly}),
        "daily_archive_symbol_count": len({r["symbol"] for r in daily}),
        "total_archive_file_count": len(records),
        "verified_file_count": counts["verified_count"],
        "failed_file_count": counts["failed_count"],
        "checksum_failed_count": counts["checksum_failed_count"],
        "missing_count": counts["missing_count"],
        "skipped_daily_overlap_count": counts["skipped_daily_overlap_count"],
        "date_min": date_min,
        "date_max": date_max,
        "known_gaps": known_gaps,
    }


def write_reports(
    config: Config, records: list[dict[str, Any]], manifest: dict[str, Any]
) -> dict[str, Any]:
    coverage = build_coverage_summary(config, records, [])
    coverage["discovered_symbol_count"] = manifest["symbol_count"]
    write_text(
        config.reports_dir / "coverage_summary.json", pretty_json(coverage)
    )
    missing = [
        _project(rec, FILE_MANIFEST_FIELDS)
        for rec in records
        if rec.get("checksum_status")
        in (CHK_MISSING_ZIP, CHK_MISSING_CHECKSUM, CHK_DOWNLOAD_FAILED)
        or rec.get("download_status") == DL_FAILED
    ]
    write_text(config.reports_dir / "missing_files.jsonl", jsonl(missing))
    failures = [
        _project(rec, FILE_MANIFEST_FIELDS)
        for rec in records
        if rec.get("checksum_status") == CHK_FAILED
    ]
    write_text(config.reports_dir / "checksum_failures.jsonl", jsonl(failures))
    write_text(config.reports_dir / "run_summary.md", render_run_summary(manifest, coverage))
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
# Catalog reload (for standalone download / verify / report phases)
# --------------------------------------------------------------------------- #


def load_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def load_catalog(config: Config) -> tuple[list[dict[str, Any]], list[str]]:
    records = load_jsonl(config.catalog_dir / "archive_files.jsonl")
    symbols_path = config.catalog_dir / "symbols.json"
    symbols: list[str] = []
    if symbols_path.exists():
        symbols = json.loads(symbols_path.read_text(encoding="utf-8")).get("symbols", [])
    # restore non-catalog runtime fields
    for rec in records:
        rec.setdefault("source_checksum_path", rec["source_path"] + ".CHECKSUM")
        rec.setdefault("remote_size", None)
    return records, symbols


def merge_existing_states(
    config: Config, records: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    existing = {
        (r["archive_package_source"], r["symbol"], r["zip_name"]): r
        for r in load_jsonl(config.manifest_dir / "files.jsonl")
    }
    for record in records:
        key = (record["archive_package_source"], record["symbol"], record["zip_name"])
        prior = existing.get(key)
        if prior:
            for field_name in ("checksum_status", "download_status", "file_size", "retry_count", "skip_reason"):
                if field_name in prior:
                    record.setdefault(field_name, prior[field_name])
    return records


# --------------------------------------------------------------------------- #
# Orchestration
# --------------------------------------------------------------------------- #


@dataclass
class RunResult:
    config: Config
    records: list[dict[str, Any]]
    symbols: list[str]
    manifest: dict[str, Any] | None
    coverage: dict[str, Any] | None
    discovery_summary: dict[str, Any]


def run_pipeline(
    config: Config,
    backend: ArchiveBackend,
    phases: set[str],
    now: str | None = None,
) -> RunResult:
    now = now or utc_now()
    run_id = f"{config.interval}-{compact_timestamp(now)}"

    need_fresh_discovery = "discover" in phases or "all" in phases
    if need_fresh_discovery:
        records, symbols = discover(config, backend, now)
    else:
        records, symbols = load_catalog(config)
        if not records:
            records, symbols = discover(config, backend, now)

    discovery_summary = build_discovery_summary(config, records, symbols, now)

    if config.dry_run:
        return RunResult(config, records, symbols, None, None, discovery_summary)

    if need_fresh_discovery:
        write_catalog(config, records, symbols, now)

    if phases == {"discover"}:
        return RunResult(config, records, symbols, None, None, discovery_summary)

    do_download = "download" in phases or "all" in phases
    do_verify = "verify" in phases or "all" in phases
    do_report = "report" in phases or "all" in phases or do_download or do_verify

    if do_download:
        # process_record already verifies each downloaded/resumed file against
        # its checksum, so no separate verify pass is needed here.
        if config.resume or "all" in phases:
            records = merge_existing_states(config, records)
        records = process_records(config, backend, records)
    elif do_verify:
        records = merge_existing_states(config, records)
        records = verify_records(records)
    elif do_report:
        records = merge_existing_states(config, records)

    manifest = build_manifest(config, records, symbols, run_id, now, None)
    write_manifests(config, records, manifest)
    coverage = None
    if do_report:
        coverage = write_reports(config, records, manifest)
        write_research_access(config, manifest, now)
    return RunResult(config, records, symbols, manifest, coverage, discovery_summary)


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
    return HttpArchiveBackend(timeout=config.timeout)


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

    result = run_pipeline(config, backend, phases)
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
    print(f"discovered_symbol_count={summary['discovered_symbol_count']}")
    print(f"total_archive_file_count={summary['total_archive_file_count']}")
    print(f"planned_download_count={summary['planned_download_count']}")
    print(f"daily_skipped_by_default_count={summary['daily_skipped_by_default_count']}")
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
