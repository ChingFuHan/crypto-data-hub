"""Universe Metadata ingestion MVP for Binance USD-M Futures exchangeInfo."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
from pathlib import Path
import re
import sys
from typing import Any
from urllib.error import URLError
from urllib.request import Request, urlopen

from datahub.validation.universe_metadata import validate_fixture

DATASET_ID = "reference.universe.metadata"
DATASET_VERSION = "v0.1.0"
SOURCE_NAME = "binance_usd_m_futures_exchange_info"
SOURCE_TYPE = "api"
SOURCE_IDENTIFIER = "https://fapi.binance.com/fapi/v1/exchangeInfo"
EXCHANGE = "binance"
RAW_DIR = Path("data/raw/reference/universe_metadata")
ARTIFACT_PATH = Path("data/reference/universe_metadata/reference.universe.metadata.json")
MANIFEST_PATH = Path("data/manifests/reference/universe_metadata/manifest.json")


class IngestionCommandError(ValueError):
    """Raised for invalid ingestion CLI use."""


class IngestionError(RuntimeError):
    """Raised when ingestion fails loud."""


@dataclass(frozen=True)
class IngestionResult:
    raw_snapshot_path: Path
    artifact_path: Path
    manifest_path: Path
    row_count: int
    artifact_checksum: str
    manifest_checksum: str
    validation_passed: bool
    reused_raw_snapshot: bool


def canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def pretty_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, indent=2) + "\n"


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha256_json(value: Any) -> str:
    return sha256_bytes(canonical_json_bytes(value))


def utc_now() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def iso_from_ms(value: Any) -> str:
    if value is None:
        raise IngestionError("onboardDate is required for listed_at")
    try:
        timestamp_ms = int(value)
    except (TypeError, ValueError) as exc:
        raise IngestionError(f"invalid onboardDate: {value}") from exc
    return (
        datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def compact_timestamp(value: str) -> str:
    return value.replace("-", "").replace(":", "").replace("+00:00", "Z")


def write_if_changed(path: Path, content: str) -> bool:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and path.read_text(encoding="utf-8") == content:
        return False
    path.write_text(content, encoding="utf-8")
    return True


def fetch_exchange_info(timeout_seconds: int = 30) -> dict[str, Any]:
    request = Request(
        SOURCE_IDENTIFIER,
        headers={"User-Agent": "crypto-data-hub-ingestion/0.5.0"},
    )
    try:
        with urlopen(request, timeout=timeout_seconds) as response:
            if response.status != 200:
                raise IngestionError(f"source returned HTTP {response.status}")
            payload = json.loads(response.read().decode("utf-8"))
    except (URLError, TimeoutError, OSError, json.JSONDecodeError) as exc:
        raise IngestionError(f"source fetch failed: {exc}") from exc
    if not isinstance(payload, dict) or not isinstance(payload.get("symbols"), list):
        raise IngestionError("exchangeInfo payload must contain symbols array")
    return payload


def find_raw_snapshot_by_checksum(repo_root: Path, raw_checksum: str) -> Path | None:
    raw_dir = repo_root / RAW_DIR
    if not raw_dir.exists():
        return None
    for path in sorted(raw_dir.glob("exchange_info_*.json")):
        try:
            envelope = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if envelope.get("raw_response_checksum") == raw_checksum:
            return path
    return None


def write_raw_snapshot(
    payload: dict[str, Any],
    repo_root: Path = Path("."),
    retrieved_at: str | None = None,
) -> tuple[Path, dict[str, Any], bool]:
    raw_checksum = sha256_json(payload)
    existing = find_raw_snapshot_by_checksum(repo_root, raw_checksum)
    if existing is not None:
        return existing, load_raw_snapshot(existing), True

    retrieved_at = retrieved_at or utc_now()
    name = f"exchange_info_{compact_timestamp(retrieved_at)}_{raw_checksum[:12]}.json"
    path = repo_root / RAW_DIR / name
    envelope = {
        "source_name": SOURCE_NAME,
        "source_type": SOURCE_TYPE,
        "source_identifier": SOURCE_IDENTIFIER,
        "retrieved_at": retrieved_at,
        "raw_response_checksum": raw_checksum,
        "raw_response": payload,
    }
    write_if_changed(path, pretty_json(envelope))
    return path, envelope, False


def load_raw_snapshot(path: str | Path) -> dict[str, Any]:
    snapshot_path = Path(path)
    try:
        envelope = json.loads(snapshot_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise IngestionError(f"raw snapshot read failed: {snapshot_path}") from exc
    if not isinstance(envelope, dict) or not isinstance(envelope.get("raw_response"), dict):
        raise IngestionError("raw snapshot must be envelope with raw_response object")
    expected = envelope.get("raw_response_checksum")
    actual = sha256_json(envelope["raw_response"])
    if expected != actual:
        raise IngestionError("raw snapshot checksum mismatch")
    return envelope


def select_offline_raw_snapshot(repo_root: Path, raw_snapshot: str | Path | None) -> Path:
    if raw_snapshot is not None:
        path = Path(raw_snapshot)
        return path if path.is_absolute() else repo_root / path

    manifest_path = repo_root / MANIFEST_PATH
    if manifest_path.exists():
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            raw_sources = manifest.get("raw_sources", [])
            if raw_sources:
                path = Path(raw_sources[0]["path"])
                return path if path.is_absolute() else repo_root / path
        except (KeyError, TypeError, OSError, json.JSONDecodeError):
            pass

    raw_dir = repo_root / RAW_DIR
    candidates = sorted(raw_dir.glob("exchange_info_*.json")) if raw_dir.exists() else []
    if not candidates:
        raise IngestionCommandError("offline mode requires committed raw snapshot")
    return candidates[-1]


def normalize_exchange_info(envelope: dict[str, Any]) -> list[dict[str, Any]]:
    payload = envelope["raw_response"]
    retrieved_at = parse_utc(envelope["retrieved_at"])
    rows = []
    for symbol in payload.get("symbols", []):
        if symbol.get("status") != "TRADING":
            continue
        listed_at = iso_from_ms(symbol.get("onboardDate"))
        if parse_utc(listed_at) > retrieved_at:
            raise IngestionError(f"listed_at after retrieved_at for {symbol.get('symbol')}")
        market_type = market_type_from_contract_type(symbol.get("contractType"))
        row = {
            "instrument_id": instrument_id_for(symbol, market_type, listed_at),
            "symbol": required_str(symbol, "symbol"),
            "exchange": EXCHANGE,
            "base_asset": optional_str(symbol.get("baseAsset")),
            "quote_asset": optional_str(symbol.get("quoteAsset")),
            "market_type": market_type,
            "contract_type": contract_type_for(market_type),
            "status": "active",
            "listed_at": listed_at,
            "delisted_at": None,
            "successor_id": None,
            "tick_size": filter_value(symbol, "PRICE_FILTER", "tickSize"),
            "step_size": filter_value(symbol, "LOT_SIZE", "stepSize"),
            "contract_size": contract_size_for(market_type),
        }
        rows.append(row)
    rows.sort(key=lambda item: item["instrument_id"])
    rows = apply_collision_suffixes(rows)
    if not rows:
        raise IngestionError("normalization produced zero active rows")
    return rows


def parse_utc(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def required_str(symbol: dict[str, Any], field: str) -> str:
    value = symbol.get(field)
    if not isinstance(value, str) or not value:
        raise IngestionError(f"missing required source field: {field}")
    return value


def optional_str(value: Any) -> str | None:
    return value if isinstance(value, str) and value else None


def market_type_from_contract_type(contract_type: Any) -> str:
    if contract_type == "PERPETUAL":
        return "perpetual"
    if isinstance(contract_type, str) and contract_type:
        return "futures"
    raise IngestionError(f"unsupported contractType: {contract_type}")


def contract_type_for(market_type: str) -> str:
    if market_type in {"perpetual", "futures"}:
        return "linear"
    raise IngestionError(f"unsupported market_type for contract_type: {market_type}")


def contract_size_for(market_type: str) -> str:
    if market_type in {"perpetual", "futures"}:
        return "1"
    raise IngestionError(f"unsupported market_type for contract_size: {market_type}")


def filter_value(symbol: dict[str, Any], filter_type: str, field: str) -> str | None:
    for item in symbol.get("filters", []):
        if isinstance(item, dict) and item.get("filterType") == filter_type:
            value = item.get(field)
            if value is None:
                return None
            try:
                if Decimal(str(value)) <= 0:
                    raise IngestionError(f"{field} must be > 0 for {symbol.get('symbol')}")
            except (InvalidOperation, ValueError) as exc:
                raise IngestionError(f"invalid {field} for {symbol.get('symbol')}") from exc
            return str(value)
    return None


def instrument_id_for(symbol: dict[str, Any], market_type: str, listed_at: str) -> str:
    source_symbol = required_str(symbol, "symbol")
    listed_date = listed_at[:10].replace("-", "")
    readable = safe_identifier(source_symbol.lower())
    return f"{EXCHANGE}.usd_m_futures.{market_type}.{readable}.{listed_date}"


def safe_identifier(value: str) -> str:
    lowered = value.lower()
    return re.sub(r"[^a-z0-9_]+", "_", lowered).strip("_")


def apply_collision_suffixes(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    counts: dict[str, int] = {}
    for row in rows:
        base_id = row["instrument_id"]
        counts[base_id] = counts.get(base_id, 0) + 1
    if all(count == 1 for count in counts.values()):
        return rows

    adjusted = []
    seen: dict[str, int] = {}
    for row in rows:
        base_id = row["instrument_id"]
        if counts[base_id] == 1:
            adjusted.append(row)
            continue
        seen[base_id] = seen.get(base_id, 0) + 1
        clone = dict(row)
        collision_input = canonical_json_bytes(
            {
                "base_id": base_id,
                "symbol": row["symbol"],
                "listed_at": row["listed_at"],
                "ordinal": seen[base_id],
            }
        )
        clone["instrument_id"] = f"{base_id}.h{sha256_bytes(collision_input)[:8]}"
        adjusted.append(clone)
    adjusted.sort(key=lambda item: item["instrument_id"])
    return adjusted


def build_manifest(
    *,
    repo_root: Path,
    raw_snapshot_path: Path,
    raw_envelope: dict[str, Any],
    artifact_path: Path,
    rows: list[dict[str, Any]],
    validation_status: dict[str, Any],
) -> dict[str, Any]:
    artifact_rel = artifact_path.relative_to(repo_root)
    raw_rel = raw_snapshot_path.relative_to(repo_root)
    artifact_checksum = sha256_json(rows)
    raw_response = raw_envelope["raw_response"]
    raw_source = {
        "source_name": SOURCE_NAME,
        "source_type": SOURCE_TYPE,
        "source_identifier": SOURCE_IDENTIFIER,
        "path": str(raw_rel),
        "retrieved_at": raw_envelope["retrieved_at"],
        "checksum": raw_envelope["raw_response_checksum"],
        "snapshot_checksum": sha256_bytes(raw_snapshot_path.read_bytes()),
        "record_count": len(raw_response.get("symbols", [])),
        "coverage_notes": [
            "Authoritative for current Binance USD-M Futures exchangeInfo symbols returned with status TRADING.",
            "Not authoritative for full historical delist, rename, or merge lifecycle.",
        ],
    }
    manifest = {
        "dataset_id": DATASET_ID,
        "dataset_version": DATASET_VERSION,
        "generated_at": raw_envelope["retrieved_at"],
        "source_count": 1,
        "row_count": len(rows),
        "raw_sources": [raw_source],
        "normalized_artifact": {
            "path": str(artifact_rel),
            "format": "json_array",
            "checksum": artifact_checksum,
            "row_count": len(rows),
            "coverage_status": "active_current",
        },
        "checksum": artifact_checksum,
        "validation_command": (
            "python -m datahub.validation --target universe-metadata "
            f"--fixture {artifact_rel}"
        ),
        "validation_status": validation_status,
        "coverage": {
            "implemented": ["active_current"],
            "historical_candidate": "not_implemented",
            "unresolved": [
                "Historical delisted symbols not covered by exchangeInfo current endpoint.",
                "Renames and merges require announcement/archive evidence in later phases.",
            ],
            "not_supported_yet": [
                "Full point-in-time historical universe reconstruction.",
                "Confirmed delisted/renamed/merged lifecycle events.",
            ],
        },
        "field_provenance": field_provenance(),
        "known_coverage_gaps": [
            "Only Binance USD-M Futures current TRADING symbols are normalized.",
            "Historical listed/delisted/renamed/merged events are not ingested.",
            "contract_size is a documented normalization convention because exchangeInfo does not expose a separate contract-size field.",
        ],
    }
    manifest_checksum = sha256_json(manifest)
    manifest["manifest_checksum"] = manifest_checksum
    return manifest


def field_provenance() -> dict[str, dict[str, str]]:
    return {
        "instrument_id": {
            "source": "derived from exchange, market_type, symbol, listed_at",
            "confidence": "high",
        },
        "symbol": {"source": "exchangeInfo.symbol", "confidence": "high"},
        "exchange": {"source": "ingestion constant: binance", "confidence": "high"},
        "base_asset": {"source": "exchangeInfo.baseAsset", "confidence": "high"},
        "quote_asset": {"source": "exchangeInfo.quoteAsset", "confidence": "high"},
        "market_type": {"source": "exchangeInfo.contractType", "confidence": "high"},
        "contract_type": {
            "source": "USD-M Futures product class; normalized as linear",
            "confidence": "high",
        },
        "status": {"source": "exchangeInfo.status == TRADING", "confidence": "high"},
        "listed_at": {"source": "exchangeInfo.onboardDate", "confidence": "high"},
        "delisted_at": {
            "source": "null because artifact covers current active symbols only",
            "confidence": "high",
        },
        "successor_id": {
            "source": "null because exchangeInfo current endpoint has no rename/merge relation",
            "confidence": "medium",
        },
        "tick_size": {"source": "exchangeInfo.filters.PRICE_FILTER.tickSize", "confidence": "high"},
        "step_size": {"source": "exchangeInfo.filters.LOT_SIZE.stepSize", "confidence": "high"},
        "contract_size": {
            "source": "normalization convention: 1 base asset quantity unit for USD-M linear futures",
            "confidence": "medium",
        },
    }


def validation_status_for(report: Any) -> dict[str, Any]:
    return {
        "status": "passed" if not report.has_failures else "failed",
        "total_checks": report.total_checks,
        "passed_checks": report.passed_checks,
        "failed_checks": report.failed_checks,
        "warning_checks": report.warning_checks,
        "error_rule_ids": [check.rule_id for check in report.error_summary],
    }


def normalize_from_snapshot(
    raw_snapshot_path: Path,
    repo_root: Path = Path("."),
) -> IngestionResult:
    raw_snapshot_path = raw_snapshot_path if raw_snapshot_path.is_absolute() else repo_root / raw_snapshot_path
    raw_envelope = load_raw_snapshot(raw_snapshot_path)
    rows = normalize_exchange_info(raw_envelope)
    artifact_path = repo_root / ARTIFACT_PATH
    write_if_changed(artifact_path, pretty_json(rows))
    validation_report = validate_fixture(artifact_path)
    validation_status = validation_status_for(validation_report)
    manifest = build_manifest(
        repo_root=repo_root,
        raw_snapshot_path=raw_snapshot_path,
        raw_envelope=raw_envelope,
        artifact_path=artifact_path,
        rows=rows,
        validation_status=validation_status,
    )
    manifest_path = repo_root / MANIFEST_PATH
    write_if_changed(manifest_path, pretty_json(manifest))
    if validation_report.has_failures:
        raise IngestionError(validation_report.render())
    return IngestionResult(
        raw_snapshot_path=raw_snapshot_path,
        artifact_path=artifact_path,
        manifest_path=manifest_path,
        row_count=len(rows),
        artifact_checksum=manifest["normalized_artifact"]["checksum"],
        manifest_checksum=manifest["manifest_checksum"],
        validation_passed=True,
        reused_raw_snapshot=False,
    )


def fetch(repo_root: Path = Path(".")) -> tuple[Path, dict[str, Any], bool]:
    payload = fetch_exchange_info()
    return write_raw_snapshot(payload, repo_root=repo_root)


def run_all(offline: bool, raw_snapshot: str | None, repo_root: Path = Path(".")) -> IngestionResult:
    if offline:
        selected = select_offline_raw_snapshot(repo_root, raw_snapshot)
        result = normalize_from_snapshot(selected, repo_root=repo_root)
        return result
    raw_path, _, reused = fetch(repo_root=repo_root)
    result = normalize_from_snapshot(raw_path, repo_root=repo_root)
    return IngestionResult(
        raw_snapshot_path=result.raw_snapshot_path,
        artifact_path=result.artifact_path,
        manifest_path=result.manifest_path,
        row_count=result.row_count,
        artifact_checksum=result.artifact_checksum,
        manifest_checksum=result.manifest_checksum,
        validation_passed=result.validation_passed,
        reused_raw_snapshot=reused,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m datahub.ingestion.universe_metadata",
        description="Ingest Universe Metadata from Binance USD-M Futures exchangeInfo.",
    )
    parser.add_argument("--fetch", action="store_true", help="fetch and store raw snapshot")
    parser.add_argument("--normalize", action="store_true", help="normalize raw snapshot")
    parser.add_argument("--all", action="store_true", help="fetch/normalize/validate or offline normalize/validate")
    parser.add_argument("--offline", action="store_true", help="use existing raw snapshot; no network")
    parser.add_argument("--raw-snapshot", help="raw snapshot path for offline/normalize mode")
    return parser


def run_cli(args: argparse.Namespace) -> int:
    repo_root = Path(".")
    if args.offline and args.fetch and not args.all:
        raise IngestionCommandError("--offline cannot be combined with --fetch")
    command_count = sum(1 for flag in (args.fetch, args.normalize, args.all) if flag)
    if command_count != 1:
        raise IngestionCommandError("choose exactly one of --fetch, --normalize, --all")

    if args.fetch:
        raw_path, envelope, reused = fetch(repo_root=repo_root)
        print(f"raw_snapshot={raw_path}")
        print(f"retrieved_at={envelope['retrieved_at']}")
        print(f"raw_checksum={envelope['raw_response_checksum']}")
        print(f"reused_raw_snapshot={str(reused).lower()}")
        return 0

    if args.normalize:
        raw_path = select_offline_raw_snapshot(repo_root, args.raw_snapshot)
        result = normalize_from_snapshot(raw_path, repo_root=repo_root)
        print_result(result)
        return 0

    result = run_all(args.offline, args.raw_snapshot, repo_root=repo_root)
    print_result(result)
    return 0


def print_result(result: IngestionResult) -> None:
    print(f"raw_snapshot={result.raw_snapshot_path}")
    print(f"normalized_artifact={result.artifact_path}")
    print(f"manifest={result.manifest_path}")
    print(f"row_count={result.row_count}")
    print(f"artifact_checksum={result.artifact_checksum}")
    print(f"manifest_checksum={result.manifest_checksum}")
    print(f"validation_passed={str(result.validation_passed).lower()}")
    print(f"reused_raw_snapshot={str(result.reused_raw_snapshot).lower()}")


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    try:
        args = parser.parse_args(argv)
        return run_cli(args)
    except IngestionCommandError as exc:
        print(f"ingestion command error: {exc}", file=sys.stderr)
        return 2
    except IngestionError as exc:
        print(f"ingestion failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
