"""Binance USD-M Futures Kline manifest validation rules.

This validator inspects a *machine-specific* ``local_data`` run manifest produced
by ``datahub.ingestion.binance_um_klines``. It is invoked explicitly with
``--manifest`` and never as part of the clone-safe ``--all`` default unless a
manifest is actually present (see ``datahub.validation.cli``).
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .result import ValidationReport

DATASET_ID = "market.binance.um.klines"
ALLOWED_INTERVALS = ("1d", "4h", "1h", "15m", "5m", "3m", "1m")
EXPECTED_PRIMARY_KEY = ["symbol", "interval", "open_time"]
GITIGNORE_LOCAL_DATA = "local_data/"


def validate_klines_manifest(
    manifest_path: str | Path,
    interval: str,
    repo_root: str | Path = ".",
) -> ValidationReport:
    report = ValidationReport()
    path = Path(manifest_path)
    repo = Path(repo_root)
    file_name = str(path)

    # Interval supported (independent of manifest contents).
    if interval in ALLOWED_INTERVALS:
        report.passed(
            "KL-INTERVAL-SUPPORTED",
            "interval is a supported Kline interval",
            file=file_name,
            dataset_id=DATASET_ID,
            field="interval",
        )
    else:
        report.failed(
            "KL-INTERVAL-SUPPORTED",
            f"interval must be one of {' '.join(ALLOWED_INTERVALS)}",
            file=file_name,
            dataset_id=DATASET_ID,
            field="interval",
            details={"value": interval},
        )

    # local_data excluded from Git (clone-safe regardless of manifest presence).
    _validate_gitignore(repo, report)

    # Manifest exists + valid JSON.
    if not path.exists():
        report.failed(
            "KL-MANIFEST-EXISTS",
            "klines manifest does not exist",
            file=file_name,
            dataset_id=DATASET_ID,
        )
        return report
    try:
        manifest = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        report.failed(
            "KL-MANIFEST-EXISTS",
            "klines manifest is not readable JSON",
            file=file_name,
            dataset_id=DATASET_ID,
            details={"error": str(exc)},
        )
        return report
    report.passed(
        "KL-MANIFEST-EXISTS",
        "klines manifest exists and is valid JSON",
        file=file_name,
        dataset_id=DATASET_ID,
    )

    _check_equals(report, manifest, "interval", interval, "KL-INTERVAL-MATCH",
                  "manifest interval matches CLI interval", file_name)
    _check_equals(report, manifest, "dataset_id", DATASET_ID, "KL-DATASET-ID",
                  "dataset_id is market.binance.um.klines", file_name)
    _check_equals(report, manifest, "dataset_variant_id", f"{DATASET_ID}.{interval}",
                  "KL-VARIANT-ID", "dataset_variant_id matches interval", file_name)

    # Primary key documented.
    if manifest.get("primary_key") == EXPECTED_PRIMARY_KEY:
        report.passed(
            "KL-PRIMARY-KEY",
            "primary key is symbol + interval + open_time",
            file=file_name,
            dataset_id=DATASET_ID,
            field="primary_key",
        )
    else:
        report.failed(
            "KL-PRIMARY-KEY",
            "primary key must be [symbol, interval, open_time]",
            file=file_name,
            dataset_id=DATASET_ID,
            field="primary_key",
            details={"value": manifest.get("primary_key")},
        )

    # checksum_failed_count == 0.
    checksum_failed = manifest.get("checksum_failed_count")
    if checksum_failed == 0:
        report.passed(
            "KL-CHECKSUM-FAILED",
            "checksum_failed_count is zero",
            file=file_name,
            dataset_id=DATASET_ID,
            field="checksum_failed_count",
        )
    else:
        report.failed(
            "KL-CHECKSUM-FAILED",
            "checksum_failed_count must be zero",
            file=file_name,
            dataset_id=DATASET_ID,
            field="checksum_failed_count",
            details={"value": checksum_failed},
        )

    # daily archive policy recorded.
    if isinstance(manifest.get("daily_delta_policy"), dict) and (
        "include_full_daily_history" in manifest
    ):
        report.passed(
            "KL-DAILY-POLICY",
            "daily archive recent-delta policy is recorded",
            file=file_name,
            dataset_id=DATASET_ID,
            field="daily_delta_policy",
        )
    else:
        report.failed(
            "KL-DAILY-POLICY",
            "daily archive policy must be recorded in the manifest",
            file=file_name,
            dataset_id=DATASET_ID,
            field="daily_delta_policy",
        )

    # File manifest exists.
    _validate_referenced_path(
        report, manifest.get("file_manifest"), "KL-FILE-MANIFEST",
        "per-file manifest", file_name,
    )
    # Coverage report exists.
    _validate_referenced_path(
        report, manifest.get("coverage_report"), "KL-COVERAGE-REPORT",
        "coverage report", file_name,
    )
    # Research access manifest exists.
    _validate_referenced_path(
        report, manifest.get("research_access"), "KL-RESEARCH-ACCESS",
        "research access manifest", file_name,
    )

    # Required files exist on disk (sampled across verified records).
    _validate_required_files(report, manifest, file_name)

    return report


def _check_equals(
    report: ValidationReport,
    manifest: dict[str, Any],
    field_name: str,
    expected: Any,
    rule_id: str,
    message: str,
    file_name: str,
) -> None:
    actual = manifest.get(field_name)
    if actual == expected:
        report.passed(rule_id, message, file=file_name, dataset_id=DATASET_ID, field=field_name)
    else:
        report.failed(
            rule_id,
            f"{message} (expected {expected!r})",
            file=file_name,
            dataset_id=DATASET_ID,
            field=field_name,
            details={"expected": expected, "value": actual},
        )


def _validate_referenced_path(
    report: ValidationReport,
    referenced: Any,
    rule_id: str,
    label: str,
    file_name: str,
) -> None:
    if isinstance(referenced, str) and Path(referenced).exists():
        report.passed(rule_id, f"{label} exists", file=referenced, dataset_id=DATASET_ID)
    else:
        report.failed(
            rule_id,
            f"{label} must exist",
            file=file_name,
            dataset_id=DATASET_ID,
            details={"value": referenced},
        )


def _validate_required_files(
    report: ValidationReport, manifest: dict[str, Any], file_name: str
) -> None:
    file_manifest = manifest.get("file_manifest")
    if not (isinstance(file_manifest, str) and Path(file_manifest).exists()):
        report.skipped(
            "KL-REQUIRED-FILES",
            "required-files check skipped because file manifest is absent",
            file=file_name,
            dataset_id=DATASET_ID,
        )
        return
    missing = []
    verified = 0
    for line in Path(file_manifest).read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        record = json.loads(line)
        if record.get("checksum_status") in ("passed", "skipped_existing_verified"):
            verified += 1
            local_zip = record.get("local_zip_path")
            if not (isinstance(local_zip, str) and Path(local_zip).exists()):
                missing.append(local_zip)
    if missing:
        report.failed(
            "KL-REQUIRED-FILES",
            "verified files referenced by the manifest must exist on disk",
            file=file_name,
            dataset_id=DATASET_ID,
            details={"missing_sample": missing[:5], "missing_count": len(missing)},
        )
    elif verified == 0:
        report.failed(
            "KL-REQUIRED-FILES",
            "manifest reports no verified files",
            file=file_name,
            dataset_id=DATASET_ID,
        )
    else:
        report.passed(
            "KL-REQUIRED-FILES",
            f"all {verified} verified files exist on disk",
            file=file_name,
            dataset_id=DATASET_ID,
        )


def _validate_gitignore(repo: Path, report: ValidationReport) -> None:
    gitignore = repo / ".gitignore"
    text = gitignore.read_text(encoding="utf-8") if gitignore.exists() else ""
    entries = {line.strip() for line in text.splitlines()}
    if GITIGNORE_LOCAL_DATA in entries or "local_data" in entries:
        report.passed(
            "KL-LOCAL-DATA-IGNORED",
            "local_data is excluded from Git",
            file=".gitignore",
            dataset_id=DATASET_ID,
        )
    else:
        report.failed(
            "KL-LOCAL-DATA-IGNORED",
            ".gitignore must exclude local_data/",
            file=".gitignore",
            dataset_id=DATASET_ID,
        )
