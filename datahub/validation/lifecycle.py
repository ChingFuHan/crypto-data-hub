"""Dataset lifecycle validation rules."""

from __future__ import annotations

import json
from pathlib import Path
import re
from typing import Any

from .result import ValidationReport

LIFECYCLE_STATES = {"draft", "active", "deprecated", "archived"}
ALLOWED_TRANSITIONS = {
    ("draft", "active"),
    ("draft", "archived"),
    ("active", "deprecated"),
    ("deprecated", "active"),
    ("deprecated", "archived"),
}


def is_valid_transition(from_state: str, to_state: str) -> bool:
    return (from_state, to_state) in ALLOWED_TRANSITIONS


def validate_lifecycle(
    registry_path: str | Path = "dataset_registry.json",
    repo_root: str | Path = ".",
) -> ValidationReport:
    repo = Path(repo_root)
    path = Path(registry_path)
    report = ValidationReport()
    try:
        registry = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        report.failed(
            "LIFECYCLE-REGISTRY-READ",
            "registry must be readable before lifecycle validation",
            file=str(path),
            details={"error": str(exc)},
        )
        return report

    report.passed(
        "LIFECYCLE-TRANSITION-RULE",
        "allowed lifecycle transition table is loaded",
        file="docs/dataset_lifecycle.md",
        details={"allowed_transitions": sorted(ALLOWED_TRANSITIONS)},
    )

    datasets = registry.get("datasets", [])
    active_seen = deprecated_seen = archived_seen = False
    for index, entry in enumerate(datasets):
        if not isinstance(entry, dict):
            continue
        dataset_id = entry.get("dataset_id")
        status = entry.get("status")
        location = f"datasets[{index}]"

        if status in LIFECYCLE_STATES:
            report.passed(
                "LIFECYCLE-STATE",
                "dataset status is a valid lifecycle state",
                file=str(path),
                dataset_id=dataset_id,
                field="status",
                location=location,
            )
        else:
            report.failed(
                "LIFECYCLE-STATE",
                "dataset status must be draft, active, deprecated, or archived",
                file=str(path),
                dataset_id=dataset_id,
                field="status",
                location=location,
                details={"value": status},
            )
            continue

        if status == "draft":
            _validate_draft(entry, report, path, repo, location)
        elif status == "active":
            active_seen = True
            _validate_active(entry, report, path, location)
        elif status == "deprecated":
            deprecated_seen = True
            _validate_deprecated(entry, report, path, location)
        elif status == "archived":
            archived_seen = True
            _validate_archived(entry, report, path, location)

    if not active_seen:
        report.skipped(
            "LIFECYCLE-ACTIVE-REQUIREMENTS",
            "active requirements skeleton skipped because no active datasets exist",
            file=str(path),
        )
    if not deprecated_seen:
        report.skipped(
            "LIFECYCLE-DEPRECATED-REQUIREMENTS",
            "deprecated requirements skeleton skipped because no deprecated datasets exist",
            file=str(path),
        )
    if not archived_seen:
        report.skipped(
            "LIFECYCLE-ARCHIVED-REQUIREMENTS",
            "archived requirements skeleton skipped because no archived datasets exist",
            file=str(path),
        )

    return report


def _validate_draft(
    entry: dict[str, Any],
    report: ValidationReport,
    registry_path: Path,
    repo: Path,
    location: str,
) -> None:
    dataset_id = entry.get("dataset_id")
    quality = entry.get("quality")
    report.passed(
        "LIFECYCLE-DRAFT-STATUS",
        "draft dataset remains in draft lifecycle state",
        file=str(registry_path),
        dataset_id=dataset_id,
        field="status",
        location=location,
    )

    if _schema_ref_exists(entry.get("schema_ref"), repo):
        report.passed(
            "LIFECYCLE-DRAFT-CONTRACT",
            "draft dataset contract exists",
            file=str(registry_path),
            dataset_id=dataset_id,
            field="schema_ref",
            location=location,
        )
    else:
        report.failed(
            "LIFECYCLE-DRAFT-CONTRACT",
            "draft dataset contract must exist",
            file=str(registry_path),
            dataset_id=dataset_id,
            field="schema_ref",
            location=location,
            details={"schema_ref": entry.get("schema_ref")},
        )

    catalog_path = repo / "DATA_CATALOG.md"
    catalog_text = catalog_path.read_text(encoding="utf-8") if catalog_path.exists() else ""
    if f"### {dataset_id}" in catalog_text:
        report.passed(
            "LIFECYCLE-DRAFT-CATALOG",
            "draft dataset has catalog entry",
            file="DATA_CATALOG.md",
            dataset_id=dataset_id,
            location=f"### {dataset_id}",
        )
    else:
        report.failed(
            "LIFECYCLE-DRAFT-CATALOG",
            "draft dataset must have catalog entry",
            file="DATA_CATALOG.md",
            dataset_id=dataset_id,
        )

    metadata_fields = (
        "dataset_id",
        "dataset_name",
        "description",
        "version",
        "owner",
        "source",
        "schema_ref",
        "primary_key",
        "provenance",
        "quality",
    )
    missing = [field for field in metadata_fields if field not in entry]
    if not missing:
        report.passed(
            "LIFECYCLE-DRAFT-METADATA",
            "draft dataset carries required metadata",
            file=str(registry_path),
            dataset_id=dataset_id,
            location=location,
        )
    else:
        report.failed(
            "LIFECYCLE-DRAFT-METADATA",
            "draft dataset metadata is incomplete",
            file=str(registry_path),
            dataset_id=dataset_id,
            location=location,
            details={"missing": missing},
        )

    if isinstance(quality, dict) and "contract_validated" in quality:
        report.passed(
            "LIFECYCLE-DRAFT-QUALITY",
            "draft dataset records contract_validated state",
            file=str(registry_path),
            dataset_id=dataset_id,
            field="quality.contract_validated",
            location=location,
        )
    else:
        report.failed(
            "LIFECYCLE-DRAFT-QUALITY",
            "draft dataset must record contract_validated",
            file=str(registry_path),
            dataset_id=dataset_id,
            field="quality.contract_validated",
            location=location,
        )

    if isinstance(quality, dict) and quality.get("contract_validated") is False:
        report.passed(
            "LIFECYCLE-DRAFT-UNTRUSTED",
            "draft dataset is not marked contract_validated",
            file=str(registry_path),
            dataset_id=dataset_id,
            field="quality.contract_validated",
            location=location,
        )
    else:
        report.failed(
            "LIFECYCLE-DRAFT-UNTRUSTED",
            "draft dataset must not be marked contract_validated",
            file=str(registry_path),
            dataset_id=dataset_id,
            field="quality.contract_validated",
            location=location,
        )


def _validate_active(
    entry: dict[str, Any],
    report: ValidationReport,
    registry_path: Path,
    location: str,
) -> None:
    dataset_id = entry.get("dataset_id")
    quality = entry.get("quality")
    snapshot = entry.get("snapshot")
    data_location_available = bool(entry.get("data_location")) or (
        isinstance(snapshot, dict) and bool(snapshot.get("snapshot_id"))
    )
    if (
        isinstance(quality, dict)
        and quality.get("contract_validated") is True
        and quality.get("last_validated_at")
        and isinstance(entry.get("provenance"), dict)
        and entry.get("earliest_timestamp")
        and entry.get("latest_timestamp")
        and data_location_available
    ):
        report.passed(
            "LIFECYCLE-ACTIVE-REQUIREMENTS",
            "active dataset has validation status, data location, provenance, and timestamps",
            file=str(registry_path),
            dataset_id=dataset_id,
            location=location,
        )
    else:
        report.failed(
            "LIFECYCLE-ACTIVE-REQUIREMENTS",
            "active dataset must have validation status, data location, provenance, and timestamps",
            file=str(registry_path),
            dataset_id=dataset_id,
            location=location,
        )


def _validate_deprecated(
    entry: dict[str, Any],
    report: ValidationReport,
    registry_path: Path,
    location: str,
) -> None:
    dataset_id = entry.get("dataset_id")
    if isinstance(entry.get("provenance"), dict):
        report.passed(
            "LIFECYCLE-DEPRECATED-REQUIREMENTS",
            "deprecated dataset retains provenance",
            file=str(registry_path),
            dataset_id=dataset_id,
            location=location,
        )
    else:
        report.failed(
            "LIFECYCLE-DEPRECATED-REQUIREMENTS",
            "deprecated dataset must retain provenance",
            file=str(registry_path),
            dataset_id=dataset_id,
            location=location,
        )


def _validate_archived(
    entry: dict[str, Any],
    report: ValidationReport,
    registry_path: Path,
    location: str,
) -> None:
    dataset_id = entry.get("dataset_id")
    snapshot = entry.get("snapshot")
    if isinstance(snapshot, dict) and snapshot.get("immutable") is True:
        report.passed(
            "LIFECYCLE-ARCHIVED-REQUIREMENTS",
            "archived dataset has immutable snapshot metadata",
            file=str(registry_path),
            dataset_id=dataset_id,
            location=location,
        )
    else:
        report.failed(
            "LIFECYCLE-ARCHIVED-REQUIREMENTS",
            "archived dataset must have immutable snapshot metadata",
            file=str(registry_path),
            dataset_id=dataset_id,
            location=location,
        )


def _schema_ref_exists(schema_ref: Any, repo: Path) -> bool:
    if not isinstance(schema_ref, str) or "#" not in schema_ref:
        return False
    file_part, anchor = schema_ref.split("#", 1)
    path = repo / file_part
    if not path.exists():
        return False
    anchors = _markdown_anchors(path.read_text(encoding="utf-8"))
    return anchor in anchors


def _markdown_anchors(markdown: str) -> set[str]:
    anchors: set[str] = set()
    for line in markdown.splitlines():
        if not line.startswith("#"):
            continue
        heading = line.lstrip("#").strip()
        heading = heading.replace("`", "")
        anchor = re.sub(r"[^a-z0-9]+", "-", heading.lower()).strip("-")
        if anchor:
            anchors.add(anchor)
    return anchors
