"""Dataset-related naming validation."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import re
from typing import Any

from .result import ValidationReport

DATASET_ID_RE = re.compile(r"^[a-z0-9]+(?:[._-][a-z0-9]+)*$")
FIELD_NAME_RE = re.compile(r"^[a-z][a-z0-9_]*$")
SEMVER_RE = re.compile(r"^v[0-9]+\.[0-9]+\.[0-9]+$")
OWNER_RE = re.compile(r"^[a-z0-9][a-z0-9._-]*$")
SCHEMA_REF_RE = re.compile(r"^[A-Z_]+\.md#[a-z0-9]+(?:-[a-z0-9]+)*$")
DOC_PATH_RE = re.compile(r"^[a-z0-9_]+\.md$")
DATA_PATH_RE = re.compile(r"^[a-z0-9_]+(?:\.[a-z0-9_]+)+$")


def is_dataset_id(value: Any) -> bool:
    return isinstance(value, str) and bool(DATASET_ID_RE.fullmatch(value))


def is_field_name(value: Any) -> bool:
    return isinstance(value, str) and bool(FIELD_NAME_RE.fullmatch(value))


def is_semver(value: Any) -> bool:
    return isinstance(value, str) and bool(SEMVER_RE.fullmatch(value))


def is_owner(value: Any) -> bool:
    return isinstance(value, str) and bool(OWNER_RE.fullmatch(value))


def is_schema_ref(value: Any) -> bool:
    return isinstance(value, str) and bool(SCHEMA_REF_RE.fullmatch(value))


def parse_utc_timestamp(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return None
    if parsed.utcoffset() != timezone.utc.utcoffset(parsed):
        return None
    return parsed.astimezone(timezone.utc)


def is_utc_timestamp(value: Any) -> bool:
    return parse_utc_timestamp(value) is not None


def is_update_frequency(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    fixed = {"realtime", "daily", "weekly", "monthly", "manual"}
    return value in fixed or bool(re.fullmatch(r"[0-9]+[mhdw]", value))


def is_timezone(value: Any) -> bool:
    if value == "UTC":
        return True
    return isinstance(value, str) and bool(
        re.fullmatch(r"[A-Za-z_]+(?:/[A-Za-z0-9_+-]+)+", value)
    )


def is_dataset_path(value: Any) -> bool:
    if not isinstance(value, str) or not value:
        return False
    path = Path(value)
    return all(
        bool(DATASET_ID_RE.fullmatch(part)) or bool(DATA_PATH_RE.fullmatch(part))
        for part in path.parts
    )


def validate_registry_naming(
    registry: dict[str, Any], *, file: str = "dataset_registry.json"
) -> ValidationReport:
    report = ValidationReport()

    registry_version = registry.get("registry_version")
    if is_semver(registry_version):
        report.passed(
            "NAMING-REGISTRY-VERSION",
            "registry_version follows v-prefixed SemVer",
            file=file,
            field="registry_version",
        )
    else:
        report.failed(
            "NAMING-REGISTRY-VERSION",
            "registry_version must follow v-prefixed SemVer",
            file=file,
            field="registry_version",
            details={"value": registry_version},
        )

    for index, entry in enumerate(registry.get("datasets", [])):
        dataset_id = entry.get("dataset_id")
        location = f"datasets[{index}]"
        if is_dataset_id(dataset_id):
            report.passed(
                "NAMING-DATASET-ID",
                "dataset_id follows registry naming pattern",
                file=file,
                dataset_id=dataset_id,
                field="dataset_id",
                location=location,
            )
        else:
            report.failed(
                "NAMING-DATASET-ID",
                "dataset_id must be lowercase and separator-safe",
                file=file,
                field="dataset_id",
                location=location,
                details={"value": dataset_id},
            )

        version = entry.get("version")
        if is_semver(version):
            report.passed(
                "NAMING-DATASET-VERSION",
                "dataset version follows v-prefixed SemVer",
                file=file,
                dataset_id=dataset_id,
                field="version",
                location=location,
            )
        else:
            report.failed(
                "NAMING-DATASET-VERSION",
                "dataset version must follow v-prefixed SemVer",
                file=file,
                dataset_id=dataset_id,
                field="version",
                location=location,
                details={"value": version},
            )

        schema_ref = entry.get("schema_ref")
        if is_schema_ref(schema_ref):
            report.passed(
                "NAMING-METADATA-REF",
                "schema_ref uses documented metadata reference naming",
                file=file,
                dataset_id=dataset_id,
                field="schema_ref",
                location=location,
            )
        else:
            report.failed(
                "NAMING-METADATA-REF",
                "schema_ref must look like DATA_CONTRACT.md#anchor",
                file=file,
                dataset_id=dataset_id,
                field="schema_ref",
                location=location,
                details={"value": schema_ref},
            )

        for field_name in ("created_at", "updated_at"):
            value = entry.get(field_name)
            if is_utc_timestamp(value):
                report.passed(
                    "NAMING-TIMESTAMP",
                    f"{field_name} uses UTC ISO 8601 timestamp",
                    file=file,
                    dataset_id=dataset_id,
                    field=field_name,
                    location=location,
                )
            else:
                report.failed(
                    "NAMING-TIMESTAMP",
                    f"{field_name} must be UTC ISO 8601 with offset",
                    file=file,
                    dataset_id=dataset_id,
                    field=field_name,
                    location=location,
                    details={"value": value},
                )

        snapshot = entry.get("snapshot")
        if isinstance(snapshot, dict) and snapshot.get("snapshot_id"):
            snapshot_id = snapshot["snapshot_id"]
            if is_dataset_path(snapshot_id):
                report.passed(
                    "NAMING-SNAPSHOT-PATH",
                    "snapshot_id uses dataset-safe path naming",
                    file=file,
                    dataset_id=dataset_id,
                    field="snapshot.snapshot_id",
                    location=location,
                )
            else:
                report.failed(
                    "NAMING-SNAPSHOT-PATH",
                    "snapshot_id must use dataset-safe path naming",
                    file=file,
                    dataset_id=dataset_id,
                    field="snapshot.snapshot_id",
                    location=location,
                    details={"value": snapshot_id},
                )
        else:
            report.skipped(
                "NAMING-SNAPSHOT-PATH",
                "snapshot path naming skipped because no snapshot is published",
                file=file,
                dataset_id=dataset_id,
                field="snapshot",
                location=location,
            )

    if DOC_PATH_RE.fullmatch("validation_framework.md"):
        report.passed(
            "NAMING-DATASET-FILE-PATH",
            "validation framework doc uses docs/ snake_case filename",
            file="docs/validation_framework.md",
        )
    else:
        report.failed(
            "NAMING-DATASET-FILE-PATH",
            "validation framework doc path must be snake_case",
            file="docs/validation_framework.md",
        )

    return report
