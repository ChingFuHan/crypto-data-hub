"""Registry validation rules."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from . import naming
from .result import ValidationReport

SOURCE_TYPES = {"api", "file", "onchain", "derived"}


def load_json(path: Path) -> tuple[ValidationReport, Any | None]:
    report = ValidationReport()
    try:
        with path.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
    except json.JSONDecodeError as exc:
        report.failed(
            "REGISTRY-JSON",
            "dataset_registry.json is not valid JSON",
            file=str(path),
            location=f"line {exc.lineno} column {exc.colno}",
        )
        return report, None
    except OSError as exc:
        report.failed(
            "REGISTRY-JSON",
            "dataset_registry.json could not be read",
            file=str(path),
            details={"error": str(exc)},
        )
        return report, None
    report.passed("REGISTRY-JSON", "dataset_registry.json is valid JSON", file=str(path))
    return report, data


def validate_registry_file(path: str | Path = "dataset_registry.json") -> ValidationReport:
    registry_path = Path(path)
    report, registry = load_json(registry_path)
    file_name = str(registry_path)
    if registry is None:
        return report

    if not isinstance(registry, dict):
        report.failed(
            "REGISTRY-ROOT",
            "registry root must be a JSON object",
            file=file_name,
        )
        return report
    report.passed("REGISTRY-ROOT", "registry root is a JSON object", file=file_name)

    _validate_top_level(registry, report, file_name)
    if not isinstance(registry.get("dataset_entry_schema"), dict):
        return report
    if not isinstance(registry.get("datasets"), list):
        return report

    _validate_entries(registry, report, file_name)
    report.extend(naming.validate_registry_naming(registry, file=file_name))
    return report


def _validate_top_level(
    registry: dict[str, Any], report: ValidationReport, file_name: str
) -> None:
    if naming.is_semver(registry.get("registry_version")):
        report.passed(
            "REGISTRY-VERSION-FORMAT",
            "registry_version follows v-prefixed SemVer",
            file=file_name,
            field="registry_version",
        )
    else:
        report.failed(
            "REGISTRY-VERSION-FORMAT",
            "registry_version must follow v-prefixed SemVer",
            file=file_name,
            field="registry_version",
        )

    conventions = registry.get("conventions")
    if isinstance(conventions, dict):
        report.passed(
            "REGISTRY-CONVENTIONS",
            "conventions block is present",
            file=file_name,
            field="conventions",
        )
        for key in (
            "dataset_id_pattern",
            "version_pattern",
            "timestamp_format",
            "default_timezone",
            "lifecycle_states",
            "status_enum",
        ):
            if key in conventions:
                report.passed(
                    "REGISTRY-CONVENTION-FIELD",
                    f"conventions.{key} is present",
                    file=file_name,
                    field=f"conventions.{key}",
                )
            else:
                report.failed(
                    "REGISTRY-CONVENTION-FIELD",
                    f"conventions.{key} is required",
                    file=file_name,
                    field=f"conventions.{key}",
                )
    else:
        report.failed(
            "REGISTRY-CONVENTIONS",
            "conventions block must be present",
            file=file_name,
            field="conventions",
        )

    if isinstance(registry.get("dataset_entry_schema"), dict):
        report.passed(
            "REGISTRY-DATASET-ENTRY-SCHEMA",
            "dataset_entry_schema block is present",
            file=file_name,
            field="dataset_entry_schema",
        )
    else:
        report.failed(
            "REGISTRY-DATASET-ENTRY-SCHEMA",
            "dataset_entry_schema block must be present",
            file=file_name,
            field="dataset_entry_schema",
        )

    if isinstance(registry.get("datasets"), list):
        report.passed(
            "REGISTRY-DATASETS-ARRAY",
            "datasets array is present",
            file=file_name,
            field="datasets",
        )
    else:
        report.failed(
            "REGISTRY-DATASETS-ARRAY",
            "datasets must be an array",
            file=file_name,
            field="datasets",
        )


def _validate_entries(
    registry: dict[str, Any], report: ValidationReport, file_name: str
) -> None:
    schema = registry["dataset_entry_schema"]
    required_fields = [
        field_name
        for field_name, field_schema in schema.items()
        if isinstance(field_schema, dict) and field_schema.get("required") is True
    ]
    seen_dataset_ids: set[str] = set()
    lifecycle_states = set(registry.get("conventions", {}).get("status_enum", []))

    for index, entry in enumerate(registry["datasets"]):
        location = f"datasets[{index}]"
        if not isinstance(entry, dict):
            report.failed(
                "REGISTRY-DATASET-ENTRY",
                "dataset entry must be an object",
                file=file_name,
                location=location,
            )
            continue
        dataset_id = entry.get("dataset_id")

        for field_name in required_fields:
            if field_name in entry:
                report.passed(
                    "REGISTRY-REQUIRED-FIELD",
                    f"{field_name} is present",
                    file=file_name,
                    dataset_id=dataset_id,
                    field=field_name,
                    location=location,
                )
            else:
                report.failed(
                    "REGISTRY-REQUIRED-FIELD",
                    f"{field_name} is required",
                    file=file_name,
                    dataset_id=dataset_id,
                    field=field_name,
                    location=location,
                )

        if dataset_id in seen_dataset_ids:
            report.failed(
                "REGISTRY-DATASET-ID-UNIQUE",
                "dataset_id must be unique",
                file=file_name,
                dataset_id=dataset_id,
                field="dataset_id",
                location=location,
            )
        else:
            seen_dataset_ids.add(dataset_id)
            report.passed(
                "REGISTRY-DATASET-ID-UNIQUE",
                "dataset_id is unique so far",
                file=file_name,
                dataset_id=dataset_id,
                field="dataset_id",
                location=location,
            )

        _validate_entry_types(entry, schema, report, file_name, location)
        _validate_entry_semantics(
            entry, report, file_name, location, lifecycle_states, seen_dataset_ids
        )

    _validate_lineage_references(registry["datasets"], report, file_name)


def _validate_entry_types(
    entry: dict[str, Any],
    schema: dict[str, Any],
    report: ValidationReport,
    file_name: str,
    location: str,
) -> None:
    dataset_id = entry.get("dataset_id")
    for field_name, field_schema in schema.items():
        if field_name not in entry or not isinstance(field_schema, dict):
            continue
        expected_type = field_schema.get("type")
        value = entry[field_name]
        if _matches_schema_type(value, expected_type):
            report.passed(
                "REGISTRY-FIELD-TYPE",
                f"{field_name} matches declared schema type",
                file=file_name,
                dataset_id=dataset_id,
                field=field_name,
                location=location,
            )
        else:
            report.failed(
                "REGISTRY-FIELD-TYPE",
                f"{field_name} does not match declared schema type",
                file=file_name,
                dataset_id=dataset_id,
                field=field_name,
                location=location,
                details={"expected": expected_type, "value": value},
            )


def _matches_schema_type(value: Any, expected_type: str | None) -> bool:
    if expected_type == "string":
        return isinstance(value, str)
    if expected_type == "enum":
        return isinstance(value, str)
    if expected_type == "object":
        return isinstance(value, dict)
    if expected_type == "array<string>":
        return isinstance(value, list) and all(isinstance(item, str) for item in value)
    if expected_type == "string | null":
        return value is None or isinstance(value, str)
    if expected_type == "object | null":
        return value is None or isinstance(value, dict)
    if expected_type == "boolean":
        return isinstance(value, bool)
    return True


def _validate_entry_semantics(
    entry: dict[str, Any],
    report: ValidationReport,
    file_name: str,
    location: str,
    lifecycle_states: set[str],
    seen_dataset_ids: set[str],
) -> None:
    dataset_id = entry.get("dataset_id")

    if naming.is_dataset_id(dataset_id):
        report.passed(
            "REGISTRY-DATASET-ID-PATTERN",
            "dataset_id matches conventions.dataset_id_pattern",
            file=file_name,
            dataset_id=dataset_id,
            field="dataset_id",
            location=location,
        )
    else:
        report.failed(
            "REGISTRY-DATASET-ID-PATTERN",
            "dataset_id does not match conventions.dataset_id_pattern",
            file=file_name,
            field="dataset_id",
            location=location,
        )

    if naming.is_semver(entry.get("version")):
        report.passed(
            "REGISTRY-DATASET-VERSION-FORMAT",
            "dataset version follows v-prefixed SemVer",
            file=file_name,
            dataset_id=dataset_id,
            field="version",
            location=location,
        )
    else:
        report.failed(
            "REGISTRY-DATASET-VERSION-FORMAT",
            "dataset version must follow v-prefixed SemVer",
            file=file_name,
            dataset_id=dataset_id,
            field="version",
            location=location,
        )

    status = entry.get("status")
    if status in lifecycle_states:
        report.passed(
            "REGISTRY-STATUS-ENUM",
            "status is a valid lifecycle state",
            file=file_name,
            dataset_id=dataset_id,
            field="status",
            location=location,
        )
    else:
        report.failed(
            "REGISTRY-STATUS-ENUM",
            "status must be one of conventions.status_enum",
            file=file_name,
            dataset_id=dataset_id,
            field="status",
            location=location,
            details={"value": status},
        )

    _validate_owner(entry, report, file_name, location)
    _validate_source(entry, report, file_name, location)
    _validate_timezone(entry, report, file_name, location)
    _validate_update_frequency(entry, report, file_name, location)
    _validate_primary_key(entry, report, file_name, location)
    _validate_schema_ref(entry, report, file_name, location)
    _validate_timestamps(entry, report, file_name, location)
    _validate_quality(entry, report, file_name, location)
    _validate_provenance(entry, report, file_name, location)


def _validate_owner(
    entry: dict[str, Any], report: ValidationReport, file_name: str, location: str
) -> None:
    owner = entry.get("owner")
    dataset_id = entry.get("dataset_id")
    if naming.is_owner(owner):
        report.passed(
            "REGISTRY-OWNER-FORMAT",
            "owner uses stable machine-friendly format",
            file=file_name,
            dataset_id=dataset_id,
            field="owner",
            location=location,
        )
    else:
        report.failed(
            "REGISTRY-OWNER-FORMAT",
            "owner must be non-empty lowercase id text",
            file=file_name,
            dataset_id=dataset_id,
            field="owner",
            location=location,
            details={"value": owner},
        )


def _validate_source(
    entry: dict[str, Any], report: ValidationReport, file_name: str, location: str
) -> None:
    source = entry.get("source")
    dataset_id = entry.get("dataset_id")
    if (
        isinstance(source, dict)
        and source.get("type") in SOURCE_TYPES
        and isinstance(source.get("reference"), str)
        and source.get("reference")
    ):
        report.passed(
            "REGISTRY-SOURCE-FORMAT",
            "source has valid type and reference",
            file=file_name,
            dataset_id=dataset_id,
            field="source",
            location=location,
        )
    else:
        report.failed(
            "REGISTRY-SOURCE-FORMAT",
            "source must contain type api|file|onchain|derived and reference",
            file=file_name,
            dataset_id=dataset_id,
            field="source",
            location=location,
            details={"value": source},
        )


def _validate_timezone(
    entry: dict[str, Any], report: ValidationReport, file_name: str, location: str
) -> None:
    timezone = entry.get("timezone")
    dataset_id = entry.get("dataset_id")
    if naming.is_timezone(timezone):
        report.passed(
            "REGISTRY-TIMEZONE-FORMAT",
            "timezone is UTC or IANA-like",
            file=file_name,
            dataset_id=dataset_id,
            field="timezone",
            location=location,
        )
    else:
        report.failed(
            "REGISTRY-TIMEZONE-FORMAT",
            "timezone must be UTC or IANA-like",
            file=file_name,
            dataset_id=dataset_id,
            field="timezone",
            location=location,
            details={"value": timezone},
        )


def _validate_update_frequency(
    entry: dict[str, Any], report: ValidationReport, file_name: str, location: str
) -> None:
    frequency = entry.get("update_frequency")
    dataset_id = entry.get("dataset_id")
    if naming.is_update_frequency(frequency):
        report.passed(
            "REGISTRY-UPDATE-FREQUENCY-FORMAT",
            "update_frequency uses documented cadence format",
            file=file_name,
            dataset_id=dataset_id,
            field="update_frequency",
            location=location,
        )
    else:
        report.failed(
            "REGISTRY-UPDATE-FREQUENCY-FORMAT",
            "update_frequency must be a documented cadence",
            file=file_name,
            dataset_id=dataset_id,
            field="update_frequency",
            location=location,
            details={"value": frequency},
        )


def _validate_primary_key(
    entry: dict[str, Any], report: ValidationReport, file_name: str, location: str
) -> None:
    primary_key = entry.get("primary_key")
    dataset_id = entry.get("dataset_id")
    if (
        isinstance(primary_key, list)
        and primary_key
        and all(naming.is_field_name(field) for field in primary_key)
    ):
        report.passed(
            "REGISTRY-PRIMARY-KEY-FORMAT",
            "primary_key is a non-empty array of field names",
            file=file_name,
            dataset_id=dataset_id,
            field="primary_key",
            location=location,
        )
    else:
        report.failed(
            "REGISTRY-PRIMARY-KEY-FORMAT",
            "primary_key must be a non-empty array of snake_case fields",
            file=file_name,
            dataset_id=dataset_id,
            field="primary_key",
            location=location,
            details={"value": primary_key},
        )


def _validate_schema_ref(
    entry: dict[str, Any], report: ValidationReport, file_name: str, location: str
) -> None:
    schema_ref = entry.get("schema_ref")
    dataset_id = entry.get("dataset_id")
    if naming.is_schema_ref(schema_ref):
        report.passed(
            "REGISTRY-SCHEMA-REF-FORMAT",
            "schema_ref points to a markdown anchor",
            file=file_name,
            dataset_id=dataset_id,
            field="schema_ref",
            location=location,
        )
    else:
        report.failed(
            "REGISTRY-SCHEMA-REF-FORMAT",
            "schema_ref must look like DATA_CONTRACT.md#contract-name",
            file=file_name,
            dataset_id=dataset_id,
            field="schema_ref",
            location=location,
            details={"value": schema_ref},
        )


def _validate_timestamps(
    entry: dict[str, Any], report: ValidationReport, file_name: str, location: str
) -> None:
    dataset_id = entry.get("dataset_id")
    created_at = naming.parse_utc_timestamp(entry.get("created_at"))
    updated_at = naming.parse_utc_timestamp(entry.get("updated_at"))
    if created_at:
        report.passed(
            "REGISTRY-CREATED-AT-FORMAT",
            "created_at is UTC ISO 8601",
            file=file_name,
            dataset_id=dataset_id,
            field="created_at",
            location=location,
        )
    else:
        report.failed(
            "REGISTRY-CREATED-AT-FORMAT",
            "created_at must be UTC ISO 8601 with offset",
            file=file_name,
            dataset_id=dataset_id,
            field="created_at",
            location=location,
        )
    if updated_at:
        report.passed(
            "REGISTRY-UPDATED-AT-FORMAT",
            "updated_at is UTC ISO 8601",
            file=file_name,
            dataset_id=dataset_id,
            field="updated_at",
            location=location,
        )
    else:
        report.failed(
            "REGISTRY-UPDATED-AT-FORMAT",
            "updated_at must be UTC ISO 8601 with offset",
            file=file_name,
            dataset_id=dataset_id,
            field="updated_at",
            location=location,
        )
    if created_at and updated_at and created_at <= updated_at:
        report.passed(
            "REGISTRY-TIMESTAMP-ORDER",
            "created_at is before or equal to updated_at",
            file=file_name,
            dataset_id=dataset_id,
            location=location,
        )
    elif created_at and updated_at:
        report.failed(
            "REGISTRY-TIMESTAMP-ORDER",
            "created_at must be before or equal to updated_at",
            file=file_name,
            dataset_id=dataset_id,
            location=location,
        )


def _validate_quality(
    entry: dict[str, Any], report: ValidationReport, file_name: str, location: str
) -> None:
    quality = entry.get("quality")
    dataset_id = entry.get("dataset_id")
    if (
        isinstance(quality, dict)
        and isinstance(quality.get("contract_validated"), bool)
        and (
            quality.get("last_validated_at") is None
            or naming.is_utc_timestamp(quality.get("last_validated_at"))
        )
    ):
        report.passed(
            "REGISTRY-QUALITY-FORMAT",
            "quality records contract_validated and validation timestamp state",
            file=file_name,
            dataset_id=dataset_id,
            field="quality",
            location=location,
        )
    else:
        report.failed(
            "REGISTRY-QUALITY-FORMAT",
            "quality must contain contract_validated boolean and nullable UTC timestamp",
            file=file_name,
            dataset_id=dataset_id,
            field="quality",
            location=location,
            details={"value": quality},
        )


def _validate_provenance(
    entry: dict[str, Any], report: ValidationReport, file_name: str, location: str
) -> None:
    provenance = entry.get("provenance")
    dataset_id = entry.get("dataset_id")
    if (
        isinstance(provenance, dict)
        and naming.is_semver(provenance.get("code_version"))
        and isinstance(provenance.get("params"), dict)
        and isinstance(provenance.get("generated_by"), str)
        and "checksum" in provenance
    ):
        report.passed(
            "REGISTRY-PROVENANCE-FORMAT",
            "provenance contains reproducibility fields",
            file=file_name,
            dataset_id=dataset_id,
            field="provenance",
            location=location,
        )
    else:
        report.failed(
            "REGISTRY-PROVENANCE-FORMAT",
            "provenance must contain code_version, params, generated_by, checksum",
            file=file_name,
            dataset_id=dataset_id,
            field="provenance",
            location=location,
            details={"value": provenance},
        )


def _validate_lineage_references(
    entries: list[dict[str, Any]], report: ValidationReport, file_name: str
) -> None:
    dataset_ids = {
        entry.get("dataset_id") for entry in entries if isinstance(entry, dict)
    }
    for index, entry in enumerate(entries):
        if not isinstance(entry, dict):
            continue
        dataset_id = entry.get("dataset_id")
        lineage = entry.get("lineage")
        location = f"datasets[{index}].lineage"
        if not isinstance(lineage, dict):
            report.skipped(
                "REGISTRY-LINEAGE-UPSTREAM",
                "lineage upstream check skipped because lineage is absent",
                file=file_name,
                dataset_id=dataset_id,
                field="lineage",
                location=location,
            )
            continue
        upstream = lineage.get("upstream", [])
        if isinstance(upstream, list) and all(item in dataset_ids for item in upstream):
            report.passed(
                "REGISTRY-LINEAGE-UPSTREAM",
                "lineage.upstream references registered dataset_ids",
                file=file_name,
                dataset_id=dataset_id,
                field="lineage.upstream",
                location=location,
            )
        else:
            report.failed(
                "REGISTRY-LINEAGE-UPSTREAM",
                "lineage.upstream must reference registered dataset_ids",
                file=file_name,
                dataset_id=dataset_id,
                field="lineage.upstream",
                location=location,
                details={"upstream": upstream},
            )
