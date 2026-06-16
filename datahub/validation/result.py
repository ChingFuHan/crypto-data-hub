"""Validation result and report model."""

from __future__ import annotations

from dataclasses import dataclass, field as dataclass_field
from typing import Any

SEVERITIES = {"error", "warning", "info"}
STATUSES = {"passed", "failed", "skipped"}


@dataclass(frozen=True)
class ValidationCheck:
    """Single validation rule outcome."""

    rule_id: str
    severity: str
    status: str
    message: str
    file: str | None = None
    dataset_id: str | None = None
    field: str | None = None
    location: str | None = None
    details: dict[str, Any] = dataclass_field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.severity not in SEVERITIES:
            raise ValueError(f"invalid severity: {self.severity}")
        if self.status not in STATUSES:
            raise ValueError(f"invalid status: {self.status}")

    def to_dict(self) -> dict[str, Any]:
        return {
            "rule_id": self.rule_id,
            "severity": self.severity,
            "status": self.status,
            "message": self.message,
            "file": self.file,
            "dataset_id": self.dataset_id,
            "field": self.field,
            "location": self.location,
            "details": self.details,
        }


@dataclass
class ValidationReport:
    """Collection of validation checks plus summary helpers."""

    checks: list[ValidationCheck] = dataclass_field(default_factory=list)

    def add(self, check: ValidationCheck) -> None:
        self.checks.append(check)

    def passed(
        self,
        rule_id: str,
        message: str,
        *,
        file: str | None = None,
        dataset_id: str | None = None,
        field: str | None = None,
        location: str | None = None,
        severity: str = "info",
        details: dict[str, Any] | None = None,
    ) -> None:
        self.add(
            ValidationCheck(
                rule_id=rule_id,
                severity=severity,
                status="passed",
                message=message,
                file=file,
                dataset_id=dataset_id,
                field=field,
                location=location,
                details=details or {},
            )
        )

    def failed(
        self,
        rule_id: str,
        message: str,
        *,
        file: str | None = None,
        dataset_id: str | None = None,
        field: str | None = None,
        location: str | None = None,
        severity: str = "error",
        details: dict[str, Any] | None = None,
    ) -> None:
        self.add(
            ValidationCheck(
                rule_id=rule_id,
                severity=severity,
                status="failed",
                message=message,
                file=file,
                dataset_id=dataset_id,
                field=field,
                location=location,
                details=details or {},
            )
        )

    def skipped(
        self,
        rule_id: str,
        message: str,
        *,
        file: str | None = None,
        dataset_id: str | None = None,
        field: str | None = None,
        location: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        self.add(
            ValidationCheck(
                rule_id=rule_id,
                severity="info",
                status="skipped",
                message=message,
                file=file,
                dataset_id=dataset_id,
                field=field,
                location=location,
                details=details or {},
            )
        )

    def extend(self, other: "ValidationReport") -> None:
        self.checks.extend(other.checks)

    @property
    def total_checks(self) -> int:
        return len(self.checks)

    @property
    def passed_checks(self) -> int:
        return sum(1 for check in self.checks if check.status == "passed")

    @property
    def failed_checks(self) -> int:
        return sum(1 for check in self.checks if check.status == "failed")

    @property
    def skipped_checks(self) -> int:
        return sum(1 for check in self.checks if check.status == "skipped")

    @property
    def warning_checks(self) -> int:
        return sum(1 for check in self.checks if check.severity == "warning")

    @property
    def affected_files(self) -> list[str]:
        return sorted({check.file for check in self.checks if check.file})

    @property
    def affected_dataset_ids(self) -> list[str]:
        return sorted({check.dataset_id for check in self.checks if check.dataset_id})

    @property
    def has_failures(self) -> bool:
        return any(
            check.status == "failed" and check.severity == "error"
            for check in self.checks
        )

    @property
    def error_summary(self) -> list[ValidationCheck]:
        return [
            check
            for check in self.checks
            if check.status == "failed" and check.severity == "error"
        ]

    def render(self) -> str:
        lines = [
            "Validation Report",
            f"total checks: {self.total_checks}",
            f"passed checks: {self.passed_checks}",
            f"failed checks: {self.failed_checks}",
            f"warning checks: {self.warning_checks}",
            f"skipped checks: {self.skipped_checks}",
            "affected files: " + (", ".join(self.affected_files) or "none"),
            "affected dataset_id: "
            + (", ".join(self.affected_dataset_ids) or "none"),
        ]
        if self.error_summary:
            lines.append("error summary:")
            for check in self.error_summary:
                parts = [check.rule_id, check.message]
                if check.file:
                    parts.append(f"file={check.file}")
                if check.dataset_id:
                    parts.append(f"dataset_id={check.dataset_id}")
                if check.field:
                    parts.append(f"field={check.field}")
                if check.location:
                    parts.append(f"location={check.location}")
                lines.append("- " + " | ".join(parts))
        else:
            lines.append("error summary: none")
        return "\n".join(lines)
