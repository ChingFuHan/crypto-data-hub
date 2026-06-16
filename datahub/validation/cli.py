"""Command line interface for validation."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

from .errors import ValidationCommandError
from .lifecycle import validate_lifecycle
from .registry import validate_registry_file
from .result import ValidationReport
from .universe_metadata import validate_fixture

DEFAULT_UNIVERSE_ARTIFACT = (
    "data/reference/universe_metadata/reference.universe.metadata.json"
)
FALLBACK_UNIVERSE_FIXTURE = (
    "tests/fixtures/universe_metadata/valid_universe_metadata.json"
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m datahub.validation",
        description="Run crypto-data-hub validation checks.",
    )
    parser.add_argument(
        "--target",
        choices=("registry", "universe-metadata"),
        default="registry",
        help="validation target (default: registry)",
    )
    parser.add_argument(
        "--fixture",
        help="fixture path for --target universe-metadata",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="run registry plus default Universe Metadata fixture validation",
    )
    return parser


def default_universe_fixture() -> Path:
    artifact = Path(DEFAULT_UNIVERSE_ARTIFACT)
    if artifact.exists():
        return artifact
    return Path(FALLBACK_UNIVERSE_FIXTURE)


def run(args: argparse.Namespace) -> ValidationReport:
    if args.all and args.fixture:
        raise ValidationCommandError("--all does not accept --fixture")

    if args.all:
        report = ValidationReport()
        report.extend(validate_registry_file("dataset_registry.json"))
        report.extend(validate_lifecycle("dataset_registry.json", "."))
        fixture = default_universe_fixture()
        if not fixture.exists():
            raise ValidationCommandError(f"default fixture not found: {fixture}")
        report.extend(validate_fixture(fixture))
        return report

    if args.target == "registry":
        if args.fixture:
            raise ValidationCommandError("--target registry does not accept --fixture")
        report = ValidationReport()
        report.extend(validate_registry_file("dataset_registry.json"))
        report.extend(validate_lifecycle("dataset_registry.json", "."))
        return report

    if args.target == "universe-metadata":
        if not args.fixture:
            raise ValidationCommandError(
                "--target universe-metadata requires --fixture"
            )
        fixture = Path(args.fixture)
        if not fixture.exists():
            raise ValidationCommandError(f"fixture not found: {fixture}")
        return validate_fixture(fixture)

    raise ValidationCommandError(f"unknown target: {args.target}")


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    try:
        args = parser.parse_args(argv)
        report = run(args)
    except ValidationCommandError as exc:
        print(f"validation command error: {exc}", file=sys.stderr)
        return 2

    print(report.render())
    return 1 if report.has_failures else 0
