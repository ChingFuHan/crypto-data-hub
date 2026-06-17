"""Command line interface for validation."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

from .binance_um_klines import ALLOWED_INTERVALS, validate_klines_manifest
from .binance_um_klines_parquet import (
    ALLOWED_INTERVALS as PARQUET_ALLOWED_INTERVALS,
    validate_parquet_manifest,
)
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


def default_klines_manifest(interval: str) -> str:
    return f"local_data/binance_um_klines/interval={interval}/manifests/manifest.json"


def default_parquet_manifest(interval: str) -> str:
    return (
        f"local_data/binance_um_klines/interval={interval}/parquet/"
        "manifests/materialization_manifest.json"
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m datahub.validation",
        description="Run crypto-data-hub validation checks.",
    )
    parser.add_argument(
        "--target",
        choices=(
            "registry",
            "universe-metadata",
            "binance-um-klines",
            "binance-um-klines-parquet",
        ),
        default="registry",
        help="validation target (default: registry)",
    )
    parser.add_argument(
        "--fixture",
        help="fixture path for --target universe-metadata",
    )
    parser.add_argument(
        "--interval",
        default="1d",
        choices=ALLOWED_INTERVALS,
        help="Kline interval for --target binance-um-klines (default: 1d)",
    )
    parser.add_argument(
        "--manifest",
        help="klines run manifest path for --target binance-um-klines",
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
        # Clone-safe: only validate the klines local_data manifest if it exists.
        klines_manifest = Path(default_klines_manifest(args.interval))
        if klines_manifest.exists():
            report.extend(
                validate_klines_manifest(klines_manifest, args.interval, ".")
            )
        else:
            report.skipped(
                "KL-MANIFEST-EXISTS",
                "klines manifest absent; skipped for clone-safe --all "
                "(provide --target binance-um-klines --manifest to validate local_data)",
                file=str(klines_manifest),
                dataset_id="market.binance.um.klines",
            )
        # Clone-safe: only validate the parquet materialized layer if present.
        parquet_manifest = Path(default_parquet_manifest(args.interval))
        if parquet_manifest.exists():
            report.extend(
                validate_parquet_manifest(parquet_manifest, args.interval, ".")
            )
        else:
            report.skipped(
                "PQ-MANIFEST-EXISTS",
                "parquet materialization manifest absent; skipped for clone-safe "
                "--all (provide --target binance-um-klines-parquet --manifest to "
                "validate local_data)",
                file=str(parquet_manifest),
                dataset_id="market.binance.um.klines.1d.parquet",
            )
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

    if args.target == "binance-um-klines":
        if not args.manifest:
            raise ValidationCommandError(
                "--target binance-um-klines requires --manifest "
                "(large local_data validation is explicit, never clone-safe default)"
            )
        return validate_klines_manifest(args.manifest, args.interval, ".")

    if args.target == "binance-um-klines-parquet":
        if args.interval not in PARQUET_ALLOWED_INTERVALS:
            raise ValidationCommandError(
                "--target binance-um-klines-parquet supports only "
                f"interval(s): {' '.join(PARQUET_ALLOWED_INTERVALS)}"
            )
        if not args.manifest:
            raise ValidationCommandError(
                "--target binance-um-klines-parquet requires --manifest "
                "(large local_data validation is explicit, never clone-safe default)"
            )
        return validate_parquet_manifest(args.manifest, args.interval, ".")

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
