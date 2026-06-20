"""Validation rules for Binance UM Kline **Parquet** materialized layers.

This validator inspects a machine-specific ``local_data`` materialization
manifest produced by
``datahub.materialization.binance_um_klines_parquet`` and queries the
resulting Parquet dataset through DuckDB. It is invoked explicitly with
``--manifest`` and only runs inside the clone-safe ``--all`` default when a
manifest is actually present (see ``datahub.validation.cli``).

Validation checks the **logical DuckDB schema** (Hive partition columns
``symbol`` and ``year`` re-exposed on top of the physical columns), not only the
physical parquet file schema.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .result import ValidationReport

RAW_DATASET_ID = "market.binance.um.klines"
ALLOWED_INTERVALS = ("1d", "4h", "1h", "15m", "5m", "3m")
INTERVAL_MILLISECONDS = {
    "1d": 86_400_000,
    "4h": 14_400_000,
    "1h": 3_600_000,
    "15m": 900_000,
    "5m": 300_000,
    "3m": 180_000,
}
ROWS_PER_SYMBOL_DATE_LIMIT = {
    "1d": 1,
    "4h": 6,
    "1h": 24,
    "15m": 96,
    "5m": 288,
    "3m": 480,
}
EXPECTED_PRIMARY_KEY = ["symbol", "interval", "open_time"]
GITIGNORE_LOCAL_DATA = "local_data/"

FULL_OUTPUT = "FULL_OUTPUT"
SAMPLE_OUTPUT = "SAMPLE_OUTPUT"

REQUIRED_LOGICAL_COLUMNS = (
    "symbol",
    "interval",
    "open_time",
    "open_time_utc",
    "open_time_taipei",
    "date",
    "year",
    "month",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "close_time",
    "quote_volume",
    "trade_count",
    "taker_buy_base_volume",
    "taker_buy_quote_volume",
    "source_archive",
    "archive_source",
    "archive_period",
)

REQUIRED_NON_NULL = REQUIRED_LOGICAL_COLUMNS  # all required logical columns non-null


def dataset_id_for_interval(interval: str) -> str:
    return f"{RAW_DATASET_ID}.{interval}.parquet"


DATASET_ID = dataset_id_for_interval("1d")


def validate_parquet_manifest(
    manifest_path: str | Path,
    interval: str,
    repo_root: str | Path = ".",
) -> ValidationReport:
    global DATASET_ID
    DATASET_ID = dataset_id_for_interval(interval)
    report = ValidationReport()
    path = Path(manifest_path)
    repo = Path(repo_root)
    file_name = str(path)

    if interval in ALLOWED_INTERVALS:
        report.passed(
            "PQ-INTERVAL-SUPPORTED",
            "interval is a supported Kline interval",
            file=file_name,
            dataset_id=DATASET_ID,
            field="interval",
        )
    else:
        report.failed(
            "PQ-INTERVAL-SUPPORTED",
            f"interval must be one of {' '.join(ALLOWED_INTERVALS)}",
            file=file_name,
            dataset_id=DATASET_ID,
            field="interval",
            details={"value": interval},
        )

    _validate_gitignore(repo, report)

    if not path.exists():
        report.failed(
            "PQ-MANIFEST-EXISTS",
            "materialization manifest does not exist",
            file=file_name,
            dataset_id=DATASET_ID,
        )
        return report
    try:
        manifest = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        report.failed(
            "PQ-MANIFEST-EXISTS",
            "materialization manifest is not readable JSON",
            file=file_name,
            dataset_id=DATASET_ID,
            details={"error": str(exc)},
        )
        return report
    report.passed(
        "PQ-MANIFEST-EXISTS",
        "materialization manifest exists and is valid JSON",
        file=file_name,
        dataset_id=DATASET_ID,
    )

    # Fixed-value manifest fields.
    _check_equals(report, manifest, "materialized_dataset_id", DATASET_ID,
                  "PQ-DATASET-ID", "materialized_dataset_id is fixed", file_name)
    _check_equals(report, manifest, "interval", interval,
                  "PQ-INTERVAL-MATCH", "manifest interval matches CLI interval",
                  file_name)
    _check_equals(report, manifest, "query_engine", "duckdb",
                  "PQ-QUERY-ENGINE", "query_engine is duckdb", file_name)
    _check_equals(report, manifest, "output_format", "parquet",
                  "PQ-OUTPUT-FORMAT", "output_format is parquet", file_name)
    _check_equals(report, manifest, "generated_csv_file_count", 0,
                  "PQ-NO-GENERATED-CSV", "generated_csv_file_count is zero",
                  file_name)

    # symbol_count / row_count / failed_symbol_count.
    _check_positive(report, manifest, "symbol_count", "PQ-SYMBOL-COUNT", file_name)
    _check_positive(report, manifest, "row_count", "PQ-ROW-COUNT", file_name)
    failed_symbol_count = manifest.get("failed_symbol_count")
    if failed_symbol_count == 0:
        report.passed(
            "PQ-FAILED-SYMBOLS", "failed_symbol_count is zero",
            file=file_name, dataset_id=DATASET_ID, field="failed_symbol_count",
        )
    else:
        report.failed(
            "PQ-FAILED-SYMBOLS", "failed_symbol_count must be zero",
            file=file_name, dataset_id=DATASET_ID, field="failed_symbol_count",
            details={"value": failed_symbol_count,
                     "failed_symbols": manifest.get("failed_symbols")},
        )

    # output_scope classification (SAMPLE separated from FULL; not a hard error).
    scope = manifest.get("output_scope")
    raw_n = manifest.get("raw_discovered_symbol_count")
    sym_n = manifest.get("symbol_count")
    delta = None if (raw_n is None or sym_n is None) else raw_n - sym_n
    if scope == FULL_OUTPUT:
        report.passed(
            "PQ-OUTPUT-SCOPE",
            f"output_scope is FULL_OUTPUT (covers the raw {interval} universe)",
            file=file_name, dataset_id=DATASET_ID, field="output_scope",
            details={"symbol_count": sym_n, "raw_discovered_symbol_count": raw_n,
                     "symbol_count_delta": delta},
        )
    elif scope == SAMPLE_OUTPUT:
        report.passed(
            "PQ-OUTPUT-SCOPE",
            f"output_scope is SAMPLE_OUTPUT (subset of the raw {interval} universe; "
            "not full completion)",
            file=file_name, dataset_id=DATASET_ID, field="output_scope",
            severity="warning",
            details={"symbol_count": sym_n, "raw_discovered_symbol_count": raw_n,
                     "symbol_count_delta": delta},
        )
    else:
        report.failed(
            "PQ-OUTPUT-SCOPE",
            "output_scope must be FULL_OUTPUT or SAMPLE_OUTPUT",
            file=file_name, dataset_id=DATASET_ID, field="output_scope",
            details={"value": scope},
        )

    if scope == FULL_OUTPUT and raw_n == sym_n and isinstance(sym_n, int) and sym_n > 0:
        report.passed(
            "PQ-FULL-SYMBOL-COVERAGE",
            "FULL_OUTPUT symbol_count equals raw_discovered_symbol_count",
            file=file_name, dataset_id=DATASET_ID, field="symbol_count",
            details={"symbol_count": sym_n, "raw_discovered_symbol_count": raw_n},
        )
    elif scope == FULL_OUTPUT:
        report.failed(
            "PQ-FULL-SYMBOL-COVERAGE",
            "FULL_OUTPUT must cover every raw discovered symbol",
            file=file_name, dataset_id=DATASET_ID, field="symbol_count",
            details={"symbol_count": sym_n, "raw_discovered_symbol_count": raw_n},
        )

    # output_root exists.
    output_root = manifest.get("output_root")
    root_path = Path(output_root) if output_root else None
    if root_path and root_path.exists():
        report.passed(
            "PQ-OUTPUT-ROOT", "output_root exists",
            file=output_root, dataset_id=DATASET_ID,
        )
    else:
        report.failed(
            "PQ-OUTPUT-ROOT", "output_root must exist",
            file=file_name, dataset_id=DATASET_ID,
            details={"value": output_root},
        )
        return report

    # Report files exist + non-empty.
    _validate_reports(report, root_path, file_name)

    # No persistent CSV anywhere under the parquet tree.
    csv_files = list(root_path.rglob("*.csv"))
    if csv_files:
        report.failed(
            "PQ-NO-CSV", "parquet output tree must contain no .csv files",
            file=output_root, dataset_id=DATASET_ID,
            details={"sample": [str(p) for p in csv_files[:5]],
                     "count": len(csv_files)},
        )
    else:
        report.passed(
            "PQ-NO-CSV", "parquet output tree contains no .csv files",
            file=output_root, dataset_id=DATASET_ID,
        )

    # Parquet files exist.
    parquet_files = list(root_path.rglob("*.parquet"))
    actual_file_count = len(parquet_files)
    if actual_file_count > 0:
        report.passed(
            "PQ-PARQUET-EXISTS", f"{actual_file_count} parquet files exist",
            file=output_root, dataset_id=DATASET_ID,
        )
    else:
        report.failed(
            "PQ-PARQUET-EXISTS", "no parquet files found under output_root",
            file=output_root, dataset_id=DATASET_ID,
        )
        return report

    # manifest file_count matches actual.
    if manifest.get("file_count") == actual_file_count:
        report.passed(
            "PQ-FILE-COUNT-MATCH",
            f"manifest file_count matches actual ({actual_file_count})",
            file=file_name, dataset_id=DATASET_ID, field="file_count",
        )
    else:
        report.failed(
            "PQ-FILE-COUNT-MATCH",
            "manifest file_count must match actual parquet file count",
            file=file_name, dataset_id=DATASET_ID, field="file_count",
            details={"manifest": manifest.get("file_count"),
                     "actual": actual_file_count},
        )

    # DuckDB-backed checks.
    _validate_with_duckdb(report, manifest, root_path, file_name, interval)

    return report


def _validate_with_duckdb(
    report: ValidationReport,
    manifest: dict[str, Any],
    root: Path,
    file_name: str,
    interval: str,
) -> None:
    try:
        import duckdb  # noqa: F401
    except ImportError as exc:
        report.failed(
            "PQ-DUCKDB-AVAILABLE",
            "duckdb is required to validate the parquet layer but is not "
            "installed (pip install duckdb)",
            file=file_name, dataset_id=DATASET_ID,
            details={"error": str(exc)},
        )
        return
    report.passed(
        "PQ-DUCKDB-AVAILABLE", "duckdb is importable",
        file=file_name, dataset_id=DATASET_ID,
    )

    import duckdb

    glob = str(root / "**" / "*.parquet")
    src = f"read_parquet('{glob}', hive_partitioning = true)"
    con = duckdb.connect()

    # DuckDB can read the parquet + logical schema.
    try:
        described = con.sql(f"DESCRIBE SELECT * FROM {src} LIMIT 1").fetchall()
    except Exception as exc:  # noqa: BLE001 - surface any DuckDB read failure
        report.failed(
            "PQ-DUCKDB-READ", "DuckDB failed to read the parquet dataset",
            file=file_name, dataset_id=DATASET_ID, details={"error": str(exc)},
        )
        return
    report.passed(
        "PQ-DUCKDB-READ", "DuckDB read the parquet dataset",
        file=file_name, dataset_id=DATASET_ID,
    )

    columns = {row[0] for row in described}
    missing = [c for c in REQUIRED_LOGICAL_COLUMNS if c not in columns]
    if missing:
        report.failed(
            "PQ-LOGICAL-SCHEMA",
            "logical DuckDB schema is missing required columns",
            file=file_name, dataset_id=DATASET_ID,
            details={"missing": missing, "present": sorted(columns)},
        )
    else:
        report.passed(
            "PQ-LOGICAL-SCHEMA",
            "logical DuckDB schema exposes all required columns "
            "(incl. Hive symbol/year)",
            file=file_name, dataset_id=DATASET_ID,
        )

    # row_count match.
    actual_rows = con.sql(f"SELECT COUNT(*) FROM {src}").fetchone()[0]
    if manifest.get("row_count") == actual_rows:
        report.passed(
            "PQ-ROW-COUNT-MATCH",
            f"manifest row_count matches DuckDB COUNT(*) ({actual_rows})",
            file=file_name, dataset_id=DATASET_ID, field="row_count",
        )
    else:
        report.failed(
            "PQ-ROW-COUNT-MATCH",
            "manifest row_count must match DuckDB COUNT(*)",
            file=file_name, dataset_id=DATASET_ID, field="row_count",
            details={"manifest": manifest.get("row_count"), "actual": actual_rows},
        )

    # Required fields non-null.
    null_pred = " OR ".join(f"{c} IS NULL" for c in REQUIRED_NON_NULL)
    bad_null = con.sql(
        f"SELECT COUNT(*) FROM {src} WHERE {null_pred}"
    ).fetchone()[0]
    _zero_check(report, bad_null, "PQ-NO-NULL",
                "required fields contain no NULL", "required fields have NULLs",
                file_name)

    # (symbol, interval, open_time) unique.
    dup_key = con.sql(
        f"SELECT COUNT(*) FROM (SELECT symbol, interval, open_time FROM {src} "
        f"GROUP BY symbol, interval, open_time HAVING COUNT(*) > 1)"
    ).fetchone()[0]
    _zero_check(report, dup_key, "PQ-UNIQUE-KEY",
                "(symbol, interval, open_time) is unique",
                "(symbol, interval, open_time) has duplicates", file_name)

    # Per-interval (symbol, date) cardinality policy.
    per_date_limit = ROWS_PER_SYMBOL_DATE_LIMIT[interval]
    max_rows_per_date = con.sql(
        f"SELECT COALESCE(MAX(n), 0) FROM ("
        f"SELECT symbol, date, COUNT(*) AS n FROM {src} "
        f"GROUP BY symbol, date)"
    ).fetchone()[0]
    if max_rows_per_date <= per_date_limit:
        report.passed(
            "PQ-SYMBOL-DATE-LIMIT",
            f"max rows per (symbol, date) <= {per_date_limit}",
            file=file_name,
            dataset_id=DATASET_ID,
            details={"max_rows_per_symbol_date": max_rows_per_date},
        )
    else:
        report.failed(
            "PQ-SYMBOL-DATE-LIMIT",
            f"max rows per (symbol, date) must be <= {per_date_limit}",
            file=file_name,
            dataset_id=DATASET_ID,
            details={"max_rows_per_symbol_date": max_rows_per_date},
        )

    # OHLC rule.
    bad_ohlc = con.sql(
        f"""
        SELECT COUNT(*) FROM {src}
        WHERE NOT (
            high >= low AND high >= open AND high >= close
            AND low <= open AND low <= close
            AND volume >= 0 AND quote_volume >= 0 AND trade_count >= 0
        )
        """
    ).fetchone()[0]
    _zero_check(report, bad_ohlc, "PQ-OHLC-RULE",
                "OHLC ordering and non-negativity rules pass",
                "OHLC rules violated", file_name)

    # Date policy: date == Taipei calendar date of open_time_taipei.
    bad_date = con.sql(
        f"SELECT COUNT(*) FROM {src} "
        f"WHERE CAST(open_time_taipei AS DATE) != CAST(date AS DATE)"
    ).fetchone()[0]
    _zero_check(report, bad_date, "PQ-DATE-POLICY",
                "date matches open_time_taipei calendar date",
                "date does not match open_time_taipei calendar date", file_name)

    interval_ms = INTERVAL_MILLISECONDS[interval]
    bad_open_alignment = con.sql(
        f"SELECT COUNT(*) FROM {src} WHERE open_time % {interval_ms} != 0"
    ).fetchone()[0]
    _zero_check(
        report,
        bad_open_alignment,
        "PQ-OPEN-TIME-ALIGNMENT",
        f"open_time is aligned to interval={interval}",
        f"open_time is not aligned to interval={interval}",
        file_name,
    )

    expected_delta = interval_ms - 1
    bad_close_time = con.sql(
        f"SELECT COUNT(*) FROM {src} "
        f"WHERE close_time != open_time + {expected_delta}"
    ).fetchone()[0]
    _zero_check(
        report,
        bad_close_time,
        "PQ-CLOSE-TIME-RULE",
        "close_time equals open_time + interval_ms - 1",
        "close_time does not equal open_time + interval_ms - 1",
        file_name,
    )

    if interval == "4h":
        _validate_row_count_regression(
            report,
            manifest,
            file_name,
            rule_id="PQ-4H-ROWS-GT-1D",
            this_interval="4h",
            base_interval="1d",
        )
    elif interval == "1h":
        _validate_row_count_regression(
            report,
            manifest,
            file_name,
            rule_id="PQ-1H-ROWS-GT-4H",
            this_interval="1h",
            base_interval="4h",
            min_ratio=3.5,
        )
    elif interval == "15m":
        _validate_row_count_regression(
            report,
            manifest,
            file_name,
            rule_id="PQ-15M-ROWS-GT-1H",
            this_interval="15m",
            base_interval="1h",
            min_ratio=3.5,
        )
    elif interval == "5m":
        _validate_row_count_regression(
            report,
            manifest,
            file_name,
            rule_id="PQ-5M-ROWS-GT-15M",
            this_interval="5m",
            base_interval="15m",
            min_ratio=2.5,
        )
    elif interval == "3m":
        _validate_row_count_regression(
            report,
            manifest,
            file_name,
            rule_id="PQ-3M-ROWS-GT-5M",
            this_interval="3m",
            base_interval="5m",
            min_ratio=1.5,
        )

    con.close()


def _validate_row_count_regression(
    report: ValidationReport,
    manifest: dict[str, Any],
    file_name: str,
    *,
    rule_id: str,
    this_interval: str,
    base_interval: str,
    min_ratio: float | None = None,
) -> None:
    """Assert this interval's FULL_OUTPUT row_count dominates a coarser interval.

    A finer interval has more bars per day, so its production row_count must be
    strictly greater than the coarser baseline. When ``min_ratio`` is given the
    ratio (this / base) must also clear that floor (e.g. 1h vs 4h >= 3.5).
    The check runs only for production FULL_OUTPUT (raw universe >= 921, i.e. the
    established symbol baseline plus any later listings); it is skipped for sample
    fixtures and when the baseline manifest is absent.
    """
    raw_discovered = manifest.get("raw_discovered_symbol_count")
    if manifest.get("output_scope") != FULL_OUTPUT or (
        not isinstance(raw_discovered, int) or raw_discovered < 921
    ):
        report.skipped(
            rule_id,
            f"{this_interval} > {base_interval} row-count check is required only "
            "for production FULL_OUTPUT",
            file=file_name,
            dataset_id=DATASET_ID,
            details={
                "output_scope": manifest.get("output_scope"),
                "raw_discovered_symbol_count": manifest.get(
                    "raw_discovered_symbol_count"
                ),
            },
        )
        return
    base_manifest = Path(
        f"local_data/binance_um_klines/interval={base_interval}/parquet/"
        "manifests/materialization_manifest.json"
    )
    if not base_manifest.exists():
        report.skipped(
            rule_id,
            f"{base_interval} materialization manifest absent; skipped "
            f"{this_interval} > {base_interval} row-count check",
            file=str(base_manifest),
            dataset_id=DATASET_ID,
        )
        return
    try:
        base = json.loads(base_manifest.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        report.failed(
            rule_id,
            f"{base_interval} materialization manifest is not readable JSON",
            file=str(base_manifest),
            dataset_id=DATASET_ID,
            details={"error": str(exc)},
        )
        return
    row_count = manifest.get("row_count")
    base_rows = base.get("row_count")
    if not (isinstance(row_count, int) and isinstance(base_rows, int) and base_rows > 0):
        report.failed(
            rule_id,
            f"{this_interval} and {base_interval} row_count must be positive integers",
            file=file_name,
            dataset_id=DATASET_ID,
            details={
                f"row_count_{this_interval}": row_count,
                f"row_count_{base_interval}": base_rows,
            },
        )
        return
    ratio = row_count / base_rows
    details = {
        f"row_count_{this_interval}": row_count,
        f"row_count_{base_interval}": base_rows,
        "ratio": ratio,
    }
    greater_ok = row_count > base_rows
    ratio_ok = min_ratio is None or ratio >= min_ratio
    if greater_ok and ratio_ok:
        msg = f"{this_interval} row_count is greater than {base_interval} row_count"
        if min_ratio is not None:
            msg += f" (ratio {ratio:.3f} >= {min_ratio})"
        report.passed(rule_id, msg, file=file_name, dataset_id=DATASET_ID,
                      details=details)
    else:
        msg = f"{this_interval} row_count must be greater than {base_interval} row_count"
        if min_ratio is not None:
            msg += f" with ratio >= {min_ratio}"
        report.failed(rule_id, msg, file=file_name, dataset_id=DATASET_ID,
                      details=details)


def _zero_check(
    report: ValidationReport, value: int, rule_id: str, ok_msg: str,
    bad_msg: str, file_name: str,
) -> None:
    if value == 0:
        report.passed(rule_id, ok_msg, file=file_name, dataset_id=DATASET_ID)
    else:
        report.failed(
            rule_id, bad_msg, file=file_name, dataset_id=DATASET_ID,
            details={"bad_rows": value},
        )


def _validate_reports(
    report: ValidationReport, root: Path, file_name: str
) -> None:
    reports_dir = root / "reports"
    required = (
        "coverage_report.json",
        "data_quality_report.json",
        "duplicate_report.json",
        "conflict_report.json",
    )
    missing = []
    empty = []
    for name in required:
        p = reports_dir / name
        if not p.exists():
            missing.append(name)
        elif p.stat().st_size == 0:
            empty.append(name)
    if missing or empty:
        report.failed(
            "PQ-REPORTS-EXIST",
            "all report files must exist and be non-empty",
            file=str(reports_dir), dataset_id=DATASET_ID,
            details={"missing": missing, "empty": empty},
        )
    else:
        report.passed(
            "PQ-REPORTS-EXIST", "all report files exist and are non-empty",
            file=str(reports_dir), dataset_id=DATASET_ID,
        )


def _check_equals(
    report: ValidationReport, manifest: dict[str, Any], field_name: str,
    expected: Any, rule_id: str, message: str, file_name: str,
) -> None:
    actual = manifest.get(field_name)
    if actual == expected:
        report.passed(rule_id, message, file=file_name, dataset_id=DATASET_ID,
                      field=field_name)
    else:
        report.failed(
            rule_id, f"{message} (expected {expected!r})",
            file=file_name, dataset_id=DATASET_ID, field=field_name,
            details={"expected": expected, "value": actual},
        )


def _check_positive(
    report: ValidationReport, manifest: dict[str, Any], field_name: str,
    rule_id: str, file_name: str,
) -> None:
    value = manifest.get(field_name)
    if isinstance(value, int) and value > 0:
        report.passed(rule_id, f"{field_name} > 0", file=file_name,
                      dataset_id=DATASET_ID, field=field_name)
    else:
        report.failed(
            rule_id, f"{field_name} must be > 0", file=file_name,
            dataset_id=DATASET_ID, field=field_name, details={"value": value},
        )


def _validate_gitignore(repo: Path, report: ValidationReport) -> None:
    gitignore = repo / ".gitignore"
    text = gitignore.read_text(encoding="utf-8") if gitignore.exists() else ""
    entries = {line.strip() for line in text.splitlines()}
    if GITIGNORE_LOCAL_DATA in entries or "local_data" in entries:
        report.passed(
            "PQ-LOCAL-DATA-IGNORED", "local_data is excluded from Git",
            file=".gitignore", dataset_id=DATASET_ID,
        )
    else:
        report.failed(
            "PQ-LOCAL-DATA-IGNORED", ".gitignore must exclude local_data/",
            file=".gitignore", dataset_id=DATASET_ID,
        )
