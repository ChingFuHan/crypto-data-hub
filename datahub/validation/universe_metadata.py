"""Universe Metadata dataset validation rules."""

from __future__ import annotations

from collections import defaultdict
from decimal import Decimal, InvalidOperation
import json
from pathlib import Path
from typing import Any

from .naming import is_dataset_id, parse_utc_timestamp
from .result import ValidationReport

DATASET_ID = "reference.universe.metadata"
REQUIRED_FIELDS = (
    "instrument_id",
    "symbol",
    "exchange",
    "market_type",
    "status",
    "listed_at",
)
ALL_FIELDS = {
    "instrument_id",
    "symbol",
    "exchange",
    "base_asset",
    "quote_asset",
    "market_type",
    "contract_type",
    "status",
    "listed_at",
    "delisted_at",
    "successor_id",
    "tick_size",
    "step_size",
    "contract_size",
}
STATUS_ENUM = {"active", "delisted", "renamed", "merged"}
MARKET_TYPES = {"spot", "futures", "perpetual", "option"}
LINEAR_TYPES = {"linear", "inverse"}
OPTION_TYPES = {"call", "put"}


def validate_fixture(path: str | Path) -> ValidationReport:
    fixture_path = Path(path)
    report = ValidationReport()
    try:
        raw = json.loads(fixture_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        report.failed(
            "UM-FIXTURE-JSON",
            "Universe Metadata fixture is not valid JSON",
            file=str(fixture_path),
            dataset_id=DATASET_ID,
            location=f"line {exc.lineno} column {exc.colno}",
        )
        return report
    except OSError as exc:
        report.failed(
            "UM-FIXTURE-JSON",
            "Universe Metadata fixture could not be read",
            file=str(fixture_path),
            dataset_id=DATASET_ID,
            details={"error": str(exc)},
        )
        return report

    records, ingestion_time = _extract_records(raw)
    if records is None:
        report.failed(
            "UM-FIXTURE-SHAPE",
            "fixture must be an array or object with records array",
            file=str(fixture_path),
            dataset_id=DATASET_ID,
        )
        return report
    report.passed(
        "UM-FIXTURE-SHAPE",
        "fixture contains records array",
        file=str(fixture_path),
        dataset_id=DATASET_ID,
    )

    report.extend(_validate_q1(records, fixture_path))
    report.extend(_validate_q2(records, fixture_path))
    report.extend(_validate_q3(records, fixture_path))
    report.extend(_validate_q4(records, fixture_path, ingestion_time))
    report.extend(_validate_q5(records, fixture_path))
    report.extend(_validate_q6(records, fixture_path))
    report.extend(_validate_pit(records, fixture_path))
    return report


def _extract_records(raw: Any) -> tuple[list[dict[str, Any]] | None, Any]:
    if isinstance(raw, list):
        return raw if all(isinstance(item, dict) for item in raw) else None, None
    if isinstance(raw, dict) and isinstance(raw.get("records"), list):
        records = raw["records"]
        if all(isinstance(item, dict) for item in records):
            return records, raw.get("ingestion_time")
    return None, None


def _validate_q1(records: list[dict[str, Any]], fixture_path: Path) -> ValidationReport:
    report = ValidationReport()
    failures = []
    for index, record in enumerate(records):
        for field in REQUIRED_FIELDS:
            if field not in record or record[field] in (None, ""):
                failures.append({"record": index, "field": field})
    if failures:
        for failure in failures:
            report.failed(
                "UM-Q1",
                "required field is missing or null",
                file=str(fixture_path),
                dataset_id=DATASET_ID,
                field=failure["field"],
                location=f"records[{failure['record']}]",
                details=failure,
            )
    else:
        report.passed(
            "UM-Q1",
            "required fields are present and non-null",
            file=str(fixture_path),
            dataset_id=DATASET_ID,
        )
    return report


def _validate_q2(records: list[dict[str, Any]], fixture_path: Path) -> ValidationReport:
    report = ValidationReport()
    instrument_ids: dict[Any, int] = {}
    symbol_eras: dict[tuple[Any, Any, Any], int] = {}
    active_symbols: dict[tuple[Any, Any], int] = {}
    failures = []

    for index, record in enumerate(records):
        instrument_id = record.get("instrument_id")
        if instrument_id in instrument_ids:
            failures.append(
                {
                    "kind": "instrument_id",
                    "record": index,
                    "first_record": instrument_ids[instrument_id],
                    "value": instrument_id,
                }
            )
        else:
            instrument_ids[instrument_id] = index

        symbol_key = (record.get("exchange"), record.get("symbol"), record.get("listed_at"))
        if symbol_key in symbol_eras:
            failures.append(
                {
                    "kind": "symbol_era",
                    "record": index,
                    "first_record": symbol_eras[symbol_key],
                    "value": symbol_key,
                }
            )
        else:
            symbol_eras[symbol_key] = index

        if record.get("status") == "active":
            active_key = (record.get("exchange"), record.get("symbol"))
            if active_key in active_symbols:
                failures.append(
                    {
                        "kind": "active_symbol",
                        "record": index,
                        "first_record": active_symbols[active_key],
                        "value": active_key,
                    }
                )
            else:
                active_symbols[active_key] = index

    if failures:
        for failure in failures:
            report.failed(
                "UM-Q2",
                "Universe Metadata uniqueness rule failed",
                file=str(fixture_path),
                dataset_id=DATASET_ID,
                location=f"records[{failure['record']}]",
                details=failure,
            )
    else:
        report.passed(
            "UM-Q2",
            "instrument_id, symbol-era, and active-symbol uniqueness hold",
            file=str(fixture_path),
            dataset_id=DATASET_ID,
        )
    return report


def _validate_q3(records: list[dict[str, Any]], fixture_path: Path) -> ValidationReport:
    report = ValidationReport()
    failures = []
    for index, record in enumerate(records):
        status = record.get("status")
        delisted_at = record.get("delisted_at")
        successor_id = record.get("successor_id")
        if status not in STATUS_ENUM:
            failures.append({"record": index, "field": "status", "value": status})
        if (status == "active") != (delisted_at is None):
            failures.append(
                {
                    "record": index,
                    "field": "delisted_at",
                    "status": status,
                    "delisted_at": delisted_at,
                }
            )
        if (status in {"renamed", "merged"}) != (successor_id is not None):
            failures.append(
                {
                    "record": index,
                    "field": "successor_id",
                    "status": status,
                    "successor_id": successor_id,
                }
            )
    if failures:
        for failure in failures:
            report.failed(
                "UM-Q3",
                "symbol lifecycle invariant failed",
                file=str(fixture_path),
                dataset_id=DATASET_ID,
                field=failure["field"],
                location=f"records[{failure['record']}]",
                details=failure,
            )
    else:
        report.passed(
            "UM-Q3",
            "status, delisted_at, and successor_id invariants hold",
            file=str(fixture_path),
            dataset_id=DATASET_ID,
        )
    return report


def _validate_q4(
    records: list[dict[str, Any]], fixture_path: Path, ingestion_time: Any
) -> ValidationReport:
    report = ValidationReport()
    ingestion_dt = parse_utc_timestamp(ingestion_time) if ingestion_time else None
    failures = []
    if ingestion_time and ingestion_dt is None:
        failures.append(
            {
                "record": None,
                "field": "ingestion_time",
                "value": ingestion_time,
                "reason": "invalid ingestion_time",
            }
        )

    for index, record in enumerate(records):
        listed_at = parse_utc_timestamp(record.get("listed_at"))
        delisted_at = (
            parse_utc_timestamp(record.get("delisted_at"))
            if record.get("delisted_at") is not None
            else None
        )
        if listed_at is None:
            failures.append(
                {
                    "record": index,
                    "field": "listed_at",
                    "value": record.get("listed_at"),
                    "reason": "invalid listed_at",
                }
            )
        if record.get("delisted_at") is not None and delisted_at is None:
            failures.append(
                {
                    "record": index,
                    "field": "delisted_at",
                    "value": record.get("delisted_at"),
                    "reason": "invalid delisted_at",
                }
            )
        if listed_at and delisted_at and delisted_at < listed_at:
            failures.append(
                {
                    "record": index,
                    "field": "delisted_at",
                    "listed_at": record.get("listed_at"),
                    "delisted_at": record.get("delisted_at"),
                    "reason": "delisted_at before listed_at",
                }
            )
        if ingestion_dt and listed_at and listed_at > ingestion_dt:
            failures.append(
                {
                    "record": index,
                    "field": "listed_at",
                    "value": record.get("listed_at"),
                    "reason": "listed_at after ingestion_time",
                }
            )
        if ingestion_dt and delisted_at and delisted_at > ingestion_dt:
            failures.append(
                {
                    "record": index,
                    "field": "delisted_at",
                    "value": record.get("delisted_at"),
                    "reason": "delisted_at after ingestion_time",
                }
            )

    if failures:
        for failure in failures:
            location = (
                "fixture.ingestion_time"
                if failure["record"] is None
                else f"records[{failure['record']}]"
            )
            report.failed(
                "UM-Q4",
                "timestamp validity rule failed",
                file=str(fixture_path),
                dataset_id=DATASET_ID,
                field=failure["field"],
                location=location,
                details=failure,
            )
    else:
        report.passed(
            "UM-Q4",
            "listed_at and delisted_at are valid UTC timestamps in order",
            file=str(fixture_path),
            dataset_id=DATASET_ID,
        )
    return report


def _validate_q5(records: list[dict[str, Any]], fixture_path: Path) -> ValidationReport:
    report = ValidationReport()
    failures = []
    for index, record in enumerate(records):
        market_type = record.get("market_type")
        contract_type = record.get("contract_type")
        contract_size = record.get("contract_size")
        if market_type not in MARKET_TYPES:
            failures.append(
                {
                    "record": index,
                    "field": "market_type",
                    "value": market_type,
                    "reason": "invalid market_type",
                }
            )
        if market_type == "spot" and contract_type is not None:
            failures.append(
                {
                    "record": index,
                    "field": "contract_type",
                    "value": contract_type,
                    "reason": "spot contract_type must be null",
                }
            )
        if market_type in {"futures", "perpetual"} and contract_type not in LINEAR_TYPES:
            failures.append(
                {
                    "record": index,
                    "field": "contract_type",
                    "value": contract_type,
                    "reason": "derivative contract_type must be linear or inverse",
                }
            )
        if market_type == "option" and contract_type not in OPTION_TYPES:
            failures.append(
                {
                    "record": index,
                    "field": "contract_type",
                    "value": contract_type,
                    "reason": "option contract_type must be call or put",
                }
            )
        if market_type == "spot" and contract_size is not None:
            failures.append(
                {
                    "record": index,
                    "field": "contract_size",
                    "value": contract_size,
                    "reason": "spot contract_size must be null",
                }
            )
        if market_type in {"futures", "perpetual", "option"} and not _positive(
            contract_size
        ):
            failures.append(
                {
                    "record": index,
                    "field": "contract_size",
                    "value": contract_size,
                    "reason": "derivatives require contract_size > 0",
                }
            )
        for field in ("tick_size", "step_size"):
            value = record.get(field)
            if value is not None and not _positive(value):
                failures.append(
                    {
                        "record": index,
                        "field": field,
                        "value": value,
                        "reason": f"{field} must be > 0 when present",
                    }
                )
    if failures:
        for failure in failures:
            report.failed(
                "UM-Q5",
                "contract information rule failed",
                file=str(fixture_path),
                dataset_id=DATASET_ID,
                field=failure["field"],
                location=f"records[{failure['record']}]",
                details=failure,
            )
    else:
        report.passed(
            "UM-Q5",
            "market_type, contract_type, and size constraints hold",
            file=str(fixture_path),
            dataset_id=DATASET_ID,
        )
    return report


def _validate_q6(records: list[dict[str, Any]], fixture_path: Path) -> ValidationReport:
    report = ValidationReport()
    by_id = {record.get("instrument_id"): record for record in records}
    failures = []
    for index, record in enumerate(records):
        instrument_id = record.get("instrument_id")
        successor_id = record.get("successor_id")
        if successor_id is None:
            continue
        if successor_id == instrument_id:
            failures.append(
                {
                    "record": index,
                    "field": "successor_id",
                    "successor_id": successor_id,
                    "reason": "self-reference",
                }
            )
        elif successor_id not in by_id:
            failures.append(
                {
                    "record": index,
                    "field": "successor_id",
                    "successor_id": successor_id,
                    "reason": "missing successor",
                }
            )

    for index, record in enumerate(records):
        if record.get("successor_id") is None:
            continue
        failure = _walk_successor_graph(record, by_id)
        if failure:
            failure["record"] = index
            failures.append(failure)

    if failures:
        for failure in failures:
            report.failed(
                "UM-Q6",
                "successor graph referential rule failed",
                file=str(fixture_path),
                dataset_id=DATASET_ID,
                field=failure["field"],
                location=f"records[{failure['record']}]",
                details=failure,
            )
    else:
        report.passed(
            "UM-Q6",
            "successor references resolve, terminate, and are acyclic",
            file=str(fixture_path),
            dataset_id=DATASET_ID,
        )
    return report


def _validate_pit(records: list[dict[str, Any]], fixture_path: Path) -> ValidationReport:
    report = ValidationReport()
    failures = []
    grouped: dict[tuple[Any, Any], list[tuple[Any, Any, dict[str, Any], int]]] = defaultdict(list)
    by_id = {record.get("instrument_id"): record for record in records}
    for index, record in enumerate(records):
        start = parse_utc_timestamp(record.get("listed_at"))
        end = (
            parse_utc_timestamp(record.get("delisted_at"))
            if record.get("delisted_at") is not None
            else None
        )
        if start is not None:
            grouped[(record.get("exchange"), record.get("symbol"))].append(
                (start, end, record, index)
            )
        if record.get("status") == "renamed":
            successor = by_id.get(record.get("successor_id"))
            successor_start = (
                parse_utc_timestamp(successor.get("listed_at"))
                if successor is not None
                else None
            )
            renamed_end = parse_utc_timestamp(record.get("delisted_at"))
            if successor is None or successor_start != renamed_end:
                failures.append(
                    {
                        "record": index,
                        "field": "successor_id",
                        "reason": "renamed successor must start at delisted_at",
                        "successor_id": record.get("successor_id"),
                    }
                )

    for key, intervals in grouped.items():
        intervals.sort(key=lambda item: item[0])
        previous_end = None
        previous_record = None
        previous_index = None
        for start, end, record, index in intervals:
            if previous_record is not None and (
                previous_end is None or previous_end > start
            ):
                failures.append(
                    {
                        "record": index,
                        "field": "listed_at",
                        "reason": "symbol-era intervals overlap",
                        "key": key,
                        "previous_record": previous_index,
                        "previous_instrument_id": previous_record.get("instrument_id"),
                    }
                )
            if end is None or previous_end is None or end > previous_end:
                previous_end = end
                previous_record = record
                previous_index = index

    if failures:
        for failure in failures:
            report.failed(
                "UM-PIT",
                "point-in-time reconstruction invariant failed",
                file=str(fixture_path),
                dataset_id=DATASET_ID,
                field=failure["field"],
                location=f"records[{failure['record']}]",
                details=failure,
            )
    else:
        report.passed(
            "UM-PIT",
            "symbol-era intervals support point-in-time reconstruction",
            file=str(fixture_path),
            dataset_id=DATASET_ID,
        )
    return report


def _walk_successor_graph(record: dict[str, Any], by_id: dict[Any, dict[str, Any]]) -> dict[str, Any] | None:
    seen: set[Any] = set()
    current = record
    while current.get("successor_id") is not None:
        instrument_id = current.get("instrument_id")
        successor_id = current.get("successor_id")
        if instrument_id in seen:
            return {
                "field": "successor_id",
                "successor_id": successor_id,
                "reason": "successor graph cycle",
            }
        seen.add(instrument_id)
        if successor_id not in by_id:
            return None
        current = by_id[successor_id]
    if current.get("status") not in {"active", "delisted"}:
        return {
            "field": "successor_id",
            "successor_id": current.get("instrument_id"),
            "reason": "successor graph must terminate at active or delisted row",
        }
    return None


def _positive(value: Any) -> bool:
    if value is None or isinstance(value, bool):
        return False
    try:
        return Decimal(str(value)) > Decimal("0")
    except (InvalidOperation, ValueError):
        return False
