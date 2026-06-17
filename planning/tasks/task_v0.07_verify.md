# task_v0.07_verify.md

# Verify Phase 6 — Binance UM Kline 1D Parquet Materialization

你是本專案的 Verification Agent。

目標：驗證 task_v0.07.md 的成果是否真的可用。

驗證重點：

    raw zip -> Parquet
    DuckDB 可讀
    無持久 CSV
    validation 可重跑
    full output 不是 sample output
    local_data 未進 Git

---

## 1. Context

Repo:

    ~/work/crypto-data-hub

目標 output：

    local_data/binance_um_klines/interval=1d/parquet/

目標 manifest：

    local_data/binance_um_klines/interval=1d/parquet/manifests/materialization_manifest.json

raw manifest：

    local_data/binance_um_klines/interval=1d/manifests/manifest.json

Expected full universe reference:

    raw discovered_symbol_count ~= 921
    full materialized symbol_count should match or be very close to the raw 1D universe
    BTCUSDT / ETHUSDT only is SAMPLE_OUTPUT, not accepted as full completion

Date policy:

    date is derived from open_time_taipei calendar date.

Partition policy:

    DuckDB logical schema must expose symbol and year.
    Hive partition columns are acceptable.
    Validation checks DuckDB logical schema.

---

## 2. Verification Scope

請檢查：

1. CLI exists
2. validation target exists
3. docs updated
4. tests added
5. parquet output exists
6. manifest exists
7. output_scope is FULL_OUTPUT for final acceptance
8. symbol_count matches raw 1D universe
9. DuckDB smoke query works
10. schema matches expected logical columns
11. required fields have no NULL
12. `(symbol, interval, open_time)` unique
13. `(symbol, date)` unique for 1D
14. OHLC rule passes
15. date matches open_time_taipei calendar date
16. manifest counts match actual parquet files and DuckDB rows
17. report files exist and are non-empty
18. parquet output tree has no `.csv`
19. generated_csv_file_count == 0
20. validation --all clone-safe
21. local_data untracked

---

## 3. Commands

Repo status：

    cd ~/work/crypto-data-hub
    git log --oneline -5
    git status --short

CLI help：

    python -m datahub.materialization.binance_um_klines_parquet --help

Tests：

    python -m unittest discover tests

Clone-safe validation：

    python -m datahub.validation --all

Idempotency check：

    python -m datahub.materialization.binance_um_klines_parquet \
      --interval 1d \
      --all \
      --workers 4 \
      --resume

Note:

    Use --resume as an idempotency check when parquet output already exists.
    If output is missing or sample-only, report MISSING_OUTPUT or SAMPLE_ONLY instead of treating a newly created output as pre-existing verification success.

Explicit parquet validation：

    python -m datahub.validation \
      --target binance-um-klines-parquet \
      --interval 1d \
      --manifest local_data/binance_um_klines/interval=1d/parquet/manifests/materialization_manifest.json

---

## 4. Direct Checks

### 4.1 Raw manifest vs materialized manifest

    python - <<'PY'
    import json
    from pathlib import Path

    raw_p = Path("local_data/binance_um_klines/interval=1d/manifests/manifest.json")
    mat_p = Path("local_data/binance_um_klines/interval=1d/parquet/manifests/materialization_manifest.json")

    print("raw_manifest_exists:", raw_p.exists())
    print("materialization_manifest_exists:", mat_p.exists())

    raw = json.loads(raw_p.read_text())
    mat = json.loads(mat_p.read_text())

    raw_symbol_count = raw.get("discovered_symbol_count") or raw.get("symbol_count")
    mat_symbol_count = mat.get("symbol_count")

    keys = [
        "materialized_dataset_id",
        "interval",
        "output_scope",
        "output_root",
        "raw_discovered_symbol_count",
        "symbol_count",
        "row_count",
        "file_count",
        "date_min",
        "date_max",
        "duplicate_count",
        "conflict_count",
        "failed_symbol_count",
        "generated_csv_file_count",
        "query_engine",
        "output_format",
    ]

    print("raw_symbol_count:", raw_symbol_count)

    for k in keys:
        print(f"{k}: {mat.get(k)}")

    print("symbol_count_delta:", None if raw_symbol_count is None else raw_symbol_count - mat_symbol_count)
    PY

Expected:

    interval=1d
    materialized_dataset_id=market.binance.um.klines.1d.parquet
    output_scope=FULL_OUTPUT
    raw_symbol_count approximately 921
    symbol_count matches raw_symbol_count, or mismatch is explicitly explained
    row_count > 0
    file_count > 0
    failed_symbol_count=0
    generated_csv_file_count=0
    query_engine=duckdb
    output_format=parquet

Decision rule:

    SAMPLE_OUTPUT is not final acceptance.
    If symbol_count is far below raw_symbol_count, return REJECT or ACCEPT_WITH_WARNINGS only with clear explanation.

---

### 4.2 Report files

    python - <<'PY'
    from pathlib import Path

    base = Path("local_data/binance_um_klines/interval=1d/parquet/reports")
    required = [
        "coverage_report.json",
        "data_quality_report.json",
        "duplicate_report.json",
        "conflict_report.json",
    ]

    for name in required:
        p = base / name
        print(name, "exists:", p.exists(), "size:", p.stat().st_size if p.exists() else None)
    PY

Expected:

    all exists=True
    all size > 0

---

### 4.3 CSV Absence

    find local_data/binance_um_klines/interval=1d/parquet -type f -name '*.csv' | wc -l

Expected:

    0

---

### 4.4 Parquet Count

    find local_data/binance_um_klines/interval=1d/parquet -type f -name '*.parquet' | wc -l

Expected:

    > 0

---

### 4.5 Manifest counts vs actual output

    python - <<'PY'
    import json
    from pathlib import Path
    import duckdb

    manifest = Path("local_data/binance_um_klines/interval=1d/parquet/manifests/materialization_manifest.json")
    data = json.loads(manifest.read_text())

    root = "local_data/binance_um_klines/interval=1d/parquet"

    actual_files = len(list(Path(root).rglob("*.parquet")))
    actual_rows = duckdb.sql(f"""
        SELECT COUNT(*) AS row_count
        FROM read_parquet('{root}/**/*.parquet', hive_partitioning = true)
    """).fetchone()[0]

    print("manifest_file_count:", data.get("file_count"))
    print("actual_file_count:", actual_files)
    print("manifest_row_count:", data.get("row_count"))
    print("actual_row_count:", actual_rows)
    PY

Expected:

    manifest_file_count == actual_file_count
    manifest_row_count == actual_row_count

---

### 4.6 DuckDB Smoke Query

    python - <<'PY'
    import duckdb

    root = "local_data/binance_um_klines/interval=1d/parquet"

    df = duckdb.sql(f"""
        SELECT
            symbol,
            MIN(date) AS date_min,
            MAX(date) AS date_max,
            COUNT(*) AS row_count
        FROM read_parquet('{root}/**/*.parquet', hive_partitioning = true)
        WHERE symbol IN ('BTCUSDT', 'ETHUSDT')
        GROUP BY symbol
        ORDER BY symbol
    """).df()

    print(df)
    PY

Expected:

    row_count > 0
    date_min / date_max are reasonable

---

### 4.7 Logical Schema

    python - <<'PY'
    import duckdb

    root = "local_data/binance_um_klines/interval=1d/parquet"

    df = duckdb.sql(f"""
        DESCRIBE
        SELECT *
        FROM read_parquet('{root}/**/*.parquet', hive_partitioning = true)
        LIMIT 1
    """).df()

    print(df)
    PY

Required logical columns:

    symbol
    interval
    open_time
    open_time_utc
    open_time_taipei
    date
    year
    month
    open
    high
    low
    close
    volume
    close_time
    quote_volume
    trade_count
    taker_buy_base_volume
    taker_buy_quote_volume
    source_archive
    archive_source
    archive_period

---

### 4.8 Required fields NULL check

    python - <<'PY'
    import duckdb

    root = "local_data/binance_um_klines/interval=1d/parquet"

    df = duckdb.sql(f"""
        SELECT COUNT(*) AS bad_null_rows
        FROM read_parquet('{root}/**/*.parquet', hive_partitioning = true)
        WHERE symbol IS NULL
           OR interval IS NULL
           OR open_time IS NULL
           OR open_time_utc IS NULL
           OR open_time_taipei IS NULL
           OR date IS NULL
           OR open IS NULL
           OR high IS NULL
           OR low IS NULL
           OR close IS NULL
           OR volume IS NULL
           OR close_time IS NULL
           OR quote_volume IS NULL
           OR trade_count IS NULL
           OR taker_buy_base_volume IS NULL
           OR taker_buy_quote_volume IS NULL
    """).df()

    print(df)
    PY

Expected:

    bad_null_rows=0

---

### 4.9 Duplicate key

    python - <<'PY'
    import duckdb

    root = "local_data/binance_um_klines/interval=1d/parquet"

    df = duckdb.sql(f"""
        SELECT
            symbol,
            interval,
            open_time,
            COUNT(*) AS n
        FROM read_parquet('{root}/**/*.parquet', hive_partitioning = true)
        GROUP BY symbol, interval, open_time
        HAVING COUNT(*) > 1
        LIMIT 20
    """).df()

    print(df)
    print("duplicate_rows_found:", len(df))
    PY

Expected:

    duplicate_rows_found=0

---

### 4.10 Duplicate date per symbol

    python - <<'PY'
    import duckdb

    root = "local_data/binance_um_klines/interval=1d/parquet"

    df = duckdb.sql(f"""
        SELECT
            symbol,
            date,
            COUNT(*) AS n
        FROM read_parquet('{root}/**/*.parquet', hive_partitioning = true)
        GROUP BY symbol, date
        HAVING COUNT(*) > 1
        LIMIT 20
    """).df()

    print(df)
    print("duplicate_symbol_date_rows_found:", len(df))
    PY

Expected:

    duplicate_symbol_date_rows_found=0

---

### 4.11 OHLC Rule

    python - <<'PY'
    import duckdb

    root = "local_data/binance_um_klines/interval=1d/parquet"

    df = duckdb.sql(f"""
        SELECT COUNT(*) AS bad_rows
        FROM read_parquet('{root}/**/*.parquet', hive_partitioning = true)
        WHERE
            high IS NULL
            OR low IS NULL
            OR open IS NULL
            OR close IS NULL
            OR volume IS NULL
            OR quote_volume IS NULL
            OR trade_count IS NULL
            OR NOT (
                high >= low
                AND high >= open
                AND high >= close
                AND low <= open
                AND low <= close
                AND volume >= 0
                AND quote_volume >= 0
                AND trade_count >= 0
            )
    """).df()

    print(df)
    PY

Expected:

    bad_rows=0

---

### 4.12 Date policy consistency

    python - <<'PY'
    import duckdb

    root = "local_data/binance_um_klines/interval=1d/parquet"

    df = duckdb.sql(f"""
        SELECT COUNT(*) AS bad_date_rows
        FROM read_parquet('{root}/**/*.parquet', hive_partitioning = true)
        WHERE CAST(open_time_taipei AS DATE) != CAST(date AS DATE)
    """).df()

    print(df)
    PY

Expected:

    bad_date_rows=0

---

## 5. Git Safety

    git status --short

    git ls-files | grep '^local_data/' || true

    git status --short | grep 'local_data/' || true

Expected:

    no tracked local_data
    no staged local_data
    no untracked local_data shown by git status

---

## 6. Final Report

請用以下格式回報：

Summary
- PASS / FAIL / PASS_WITH_WARNINGS
- main finding

Implementation
- CLI exists
- validation target exists
- docs updated
- tests added

Materialization Output
- output_scope
- parquet root exists
- parquet file count
- manifest exists
- raw_symbol_count
- symbol_count
- symbol_count_delta
- row_count
- actual_duckdb_row_count
- date_min
- date_max
- generated_csv_file_count

DuckDB Checks
- smoke query
- schema check
- null check
- duplicate key check
- duplicate date check
- OHLC rule check
- date policy check
- manifest vs actual count check

Validation
- unittest
- validation --all
- explicit parquet validation

Git Safety
- local_data tracked
- git status summary
- commit hash

Problems
- concrete issues
- exact evidence
- minimal fix

Final Decision
- ACCEPT
- REJECT
- ACCEPT_WITH_WARNINGS
