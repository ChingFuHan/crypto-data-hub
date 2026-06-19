# task_v0.11_verify.md

# Verify Phase 10 — Binance UM Kline 5M Parquet Materialization

你是本專案的 Verification Agent。

任務：驗證 Phase 10 的 5M Parquet 是否真的可用。

本階段驗證範圍：

    5M raw archive
    5M Parquet
    DuckDB read
    validation
    1D regression
    4H regression
    1H regression
    15M regression
    Git safety

PostgreSQL、live API、策略、交易層由後續 Phase 驗證。

---

## 1. 驗證目標

確認：

    5M raw zip -> 5M Parquet
    DuckDB 可讀
    output_scope=FULL_OUTPUT
    symbol_count=921
    row_count > 61055727
    row_count / 61055727 >= 2.5
    5M time policy 正確
    5M key policy 正確
    1D / 4H / 1H / 15M regression 通過
    persistent CSV count = 0
    validation 可重跑
    git tracked local_data count = 0

---

## 2. 路徑

Repo：

    ~/work/crypto-data-hub

5M raw manifest：

    local_data/binance_um_klines/interval=5m/manifests/manifest.json

5M parquet root：

    local_data/binance_um_klines/interval=5m/parquet/

5M materialization manifest：

    local_data/binance_um_klines/interval=5m/parquet/manifests/materialization_manifest.json

Regression manifests：

    local_data/binance_um_klines/interval=1d/parquet/manifests/materialization_manifest.json
    local_data/binance_um_klines/interval=4h/parquet/manifests/materialization_manifest.json
    local_data/binance_um_klines/interval=1h/parquet/manifests/materialization_manifest.json
    local_data/binance_um_klines/interval=15m/parquet/manifests/materialization_manifest.json

---

## 3. 預期值

5M raw：

    interval=5m
    symbol_count=921
    downloaded_count=34238
    verified_count=34238
    failed_count=0
    checksum_failed_count=0
    missing_count=0
    date_min=2019-12-31
    date_max=2026-06-17

5M parquet：

    materialized_dataset_id=market.binance.um.klines.5m.parquet
    interval=5m
    output_scope=FULL_OUTPUT
    symbol_count=921
    failed_symbol_count=0
    generated_csv_file_count=0

5M rules：

    unique key = (symbol, interval, open_time)
    max rows per (symbol, date) <= 288
    open_time % 300000 == 0
    close_time = open_time + 299999
    date = open_time_taipei 的日期

Regression：

    1D / 4H / 1H / 15M parquet validation pass

---

## 4. 必跑指令

進 repo：

    cd ~/work/crypto-data-hub

Repo：

    git log --oneline -8
    git status --short
    cat VERSION

CLI：

    python -m datahub.materialization.binance_um_klines_parquet --help

Tests：

    python -m unittest discover tests

Validation all：

    python -m datahub.validation --all

Raw 5M validation：

    python -m datahub.validation \
      --target binance-um-klines \
      --interval 5m \
      --manifest local_data/binance_um_klines/interval=5m/manifests/manifest.json

Explicit 5M validation：

    python -m datahub.validation \
      --target binance-um-klines-parquet \
      --interval 5m \
      --manifest local_data/binance_um_klines/interval=5m/parquet/manifests/materialization_manifest.json

Regression validation：

    python -m datahub.validation --target binance-um-klines-parquet --interval 1d --manifest local_data/binance_um_klines/interval=1d/parquet/manifests/materialization_manifest.json
    python -m datahub.validation --target binance-um-klines-parquet --interval 4h --manifest local_data/binance_um_klines/interval=4h/parquet/manifests/materialization_manifest.json
    python -m datahub.validation --target binance-um-klines-parquet --interval 1h --manifest local_data/binance_um_klines/interval=1h/parquet/manifests/materialization_manifest.json
    python -m datahub.validation --target binance-um-klines-parquet --interval 15m --manifest local_data/binance_um_klines/interval=15m/parquet/manifests/materialization_manifest.json

Resume idempotency：

    python -m datahub.materialization.binance_um_klines_parquet \
      --interval 5m \
      --all \
      --workers 4 \
      --resume

Resume 成功標準：

    output_scope=FULL_OUTPUT
    symbol_count=921
    row_count 維持一致
    file_count 維持一致
    failed_symbol_count=0

---

## 5. Manifest 檢查

Raw manifest：

    python - <<'PY'
    import json
    from pathlib import Path

    p = Path("local_data/binance_um_klines/interval=5m/manifests/manifest.json")
    data = json.loads(p.read_text())

    for k in [
        "dataset_id",
        "dataset_variant_id",
        "interval",
        "archive_package_sources",
        "symbol_count",
        "file_count",
        "downloaded_count",
        "verified_count",
        "failed_count",
        "checksum_failed_count",
        "missing_count",
        "total_bytes",
        "date_min",
        "date_max",
    ]:
        print(f"{k}: {data.get(k)}")
    PY

通過標準：

    interval=5m
    symbol_count=921
    downloaded_count=verified_count=34238
    failed_count=0
    checksum_failed_count=0
    missing_count=0

Materialization manifest：

    python - <<'PY'
    import json
    from pathlib import Path

    raw_p = Path("local_data/binance_um_klines/interval=5m/manifests/manifest.json")
    mat_p = Path("local_data/binance_um_klines/interval=5m/parquet/manifests/materialization_manifest.json")

    raw = json.loads(raw_p.read_text())
    mat = json.loads(mat_p.read_text())

    for k in [
        "materialized_dataset_id",
        "interval",
        "output_scope",
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
        "git_commit",
    ]:
        print(f"{k}: {mat.get(k)}")

    print("symbol_count_delta:", raw.get("symbol_count") - mat.get("symbol_count"))
    PY

通過標準：

    materialized_dataset_id=market.binance.um.klines.5m.parquet
    interval=5m
    output_scope=FULL_OUTPUT
    raw_discovered_symbol_count=921
    symbol_count=921
    symbol_count_delta=0
    row_count > 0
    file_count > 0
    failed_symbol_count=0
    generated_csv_file_count=0
    query_engine=duckdb
    output_format=parquet

---

## 6. DuckDB 整合檢查

執行：

    python - <<'PY'
    import json
    from pathlib import Path
    import duckdb

    root_5m = "local_data/binance_um_klines/interval=5m/parquet"
    root_15m = "local_data/binance_um_klines/interval=15m/parquet"
    manifest = Path("local_data/binance_um_klines/interval=5m/parquet/manifests/materialization_manifest.json")
    mat = json.loads(manifest.read_text())

    q5 = f"read_parquet('{root_5m}/**/*.parquet', hive_partitioning=true)"
    q15 = f"read_parquet('{root_15m}/**/*.parquet', hive_partitioning=true)"

    actual_files = len(list(Path(root_5m).rglob("*.parquet")))
    actual_rows = duckdb.sql(f"SELECT COUNT(*) FROM {q5}").fetchone()[0]
    rows_15m = duckdb.sql(f"SELECT COUNT(*) FROM {q15}").fetchone()[0]
    ratio = actual_rows / rows_15m if rows_15m else None

    checks = {
        "manifest_file_match": actual_files == mat.get("file_count"),
        "manifest_row_match": actual_rows == mat.get("row_count"),
        "rows_gt_15m": actual_rows > rows_15m,
        "ratio_ge_2_5": ratio is not None and ratio >= 2.5,
        "bad_null_rows": duckdb.sql(f"""
            SELECT COUNT(*) FROM {q5}
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
        """).fetchone()[0],
        "duplicate_key_rows": duckdb.sql(f"""
            SELECT COUNT(*) FROM (
                SELECT symbol, interval, open_time, COUNT(*) AS n
                FROM {q5}
                GROUP BY symbol, interval, open_time
                HAVING COUNT(*) > 1
            )
        """).fetchone()[0],
        "symbol_date_rows_gt_288": duckdb.sql(f"""
            SELECT COUNT(*) FROM (
                SELECT symbol, date, COUNT(*) AS n
                FROM {q5}
                GROUP BY symbol, date
                HAVING COUNT(*) > 288
            )
        """).fetchone()[0],
        "bad_alignment_rows": duckdb.sql(f"""
            SELECT COUNT(*) FROM {q5}
            WHERE open_time % 300000 != 0
        """).fetchone()[0],
        "bad_close_time_rows": duckdb.sql(f"""
            SELECT COUNT(*) FROM {q5}
            WHERE close_time != open_time + 299999
        """).fetchone()[0],
        "bad_ohlc_rows": duckdb.sql(f"""
            SELECT COUNT(*) FROM {q5}
            WHERE NOT (
                high >= low
                AND high >= open
                AND high >= close
                AND low <= open
                AND low <= close
                AND volume >= 0
                AND quote_volume >= 0
                AND trade_count >= 0
            )
        """).fetchone()[0],
        "bad_date_rows": duckdb.sql(f"""
            SELECT COUNT(*) FROM {q5}
            WHERE CAST(open_time_taipei AS DATE) != CAST(date AS DATE)
        """).fetchone()[0],
        "persistent_csv_count": len(list(Path(root_5m).rglob("*.csv"))),
    }

    print("actual_files:", actual_files)
    print("actual_rows:", actual_rows)
    print("rows_15m:", rows_15m)
    print("ratio:", ratio)
    for k, v in checks.items():
        print(f"{k}: {v}")
    PY

通過標準：

    manifest_file_match=True
    manifest_row_match=True
    rows_gt_15m=True
    ratio_ge_2_5=True
    bad_null_rows=0
    duplicate_key_rows=0
    symbol_date_rows_gt_288=0
    bad_alignment_rows=0
    bad_close_time_rows=0
    bad_ohlc_rows=0
    bad_date_rows=0
    persistent_csv_count=0

---

## 7. Smoke / Schema

Smoke query：

    python - <<'PY'
    import duckdb

    root = "local_data/binance_um_klines/interval=5m/parquet"

    df = duckdb.sql(f"""
        SELECT symbol, MIN(date) AS date_min, MAX(date) AS date_max, COUNT(*) AS row_count
        FROM read_parquet('{root}/**/*.parquet', hive_partitioning=true)
        WHERE symbol IN ('BTCUSDT', 'ETHUSDT')
        GROUP BY symbol
        ORDER BY symbol
    """).df()

    print(df)
    PY

Schema：

    python - <<'PY'
    import duckdb

    root = "local_data/binance_um_klines/interval=5m/parquet"

    df = duckdb.sql(f"""
        DESCRIBE
        SELECT *
        FROM read_parquet('{root}/**/*.parquet', hive_partitioning=true)
        LIMIT 1
    """).df()

    print(df)
    PY

必要欄位：

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

## 8. Git Safety

執行：

    git status --short
    git ls-files local_data | wc -l
    git status --short | grep 'local_data/' || true

通過標準：

    git ls-files local_data = 0
    local_data staged count = 0
    local_data tracked count = 0

---

## 9. Docs / Registry

執行：

    grep -RIn "5m" docs DATA_CATALOG.md dataset_registry.json 2>/dev/null | head -50
    ls -lh planning/tasks/task_v0.11.md planning/tasks/task_v0.11_verify.md

通過標準：

    docs 有 5M Parquet 使用說明
    DATA_CATALOG.md 有 5M materialized layer
    dataset_registry.json 有 market.binance.um.klines.5m.parquet
    task files 存在

---

## 10. 最終回報格式

請用以下格式：

Summary
- PASS / FAIL / PASS_WITH_WARNINGS
- 核心結論

Implementation
- CLI exists
- interval=5m supported
- interval=1d regression preserved
- interval=4h regression preserved
- interval=1h regression preserved
- interval=15m regression preserved
- validation target exists
- docs updated
- tests added

Raw 5M
- symbol_count
- downloaded_count
- verified_count
- failed_count
- checksum_failed_count
- missing_count
- date_min/date_max

Materialization
- output_scope
- symbol_count
- row_count
- file_count
- actual_duckdb_row_count
- row_count ratio vs 15M
- failed_symbol_count
- generated_csv_file_count

DuckDB Checks
- smoke query
- schema
- null
- duplicate key
- symbol/date max rows
- open_time alignment
- close_time
- OHLC
- date policy
- manifest vs actual

Validation
- unittest
- raw 5M validation
- explicit 5M validation
- explicit 1D regression
- explicit 4H regression
- explicit 1H regression
- explicit 15M regression
- validation --all
- resume idempotency

Git
- git status
- local_data tracked count
- commit hash
- VERSION

Problems
- 問題
- 證據
- 最小修法

Final Decision
- ACCEPT
- REJECT
- ACCEPT_WITH_WARNINGS

---

## 11. 判定規則

ACCEPT 條件：

    output_scope=FULL_OUTPUT
    symbol_count=921
    row_count > 61055727
    row_count / 61055727 >= 2.5
    failed_symbol_count=0
    generated_csv_file_count=0
    DuckDB checks 全過
    explicit 5M validation 通過
    explicit 1D / 4H / 1H / 15M regression 通過
    validation --all 通過
    unit tests 通過
    git tracked local_data count = 0
    working tree clean

REJECT 條件：

    SAMPLE_OUTPUT
    symbol_count 不足
    row_count <= 61055727
    row_count / 61055727 < 2.5
    DuckDB 讀不到
    duplicate key count > 0
    required fields NULL count > 0
    OHLC fail
    open_time alignment fail
    close_time fail
    manifest count 對不上
    regression fail
    git tracked local_data count > 0
    validation fail

ACCEPT_WITH_WARNINGS 條件：

    功能正確
    資料完整
    問題只影響 metadata 或文件
    無資料完整性風險
