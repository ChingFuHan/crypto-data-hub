# task_v0.12_verify.md

# Verify Phase 11 — Binance UM Kline 3M Raw + Parquet Materialization

你是本專案的 Verification Agent。

任務：驗證 Phase 11 的 3M raw ingestion support、3M raw archive、3M Parquet materialization 是否真的可用。

本階段驗證範圍：

    3M raw ingestion support
    3M raw archive
    3M raw validation
    3M Parquet
    DuckDB read
    3M Parquet validation
    1D regression
    4H regression
    1H regression
    15M regression
    5M regression
    Git safety

後續 Phase：

    1M
    PostgreSQL serving layer
    live API
    strategy
    trading system

---

## 1. 驗證目標

確認：

    3M interval 已納入 raw ingestion
    3M interval 已納入 raw validation
    3M interval 已納入 Parquet materializer
    3M interval 已納入 Parquet validation
    3M raw zip -> 3M Parquet
    DuckDB 可讀
    output_scope=FULL_OUTPUT
    raw_discovered_symbol_count >= 921
    symbol_count == raw_discovered_symbol_count (3m 實際 922)
    row_count > 183166850
    row_count / 183166850 >= 1.5
    3M time policy 正確
    3M key policy 正確
    1D / 4H / 1H / 15M / 5M regression 通過
    persistent CSV count = 0
    validation 可重跑
    git tracked local_data count = 0

---

## 2. 路徑

Repo：

    ~/work/crypto-data-hub

3M raw root：

    local_data/binance_um_klines/interval=3m/

3M raw manifest：

    local_data/binance_um_klines/interval=3m/manifests/manifest.json

3M parquet root：

    local_data/binance_um_klines/interval=3m/parquet/

3M materialization manifest：

    local_data/binance_um_klines/interval=3m/parquet/manifests/materialization_manifest.json

Regression manifests：

    local_data/binance_um_klines/interval=1d/parquet/manifests/materialization_manifest.json
    local_data/binance_um_klines/interval=4h/parquet/manifests/materialization_manifest.json
    local_data/binance_um_klines/interval=1h/parquet/manifests/materialization_manifest.json
    local_data/binance_um_klines/interval=15m/parquet/manifests/materialization_manifest.json
    local_data/binance_um_klines/interval=5m/parquet/manifests/materialization_manifest.json

---

## 3. 預期值

3M raw：

    interval=3m
    symbol_count >= 921 (3m 實際 922；+REUSDT 新上市)
    downloaded_count=verified_count
    failed_count=0
    checksum_failed_count=0
    missing_count=0

3M parquet：

    materialized_dataset_id=market.binance.um.klines.3m.parquet
    interval=3m
    output_scope=FULL_OUTPUT
    symbol_count == raw_discovered_symbol_count (3m 實際 922)
    failed_symbol_count=0
    generated_csv_file_count=0

3M rules：

    unique key = (symbol, interval, open_time)
    max rows per (symbol, date) <= 480
    open_time % 180000 == 0
    close_time = open_time + 179999
    date = open_time_taipei 的日期

3M baseline：

    base interval = 5m
    base row_count = 183166850
    row_count > 183166850
    row_count / 183166850 >= 1.5

Regression：

    1D / 4H / 1H / 15M / 5M parquet validation pass

---

## 4. 必跑指令

進 repo：

    cd ~/work/crypto-data-hub

Repo：

    git log --oneline -8
    git status --short
    cat VERSION

CLI：

    python -m datahub.ingestion.binance_um_klines --help
    python -m datahub.materialization.binance_um_klines_parquet --help

Tests：

    python -m unittest discover tests

Validation all：

    python -m datahub.validation --all

Raw 3M validation：

    python -m datahub.validation \
      --target binance-um-klines \
      --interval 3m \
      --manifest local_data/binance_um_klines/interval=3m/manifests/manifest.json

Explicit 3M validation：

    python -m datahub.validation \
      --target binance-um-klines-parquet \
      --interval 3m \
      --manifest local_data/binance_um_klines/interval=3m/parquet/manifests/materialization_manifest.json

Regression validation：

    python -m datahub.validation --target binance-um-klines-parquet --interval 1d --manifest local_data/binance_um_klines/interval=1d/parquet/manifests/materialization_manifest.json
    python -m datahub.validation --target binance-um-klines-parquet --interval 4h --manifest local_data/binance_um_klines/interval=4h/parquet/manifests/materialization_manifest.json
    python -m datahub.validation --target binance-um-klines-parquet --interval 1h --manifest local_data/binance_um_klines/interval=1h/parquet/manifests/materialization_manifest.json
    python -m datahub.validation --target binance-um-klines-parquet --interval 15m --manifest local_data/binance_um_klines/interval=15m/parquet/manifests/materialization_manifest.json
    python -m datahub.validation --target binance-um-klines-parquet --interval 5m --manifest local_data/binance_um_klines/interval=5m/parquet/manifests/materialization_manifest.json

Resume idempotency：

    python -m datahub.materialization.binance_um_klines_parquet \
      --interval 3m \
      --all \
      --workers 4 \
      --resume

Resume 成功標準：

    output_scope=FULL_OUTPUT
    symbol_count == raw_discovered_symbol_count (3m 實際 922)
    row_count 維持一致
    file_count 維持一致
    failed_symbol_count=0

---

## 5. Raw manifest 檢查

執行：

    python - <<'PY'
    import json
    from pathlib import Path

    p = Path("local_data/binance_um_klines/interval=3m/manifests/manifest.json")
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
        "skipped_count",
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

    dataset_variant_id=market.binance.um.klines.3m
    interval=3m
    symbol_count >= 921 (3m 實際 922；+REUSDT 新上市)
    downloaded_count=verified_count
    failed_count=0
    checksum_failed_count=0
    missing_count=0

---

## 6. Materialization manifest 檢查

執行：

    python - <<'PY'
    import json
    from pathlib import Path

    raw_p = Path("local_data/binance_um_klines/interval=3m/manifests/manifest.json")
    mat_p = Path("local_data/binance_um_klines/interval=3m/parquet/manifests/materialization_manifest.json")

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

    materialized_dataset_id=market.binance.um.klines.3m.parquet
    interval=3m
    output_scope=FULL_OUTPUT
    raw_discovered_symbol_count >= 921 (3m 實際 922)
    symbol_count == raw_discovered_symbol_count (3m 實際 922)
    symbol_count_delta=0
    row_count > 183166850
    row_count / 183166850 >= 1.5
    file_count > 0
    failed_symbol_count=0
    generated_csv_file_count=0
    query_engine=duckdb
    output_format=parquet

---

## 7. DuckDB 整合檢查

執行：

    python - <<'PY'
    import json
    from pathlib import Path
    import duckdb

    root_3m = "local_data/binance_um_klines/interval=3m/parquet"
    root_5m = "local_data/binance_um_klines/interval=5m/parquet"
    manifest = Path("local_data/binance_um_klines/interval=3m/parquet/manifests/materialization_manifest.json")
    mat = json.loads(manifest.read_text())

    q3 = f"read_parquet('{root_3m}/**/*.parquet', hive_partitioning=true)"
    q5 = f"read_parquet('{root_5m}/**/*.parquet', hive_partitioning=true)"

    actual_files = len(list(Path(root_3m).rglob("*.parquet")))
    actual_rows = duckdb.sql(f"SELECT COUNT(*) FROM {q3}").fetchone()[0]
    rows_5m = duckdb.sql(f"SELECT COUNT(*) FROM {q5}").fetchone()[0]
    ratio = actual_rows / rows_5m if rows_5m else None

    checks = {
        "manifest_file_match": actual_files == mat.get("file_count"),
        "manifest_row_match": actual_rows == mat.get("row_count"),
        "rows_gt_5m": actual_rows > rows_5m,
        "ratio_ge_1_5": ratio is not None and ratio >= 1.5,
        "bad_null_rows": duckdb.sql(f"""
            SELECT COUNT(*) FROM {q3}
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
                FROM {q3}
                GROUP BY symbol, interval, open_time
                HAVING COUNT(*) > 1
            )
        """).fetchone()[0],
        "symbol_date_rows_gt_480": duckdb.sql(f"""
            SELECT COUNT(*) FROM (
                SELECT symbol, date, COUNT(*) AS n
                FROM {q3}
                GROUP BY symbol, date
                HAVING COUNT(*) > 480
            )
        """).fetchone()[0],
        "bad_alignment_rows": duckdb.sql(f"""
            SELECT COUNT(*) FROM {q3}
            WHERE open_time % 180000 != 0
        """).fetchone()[0],
        "bad_close_time_rows": duckdb.sql(f"""
            SELECT COUNT(*) FROM {q3}
            WHERE close_time != open_time + 179999
        """).fetchone()[0],
        "bad_ohlc_rows": duckdb.sql(f"""
            SELECT COUNT(*) FROM {q3}
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
            SELECT COUNT(*) FROM {q3}
            WHERE CAST(open_time_taipei AS DATE) != CAST(date AS DATE)
        """).fetchone()[0],
        "persistent_csv_count": len(list(Path(root_3m).rglob("*.csv"))),
    }

    print("actual_files:", actual_files)
    print("actual_rows:", actual_rows)
    print("rows_5m:", rows_5m)
    print("ratio:", ratio)
    for k, v in checks.items():
        print(f"{k}: {v}")
    PY

通過標準：

    manifest_file_match=True
    manifest_row_match=True
    rows_gt_5m=True
    ratio_ge_1_5=True
    bad_null_rows=0
    duplicate_key_rows=0
    symbol_date_rows_gt_480=0
    bad_alignment_rows=0
    bad_close_time_rows=0
    bad_ohlc_rows=0
    bad_date_rows=0
    persistent_csv_count=0

---

## 8. Smoke / Schema

Smoke query：

    python - <<'PY'
    import duckdb

    root = "local_data/binance_um_klines/interval=3m/parquet"

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

    root = "local_data/binance_um_klines/interval=3m/parquet"

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

## 9. Git Safety

執行：

    git status --short
    git ls-files local_data | wc -l
    git status --short | grep 'local_data/' || true

通過標準：

    git ls-files local_data = 0
    local_data staged count = 0
    local_data tracked count = 0

---

## 10. Docs / Registry

執行：

    grep -RIn "3m" docs DATA_CATALOG.md dataset_registry.json CHANGELOG.md VERSION 2>/dev/null | head -80
    ls -lh planning/tasks/task_v0.12.md planning/tasks/task_v0.12_verify.md

通過標準：

    VERSION=v0.12.0
    docs 有 3M raw / Parquet 使用說明
    DATA_CATALOG.md 有 3M materialized layer
    dataset_registry.json 有 market.binance.um.klines.3m.parquet
    CHANGELOG.md 有 v0.12.0
    task files 存在

---

## 11. 最終回報格式

請用以下格式：

Summary
- PASS / FAIL / PASS_WITH_WARNINGS
- 核心結論

Implementation
- raw ingestion interval=3m supported
- raw validation interval=3m supported
- parquet materializer interval=3m supported
- parquet validation interval=3m supported
- interval=1d regression preserved
- interval=4h regression preserved
- interval=1h regression preserved
- interval=15m regression preserved
- interval=5m regression preserved
- docs updated
- tests added

Raw 3M
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
- row_count ratio vs 5M
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
- raw 3M validation
- explicit 3M validation
- explicit 1D regression
- explicit 4H regression
- explicit 1H regression
- explicit 15M regression
- explicit 5M regression
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

## 12. 判定規則

ACCEPT 條件：

    output_scope=FULL_OUTPUT
    raw_discovered_symbol_count >= 921
    symbol_count == raw_discovered_symbol_count (3m 實際 922)
    row_count > 183166850
    row_count / 183166850 >= 1.5
    failed_symbol_count=0
    generated_csv_file_count=0
    DuckDB checks 全過
    explicit 3M validation 通過
    explicit 1D / 4H / 1H / 15M / 5M regression 通過
    validation --all 通過
    unit tests 通過
    git tracked local_data count = 0
    working tree clean
    VERSION=v0.12.0

REJECT 條件：

    SAMPLE_OUTPUT
    symbol_count 不足
    row_count <= 183166850
    row_count / 183166850 < 1.5
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
