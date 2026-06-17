# task_v0.07.md

# Phase 6 — Binance UM Kline 1D Parquet Materialization

你是本專案的 Data Engineering 實作 Agent。

目標：把已完成下載與驗證的 Binance USD-M Futures 1D raw archive，轉成 DuckDB 可直接查詢的 Parquet dataset。

請優先完成可用的 1D materialized layer。  
請避免大重構。  
請讓結果可重跑、可 resume、可驗證。

---

## 1. Context

Repo:

    ~/work/crypto-data-hub

目前重要 commit：

    2feff57 fix: reduce Binance UM Kline discovery memory usage
    89ead6f feat: Phase 5 — Binance USD-M Kline Historical Pipeline (v0.6.0)

已完成的 1D raw archive：

    local_data/binance_um_klines/interval=1d/raw/
    local_data/binance_um_klines/interval=1d/manifests/manifest.json

已知 Phase 5 結果約為：

    interval=1d
    discovered_symbol_count=921
    planned_download_count=32656
    downloaded_count=32656
    verified_count=32656
    failed_count=0
    checksum_failed_count=0
    missing_count=0
    date_min=2019-12-31
    date_max=2026-06-15

Full completion reference:

    Full materialization should cover the raw 1D universe derived from the Phase 5 manifest/catalog.
    Expected symbol count is approximately 921.
    A BTCUSDT / ETHUSDT sample run is only SAMPLE_OUTPUT, not full completion.

---

## 2. Output Target

產出位置：

    local_data/binance_um_klines/interval=1d/parquet/

資料層規則：

    raw zip archive = immutable source layer
    parquet = query/materialized layer
    duckdb = standard query engine

CSV 僅存在於 zip 內部解析流程。  
Phase 6 產出的可查詢本地資料層一律為 Parquet。

Date policy:

    date is derived from open_time_taipei calendar date.
    open_time_utc and open_time_taipei must both be present.
    The date policy must be documented and used consistently in validation.

Partition columns policy:

    The logical DuckDB result must expose symbol and year.
    Hive-style paths may provide symbol/year partition columns.
    Physical parquet files may omit partition columns if DuckDB exposes them through hive_partitioning=true.
    Avoid duplicate or ambiguous symbol/year columns when querying with DuckDB.
    Validation should check the logical DuckDB schema, not only physical parquet file schema.

---

## 3. Required CLI

新增 module：

    datahub/materialization/binance_um_klines_parquet.py

CLI：

    python -m datahub.materialization.binance_um_klines_parquet \
      --interval 1d \
      --all

支援參數：

    --interval 1d
    --all
    --symbols BTCUSDT ETHUSDT
    --raw-root local_data/binance_um_klines/interval=1d/raw
    --manifest local_data/binance_um_klines/interval=1d/manifests/manifest.json
    --output-root local_data/binance_um_klines/interval=1d/parquet
    --resume
    --overwrite
    --workers 4
    --strict

行為要求：

- `--interval` 目前只支援 `1d`
- `--all` 處理 manifest 中所有 verified records
- `--symbols` 處理指定 symbols
- `--resume` 跳過已完成且可驗證的 symbol
- `--overwrite` 重建既有 output
- 嚴重錯誤回傳 non-zero exit
- 輸出清楚 progress log
- full run 完成時，manifest 必須能區分 FULL_OUTPUT 與 SAMPLE_OUTPUT

---

## 4. Input Parsing

Binance Kline CSV 欄位：

    open_time
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
    ignore

解析要求：

- 支援 zip 內無 header CSV
- 支援有 header CSV
- timestamp 為 millisecond epoch
- OHLCV 欄位轉 numeric
- `trade_count` 轉 integer nullable
- `ignore` 欄位不進 Parquet
- required identity/time/OHLCV fields must be non-null

Required non-null fields:

    symbol
    interval
    open_time
    open_time_utc
    open_time_taipei
    date
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

---

## 5. Parquet Schema

Logical DuckDB-readable schema 至少包含：

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

欄位要求：

    symbol: string
    interval: fixed value "1d"
    open_time: int64 millisecond epoch
    open_time_utc: UTC timestamp or ISO string
    open_time_taipei: Asia/Taipei timestamp or ISO string
    date: YYYY-MM-DD, derived from open_time_taipei
    year: int, exposed through physical column or Hive partition
    month: int
    OHLCV fields: numeric
    source_archive: relative path to source zip
    archive_source: monthly or daily
    archive_period: YYYY-MM or YYYY-MM-DD

---

## 6. Partition Layout

使用 Hive-style partition：

    local_data/binance_um_klines/interval=1d/parquet/
      symbol=BTCUSDT/
        year=2019/
          part-000.parquet
        year=2020/
          part-000.parquet
      symbol=ETHUSDT/
        year=2019/
          part-000.parquet

要求：

- DuckDB 可用 recursive glob 讀取
- DuckDB 可透過 `hive_partitioning=true` 讀出 Hive partition 欄位
- 同一 `(symbol, interval, open_time)` 唯一
- 同一 symbol 內依 `open_time` 排序
- 同一 symbol 的 1D `date` 不重複
- 以 symbol-by-symbol processing 為主

---

## 7. Dedup / Conflict Policy

Dedup key：

    symbol
    interval
    open_time

Policy：

- monthly 與 daily 同一根 K 完全一致：保留一筆
- monthly 與 daily 同一根 K 不一致：daily 優先，記錄 conflict
- 同 source 重複且一致：去重，記錄 duplicate
- 同 source 重複但不一致：記錄 data quality issue
- `--strict` 遇 conflict / quality issue 時 fail
- 非 strict 模式完成 materialization，並在 report 揭露問題

---

## 8. Reports

產生：

    local_data/binance_um_klines/interval=1d/parquet/manifests/materialization_manifest.json
    local_data/binance_um_klines/interval=1d/parquet/reports/coverage_report.json
    local_data/binance_um_klines/interval=1d/parquet/reports/data_quality_report.json
    local_data/binance_um_klines/interval=1d/parquet/reports/duplicate_report.json
    local_data/binance_um_klines/interval=1d/parquet/reports/conflict_report.json

manifest 至少包含：

    materialized_dataset_id
    interval
    input_manifest
    output_root
    raw_discovered_symbol_count
    symbol_count
    row_count
    file_count
    date_min
    date_max
    duplicate_count
    conflict_count
    failed_symbol_count
    failed_symbols
    generated_csv_file_count
    query_engine
    output_format
    output_scope
    git_commit
    created_at_utc

固定值：

    materialized_dataset_id=market.binance.um.klines.1d.parquet
    query_engine=duckdb
    output_format=parquet
    generated_csv_file_count=0

output_scope:

    FULL_OUTPUT when symbol_count matches the raw 1D universe from Phase 5 manifest/catalog.
    SAMPLE_OUTPUT when only a subset is materialized.

基本 quality rules：

    required identity/time/OHLCV fields are non-null
    high >= low
    high >= open
    high >= close
    low <= open
    low <= close
    volume >= 0
    quote_volume >= 0
    trade_count >= 0
    date matches open_time_taipei calendar date
    no duplicate date per symbol for interval=1d

---

## 9. Validation Target

新增 validation target：

    binance-um-klines-parquet

可執行：

    python -m datahub.validation \
      --target binance-um-klines-parquet \
      --interval 1d \
      --manifest local_data/binance_um_klines/interval=1d/parquet/manifests/materialization_manifest.json

驗證項目：

- manifest exists
- output_root exists
- symbol_count > 0
- row_count > 0
- failed_symbol_count == 0
- parquet files exist
- DuckDB 可讀取 parquet
- logical DuckDB schema 必要欄位存在
- `(symbol, interval, open_time)` 無重複
- 同一 symbol 的 `date` 無重複
- required fields 無 NULL
- OHLC rule 通過
- date matches open_time_taipei calendar date
- manifest file_count matches actual parquet file count
- manifest row_count matches DuckDB COUNT(*)
- generated_csv_file_count == 0
- parquet output tree 下沒有 `.csv`
- `python -m datahub.validation --all` clone-safe
- full output validation should classify SAMPLE_OUTPUT separately from FULL_OUTPUT

---

## 10. Tests

新增 unit tests，小型 fixture 即可。

測試項目：

1. 無 header CSV zip
2. 有 header CSV zip
3. schema 轉換
4. timestamp 轉換
5. date derived from open_time_taipei
6. ignore 欄位不進 parquet
7. required fields non-null check
8. duplicate exact match 去重
9. duplicate date per symbol 被偵測
10. conflict 時 daily 優先並記錄
11. strict 遇 conflict fail
12. invalid OHLC rule 被偵測
13. resume skip 已完成 symbol
14. DuckDB 可讀 parquet
15. validation target clone-safe skip
16. explicit manifest validation 可檢查 parquet
17. manifest row_count / file_count 與實體 parquet 一致

測試命令：

    python -m unittest discover tests

---

## 11. Dependencies

DuckDB 是標準查詢引擎。

依賴管理沿用 repo 既有方式：

    pyproject.toml
    requirements.txt
    setup.cfg

需要時新增：

    duckdb
    pyarrow

Parquet 寫入可使用：

    pyarrow
    pandas + pyarrow
    DuckDB COPY TO parquet
    repo existing writer

缺 dependency 時，CLI 給清楚錯誤訊息。

---

## 12. Docs

新增或更新：

    docs/binance_um_klines_parquet_materialization.md
    docs/klines_access.md

內容包含：

- raw archive layer vs parquet materialized layer
- CSV 僅為 zip 內部來源格式
- query layer 使用 Parquet
- DuckDB 為標準讀取方式
- CLI 使用方式
- resume 使用方式
- validation 使用方式
- schema
- partition columns policy
- date policy: date derived from open_time_taipei
- UTC / Asia/Taipei 說明
- duplicate / conflict policy
- FULL_OUTPUT vs SAMPLE_OUTPUT

DuckDB 範例：

    import duckdb

    root = "local_data/binance_um_klines/interval=1d/parquet"

    df = duckdb.sql(f"""
        SELECT
            symbol,
            MIN(date) AS date_min,
            MAX(date) AS date_max,
            COUNT(*) AS row_count
        FROM read_parquet('{root}/**/*.parquet', hive_partitioning = true)
        WHERE symbol IN ('BTCUSDT', 'ETHUSDT', 'SOLUSDT')
        GROUP BY symbol
        ORDER BY symbol
    """).df()

    print(df)

---

## 13. Registry / Catalog

視既有設計更新：

    dataset_registry.json
    DATA_CATALOG.md

需要反映：

    market.binance.um.klines.1d
    raw archive layer

    market.binance.um.klines.1d.parquet
    materialized layer
    format=parquet
    query_engine=duckdb

---

## 14. Required Commands

先跑 tests / validation：

    python -m unittest discover tests
    python -m datahub.validation --all

小樣本：

    python -m datahub.materialization.binance_um_klines_parquet \
      --interval 1d \
      --symbols BTCUSDT ETHUSDT \
      --workers 2 \
      --overwrite

確認小樣本標記為 SAMPLE_OUTPUT 後，跑全量：

    python -m datahub.materialization.binance_um_klines_parquet \
      --interval 1d \
      --all \
      --workers 4 \
      --resume

explicit validation：

    python -m datahub.validation \
      --target binance-um-klines-parquet \
      --interval 1d \
      --manifest local_data/binance_um_klines/interval=1d/parquet/manifests/materialization_manifest.json

DuckDB smoke query：

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

Manifest vs actual count check：

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

CSV absence check：

    find local_data/binance_um_klines/interval=1d/parquet -type f -name '*.csv' | wc -l

最後：

    python -m unittest discover tests
    python -m datahub.validation --all
    git status --short

---

## 15. Completion Criteria

完成條件：

1. 1D raw zip 已 materialize 成 Parquet
2. Full output 覆蓋 raw 1D universe，約 921 symbols
3. DuckDB 可以 read_parquet
4. Hive partition 可用
5. schema 穩定
6. date policy 一致，date derived from open_time_taipei
7. duplicate / conflict policy 已實作
8. required fields 無 NULL
9. reports / manifest 已產生
10. manifest counts 與實體 parquet 一致
11. explicit parquet validation 通過
12. validation --all clone-safe 通過
13. unit tests 通過
14. docs 已更新
15. local_data 未被 Git 追蹤
16. parquet output tree 沒有 `.csv`
17. generated_csv_file_count == 0
18. output_scope=FULL_OUTPUT

---

## 16. Final Report

完成後回報：

Summary
- implemented items
- files changed
- CLI added
- output location

Materialization Result
- output_scope
- raw_discovered_symbol_count
- symbol_count
- row_count
- file_count
- actual_parquet_file_count
- actual_duckdb_row_count
- date_min
- date_max
- duplicate_count
- conflict_count
- failed_symbol_count
- generated_csv_file_count

DuckDB
- smoke query result

Validation
- unittest result
- validation --all result
- explicit parquet validation result
- manifest vs actual count result
- CSV absence check result

Git
- git status --short
- commit hash if committed

Warnings / Follow-up
- failed symbols
- data quality issues
- dependency caveats
- VERSION bump status

---

## 17. Commit

如果 tests / validations 全部通過，請 commit。

Commit message：

    feat: Phase 6 — materialize Binance UM 1D klines to parquet
