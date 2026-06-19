# task_v0.11.md

# Phase 10 — Binance UM Kline 5M Parquet Materialization

你是本專案的 Data Engineering 實作 Agent。

任務：沿用 Phase 9 的 15M Parquet materialization 成熟流程，將 Binance USD-M Futures 5m raw archive 轉成 DuckDB 可查詢的 Parquet dataset。

本階段交付物：

    5m raw archive -> 5m Parquet -> DuckDB readable -> validation pass

請貼近 v0.10 的 15M 實作，只調整 5M 週期規則、驗證規則、文件與 registry。

---

## 1. 專案背景

Repo：

    ~/work/crypto-data-hub

目前版本：

    v0.10.0

最新重要 commit：

    18f3543 feat: Phase 9 — materialize Binance UM 15M klines to parquet

已完成 Parquet：

    1D:
      output_scope=FULL_OUTPUT
      symbol_count=921
      row_count=634877
      file_count=2666
      failed_symbol_count=0

    4H:
      output_scope=FULL_OUTPUT
      symbol_count=921
      row_count=3806991
      file_count=2666
      failed_symbol_count=0

    1H:
      output_scope=FULL_OUTPUT
      symbol_count=921
      row_count=15245266
      file_count=2666
      failed_symbol_count=0

    15M:
      output_scope=FULL_OUTPUT
      symbol_count=921
      row_count=61055727
      file_count=2666
      failed_symbol_count=0

已完成 5M raw archive：

    local_data/binance_um_klines/interval=5m/raw/
    local_data/binance_um_klines/interval=5m/manifests/manifest.json

5M raw 結果：

    interval=5m
    symbol_count=921
    file_count=657043
    downloaded_count=34238
    verified_count=34238
    skipped_count=622805
    failed_count=0
    checksum_failed_count=0
    missing_count=0
    total_bytes=6467270279
    date_min=2019-12-31
    date_max=2026-06-17

5M raw validation：

    total checks=13
    passed checks=13
    failed checks=0

---

## 2. 目標

產出：

    local_data/binance_um_klines/interval=5m/parquet/

Manifest：

    local_data/binance_um_klines/interval=5m/parquet/manifests/materialization_manifest.json

Dataset ID：

    market.binance.um.klines.5m.parquet

DuckDB 讀取方式：

    read_parquet('local_data/binance_um_klines/interval=5m/parquet/**/*.parquet', hive_partitioning=true)

Full output 標準：

    output_scope=FULL_OUTPUT
    symbol_count=921
    failed_symbol_count=0
    row_count > 61055727
    row_count / 61055727 >= 2.5

---

## 3. 實作策略

沿用既有 interval-aware materializer：

    datahub/materialization/binance_um_klines_parquet.py

既有支援：

    interval=1d
    interval=4h
    interval=1h
    interval=15m

本階段新增：

    interval=5m

實作重點：

    共用既有架構
    新增 5M interval 設定
    新增 5M time policy
    保留 1D / 4H / 1H / 15M regression
    更新 validation / tests / docs / registry
    VERSION bump 到 v0.11.0

Small sample 與 full run 行為：

    small sample 可產生 SAMPLE_OUTPUT
    full --all --resume 必須升級為 FULL_OUTPUT
    full output 必須覆蓋 921 symbols

範圍收斂：

    本階段只處理 5M Parquet
    既有 1D / 4H / 1H / 15M 只做 regression
    PostgreSQL / live API / strategy / trading 留給後續 Phase

---

## 4. CLI

必須支援：

    python -m datahub.materialization.binance_um_klines_parquet \
      --interval 5m \
      --all

參數：

    --interval 1d|4h|1h|15m|5m
    --all
    --symbols BTCUSDT ETHUSDT
    --raw-root
    --manifest
    --output-root
    --resume
    --overwrite
    --workers 4
    --strict

要求：

    1D / 4H / 1H / 15M 維持可用
    5M 新增可用
    --resume 跳過已完成項目並更新 full manifest
    921 symbols full output 標為 FULL_OUTPUT
    小樣本 output 標為 SAMPLE_OUTPUT

---

## 5. 資料與 Schema 規則

資料層：

    raw zip archive = immutable source layer
    parquet = materialized query layer
    duckdb = standard query engine

CSV：

    CSV 只存在於 zip 內部解析流程
    persistent CSV file count = 0
    generated_csv_file_count = 0

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

Parquet logical schema 至少包含：

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

Partition layout 沿用：

    local_data/binance_um_klines/interval=5m/parquet/
      symbol=BTCUSDT/
        year=2019/
          part-000.parquet

---

## 6. 5M 週期規則

date 定義：

    date = open_time_taipei 的日期

5M time policy：

    open_time % 300000 == 0
    close_time = open_time + 299999

5M key policy：

    唯一鍵 = (symbol, interval, open_time)
    每個 (symbol, date) rows <= 288

Dedup key：

    symbol
    interval
    open_time

Dedup / conflict 規則沿用 v0.10：

    monthly / daily 完全相同：保留一筆
    monthly / daily 不同：daily 優先，記錄 conflict
    同 source 重複且一致：去重，記錄 duplicate
    同 source 重複但不一致：記錄 data quality issue
    strict mode 遇 conflict / quality issue 回傳 non-zero
    非 strict mode 可完成，report 必須揭露問題

---

## 7. 品質檢查

必須檢查：

    required fields NULL count = 0
    high >= low
    high >= open
    high >= close
    low <= open
    low <= close
    volume >= 0
    quote_volume >= 0
    trade_count >= 0
    date = open_time_taipei 的日期
    open_time % 300000 == 0
    close_time = open_time + 299999
    duplicate key count = 0 for (symbol, interval, open_time)
    max rows per (symbol, date) <= 288

---

## 8. Reports / Manifest

產生：

    local_data/binance_um_klines/interval=5m/parquet/manifests/materialization_manifest.json
    local_data/binance_um_klines/interval=5m/parquet/reports/coverage_report.json
    local_data/binance_um_klines/interval=5m/parquet/reports/data_quality_report.json
    local_data/binance_um_klines/interval=5m/parquet/reports/duplicate_report.json
    local_data/binance_um_klines/interval=5m/parquet/reports/conflict_report.json

Manifest 至少包含：

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

5M 固定值：

    materialized_dataset_id=market.binance.um.klines.5m.parquet
    interval=5m
    query_engine=duckdb
    output_format=parquet
    generated_csv_file_count=0

---

## 9. Validation

延伸既有 target：

    binance-um-klines-parquet

支援：

    --interval 1d
    --interval 4h
    --interval 1h
    --interval 15m
    --interval 5m

5M validation：

    python -m datahub.validation \
      --target binance-um-klines-parquet \
      --interval 5m \
      --manifest local_data/binance_um_klines/interval=5m/parquet/manifests/materialization_manifest.json

驗證重點：

    output_scope=FULL_OUTPUT
    symbol_count=921
    row_count > 61055727
    row_count / 61055727 >= 2.5
    failed_symbol_count=0
    DuckDB 可讀
    schema 欄位完整
    required fields NULL count = 0
    duplicate key count = 0
    max rows per (symbol, date) <= 288
    OHLC 通過
    open_time 對齊 5M
    close_time 規則通過
    date policy 通過
    manifest row_count/file_count 等於實際結果
    generated_csv_file_count=0
    parquet tree .csv count = 0
    1D / 4H / 1H / 15M regression validation 通過
    validation --all 通過

---

## 10. Tests

新增或延伸 tests：

    5M 無 header CSV
    5M 有 header CSV
    open_time 對齊 5M
    close_time = open_time + 299999
    5M 同日多筆合法
    5M 同日超過 288 筆被偵測
    DuckDB 可讀 5M
    explicit 5M validation
    resume 後 SAMPLE_OUTPUT 可升級為 FULL_OUTPUT
    1D / 4H / 1H / 15M regression validation
    validation --all clone-safe
    manifest count 等於實體 output

測試：

    python -m unittest discover tests

---

## 11. Docs / Registry

更新：

    docs/binance_um_klines_parquet_materialization.md
    docs/klines_access.md
    DATA_CATALOG.md
    dataset_registry.json
    VERSION
    CHANGELOG.md

內容：

    1D / 4H / 1H / 15M / 5M Parquet dataset
    DuckDB 查詢方式
    CLI 使用方式
    validation
    schema
    partition policy
    date policy
    5M key policy
    FULL_OUTPUT vs SAMPLE_OUTPUT

---

## 12. 必跑指令

Baseline：

    python -m unittest discover tests
    python -m datahub.validation --all

Validate 5M raw：

    python -m datahub.validation \
      --target binance-um-klines \
      --interval 5m \
      --manifest local_data/binance_um_klines/interval=5m/manifests/manifest.json

Small sample：

    python -m datahub.materialization.binance_um_klines_parquet \
      --interval 5m \
      --symbols BTCUSDT ETHUSDT \
      --workers 2 \
      --overwrite

Full 5M：

    python -m datahub.materialization.binance_um_klines_parquet \
      --interval 5m \
      --all \
      --workers 4 \
      --resume

Explicit 5M validation：

    python -m datahub.validation \
      --target binance-um-klines-parquet \
      --interval 5m \
      --manifest local_data/binance_um_klines/interval=5m/parquet/manifests/materialization_manifest.json

Regression：

    python -m datahub.validation --target binance-um-klines-parquet --interval 1d --manifest local_data/binance_um_klines/interval=1d/parquet/manifests/materialization_manifest.json
    python -m datahub.validation --target binance-um-klines-parquet --interval 4h --manifest local_data/binance_um_klines/interval=4h/parquet/manifests/materialization_manifest.json
    python -m datahub.validation --target binance-um-klines-parquet --interval 1h --manifest local_data/binance_um_klines/interval=1h/parquet/manifests/materialization_manifest.json
    python -m datahub.validation --target binance-um-klines-parquet --interval 15m --manifest local_data/binance_um_klines/interval=15m/parquet/manifests/materialization_manifest.json

Final：

    python -m unittest discover tests
    python -m datahub.validation --all
    git status --short

---

## 13. 完成條件

必須全部達成：

    5M raw validation 通過
    5M Parquet 產出
    output_scope=FULL_OUTPUT
    symbol_count=921
    row_count > 61055727
    row_count / 61055727 >= 2.5
    DuckDB 可讀
    schema 完整
    required fields NULL count = 0
    duplicate key count = 0
    max rows per (symbol, date) <= 288
    open_time 對齊 5M
    close_time 規則通過
    OHLC 通過
    manifest count 等於實體 output
    explicit 5M validation 通過
    1D / 4H / 1H / 15M regression validation 通過
    validation --all 通過
    unit tests 通過
    docs/catalog/registry 更新
    git tracked local_data count = 0
    generated_csv_file_count=0
    parquet tree .csv count = 0
    VERSION bump 到 v0.11.0

---

## 14. Commit

全部通過後 commit。

Commit message：

    feat: Phase 10 — materialize Binance UM 5M klines to parquet
