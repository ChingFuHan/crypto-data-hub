# task_v0.12.md

# Phase 11 — Binance UM Kline 3M Raw + Parquet Materialization

你是本專案的 Data Engineering 實作 Agent。

任務：新增 Binance USD-M Futures 3m interval 支援，完成 3m raw ingestion、raw validation、Parquet materialization、Parquet validation，並保留既有 1D / 4H / 1H / 15M / 5M regression。

本階段交付物：

    3m raw ingestion support
    -> 3m raw archive
    -> 3m raw validation pass
    -> 3m Parquet
    -> DuckDB readable
    -> 3m Parquet validation pass
    -> regression validation pass
    -> local commit

本 Phase 版本：

    v0.12.0

---

## 1. 專案背景

Repo：

    ~/work/crypto-data-hub

目前版本：

    v0.11.0

最新完成 commit：

    00e3954 feat: Phase 10 — materialize Binance UM 5M klines to parquet

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

    5M:
      output_scope=FULL_OUTPUT
      symbol_count=921
      row_count=183166850
      file_count=2666
      failed_symbol_count=0

目前 ingestion allowed intervals：

    1d
    4h
    1h
    15m
    5m
    1m

本 Phase 新增 interval：

    3m

---

## 2. 本階段範圍

本階段處理：

    raw ingestion 3m support
    raw validation 3m support
    parquet materialization 3m support
    parquet validation 3m support
    tests
    docs
    registry
    changelog
    local commit

後續 Phase 處理：

    1m
    PostgreSQL serving layer
    live API
    strategy
    trading system

---

## 3. 3M 規則

Interval：

    3m

Milliseconds：

    180000

Time policy：

    open_time % 180000 == 0
    close_time = open_time + 179999

Key policy：

    unique key = (symbol, interval, open_time)

Rows per symbol/date：

    max rows per (symbol, date) <= 480

原因：

    24 * 60 / 3 = 480

3M baseline：

    base interval = 5m
    base row_count = 183166850
    row_count > 183166850
    row_count / 183166850 >= 1.5

Dataset ID：

    raw:
      market.binance.um.klines.3m

    parquet:
      market.binance.um.klines.3m.parquet

---

## 4. Raw ingestion 支援 3m

修改：

    datahub/ingestion/binance_um_klines.py

新增 3m 到：

    interval allowlist
    argparse choices
    validation error message
    interval-related constants

完成後確認指令可執行：

    python -m datahub.ingestion.binance_um_klines \
      --interval 3m \
      --all \
      --archive-source both \
      --workers 4 \
      --retries 5 \
      --timeout 60

Raw output path：

    local_data/binance_um_klines/interval=3m/

Raw manifest：

    local_data/binance_um_klines/interval=3m/manifests/manifest.json

---

## 5. Raw validation 支援 3m

修改：

    datahub/validation/binance_um_klines.py

新增 3m 到 raw kline validation 支援範圍。

Raw validation command：

    python -m datahub.validation \
      --target binance-um-klines \
      --interval 3m \
      --manifest local_data/binance_um_klines/interval=3m/manifests/manifest.json

Raw validation 通過標準：

    total checks pass
    interval=3m
    symbol_count >= 921 (3m 實際 922；+REUSDT 為 prior phases 後新上市 symbol)
    downloaded_count=verified_count
    failed_count=0
    checksum_failed_count=0
    missing_count=0

---

## 6. Parquet materializer 支援 3m

修改：

    datahub/materialization/binance_um_klines_parquet.py

新增：

    ALLOWED_INTERVALS += "3m"
    INTERVAL_MILLISECONDS["3m"] = 180000
    ROWS_PER_SYMBOL_DATE_LIMIT["3m"] = 480
    CODE_VERSION = "v0.12.0"

Parquet output：

    local_data/binance_um_klines/interval=3m/parquet/

Parquet manifest：

    local_data/binance_um_klines/interval=3m/parquet/manifests/materialization_manifest.json

CLI 必須支援：

    python -m datahub.materialization.binance_um_klines_parquet \
      --interval 3m \
      --all

參數：

    --interval 1d|4h|1h|15m|5m|3m
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

    3M 新增可用
    1D / 4H / 1H / 15M / 5M 維持可用
    small sample 可產生 SAMPLE_OUTPUT
    full --all --resume 必須升級為 FULL_OUTPUT
    full output 必須覆蓋 raw universe 全部 symbols (symbol_count == raw_discovered_symbol_count >= 921；3m 實際 922)

---

## 7. Parquet validation 支援 3m

修改：

    datahub/validation/binance_um_klines_parquet.py

新增 3m validation。

3M validation baseline：

    base interval = 5m
    base row_count = 183166850
    min_ratio = 1.5

新增 check ID：

    PQ-3M-ROWS-GT-5M

Validation command：

    python -m datahub.validation \
      --target binance-um-klines-parquet \
      --interval 3m \
      --manifest local_data/binance_um_klines/interval=3m/parquet/manifests/materialization_manifest.json

Validation 條件：

    output_scope=FULL_OUTPUT
    raw_discovered_symbol_count >= 921
    symbol_count == raw_discovered_symbol_count (3m 實際 922)
    row_count > 183166850
    row_count / 183166850 >= 1.5
    failed_symbol_count=0
    generated_csv_file_count=0
    DuckDB readable
    schema complete
    required fields NULL count=0
    duplicate key count=0
    max rows per (symbol, date) <= 480
    open_time % 180000 == 0
    close_time = open_time + 179999
    OHLC pass
    date policy pass
    manifest row_count/file_count equals actual output
    parquet tree .csv count=0

---

## 8. 資料與 Schema 規則

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

Partition layout：

    local_data/binance_um_klines/interval=3m/parquet/
      symbol=BTCUSDT/
        year=2019/
          part-000.parquet

DuckDB 讀取方式：

    read_parquet('local_data/binance_um_klines/interval=3m/parquet/**/*.parquet', hive_partitioning=true)

---

## 9. Dedup / Conflict

Dedup key：

    symbol
    interval
    open_time

Dedup / conflict 規則沿用 v0.11：

    monthly / daily 完全相同：保留一筆
    monthly / daily 不同：daily 優先，記錄 conflict
    同 source 重複且一致：去重，記錄 duplicate
    同 source 重複但不一致：記錄 data quality issue
    strict mode 遇 conflict / quality issue 回傳 non-zero
    非 strict mode 可完成，report 必須揭露問題

---

## 10. Reports / Manifest

產生：

    local_data/binance_um_klines/interval=3m/parquet/manifests/materialization_manifest.json
    local_data/binance_um_klines/interval=3m/parquet/reports/coverage_report.json
    local_data/binance_um_klines/interval=3m/parquet/reports/data_quality_report.json
    local_data/binance_um_klines/interval=3m/parquet/reports/duplicate_report.json
    local_data/binance_um_klines/interval=3m/parquet/reports/conflict_report.json

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

3M 固定值：

    materialized_dataset_id=market.binance.um.klines.3m.parquet
    interval=3m
    query_engine=duckdb
    output_format=parquet
    generated_csv_file_count=0

---

## 11. Tests

新增或延伸 tests：

    3m 無 header CSV parse
    3m 有 header CSV parse
    open_time 對齊 3m
    close_time = open_time + 179999
    3m 同日 rows <= 480 合法
    3m 同日 rows > 480 被偵測
    DuckDB 可讀 3m
    explicit 3m validation
    resume 後 SAMPLE_OUTPUT 可升級為 FULL_OUTPUT
    1D / 4H / 1H / 15M / 5M regression validation 維持通過

測試：

    python -m unittest discover tests

---

## 12. Docs / Registry

更新：

    VERSION
    CHANGELOG.md
    DATA_CATALOG.md
    dataset_registry.json
    docs/binance_um_klines_parquet_materialization.md
    docs/klines_access.md

版本：

    VERSION = v0.12.0

文件需包含：

    3M raw dataset
    3M Parquet dataset
    DuckDB query method
    CLI usage
    validation command
    schema
    partition policy
    date policy
    3M key policy
    FULL_OUTPUT vs SAMPLE_OUTPUT

---

## 13. 必跑指令

Baseline：

    python -m unittest discover tests
    python -m datahub.validation --all

3M raw ingestion：

    PYTHONUNBUFFERED=1 /usr/bin/time -v python -m datahub.ingestion.binance_um_klines \
      --interval 3m \
      --all \
      --archive-source both \
      --workers 4 \
      --retries 5 \
      --timeout 60

3M raw validation：

    python -m datahub.validation \
      --target binance-um-klines \
      --interval 3m \
      --manifest local_data/binance_um_klines/interval=3m/manifests/manifest.json

3M small sample：

    python -m datahub.materialization.binance_um_klines_parquet \
      --interval 3m \
      --symbols BTCUSDT ETHUSDT \
      --workers 2 \
      --overwrite

3M full materialization：

    python -m datahub.materialization.binance_um_klines_parquet \
      --interval 3m \
      --all \
      --workers 4 \
      --resume

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

Final checks：

    python -m unittest discover tests
    python -m datahub.validation --all
    git status --short
    git ls-files local_data | wc -l

---

## 14. 完成條件

全部達成才算完成：

    3m raw validation 通過
    3m Parquet 產出
    output_scope=FULL_OUTPUT
    raw_discovered_symbol_count >= 921
    symbol_count == raw_discovered_symbol_count (3m 實際 922；+REUSDT 新上市)
    row_count > 183166850
    row_count / 183166850 >= 1.5
    failed_symbol_count=0
    generated_csv_file_count=0
    DuckDB 可讀
    schema 完整
    required fields NULL count=0
    duplicate key count=0
    max rows per (symbol, date) <= 480
    open_time % 180000 == 0
    close_time = open_time + 179999
    OHLC 通過
    date policy 通過
    manifest count 等於實體 output
    explicit 3m validation 通過
    1D / 4H / 1H / 15M / 5M regression 通過
    validation --all 通過
    unit tests 通過
    git tracked local_data count = 0
    VERSION = v0.12.0

---

## 15. Commit

全部完成後建立本地 commit：

    feat: Phase 11 — materialize Binance UM 3M klines to parquet

完成後回報：

    git log --oneline -3
    cat VERSION
    git status --short
    git ls-files local_data | wc -l

Push 由使用者後續處理。
