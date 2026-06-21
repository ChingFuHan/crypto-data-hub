# task_v0.13.md

# Phase 12 — Binance UM Kline 1M Parquet Materialization

你是本專案的 Data Engineering 實作 Agent。

任務：沿用 Phase 11 的 3M Parquet materialization 成熟流程，將 Binance USD-M Futures 1m raw archive 轉成 DuckDB 可查詢的 Parquet dataset。

本階段交付物：

    1m raw validation pass
    -> 1m Parquet
    -> DuckDB readable
    -> 1m Parquet validation pass
    -> 1D / 4H / 1H / 15M / 5M / 3M regression validation pass
    -> local commit

本 Phase 版本：

    v0.13.0

---

## 1. 專案背景

Repo：

    ~/work/crypto-data-hub

目前版本：

    v0.12.0

最新完成 commit：

    b553aec feat: Phase 11 — materialize Binance UM 3M klines to parquet

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

    3M:
      output_scope=FULL_OUTPUT
      raw_discovered_symbol_count=922
      symbol_count=922
      row_count=305657915
      file_count=2667
      failed_symbol_count=0
      extra symbol=REUSDT

已完成 1M raw archive：

    local_data/binance_um_klines/interval=1m/raw/
    local_data/binance_um_klines/interval=1m/manifests/manifest.json

1M raw 結果：

    interval=1m
    dataset_id=market.binance.um.klines
    dataset_variant_id=market.binance.um.klines.1m
    archive_package_sources=['daily', 'monthly']
    symbol_count=922
    file_count=658627
    downloaded_count=35822
    verified_count=35822
    skipped_count=622805
    failed_count=0
    checksum_failed_count=0
    missing_count=0
    total_bytes=28857297954
    date_min=2019-12-31
    date_max=2026-06-19

1M raw validation：

    total checks=13
    passed checks=13
    failed checks=0
    warning checks=0
    skipped checks=0

---

## 2. 本階段範圍

本階段處理：

    raw validation 1m confirmation
    parquet materialization 1m support
    parquet validation 1m support
    tests
    docs
    registry
    changelog
    local commit

本階段起點：

    1M raw ingestion 已完成
    1M raw validation 已通過
    從既有 1M raw manifest 接續 Parquet materialization

本階段使用的 raw manifest：

    local_data/binance_um_klines/interval=1m/manifests/manifest.json

後續 Phase 處理：

    PostgreSQL serving layer
    live API
    strategy
    trading system
    incremental update workflow

---

## 3. 1M 規則

Interval：

    1m

Milliseconds：

    60000

Time policy：

    open_time % 60000 == 0
    close_time = open_time + 59999

Key policy：

    unique key = (symbol, interval, open_time)

Rows per symbol/date：

    max rows per (symbol, date) <= 1440

原因：

    24 * 60 = 1440

1M baseline：

    base interval = 3m
    base row_count = 305657915
    row_count > 305657915
    row_count / 305657915 >= 2.5

預估 row_count：

    305657915 * 3 ~= 916973745

Symbol universe policy：

    raw_discovered_symbol_count >= 922
    symbol_count == raw_discovered_symbol_count
    failed_symbol_count=0

Dataset ID：

    raw:
      market.binance.um.klines.1m

    parquet:
      market.binance.um.klines.1m.parquet

---

## 4. Parquet materializer 支援 1m

修改：

    datahub/materialization/binance_um_klines_parquet.py

新增或確認：

    ALLOWED_INTERVALS includes "1m"
    INTERVAL_MILLISECONDS["1m"] = 60000
    ROWS_PER_SYMBOL_DATE_LIMIT["1m"] = 1440
    CODE_VERSION = "v0.13.0"

Parquet output：

    local_data/binance_um_klines/interval=1m/parquet/

Parquet manifest：

    local_data/binance_um_klines/interval=1m/parquet/manifests/materialization_manifest.json

CLI 必須支援：

    python -m datahub.materialization.binance_um_klines_parquet \
      --interval 1m \
      --all

參數：

    --interval 1d|4h|1h|15m|5m|3m|1m
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

    1M 新增可用
    1D / 4H / 1H / 15M / 5M / 3M 維持可用
    small sample 可產生 SAMPLE_OUTPUT
    full --all --resume 必須升級為 FULL_OUTPUT
    full output 必須覆蓋 raw_discovered_symbol_count symbols

---

## 5. Parquet validation 支援 1m

修改：

    datahub/validation/binance_um_klines_parquet.py

新增 1m validation。

1M validation baseline：

    base interval = 3m
    base row_count = 305657915
    min_ratio = 2.5

新增 check ID：

    PQ-1M-ROWS-GT-3M

Validation command：

    python -m datahub.validation \
      --target binance-um-klines-parquet \
      --interval 1m \
      --manifest local_data/binance_um_klines/interval=1m/parquet/manifests/materialization_manifest.json

Validation 條件：

    output_scope=FULL_OUTPUT
    raw_discovered_symbol_count >= 922
    symbol_count == raw_discovered_symbol_count
    row_count > 305657915
    row_count / 305657915 >= 2.5
    failed_symbol_count=0
    generated_csv_file_count=0
    DuckDB readable
    schema complete
    required fields NULL count=0
    duplicate key count=0
    max rows per (symbol, date) <= 1440
    open_time % 60000 == 0
    close_time = open_time + 59999
    OHLC pass
    date policy pass
    manifest row_count/file_count equals actual output
    parquet tree .csv count=0

---

## 6. 資料與 Schema 規則

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

    local_data/binance_um_klines/interval=1m/parquet/
      symbol=BTCUSDT/
        year=2019/
          part-000.parquet

DuckDB 讀取方式：

    read_parquet('local_data/binance_um_klines/interval=1m/parquet/**/*.parquet', hive_partitioning=true)

---

## 7. Dedup / Conflict

Dedup key：

    symbol
    interval
    open_time

Dedup / conflict 規則沿用 v0.12：

    monthly / daily 完全相同：保留一筆
    monthly / daily 不同：daily 優先，記錄 conflict
    同 source 重複且一致：去重，記錄 duplicate
    同 source 重複但不一致：記錄 data quality issue
    strict mode 遇 conflict / quality issue 回傳 non-zero
    非 strict mode 可完成，report 必須揭露問題

---

## 8. Reports / Manifest

產生：

    local_data/binance_um_klines/interval=1m/parquet/manifests/materialization_manifest.json
    local_data/binance_um_klines/interval=1m/parquet/reports/coverage_report.json
    local_data/binance_um_klines/interval=1m/parquet/reports/data_quality_report.json
    local_data/binance_um_klines/interval=1m/parquet/reports/duplicate_report.json
    local_data/binance_um_klines/interval=1m/parquet/reports/conflict_report.json

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

1M 固定值：

    materialized_dataset_id=market.binance.um.klines.1m.parquet
    interval=1m
    query_engine=duckdb
    output_format=parquet
    generated_csv_file_count=0

---

## 9. Tests

新增或延伸 tests：

    1m 無 header CSV parse
    1m 有 header CSV parse
    open_time 對齊 1m
    close_time = open_time + 59999
    1m 同日 rows <= 1440 合法
    1m 同日 rows > 1440 被偵測
    DuckDB 可讀 1m
    explicit 1m validation
    resume 後 SAMPLE_OUTPUT 可升級為 FULL_OUTPUT
    1D / 4H / 1H / 15M / 5M / 3M regression validation 維持通過

測試：

    python -m unittest discover tests

---

## 10. Docs / Registry

更新：

    VERSION
    CHANGELOG.md
    DATA_CATALOG.md
    dataset_registry.json
    docs/binance_um_klines_parquet_materialization.md
    docs/klines_access.md
    docs/binance_um_klines_dataset.md
    docs/research_agent_klines_access.md

版本：

    VERSION = v0.13.0

文件需包含：

    1M raw dataset
    1M Parquet dataset
    DuckDB query method
    CLI usage
    validation command
    schema
    partition policy
    date policy
    1M key policy
    FULL_OUTPUT vs SAMPLE_OUTPUT
    symbol_count == raw_discovered_symbol_count policy

---

## 11. Resource note

目前資源狀態：

    Windows Host C free ~= 205G
    VM free ~= 279G
    current local_data/binance_um_klines ~= 93G
    1M raw ~= 27G

1M materialization 可能產生大量 Parquet output。執行前與執行中應檢查：

    df -h .
    df -i .
    du -sh local_data/binance_um_klines/interval=1m
    du -sh local_data/binance_um_klines/interval=1m/parquet 2>/dev/null

資源判定：

    若 VM 可用空間低於 120G，先停止並回報空間狀態
    若 materialization log 出現 No space left on device，先停止並回報，不進入後續 validation
    full materialization 執行期間維持單一 process

---

## 12. 必跑指令

Baseline：

    python -m unittest discover tests
    python -m datahub.validation --all

Confirm 1M raw validation：

    python -m datahub.validation \
      --target binance-um-klines \
      --interval 1m \
      --manifest local_data/binance_um_klines/interval=1m/manifests/manifest.json

Pre-flight resource check：

    df -h .
    df -i .
    du -sh local_data/binance_um_klines/interval=1m
    du -sh local_data/binance_um_klines/interval=1m/parquet 2>/dev/null

1M small sample：

    python -m datahub.materialization.binance_um_klines_parquet \
      --interval 1m \
      --symbols BTCUSDT ETHUSDT \
      --workers 2 \
      --overwrite

1M full materialization：

    python -m datahub.materialization.binance_um_klines_parquet \
      --interval 1m \
      --all \
      --workers 4 \
      --resume

Explicit 1M validation：

    python -m datahub.validation \
      --target binance-um-klines-parquet \
      --interval 1m \
      --manifest local_data/binance_um_klines/interval=1m/parquet/manifests/materialization_manifest.json

Regression validation：

    python -m datahub.validation --target binance-um-klines-parquet --interval 1d --manifest local_data/binance_um_klines/interval=1d/parquet/manifests/materialization_manifest.json
    python -m datahub.validation --target binance-um-klines-parquet --interval 4h --manifest local_data/binance_um_klines/interval=4h/parquet/manifests/materialization_manifest.json
    python -m datahub.validation --target binance-um-klines-parquet --interval 1h --manifest local_data/binance_um_klines/interval=1h/parquet/manifests/materialization_manifest.json
    python -m datahub.validation --target binance-um-klines-parquet --interval 15m --manifest local_data/binance_um_klines/interval=15m/parquet/manifests/materialization_manifest.json
    python -m datahub.validation --target binance-um-klines-parquet --interval 5m --manifest local_data/binance_um_klines/interval=5m/parquet/manifests/materialization_manifest.json
    python -m datahub.validation --target binance-um-klines-parquet --interval 3m --manifest local_data/binance_um_klines/interval=3m/parquet/manifests/materialization_manifest.json

Final checks：

    python -m unittest discover tests
    python -m datahub.validation --all
    git status --short
    git ls-files local_data | wc -l

---

## 13. 完成條件

全部達成才算完成：

    1m raw validation 通過
    1m Parquet 產出
    output_scope=FULL_OUTPUT
    raw_discovered_symbol_count >= 922
    symbol_count == raw_discovered_symbol_count
    row_count > 305657915
    row_count / 305657915 >= 2.5
    failed_symbol_count=0
    generated_csv_file_count=0
    DuckDB 可讀
    schema 完整
    required fields NULL count=0
    duplicate key count=0
    max rows per (symbol, date) <= 1440
    open_time % 60000 == 0
    close_time = open_time + 59999
    OHLC 通過
    date policy 通過
    manifest count 等於實體 output
    explicit 1m validation 通過
    1D / 4H / 1H / 15M / 5M / 3M regression 通過
    validation --all 通過
    unit tests 通過
    git tracked local_data count = 0
    VERSION = v0.13.0
    commit message 不含 Co-Authored-By trailer

---

## 14. Commit

全部完成後建立本地 commit：

    feat: Phase 12 — materialize Binance UM 1M klines to parquet

Commit message 不加入 Co-Authored-By trailer。

完成後回報：

    git log --oneline -3
    cat VERSION
    git status --short
    git ls-files local_data | wc -l

Push 由使用者後續處理。
