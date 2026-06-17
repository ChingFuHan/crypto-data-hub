# task_v0.09.md

# Phase 8 — Binance UM Kline 1H Parquet Materialization

你是本專案的 Data Engineering 實作 Agent。

任務：沿用 Phase 7 的 4H Parquet materialization 成熟流程，將 Binance USD-M Futures 1h raw archive 轉成 DuckDB 可查詢的 Parquet dataset。

本階段交付物：

    1h raw archive -> 1h Parquet -> DuckDB readable -> validation pass

本任務重點是延伸既有 interval-aware 架構。  
請盡量貼近 v0.08 的 4H 實作，只調整 1H 週期規則、驗證規則、文件與 registry。

---

## 1. 專案背景

Repo：

    ~/work/crypto-data-hub

目前版本：

    v0.8.0

重要 commit：

    7c683c8 docs: align phase 7 task specs with manifest schema
    8603a0a feat: Phase 7 — materialize Binance UM 4H klines to parquet
    6043844 feat: Phase 6 — materialize Binance UM 1D klines to parquet

已完成 1D Parquet：

    local_data/binance_um_klines/interval=1d/parquet/

1D 已驗證：

    output_scope=FULL_OUTPUT
    symbol_count=921
    row_count=634877
    file_count=2666
    failed_symbol_count=0
    generated_csv_file_count=0

已完成 4H Parquet：

    local_data/binance_um_klines/interval=4h/parquet/

4H 已驗證：

    output_scope=FULL_OUTPUT
    symbol_count=921
    row_count=3806991
    file_count=2666
    failed_symbol_count=0
    generated_csv_file_count=0

已完成 1H raw archive：

    local_data/binance_um_klines/interval=1h/raw/
    local_data/binance_um_klines/interval=1h/manifests/manifest.json

1H raw 結果：

    interval=1h
    symbol_count=921
    file_count=656252
    downloaded_count=33447
    verified_count=33447
    failed_count=0
    checksum_failed_count=0
    missing_count=0
    total_bytes=634059869
    date_min=2019-12-31
    date_max=2026-06-16

---

## 2. 目標

產出：

    local_data/binance_um_klines/interval=1h/parquet/

Manifest：

    local_data/binance_um_klines/interval=1h/parquet/manifests/materialization_manifest.json

Dataset ID：

    market.binance.um.klines.1h.parquet

DuckDB 讀取方式：

    read_parquet('local_data/binance_um_klines/interval=1h/parquet/**/*.parquet', hive_partitioning=true)

Full output 標準：

    output_scope=FULL_OUTPUT
    symbol_count=921
    failed_symbol_count=0
    row_count > 4H row_count
    row_count / 4H row_count >= 3.5

---

## 3. 實作策略

沿用 Phase 7 的 interval-aware materializer。

既有支援：

    interval=1d
    interval=4h

本階段新增：

    interval=1h

實作重點：

    共用既有架構
    延伸 interval 設定
    新增 1H time policy
    保留 1D regression
    保留 4H regression
    更新 validation / tests / docs / registry
    VERSION bump 到 v0.9.0

Small sample 與 full run 行為：

    small sample 可產生 SAMPLE_OUTPUT
    full --all --resume 必須升級為 FULL_OUTPUT
    full output 必須覆蓋 921 symbols

---

## 4. CLI

延伸既有 module：

    datahub/materialization/binance_um_klines_parquet.py

必須支援：

    python -m datahub.materialization.binance_um_klines_parquet \
      --interval 1h \
      --all

參數：

    --interval 1d|4h|1h
    --all
    --symbols BTCUSDT ETHUSDT
    --raw-root
    --manifest
    --output-root
    --resume
    --overwrite
    --workers 4
    --strict

行為：

- `--interval 1d` 維持可用。
- `--interval 4h` 維持可用。
- `--interval 1h` 新增可用。
- `--all` 處理 manifest 中所有 verified records。
- `--symbols` 只處理指定 symbols。
- `--resume` 跳過已完成項目，並更新 full manifest。
- `--overwrite` 重建 output。
- `--strict` 遇資料衝突或品質問題回傳 non-zero。
- full 921 symbols 輸出標為 `FULL_OUTPUT`。
- 小樣本輸出標為 `SAMPLE_OUTPUT`。

---

## 5. 資料層規則

資料層：

    raw zip archive = immutable source layer
    parquet = materialized query layer
    duckdb = standard query engine

CSV 規則：

    CSV 只存在於 zip 內部解析流程。
    persistent CSV file count = 0。
    generated_csv_file_count = 0。

後續階段：

    PostgreSQL serving layer
    live API updater
    strategy / trading / research workspace

---

## 6. 輸入格式

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

要求：

- 支援有 header / 無 header CSV。
- timestamp 為 millisecond epoch。
- OHLCV 轉 numeric。
- trade_count 轉 integer。
- ignore 不寫入 Parquet。
- source_archive 記錄來源 zip。
- archive_source 為 monthly 或 daily。
- archive_period 為 YYYY-MM 或 YYYY-MM-DD。

必填欄位：

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

必填欄位 NULL count 必須為 0。

---

## 7. 時間規則

欄位：

    open_time: int64 ms epoch
    close_time: int64 ms epoch
    open_time_utc
    open_time_taipei
    date
    year
    month

date 定義：

    date = open_time_taipei 的日期

1H 規則：

    open_time % 3600000 == 0
    close_time = open_time + 3599999

1H key policy：

    唯一鍵 = (symbol, interval, open_time)
    每個 (symbol, date) rows <= 24
    (symbol, date) 是日期分組欄位，不是唯一鍵

---

## 8. Parquet Schema

DuckDB logical schema 至少包含：

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

Partition 欄位規則：

- DuckDB logical schema 必須看得到 `symbol` 和 `year`。
- `symbol/year` 可由 Hive partition path 提供。
- schema 檢查以 DuckDB logical schema 為準。

---

## 9. Partition Layout

沿用既有 layout：

    local_data/binance_um_klines/interval=1h/parquet/
      symbol=BTCUSDT/
        year=2019/
          part-000.parquet
        year=2020/
          part-000.parquet

要求：

- DuckDB recursive glob 可讀。
- `hive_partitioning=true` 可用。
- `(symbol, interval, open_time)` 唯一。
- `(symbol, date)` rows <= 24。

---

## 10. Dedup / Conflict

Dedup key：

    symbol
    interval
    open_time

規則：

- monthly / daily 完全相同：保留一筆。
- monthly / daily 不同：daily 優先，記錄 conflict。
- 同 source 重複且一致：去重，記錄 duplicate。
- 同 source 重複但不一致：記錄 data quality issue。
- strict mode 遇 conflict / quality issue 回傳 non-zero。
- 非 strict mode 可完成，report 必須揭露問題。

---

## 11. 品質檢查

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
    open_time % 3600000 == 0
    close_time = open_time + 3599999
    duplicate key count = 0 for (symbol, interval, open_time)
    max rows per (symbol, date) <= 24

---

## 12. Reports

產生：

    local_data/binance_um_klines/interval=1h/parquet/manifests/materialization_manifest.json
    local_data/binance_um_klines/interval=1h/parquet/reports/coverage_report.json
    local_data/binance_um_klines/interval=1h/parquet/reports/data_quality_report.json
    local_data/binance_um_klines/interval=1h/parquet/reports/duplicate_report.json
    local_data/binance_um_klines/interval=1h/parquet/reports/conflict_report.json

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

1H 固定值：

    materialized_dataset_id=market.binance.um.klines.1h.parquet
    interval=1h
    query_engine=duckdb
    output_format=parquet
    generated_csv_file_count=0

Full output：

    output_scope=FULL_OUTPUT
    raw_discovered_symbol_count=921
    symbol_count=921
    failed_symbol_count=0

---

## 13. Validation

延伸既有 target：

    binance-um-klines-parquet

支援：

    --interval 1d
    --interval 4h
    --interval 1h

1H validation：

    python -m datahub.validation \
      --target binance-um-klines-parquet \
      --interval 1h \
      --manifest local_data/binance_um_klines/interval=1h/parquet/manifests/materialization_manifest.json

驗證項目：

- manifest exists
- output_root exists
- dataset id 正確
- interval=1h
- output_scope=FULL_OUTPUT
- symbol_count=921
- row_count > 4H row_count
- row_count / 4H row_count >= 3.5
- failed_symbol_count=0
- DuckDB 可讀
- schema 欄位完整
- required fields NULL count = 0
- duplicate key count = 0 for `(symbol, interval, open_time)`
- max rows per `(symbol, date)` <= 24
- OHLC 通過
- open_time 對齊 1H
- close_time 規則通過
- date policy 通過
- manifest row_count/file_count 等於實際結果
- generated_csv_file_count=0
- parquet tree `.csv` count = 0
- `validation --all` clone-safe
- 1D regression validation 通過
- 4H regression validation 通過

---

## 14. Tests

新增或延伸 tests：

1. 1D 既有測試通過
2. 4H 既有測試通過
3. 1H 無 header CSV
4. 1H 有 header CSV
5. schema 轉換
6. timestamp 轉換
7. date 由 open_time_taipei 產生
8. open_time 對齊 1H
9. close_time = open_time + 3599999
10. ignore 不進 Parquet
11. required fields NULL count = 0
12. duplicate exact match 去重
13. monthly/daily conflict daily 優先
14. strict conflict fail
15. invalid OHLC 被偵測
16. 1H 同日多筆合法
17. 1H 同日超過 24 筆被偵測
18. resume skip
19. resume 後 SAMPLE_OUTPUT 可升級為 FULL_OUTPUT
20. DuckDB 可讀 1H
21. explicit 1H validation
22. 1D regression validation
23. 4H regression validation
24. validation --all clone-safe
25. manifest count 等於實體 output

測試：

    python -m unittest discover tests

---

## 15. Docs / Registry

更新：

    docs/binance_um_klines_parquet_materialization.md
    docs/klines_access.md
    DATA_CATALOG.md
    dataset_registry.json
    VERSION
    CHANGELOG.md

內容：

- 1D / 4H / 1H Parquet dataset
- raw vs parquet layer
- DuckDB 查詢方式
- CLI 使用方式
- resume
- validation
- schema
- partition policy
- date policy
- 1H key policy
- 1D / 4H / 1H 差異
- FULL_OUTPUT vs SAMPLE_OUTPUT
- PostgreSQL future phase

---

## 16. 必跑指令

Baseline：

    python -m unittest discover tests
    python -m datahub.validation --all

Validate 1H raw：

    python -m datahub.validation \
      --target binance-um-klines \
      --interval 1h \
      --manifest local_data/binance_um_klines/interval=1h/manifests/manifest.json

Small sample：

    python -m datahub.materialization.binance_um_klines_parquet \
      --interval 1h \
      --symbols BTCUSDT ETHUSDT \
      --workers 2 \
      --overwrite

Full 1H：

    python -m datahub.materialization.binance_um_klines_parquet \
      --interval 1h \
      --all \
      --workers 4 \
      --resume

Explicit 1H validation：

    python -m datahub.validation \
      --target binance-um-klines-parquet \
      --interval 1h \
      --manifest local_data/binance_um_klines/interval=1h/parquet/manifests/materialization_manifest.json

1D regression：

    python -m datahub.validation \
      --target binance-um-klines-parquet \
      --interval 1d \
      --manifest local_data/binance_um_klines/interval=1d/parquet/manifests/materialization_manifest.json

4H regression：

    python -m datahub.validation \
      --target binance-um-klines-parquet \
      --interval 4h \
      --manifest local_data/binance_um_klines/interval=4h/parquet/manifests/materialization_manifest.json

Final：

    python -m unittest discover tests
    python -m datahub.validation --all
    git status --short

---

## 17. 完成條件

必須全部達成：

1. 1H raw validation 通過
2. 1H Parquet 產出
3. output_scope=FULL_OUTPUT
4. symbol_count=921
5. row_count > 4H row_count
6. row_count / 4H row_count >= 3.5
7. DuckDB 可讀
8. schema 完整
9. required fields NULL count = 0
10. duplicate key count = 0
11. max rows per `(symbol, date)` <= 24
12. open_time 對齊 1H
13. close_time 規則通過
14. OHLC 通過
15. manifest count 等於實體 output
16. explicit 1H validation 通過
17. 1D regression validation 通過
18. 4H regression validation 通過
19. validation --all 通過
20. unit tests 通過
21. docs/catalog/registry 更新
22. git tracked local_data count = 0
23. generated_csv_file_count=0
24. parquet tree `.csv` count = 0
25. VERSION bump 到 v0.9.0

---

## 18. Commit

全部通過後 commit。

Commit message：

    feat: Phase 8 — materialize Binance UM 1H klines to parquet
