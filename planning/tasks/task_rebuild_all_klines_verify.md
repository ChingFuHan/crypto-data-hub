# task_rebuild_all_klines_verify.md

# Verify Rebuild All Binance UM Kline Data

你是本專案的 Rebuild Verification Agent。

任務：驗證 `task_rebuild_all_klines.md` 產生的完整 local_data 是否正確、完整、可查詢、可重跑，且 Git repo 維持安全狀態。

任務類型：

    verification
    maintenance
    bootstrap check
    disaster recovery check

本任務是只讀驗證任務。
本任務完成後輸出 verify report。

---

## 1. 驗證目標

確認從零重建後的資料層完整可用：

    raw archive all intervals
    raw validation all intervals
    parquet all intervals
    parquet validation all intervals
    DuckDB readable all intervals
    manifest consistent all intervals
    validation --all pass
    unit tests pass
    git safety pass

Intervals：

    1d
    4h
    1h
    15m
    5m
    3m
    1m

---

## 2. Verification scope

本任務處理：

    repo checks
    raw manifest checks
    raw validation
    parquet manifest checks
    parquet validation
    DuckDB smoke checks
    full validation
    git safety
    disk report
    conditional resume idempotency
    final decision

本任務範圍外：

    code modification
    docs modification
    registry modification
    VERSION bump
    changelog update
    strategy development
    PostgreSQL serving layer
    live API
    incremental update workflow
    commit
    push

若發現問題：

    回報問題
    回報 evidence
    回報 smallest safe fix
    停在 verify report

---

## 3. Repo checks

進入 repo：

    cd ~/work/crypto-data-hub

執行：

    git log --oneline -8
    git status --short
    cat VERSION
    git ls-files local_data | wc -l
    python -m datahub.ingestion.binance_um_klines --help
    python -m datahub.materialization.binance_um_klines_parquet --help
    python -m datahub.validation --help

通過標準：

    VERSION >= v0.13.0
    materializer CLI supports 1d / 4h / 1h / 15m / 5m / 3m / 1m
    local_data tracked count = 0
    code/docs/test/registry have no unexpected changes

---

## 4. Expected data paths

Raw manifests：

    local_data/binance_um_klines/interval=1d/manifests/manifest.json
    local_data/binance_um_klines/interval=4h/manifests/manifest.json
    local_data/binance_um_klines/interval=1h/manifests/manifest.json
    local_data/binance_um_klines/interval=15m/manifests/manifest.json
    local_data/binance_um_klines/interval=5m/manifests/manifest.json
    local_data/binance_um_klines/interval=3m/manifests/manifest.json
    local_data/binance_um_klines/interval=1m/manifests/manifest.json

Parquet manifests：

    local_data/binance_um_klines/interval=1d/parquet/manifests/materialization_manifest.json
    local_data/binance_um_klines/interval=4h/parquet/manifests/materialization_manifest.json
    local_data/binance_um_klines/interval=1h/parquet/manifests/materialization_manifest.json
    local_data/binance_um_klines/interval=15m/parquet/manifests/materialization_manifest.json
    local_data/binance_um_klines/interval=5m/parquet/manifests/materialization_manifest.json
    local_data/binance_um_klines/interval=3m/parquet/manifests/materialization_manifest.json
    local_data/binance_um_klines/interval=1m/parquet/manifests/materialization_manifest.json

通過標準：

    all raw manifests exist
    all parquet manifests exist

---

## 5. Raw manifest checks

對每個 interval 檢查 raw manifest：

    1d
    4h
    1h
    15m
    5m
    3m
    1m

每個 raw manifest 必須符合：

    dataset_id = market.binance.um.klines
    dataset_variant_id = market.binance.um.klines.<INTERVAL>
    interval = <INTERVAL>
    archive_package_sources contains daily and monthly
    symbol_count >= interval-specific v0.13.0 symbol floor
    downloaded_count == verified_count
    failed_count = 0
    checksum_failed_count = 0
    missing_count = 0
    total_bytes > 0
    date_min <= 2019-12-31
    date_max >= interval-specific v0.13.0 raw date_max floor

Dynamic raw universe policy：

    raw manifest `symbol_count` 是該次 rebuild symbol universe authority
    parquet `raw_discovered_symbol_count` 必須等於 raw manifest `symbol_count`
    parquet `symbol_count` 必須等於 `raw_discovered_symbol_count`
    raw / parquet symbol_count 高於 floor 屬合法狀態
    Binance 新上市 symbol 使 symbol_count 成長，例如 940，屬合法完整狀態

v0.13.0 minimum symbol floor：

    1d:
      symbol_count >= 921

    4h:
      symbol_count >= 921

    1h:
      symbol_count >= 921

    15m:
      symbol_count >= 921

    5m:
      symbol_count >= 921

    3m:
      symbol_count >= 922

    1m:
      symbol_count >= 922

v0.13.0 minimum raw date_max floor：

    1d:
      raw date_max >= 2026-06-15

    4h:
      raw date_max >= 2026-06-15

    1h:
      raw date_max >= 2026-06-16

    15m:
      raw date_max >= 2026-06-17

    5m:
      raw date_max >= 2026-06-17

    3m:
      raw date_max >= 2026-06-18

    1m:
      raw date_max >= 2026-06-19

Freshness policy：

    raw date_max 高於 v0.13.0 floor = 合法狀態
    raw date_max 低於使用者指定 freshness target 但仍高於 v0.13.0 floor = PASS_WITH_WARNINGS 或 report freshness lag
    只有 raw date_max 低於 interval-specific v0.13.0 floor 才 REJECT

---

## 6. Raw validation

每個 interval 執行：

    python -m datahub.validation \
      --target binance-um-klines \
      --interval <INTERVAL> \
      --manifest local_data/binance_um_klines/interval=<INTERVAL>/manifests/manifest.json

通過標準：

    failed checks = 0
    error summary = none
    freshness lag warning 可接受，但必須在 final report 記錄
    只有低於 interval-specific v0.13.0 floor 才算 completeness failure

---

## 7. Parquet manifest checks

對每個 interval 檢查 materialization manifest。

每個 manifest 必須符合：

    materialized_dataset_id = market.binance.um.klines.<INTERVAL>.parquet
    interval = <INTERVAL>
    output_scope = FULL_OUTPUT
    raw_discovered_symbol_count == raw manifest symbol_count
    raw_discovered_symbol_count >= interval-specific v0.13.0 symbol floor
    symbol_count == raw_discovered_symbol_count
    row_count > 0
    file_count > 0
    failed_symbol_count = 0
    generated_csv_file_count = 0
    query_engine = duckdb
    output_format = parquet
    date_max >= interval-specific v0.13.0 parquet date_max floor

v0.13.0 minimum symbol floor：

    1d:
      symbol_count >= 921

    4h:
      symbol_count >= 921

    1h:
      symbol_count >= 921

    15m:
      symbol_count >= 921

    5m:
      symbol_count >= 921

    3m:
      symbol_count >= 922

    1m:
      symbol_count >= 922

v0.13.0 minimum parquet date_max floor：

    1d:
      parquet date_max >= 2026-06-15

    4h:
      parquet date_max >= 2026-06-16

    1h:
      parquet date_max >= 2026-06-17

    15m:
      parquet date_max >= 2026-06-18

    5m:
      parquet date_max >= 2026-06-18

    3m:
      parquet date_max >= 2026-06-19

    1m:
      parquet date_max >= 2026-06-20

最低 row_count baseline：

    1d:
      row_count >= 634877

    4h:
      row_count >= 3806991

    1h:
      row_count >= 15245266

    15m:
      row_count >= 61055727

    5m:
      row_count >= 183166850

    3m:
      row_count >= 305657915

    1m:
      row_count >= 918113837

row_count baseline policy：

    row_count 一律使用 >= v0.13.0 baseline
    不要求 exact match

Ratio baseline：

    4h row_count / 1d row_count >= 5.0
    1h row_count / 4h row_count >= 3.5
    15m row_count / 1h row_count >= 3.5
    5m row_count / 15m row_count >= 2.5
    3m row_count / 5m row_count >= 1.5
    1m row_count / 3m row_count >= 2.5

Freshness policy：

    parquet date_max 高於 v0.13.0 floor = 合法狀態
    parquet date_max 低於使用者指定 freshness target 但仍高於 v0.13.0 floor = PASS_WITH_WARNINGS 或 report freshness lag
    只有 parquet date_max 低於 interval-specific v0.13.0 floor 才 REJECT

---

## 8. Parquet validation

每個 interval 執行：

    python -m datahub.validation \
      --target binance-um-klines-parquet \
      --interval <INTERVAL> \
      --manifest local_data/binance_um_klines/interval=<INTERVAL>/parquet/manifests/materialization_manifest.json

通過標準：

    failed checks = 0
    output_scope = FULL_OUTPUT
    symbol_count == raw_discovered_symbol_count
    failed_symbol_count = 0
    generated_csv_file_count = 0
    duplicate key count = 0
    required fields NULL count = 0
    time alignment pass
    close_time policy pass
    OHLC pass
    date policy pass
    parquet date_max >= interval-specific v0.13.0 parquet floor
    manifest row_count/file_count equals actual output
    persistent_csv_count = 0
    freshness lag warning 可接受，但必須在 final report 記錄

---

## 9. DuckDB smoke checks

每個 interval 執行 DuckDB smoke query。

檢查：

    BTCUSDT exists
    ETHUSDT exists
    each has row_count > 0
    date_min <= 2019-12-31
    date_max >= interval-specific v0.13.0 parquet date_max floor
    required columns exist
    若低於使用者指定 freshness target 但仍高於 v0.13.0 floor，回報 freshness lag，不因此 REJECT

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

DuckDB read pattern：

    read_parquet('local_data/binance_um_klines/interval=<INTERVAL>/parquet/**/*.parquet', hive_partitioning=true)

---

## 10. Full validation

執行：

    python -m unittest discover tests

    python -m datahub.validation --all

通過標準：

    unit tests pass
    validation --all failed = 0

允許：

    validation --all optional / conditional skipped checks

Fail 條件：

    failed checks > 0

---

## 11. Git safety

執行：

    git status --short
    git ls-files local_data | wc -l
    git status --short | grep 'local_data/' || true

通過標準：

    git ls-files local_data = 0
    local_data staged count = 0
    local_data tracked count = 0
    code/docs/test/registry have no unexpected changes

若 git status 顯示 code/docs/test/registry 有變更：

    標記為 FAIL
    在 Problems 區塊列出檔案
    提供 smallest safe fix

---

## 12. Disk report

執行：

    df -h .
    df -i .
    du -sh local_data
    du -sh local_data/binance_um_klines
    du -sh local_data/binance_um_klines/interval=1d
    du -sh local_data/binance_um_klines/interval=4h
    du -sh local_data/binance_um_klines/interval=1h
    du -sh local_data/binance_um_klines/interval=15m
    du -sh local_data/binance_um_klines/interval=5m
    du -sh local_data/binance_um_klines/interval=3m
    du -sh local_data/binance_um_klines/interval=1m

用途：

    記錄重建後資料量
    發現異常膨脹
    發現磁碟風險

判定：

    free disk >= 50G:
      disk report pass

    free disk < 50G:
      data checks 仍可 ACCEPT_WITH_WARNINGS
      Problems 區塊註記 disk risk

---

## 13. Resume idempotency

預設可跳過 resume idempotency。

未經使用者授權時：

    resume idempotency = SKIPPED_BY_USER_POLICY
    不因此 REJECT

若使用者明確授權，再對每個 interval 執行：

    python -m datahub.materialization.binance_um_klines_parquet \
      --interval <INTERVAL> \
      --all \
      --workers 4 \
      --resume

通過標準：

    output_scope = FULL_OUTPUT
    symbol_count == raw_discovered_symbol_count
    row_count stable
    file_count stable
    failed_symbol_count = 0

完整驗證建議跑全部 intervals。

時間有限時最低必跑：

    1d
    3m
    1m

---

## 14. Final report format

請用以下格式回報：

Summary
- PASS / FAIL / PASS_WITH_WARNINGS
- 核心結論

Environment
- repo path
- VERSION
- git HEAD
- disk free
- inode status

Raw Manifests
- interval
- symbol_count
- v0.13_symbol_floor
- symbol_count_delta_vs_floor
- downloaded_count
- verified_count
- failed_count
- checksum_failed_count
- missing_count
- date_min
- raw_date_max
- v0.13_raw_date_max_floor
- freshness_status

Raw Validation
- interval
- checks passed
- checks failed
- result

Parquet Manifests
- interval
- output_scope
- raw_discovered_symbol_count
- symbol_count
- v0.13_symbol_floor
- symbol_count_delta_vs_floor
- row_count
- file_count
- failed_symbol_count
- generated_csv_file_count
- parquet_date_max
- v0.13_parquet_date_max_floor
- freshness_status

Parquet Validation
- interval
- checks passed
- checks failed
- result

DuckDB Smoke
- interval
- BTCUSDT row_count/date range
- ETHUSDT row_count/date range
- schema result

Global Validation
- unittest
- validation --all

Resume Idempotency
- interval
- resume_idempotency_status
- row_count stable
- file_count stable
- result

Git Safety
- git status
- local_data tracked count
- unexpected tracked/staged files

Disk
- total local_data size
- per interval size
- free space after rebuild

Problems
- problem
- evidence
- smallest safe fix

Final Decision
- ACCEPT
- REJECT
- ACCEPT_WITH_WARNINGS

---

## 15. ACCEPT / REJECT rules

ACCEPT 條件：

    all raw manifests exist
    all parquet manifests exist
    all raw validations pass
    all parquet validations pass
    all intervals output_scope = FULL_OUTPUT
    all intervals symbol_count == raw_discovered_symbol_count
    all intervals raw_discovered_symbol_count >= interval-specific v0.13.0 symbol floor
    all intervals row_count >= v0.13.0 row_count floor
    all intervals raw_date_max >= interval-specific v0.13.0 raw date_max floor
    all intervals parquet_date_max >= interval-specific v0.13.0 parquet date_max floor
    all intervals failed_symbol_count = 0
    all intervals generated_csv_file_count = 0
    all intervals duplicate key count = 0
    all intervals required fields NULL count = 0
    all intervals OHLC pass
    all intervals time alignment pass
    all intervals close_time policy pass
    all intervals date policy pass
    all intervals manifest row_count/file_count equals actual output
    all intervals persistent_csv_count = 0
    DuckDB smoke pass
    unit tests pass
    validation --all failed = 0
    git tracked local_data count = 0
    code/docs/test/registry have no unexpected changes

REJECT 條件：

    any raw manifest missing
    any parquet manifest missing
    any raw validation failed
    any parquet validation failed
    any interval output_scope != FULL_OUTPUT
    symbol_count != raw_discovered_symbol_count
    raw_discovered_symbol_count below interval-specific v0.13.0 symbol floor
    row_count below v0.13.0 floor
    raw_date_max below interval-specific v0.13.0 raw date_max floor
    parquet_date_max below interval-specific v0.13.0 parquet date_max floor
    failed_symbol_count > 0
    generated_csv_file_count > 0
    duplicate key count > 0
    required fields NULL count > 0
    OHLC fail
    time alignment fail
    close_time policy fail
    date policy fail
    manifest row_count/file_count mismatch
    persistent_csv_count > 0
    DuckDB unreadable
    validation --all failed > 0
    unit tests failed
    local_data tracked count > 0
    unexpected code/docs/test/registry changes

ACCEPT_WITH_WARNINGS 條件：

    data completeness pass
    validations pass
    git safety pass
    date_max lower than user-specified freshness target but still >= interval-specific v0.13.0 floor
    freshness lag is recorded in final report
    resume_idempotency_status = SKIPPED_BY_USER_POLICY
    disk free < 50G
    optional skipped checks require note
