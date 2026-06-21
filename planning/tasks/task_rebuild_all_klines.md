# task_rebuild_all_klines.md

# Rebuild All Binance UM Kline Data From Scratch

你是本專案的 DataHub Rebuild Agent。

任務：在新電腦、新 VM、或 `local_data/` 無法搬移的情況下，使用目前 repo 內已成熟的 v0.13.0 程式碼，從 Binance public archive 重新建立完整 Binance USD-M Futures Kline 資料層。

任務類型：

    maintenance
    bootstrap
    disaster recovery
    local data rebuild

本任務不是新功能 Phase。
本任務不 bump VERSION。
本任務不產生 release commit。
本任務完成後只回報 rebuild 結果。

---

## 1. 使用情境

本任務用於：

    新電腦 clone repo
    新 VM 建置
    local_data 搬移失敗
    local_data 遺失
    local_data 損毀
    VM / disk 災難復原
    需要從 Binance archive 從零重建 raw + parquet

歷史 Phase 任務用途：

    task_v0.07 ~ task_v0.13 是開發與驗收歷史
    本任務使用目前成熟程式碼直接 rebuild
    本任務的執行清單以本檔為準

---

## 2. 專案背景

Repo：

    ~/work/crypto-data-hub

最低要求版本：

    v0.13.0

v0.13.0 已支援完整 intervals：

    1d
    4h
    1h
    15m
    5m
    3m
    1m

資料架構：

    raw zip archive = immutable source layer
    parquet = materialized query layer
    duckdb = standard query engine
    local_data = local generated data, gitignored

GitHub repo 保存：

    code
    docs
    tests
    registry
    planning/tasks

local generated layer：

    local_data/
    raw zip archive
    parquet output
    rebuild logs
    manifests
    reports

---

## 3. 本任務範圍

本任務處理：

    environment preflight
    repo sanity check
    raw ingestion for all intervals
    raw validation for all intervals
    parquet materialization for all intervals
    parquet validation for all intervals
    unit tests
    validation --all
    git safety check
    final rebuild report

本任務範圍外：

    code refactor
    schema redesign
    strategy development
    PostgreSQL serving layer
    live API
    trading system
    incremental update workflow
    GitHub push
    VERSION bump
    release commit

若遇到 repo / validator / CLI blocker：

    停在 blocker
    回報 evidence
    回報 smallest safe fix
    等待使用者決策

---

## 4. Rebuild 原則

重建資料應產生：

    local_data/binance_um_klines/interval=1d/
    local_data/binance_um_klines/interval=4h/
    local_data/binance_um_klines/interval=1h/
    local_data/binance_um_klines/interval=15m/
    local_data/binance_um_klines/interval=5m/
    local_data/binance_um_klines/interval=3m/
    local_data/binance_um_klines/interval=1m/

每個 interval 應包含：

    raw/
    manifests/
    reports/
    catalog/
    parquet/
    parquet/manifests/
    parquet/reports/

執行順序：

    1d
    4h
    1h
    15m
    5m
    3m
    1m

理由：

    從小資料量到大資料量
    先暴露環境問題
    1m 最重，最後處理

---

## 5. Existing local_data policy

依照 `local_data/` 狀態選擇路徑：

    local_data 不存在：
      fresh rebuild

    local_data 存在且為前次中斷結果：
      resume rebuild

    local_data 存在且看起來完整：
      回報目前狀態
      等待使用者選擇 verify existing / resume / rebuild to another path

    local_data 存在但狀態不明：
      先產生 inventory report
      等待使用者決策

Inventory report 建議包含：

    du -sh local_data 2>/dev/null || true
    find local_data/binance_um_klines -maxdepth 2 -type d 2>/dev/null | sort | head -100
    find local_data/binance_um_klines -name manifest.json 2>/dev/null | sort
    find local_data/binance_um_klines -name materialization_manifest.json 2>/dev/null | sort

---

## 6. Resource preflight

開始前檢查：

    df -h .
    df -i .
    git status --short
    git ls-files local_data | wc -l
    cat VERSION
    python -m datahub.materialization.binance_um_klines_parquet --help

最低建議資源：

    VM free disk >= 300G
    更安全 VM free disk >= 400G
    VirtualBox dynamic VDI 情境下，Host free disk >= 300G
    inode 使用率維持安全餘裕

停損條件：

    VM free disk < 120G
    inode free 過低
    No space left on device
    raw validation failed
    parquet validation failed
    checksum_failed_count > 0
    missing_count > 0
    failed_count > 0

停損時輸出：

    current interval
    current command
    error summary
    df -h .
    df -i .
    latest log path
    smallest safe next step

---

## 7. Preflight commands

進入 repo：

    cd ~/work/crypto-data-hub

確認 repo：

    git log --oneline -5
    cat VERSION
    git status --short
    git ls-files local_data | wc -l

確認 CLI：

    python -m datahub.ingestion.binance_um_klines --help
    python -m datahub.materialization.binance_um_klines_parquet --help
    python -m datahub.validation --help

確認資源：

    df -h .
    df -i .
    du -sh local_data 2>/dev/null || true

確認測試：

    python -m unittest discover tests

---

## 8. Raw ingestion

對每個 interval 執行 raw ingestion。

Intervals：

    1d
    4h
    1h
    15m
    5m
    3m
    1m

Command template：

    python -m datahub.ingestion.binance_um_klines \
      --interval <INTERVAL> \
      --all \
      --archive-source both \
      --workers 4 \
      --retries 5 \
      --timeout 60

執行規則：

    每個 interval 完成後立即 raw validation
    每個 interval 完成後檢查 manifest
    每個 interval 完成後檢查 disk
    任一 interval raw validation failed 時停在該 interval 並回報

Log 位置：

    local_data/rebuild_logs/

Log 檔名：

    rebuild_raw_<INTERVAL>_<UTC_TIMESTAMP>.log

---

## 9. Raw validation

每個 interval ingestion 完成後執行：

    python -m datahub.validation \
      --target binance-um-klines \
      --interval <INTERVAL> \
      --manifest local_data/binance_um_klines/interval=<INTERVAL>/manifests/manifest.json

Raw validation 通過標準：

    failed checks = 0
    warning checks = 0
    failed_count = 0
    checksum_failed_count = 0
    missing_count = 0
    downloaded_count == verified_count

Raw manifest：

    local_data/binance_um_klines/interval=<INTERVAL>/manifests/manifest.json

Raw report：

    local_data/binance_um_klines/interval=<INTERVAL>/reports/coverage_summary.json

---

## 10. Parquet materialization

全部 raw interval 完成並通過 raw validation 後，開始 Parquet materialization。

Intervals：

    1d
    4h
    1h
    15m
    5m
    3m
    1m

Command template：

    python -m datahub.materialization.binance_um_klines_parquet \
      --interval <INTERVAL> \
      --all \
      --workers 4 \
      --resume

執行規則：

    使用 --resume
    full output 必須為 FULL_OUTPUT
    raw manifest `symbol_count` 是該 interval rebuild symbol universe authority
    parquet `raw_discovered_symbol_count` 必須等於 raw manifest `symbol_count`
    parquet `symbol_count` 必須等於 `raw_discovered_symbol_count`
    未經使用者授權時，不額外執行 resume idempotency probe；final report 標記 SKIPPED_BY_USER_POLICY
    若使用者明確授權 resume idempotency，再額外重跑 --resume 檢查 row_count / file_count stable
    每個 interval 完成後立即 explicit parquet validation
    任一 interval parquet validation failed 時停在該 interval 並回報

Log 位置：

    local_data/rebuild_logs/

Log 檔名：

    rebuild_parquet_<INTERVAL>_<UTC_TIMESTAMP>.log

---

## 11. Parquet validation

每個 interval materialization 完成後執行：

    python -m datahub.validation \
      --target binance-um-klines-parquet \
      --interval <INTERVAL> \
      --manifest local_data/binance_um_klines/interval=<INTERVAL>/parquet/manifests/materialization_manifest.json

Parquet validation 通過標準：

    failed checks = 0
    output_scope = FULL_OUTPUT
    symbol_count == raw_discovered_symbol_count
    failed_symbol_count = 0
    generated_csv_file_count = 0
    DuckDB readable
    schema complete
    duplicate key count = 0
    required fields NULL count = 0
    OHLC pass
    time alignment pass
    date policy pass
    manifest count matches actual output
    parquet tree .csv count = 0

---

## 12. Expected minimum baselines and freshness policy

下列 minimum floor 來自 v0.13.0 完整資料層。

Binance archive 會隨時間增加新資料與新上市 symbol。實際 symbol_count、date_max、row_count 可高於 floor；高於 floor 為合法狀態。

Symbol universe authority：

    每次 rebuild 的 symbol universe authority 來自 raw manifest `symbol_count`
    parquet `raw_discovered_symbol_count` 必須等於 raw manifest `symbol_count`
    parquet `symbol_count` 必須等於 `raw_discovered_symbol_count`
    raw_discovered_symbol_count 可隨 Binance 新上市 symbol 成長
    例如 raw_discovered_symbol_count = 940 屬合法完整狀態

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

    date_max 高於 v0.13.0 floor = 合法狀態
    date_max 低於使用者指定 freshness target 但仍高於 v0.13.0 floor = PASS_WITH_WARNINGS 或在 final report 註記 freshness lag
    只有 date_max 低於 interval-specific v0.13.0 floor 才 FAIL

---

## 13. Final verification

全部 interval raw + parquet 完成後執行：

    python -m unittest discover tests

    python -m datahub.validation --all

    git status --short

    git ls-files local_data | wc -l

Final 通過標準：

    unit tests pass
    validation --all failed = 0
    git ls-files local_data = 0
    all intervals raw_discovered_symbol_count >= interval-specific v0.13.0 symbol floor
    all intervals symbol_count == raw_discovered_symbol_count
    all intervals row_count >= v0.13.0 row_count baseline
    all intervals raw date_max >= interval-specific v0.13.0 raw date_max floor
    all intervals parquet date_max >= interval-specific v0.13.0 parquet date_max floor
    working tree contains only expected rebuild artifacts under ignored local_data

允許：

    validation --all optional / conditional skipped checks
    date_max 低於使用者指定 freshness target 但仍高於 v0.13.0 floor：PASS_WITH_WARNINGS 或在 final report 註記 freshness lag
    resume idempotency 未經使用者授權而跳過：標記 SKIPPED_BY_USER_POLICY，不因此 FAIL

Fail 條件：

    failed checks > 0
    local_data tracked count > 0
    raw_discovered_symbol_count below interval-specific v0.13.0 symbol floor
    symbol_count != raw_discovered_symbol_count
    row_count below v0.13.0 baseline
    raw date_max below interval-specific v0.13.0 raw floor
    parquet date_max below interval-specific v0.13.0 parquet floor
    unexpected code/docs/test/registry changes
    VERSION changed
    dataset_registry changed
    docs changed

---

## 14. Git policy

本任務是資料重建任務。
預期 Git 結果：

    VERSION unchanged
    CHANGELOG.md unchanged
    DATA_CATALOG.md unchanged
    dataset_registry.json unchanged
    docs unchanged
    tests unchanged
    no commit created
    no push performed

預期產物：

    local_data/

local_data 應保持 gitignored。

---

## 15. Completion criteria

全部達成才算完成：

    repo VERSION >= v0.13.0
    materializer CLI supports 1d / 4h / 1h / 15m / 5m / 3m / 1m
    raw ingestion all intervals complete
    raw validation all intervals pass
    parquet materialization all intervals complete
    parquet validation all intervals pass
    output_scope all intervals = FULL_OUTPUT
    symbol_count all intervals == raw_discovered_symbol_count
    raw_discovered_symbol_count all intervals >= interval-specific v0.13.0 symbol floor
    failed_symbol_count all intervals = 0
    generated_csv_file_count all intervals = 0
    row_count all intervals >= v0.13.0 minimum baseline
    raw date_max all intervals >= interval-specific v0.13.0 raw date_max floor
    parquet date_max all intervals >= interval-specific v0.13.0 parquet date_max floor
    DuckDB readable all intervals
    validation --all failed = 0
    unit tests pass
    git tracked local_data count = 0
    code/docs/test/registry unchanged
    final report produced

---

## 16. Final report format

請用以下格式回報：

Summary
- PASS / FAIL / PASS_WITH_WARNINGS
- 核心結論

Environment
- repo path
- VERSION
- git HEAD
- disk free before
- disk free after
- inode status

Raw Rebuild
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
- raw validation result

Parquet Rebuild
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
- resume_idempotency_status
- parquet validation result

Validation
- unittest
- validation --all
- explicit raw validation per interval
- explicit parquet validation per interval

Git Safety
- git status
- local_data tracked count
- code/docs/test/registry changes

Problems
- problem
- evidence
- smallest safe fix

Final Decision
- ACCEPT
- REJECT
- ACCEPT_WITH_WARNINGS
