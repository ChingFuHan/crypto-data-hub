# task_v0.06.md

請先完整理解本專案目前狀態。

目前：

- Phase 0~4 已完成
- repo version = v0.5.0
- Universe Metadata Ingestion MVP 已完成
- Validation Foundation 已完成
- `python -m datahub.validation --all` 可執行
- `python -m datahub.ingestion.universe_metadata --offline --all` 可執行
- local large market data 尚未建立

本次任務：

Phase 5

Binance USD-M Futures Kline Historical Pipeline

First production interval:

    1d

---

# Mission

建立 Binance USD-M Futures Kline 的參數化歷史資料 pipeline。

本階段先用：

    interval = 1d

跑全歷史 / 全品種。

但 pipeline 必須支援：

- 1d
- 4h
- 1h
- 15m
- 5m
- 1m

也就是：

    1d 是第一個正式執行週期
    script 不可寫死 1d

---

# Important Terminology

請明確區分：

## Kline interval

交易資料週期：

- 1d
- 4h
- 1h
- 15m
- 5m
- 1m

## Archive package source

Binance Data Vision 的檔案打包方式：

- monthly archive package
- daily archive package

monthly / daily 在本任務中不是 Kline interval。

Archive path pattern：

    data/futures/um/monthly/klines/<SYMBOL>/<INTERVAL>/
    data/futures/um/daily/klines/<SYMBOL>/<INTERVAL>/

---

# Scope

建立一套可重複執行的 Kline ingestion pipeline。

本階段包含：

- archive discovery
- full historical symbol discovery
- zip + CHECKSUM download
- checksum verification
- resume / skip verified files
- local_data storage
- manifest
- coverage report
- missing file report
- validation integration
- research-agent access metadata
- tests
- docs

大型 market data 只能放在：

    local_data/

不可 commit full market data。

---

# Dataset

Base dataset_id：

    market.binance.um.klines

First interval variant：

    market.binance.um.klines.1d

Primary key：

    symbol + interval + open_time

Reason：

    Kline rows are uniquely identified by open time for a given symbol and interval.

Dataset lifecycle status：

    draft

---

# Source Policy

主要來源：

    Binance Data Vision public archive

資料範圍：

- Binance USD-M Futures
- Kline
- interval parameterized
- first interval = 1d
- all symbols discoverable from archive index
- all available historical monthly archive package files
- daily archive package only as recent delta by default

Source priority：

1. monthly archive package as historical base
2. daily archive package as recent delta
3. Universe Metadata only as reference / cross-check

Universe Metadata 不可作為唯一 symbol source，因為 current active universe 會漏掉 delisted symbols。

Private API key、account endpoint、trading endpoint 不屬於本階段。

---

# Daily Archive Scope Policy

Daily archive package 是 recent delta，不是 historical base。

Default behavior：

- download monthly archive package as historical base
- download daily archive package only for dates not covered by available monthly archive packages
- avoid downloading full historical daily archive when monthly archive already covers the same period

Full historical daily download requires explicit flag：

    --include-full-daily-history

If `--include-full-daily-history` is used：

- record decision in manifest
- record extra file count
- record expected duplicate / overlap risk
- preserve monthly archive package as canonical historical coverage
- avoid duplicate normalized rows if materialization exists

Default run for interval=1d should not download full historical daily archive when monthly archive package is available.

---

# Storage Policy

大型資料放：

    local_data/binance_um_klines/interval=<INTERVAL>/

First interval：

    local_data/binance_um_klines/interval=1d/

建議 layout：

    local_data/binance_um_klines/interval=<INTERVAL>/
    ├── raw/
    │   ├── monthly/
    │   └── daily/
    ├── checksums/
    ├── manifests/
    ├── reports/
    ├── catalog/
    └── tmp/

Repo 必須確認 `.gitignore` 包含：

    local_data/

Commit 僅包含：

- code
- docs
- tests
- fixtures
- small sample metadata
- registry/catalog changes if needed

不可 commit full historical Kline data。

Before commit：

- run `git status --short`
- confirm local_data files are not staged
- confirm local_data files do not appear as untracked files
- if local_data appears in git status, stop and fix `.gitignore`

---

# CLI Requirements

建立 module：

    datahub/ingestion/binance_um_klines.py

主要命令：

    python -m datahub.ingestion.binance_um_klines --interval 1d --all

必須支援：

    --interval <INTERVAL>
    --discover
    --download
    --verify
    --report
    --all
    --resume
    --dry-run
    --workers <N>
    --timeout <SECONDS>
    --retries <N>
    --archive-source monthly|daily|both
    --symbols-file <PATH>
    --max-symbols <N>
    --local-root <PATH>
    --include-full-daily-history

Allowed intervals 集中定義：

- 1d
- 4h
- 1h
- 15m
- 5m
- 1m

Unsupported interval：

- fail loud
- exit code 2
- print allowed intervals

---

# Discovery Requirements

Discovery 必須支援：

- selected interval
- first interval = 1d
- monthly archive package
- daily archive package
- all symbols discoverable from archive index

Discovery output：

    local_data/binance_um_klines/interval=<INTERVAL>/catalog/archive_files.jsonl
    local_data/binance_um_klines/interval=<INTERVAL>/catalog/symbols.json
    local_data/binance_um_klines/interval=<INTERVAL>/catalog/discovery_summary.json

Each archive file record 至少包含：

- symbol
- interval
- archive_package_source
- archive_period
- zip_name
- checksum_name
- source_path
- local_zip_path
- local_checksum_path
- discovered_at

Discovery should mark whether each daily archive file is:

- required_delta
- covered_by_monthly
- skipped_by_default
- included_by_explicit_full_daily_history

---

# Download Requirements

Downloader 必須：

- download zip
- download `.CHECKSUM`
- verify checksum
- skip already verified files
- support resume
- retry transient network errors
- record failures
- avoid duplicate downloads
- avoid full historical daily download by default when monthly archive covers the period

Checksum status 至少包含：

- passed
- failed
- missing_checksum
- missing_zip
- skipped_existing_verified
- download_failed
- not_attempted

Checksum mismatch = error。

---

# Monthly / Daily Archive Policy

monthly / daily 指 archive package source，不是 Kline interval。

Monthly archive package 是 historical base。

Daily archive package 是 recent delta。

若 monthly 與 daily overlap：

- record overlap
- prefer monthly package for canonical historical coverage
- daily archive may be skipped by default if already covered by monthly
- avoid duplicate normalized rows by primary key if materialization exists

本階段可 focus on raw archive inventory + checksum verification。

Full Parquet materialization 可延後到 Phase 6。

---

# Manifest Requirements

Main manifest：

    local_data/binance_um_klines/interval=<INTERVAL>/manifests/manifest.json

Per-file manifest：

    local_data/binance_um_klines/interval=<INTERVAL>/manifests/files.jsonl

Research access manifest：

    local_data/binance_um_klines/interval=<INTERVAL>/catalog/research_access.json

Main manifest 至少包含：

- dataset_id
- dataset_variant_id
- interval
- local_root
- run_id
- symbol_count
- file_count
- downloaded_count
- verified_count
- skipped_count
- failed_count
- checksum_failed_count
- missing_count
- total_bytes
- date_min
- date_max
- archive_package_sources
- include_full_daily_history
- daily_delta_policy
- validation_summary

Per-file manifest 至少包含：

- symbol
- interval
- archive_package_source
- archive_period
- zip_name
- checksum_name
- source_path
- local_zip_path
- local_checksum_path
- checksum_status
- download_status
- file_size
- retry_count
- skip_reason

---

# Coverage Report

建立：

    local_data/binance_um_klines/interval=<INTERVAL>/reports/coverage_summary.json
    local_data/binance_um_klines/interval=<INTERVAL>/reports/missing_files.jsonl
    local_data/binance_um_klines/interval=<INTERVAL>/reports/checksum_failures.jsonl
    local_data/binance_um_klines/interval=<INTERVAL>/reports/run_summary.md

Coverage summary 至少包含：

- interval
- discovered_symbol_count
- monthly_archive_symbol_count
- daily_archive_symbol_count
- total_archive_file_count
- verified_file_count
- failed_file_count
- checksum_failed_count
- missing_count
- skipped_daily_overlap_count
- date_min
- date_max
- known_gaps

---

# Validation Integration

新增 validation target：

    binance-um-klines

建議命令：

    python -m datahub.validation --target binance-um-klines --interval 1d --manifest local_data/binance_um_klines/interval=1d/manifests/manifest.json

Validation 至少檢查：

- manifest exists
- file manifest exists
- interval supported
- manifest interval matches CLI interval
- dataset_id = market.binance.um.klines
- dataset_variant_id = market.binance.um.klines.<INTERVAL>
- checksum_failed_count = 0
- required files exist
- coverage report exists
- research access manifest exists
- local_data excluded from Git
- primary key documented
- daily archive policy recorded

Full row-level validation 可作為 optional deep mode。

Clone-safe rule：

    python -m datahub.validation --all

must remain repo-safe and clone-safe.

`--all` should not require local_data to exist.

Large local_data validation should run only when manifest is explicitly provided.

If target `binance-um-klines` is invoked without `--manifest`：

- return exit code 2
- or skip with clear message when called through clone-safe `--all`

Fresh clone repo should still pass：

    python -m datahub.validation --all
    python -m unittest discover tests

---

# Research Agent Access

建立文件：

    docs/research_agent_klines_access.md

內容至少包含：

- dataset_id
- interval variant
- local_data path
- manifest path
- file manifest path
- raw layout
- schema
- primary key
- how to locate files by symbol
- how to locate files by interval
- how to check coverage
- how to check checksum status
- warning that local_data is not committed

Research-agent-ready 定義：

- Agent can locate verified raw Kline archives
- Agent can inspect coverage / missing files
- Agent can distinguish Kline interval from archive package source
- Agent does not assume current active universe equals historical universe

Provide a minimal read example：

- read one symbol
- read one archive zip / csv
- print first rows
- explain expected Kline columns

Full Parquet materialization remains Phase 6 candidate.

---

# Tests

建立 tests / fixtures。

Fixtures 建議位置：

    tests/fixtures/ingestion/binance_um_klines/

Tests 至少涵蓋：

- interval parameter handling
- supported intervals include 1d / 4h / 1h / 15m / 5m / 1m
- unsupported interval failure
- archive discovery parsing
- symbol discovery
- local path generation with interval
- checksum verification
- skip existing verified file
- resume rerun skips verified files
- manifest generation
- coverage report generation
- research access manifest generation
- validation integration
- clone-safe `python -m datahub.validation --all`
- validation target requiring explicit manifest for local_data validation
- `.gitignore` contains local_data
- daily archive recent-delta behavior
- `--include-full-daily-history` behavior

優先使用 Python standard library。

若新增外部依賴：

- document reason
- update QUICKSTART.md

---

# Documentation

建立或更新：

- docs/binance_um_klines_dataset.md
- docs/research_agent_klines_access.md
- docs/market_data_storage_policy.md
- docs/validation_framework.md
- QUICKSTART.md
- README.md if needed

文件需說明：

- source authority
- archive paths
- Kline interval vs archive package source
- supported intervals: 1d / 4h / 1h / 15m / 5m / 1m
- first production interval = 1d
- local_data policy
- daily recent-delta policy
- `--include-full-daily-history`
- download commands
- resume behavior
- checksum verification
- manifest format
- coverage report
- research agent access
- minimal read example
- known gaps

---

# Registry / Catalog Policy

可註冊 dataset family：

    market.binance.um.klines

或 interval-specific variant：

    market.binance.um.klines.1d

若 registry 現有 schema 不適合紀錄 machine-specific local_data 狀態：

- 不要硬改 registry
- 記錄 gap
- 在 HANDOFF.md 說明 decision

Registry 不應存 machine-specific full local_data run checksum，除非 registry contract 明確支援。

DATA_CATALOG.md 可新增 human-readable draft entry。

---

# Repository State Update

更新：

- VERSION → v0.6.0
- CHANGELOG.md
- AGENTS.md
- HANDOFF.md

HANDOFF.md 必須記錄：

- base dataset_id decision
- interval variant decision
- supported intervals decision
- source authority decision
- Kline interval vs archive package source decision
- daily recent-delta decision
- local_data decision
- no-large-data-commit decision
- clone-safe validation decision
- manifest decision
- research access decision
- registry/catalog decision
- recommended Phase 6

---

# Completion Criteria

以下全部成立：

✓ Parameterized Kline Pipeline Implemented

✓ Supported Intervals Include 1d / 4h / 1h / 15m / 5m / 1m

✓ First Production Interval = 1d

✓ Base Dataset ID Defined

✓ Interval Variant Defined

✓ Source Policy Documented

✓ Daily Archive Recent-Delta Policy Implemented

✓ Storage Policy Documented

✓ local_data Git Ignore Confirmed

✓ Archive Discovery Implemented

✓ Full Historical Symbol Discovery Implemented For interval=1d

✓ Monthly Archive Package Discovery Implemented

✓ Daily Archive Package Discovery Implemented

✓ Downloader Implemented

✓ Resume / Skip Verified Files Implemented

✓ Resume Rerun Skips Verified Files

✓ Zip + CHECKSUM Download Implemented

✓ Checksum Verification Implemented

✓ Main Manifest Generated

✓ Per-file Manifest Generated

✓ Coverage Report Generated

✓ Missing File Report Generated

✓ Research Access Manifest Generated

✓ Minimal Read Example Documented

✓ Validation Integration Implemented

✓ `python -m datahub.validation --all` Remains Clone-Safe

✓ Tests Added

✓ Documentation Updated

✓ VERSION Updated To v0.6.0

✓ CHANGELOG.md Updated

✓ AGENTS.md Updated

✓ HANDOFF.md Updated

✓ Full local Kline archive command documented

✓ Commit Contains Code / Docs / Tests Only

✓ local_data Not Staged Or Committed

✓ `python -m datahub.validation --all` Passes

✓ `python -m unittest discover tests` Passes

✓ Review Package Produced

---

# Required Verification Commands

完成後必須執行：

    python -m datahub.ingestion.binance_um_klines --interval 1d --discover
    python -m datahub.ingestion.binance_um_klines --interval 1d --dry-run
    python -m datahub.ingestion.binance_um_klines --interval 1d --all
    python -m datahub.ingestion.binance_um_klines --interval 1d --resume --all
    python -m datahub.validation --target binance-um-klines --interval 1d --manifest local_data/binance_um_klines/interval=1d/manifests/manifest.json
    python -m datahub.validation --all
    python -m unittest discover tests
    git status --short
    git diff --stat

Expected behavior：

- first `--all` downloads / verifies required files
- second `--resume --all` skips verified files
- checksum_failed_count = 0
- local_data files do not appear in git status
- validation --all remains clone-safe
- unittest passes

If full download is time-consuming:

- run discovery first
- run dry-run
- then run full download
- report elapsed time and progress
- preserve resume support

For final review, report:

- interval
- discovered_symbol_count
- total_archive_file_count
- downloaded_count
- verified_count
- skipped_count
- failed_count
- checksum_failed_count
- missing_count
- skipped_daily_overlap_count
- total_bytes
- date_min
- date_max
- local_data path
- manifest path

---

# Output Requirement

完成後輸出：

## Completed

## Decisions

## Risks

## Open Questions

## Download Result

## Validation Result

## Coverage Summary

## Local Data Summary

## Recommended Phase 6

---

# Commit Requirement

完成後建立單一 commit。

建議 commit message：

    feat: Phase 5 — Binance USD-M Kline Historical Pipeline (v0.6.0)

Commit should include code/docs/tests only.

Full local_data market data should remain uncommitted.

Before commit：

    git status --short

確認：

- local_data not staged
- local_data not untracked
- only intended code/docs/tests/metadata files are included

---

# Review Gate

Phase 5 完成後：

提交 Review Package。

等待 Review。

下一階段將於 Review 完成後定義。
