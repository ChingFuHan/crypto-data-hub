# task_v0.05.md

請先完整理解本專案目前狀態。

目前：

- Phase 0 已完成
- Repository Foundation 已建立
- Phase 1 已完成
- Data Governance Foundation 已建立
- Phase 2 已完成
- Universe Metadata Dataset Design 已建立
- Phase 3 已完成
- Validation Foundation 已建立
- `python -m datahub.validation --all` 可執行
- Universe Metadata validation rules 已具備 fixture-based validation
- dataset_id = `reference.universe.metadata`
- Universe Metadata 目前狀態為 draft
- repo version 目前為 v0.4.0
- registry_version 目前維持 v0.2.0

本次任務：

Phase 4

Universe Metadata Ingestion MVP

---

# Mission

建立 Universe Metadata Dataset 的第一版資料匯入流程。

本階段目標：

產生第一個可驗證的 Universe Metadata draft artifact。

本階段重點：

- source authority review
- public source ingestion
- raw source snapshot
- normalized Universe Metadata artifact
- provenance tracking
- checksum tracking
- validation integration
- registry / catalog / handoff update

本階段產出應能被 Phase 3 Validation Framework 驗證。

---

# Core Principles

所有決策遵守：

1. Maintainability
2. Reproducibility
3. Scalability
4. Data Quality
5. Automation

若有衝突：

依 ROOT.md 為準。

---

# Phase Objective

完成 Universe Metadata 的第一版 ingestion MVP。

本階段 primary path：

1. fetch Binance USDⓈ-M Futures current exchangeInfo
2. store raw snapshot
3. normalize current active universe artifact
4. generate manifest
5. validate artifact
6. update registry / catalog / handoff
7. provide offline deterministic verification

本階段資料目標：

- Binance USDⓈ-M Futures metadata
- current active / trading symbols
- explicit provenance for every source-derived field
- explicit coverage status
- deterministic offline re-run from raw snapshot

Historical symbol candidates may be explored only if public archive index discovery remains small, deterministic, and clearly marked as non-authoritative.

Announcement-based delist / rename / merge evidence collection is documentation-only in this phase unless implementation remains small and deterministic.

本階段結果：

- Universe Metadata dataset 仍維持 draft lifecycle status
- 產生 validated draft artifact
- 更新 registry / catalog 中的 validation 與 provenance 狀態
- 為後續 full historical universe reconstruction 奠定基礎

---

# Scope

本階段主要建立與更新：

- datahub/
- scripts/
- tests/
- docs/
- dataset_registry.json
- DATA_CATALOG.md
- DATA_CONTRACT.md
- AGENTS.md
- HANDOFF.md
- CHANGELOG.md
- VERSION

可新增資料目錄：

    data/

建議資料目錄：

    data/
    ├── raw/
    │   └── reference/
    │       └── universe_metadata/
    ├── reference/
    │   └── universe_metadata/
    └── manifests/
        └── reference/
            └── universe_metadata/

若採用不同資料目錄：

需在 HANDOFF.md 與 docs 中說明原因。

---

# Data Artifact Commit Policy

本階段可 commit 小型 reference artifacts，以支援 reproducible validation。

可 commit：

- small raw exchangeInfo snapshot
- normalized Universe Metadata draft artifact
- manifest
- checksum metadata
- minimal offline fixtures

本階段不處理大型 market data。

Large OHLCV / funding / open interest / order book data files 不屬於本階段範圍。

Committed data artifacts 應符合：

- small
- reviewable
- deterministic
- useful for offline validation

若 generated artifacts 超過合理 repo size：

- keep only minimal offline fixture
- document external artifact location strategy
- defer large artifact storage policy to future snapshot phase

---

# Source Policy

本階段可使用公開資料來源。

優先資料來源：

1. Binance official USDⓈ-M Futures exchangeInfo
2. Binance public data / Data Vision archive index
3. Binance official announcements only when needed for delist / rename / merge evidence

Source usage 原則：

- 使用 public data source
- 記錄 source URL 或 source identifier
- 記錄 retrieved_at UTC timestamp
- 記錄 raw response checksum
- 記錄 normalized artifact checksum
- 記錄欄位來源
- 記錄欄位信心等級

本階段 primary implementation 以 exchangeInfo current active universe 為主。

Archive index discovery 為 optional exploratory support。

Announcements 為 source authority review 與 future full historical reconstruction 的參考，不作為本階段主要 ingestion implementation。

不使用 private API key。

不使用 account-level endpoint。

不使用 trading endpoint。

---

# Coverage Policy

本階段 Universe Metadata artifact 必須明確標示 coverage。

至少支援 coverage concepts：

- `active_current`
- `historical_candidate`
- `unresolved`
- `not_supported_yet`

Coverage status 與 Universe Metadata row status 是不同概念。

Universe Metadata row status 仍遵守 DATA_CONTRACT.md 定義，例如：

- active
- delisted
- renamed
- merged

Coverage status 應使用獨立欄位、manifest-level coverage metadata、或 provenance metadata 表達。

`historical_candidate`、`unresolved`、`not_supported_yet` 不應直接寫入 row status，除非 DATA_CONTRACT.md 明確更新 status enum。

Current active symbols 可由 exchangeInfo 建立。

Historical candidates 可由 public archive index discovery 建立。

若某 symbol 只能從 archive index 發現：

- 可建立 candidate record 或 manifest coverage note
- row status 不應偽裝成 confirmed lifecycle status
- listed_at / delisted_at 不可用未證實推論偽裝成 confirmed value
- provenance 應標示為 archive-index-derived
- confidence level 應明確標示

---

# Dataset Lifecycle Target

本階段目標狀態：

    validated draft

Universe Metadata dataset lifecycle status 保持：

    draft

Artifact validation success does not automatically imply dataset lifecycle promotion.

若 ingestion artifact 通過 validation：

- 可更新 validation metadata
- 可更新 last_validated_at
- 可更新 checksum / provenance
- 可更新 quality validation status

contract_validated 應保持與目前 registry semantics 一致。

若更新 contract_validated：

- 說明 exact meaning
- 區分 contract validation 與 artifact validation
- 保持 dataset lifecycle status = draft
- 將決策寫入 HANDOFF.md

若 contract_validated 語義不明：

- 維持既有值
- 在 Open Questions 記錄

是否轉為 active：

留待 Phase 4 Review 後決定。

---

# Determinism And Idempotency Policy

Offline mode should be idempotent.

Repeated execution with the same raw snapshot should produce the same normalized artifact checksum.

Offline mode should not create git diffs when inputs are unchanged.

Repeated offline execution with the same raw snapshot should preserve:

- normalized artifact checksum
- manifest checksum
- committed artifact content

If `manifest.generated_at` or `manifest.regenerated_at` is needed:

- write it only when artifact content changes
- or record it in command output instead of committed manifest
- or derive it deterministically from source snapshot metadata where practical

Committed manifest fields should be deterministic from source snapshot metadata where practical.

Online fetch may create a new raw snapshot.

Normalization from an unchanged raw snapshot should be deterministic.

Generated timestamps should be limited to manifest generation metadata and should not alter row-level deterministic fields unless source data changes.

Required deterministic verification targets are:

    python -m datahub.ingestion.universe_metadata --offline --all
    python -m datahub.validation --all
    python -m unittest discover tests

Online command validates current source availability and creates a new raw snapshot.

Online command may fail due to network or source availability.

Online failure should be reported as source/network risk rather than deterministic test failure, when offline verification still passes.

---

# Raw Snapshot Policy

Raw snapshots should be immutable.

Recommended raw snapshot naming:

    data/raw/reference/universe_metadata/exchange_info_<retrieved_at_utc>_<sha256_prefix>.json

Naming requirements:

- include source type or source name
- include retrieved_at UTC timestamp or deterministic source timestamp
- include sha256 prefix
- avoid overwriting existing raw snapshots

A stable latest pointer may be created only if documented.

If a latest pointer is created:

- document whether it is committed
- document whether it is regenerated
- keep deterministic offline verification clean
- avoid unnecessary git diffs when underlying source content is unchanged

Raw snapshot overwrite behavior must be explicit.

If existing raw snapshot checksum matches fetched content:

- reuse existing snapshot where practical
- avoid duplicate files
- record reuse behavior in manifest or command output

If fetched content changes:

- create a new immutable raw snapshot
- generate a new manifest
- regenerate normalized artifact
- run validation

---

# Normalized Artifact Format

Normalized artifact format must match the input format accepted by Phase 3 Universe Metadata validator.

Before implementation:

- inspect existing validation fixtures
- inspect validator input assumptions
- align artifact format with existing validator expectations

Preferred artifact format:

    JSON array of Universe Metadata rows

Manifest and dataset-level metadata should live outside the normalized row artifact unless DATA_CONTRACT.md defines otherwise.

If artifact format differs from existing fixtures:

- update fixtures
- update validator
- update docs/validation_framework.md
- preserve backward compatibility where practical

---

# Deliverables

## 1. Source Authority Review

建立 Source Authority Review。

建議文件：

    docs/universe_metadata_sources.md

內容至少包含：

- approved sources
- source purpose
- source limitations
- field coverage
- source reliability
- refresh strategy
- known gaps
- unresolved source questions

至少評估：

- exchangeInfo 對 current trading universe 的適用性
- public archive index 對 historical symbol discovery 的適用性
- announcements 對 delist / rename / merge evidence 的適用性

本階段應明確標示：

- exchangeInfo is primary implementation source
- archive index is optional exploratory source
- announcements are documentation / future evidence source

---

## 2. Ingestion Architecture

建立 Universe Metadata ingestion architecture。

建議模組：

    datahub/ingestion/
    datahub/ingestion/universe_metadata.py

建議 CLI：

    python -m datahub.ingestion.universe_metadata

Ingestion package 應支援 module execution。

基本要求：

- `datahub/ingestion/__init__.py` 存在
- `datahub/ingestion/universe_metadata.py` 可執行
- CLI 可從 repo root 執行
- CLI return code 能正確傳回 shell
- CLI 支援 online fetch
- CLI 支援 offline mode 使用既有 raw snapshot

---

## 3. Source Fetching

建立 source fetching 流程。

至少支援：

    python -m datahub.ingestion.universe_metadata --fetch

Fetch 行為：

- 取得 current exchangeInfo
- 儲存 immutable raw response snapshot
- 記錄 retrieved_at UTC
- 記錄 source identifier
- 計算 sha256 checksum
- 建立 raw manifest
- 遵守 Raw Snapshot Policy

若實作 archive index discovery：

- 儲存 discovery result
- 記錄 discovery method
- 記錄 source coverage
- 記錄不確定性
- 標示 archive-derived data as non-authoritative unless verified

Fetch 需具備 fail-loud behavior。

---

## 4. Normalization

建立 Universe Metadata normalization。

輸入：

- raw exchangeInfo snapshot
- optional archive index discovery result

輸出：

    data/reference/universe_metadata/reference.universe.metadata.json

Normalized rows 應符合 Phase 2 Universe Metadata Contract。

Normalized artifact 應符合本文件的 Normalized Artifact Format。

至少映射或處理：

- instrument_id
- symbol
- exchange
- market_type
- contract_type
- status
- listed_at
- delisted_at
- tick_size
- step_size
- contract_size
- base_asset
- quote_asset
- successor_id

欄位來源缺失時：

- 遵守 DATA_CONTRACT.md 的 Null Policy
- 記錄 reason
- 記錄 source limitation
- 避免以猜測值填充 confirmed 欄位

Coverage information 應透過獨立 coverage metadata、manifest、或 provenance 表達。

Coverage information 不應污染 row status enum。

---

## 5. Deterministic Instrument ID

建立 deterministic instrument_id generation scheme。

要求：

- deterministic
- reproducible
- stable across repeated ingestion
- supports symbol-era model
- avoids plain symbol as primary key

建議評估：

- exchange
- market_type
- symbol
- listed_at / onboardDate
- source-specific lifecycle marker

若 source 不提供 reliable listed_at：

- 記錄 limitation
- 選擇 deterministic fallback
- 說明 fallback collision risk
- 保持 row-level ID stable across repeated ingestion

若使用 hash：

- 記錄 hash input
- 記錄 hash algorithm
- 記錄 collision handling

若使用 human-readable ID：

- 記錄 format
- 記錄 collision handling

決策寫入：

- docs/universe_metadata_sources.md
- HANDOFF.md

---

## 6. Provenance Model

建立 Universe Metadata provenance model。

每次 ingestion 至少產生：

    data/manifests/reference/universe_metadata/manifest.json

Manifest 至少包含：

- dataset_id
- dataset_version
- generated_at
- source_count
- row_count
- raw_sources
- normalized_artifact
- checksum
- validation_command
- validation_status
- known_coverage_gaps

每個 source entry 至少包含：

- source_name
- source_type
- source_identifier
- retrieved_at
- checksum
- record_count
- coverage_notes

Manifest 應明確區分：

- raw source metadata
- normalized artifact metadata
- validation metadata
- coverage metadata

Manifest should follow Determinism And Idempotency Policy.

Repeated offline generation with unchanged inputs should preserve committed manifest content where practical.

---

## 7. Validation Integration

整合 Phase 3 validation。

完成 ingestion 後必須執行：

    python -m datahub.validation --target universe-metadata --fixture data/reference/universe_metadata/reference.universe.metadata.json

以及：

    python -m datahub.validation --all

若目前 validation CLI 使用 `--fixture` 命名：

可沿用既有介面。

若需要新增 `--input` 或 `--artifact`：

需保持 backward compatibility 或更新 docs/validation_framework.md。

Validation result 應寫入 manifest 或 registry metadata。

Validation success 應區分：

- artifact validation success
- contract validation status
- dataset lifecycle status

---

## 8. Registry Update

更新 dataset_registry.json。

至少更新或評估：

- Universe Metadata artifact location
- provenance location
- checksum
- earliest_timestamp
- latest_timestamp
- quality validation status
- last_validated_at
- contract_validated

Registry update 原則：

- registry_version 維持 v0.2.0，若 registry contract shape 沒有實質變更
- repo version 更新為 v0.5.0
- dataset lifecycle status 維持 draft
- source coverage 明確記錄

若需要新增 registry field：

- 說明原因
- 評估 registry_version 是否需要更新
- 更新 docs/registry_standard.md
- 更新 HANDOFF.md

若 registry contract shape 沒有變更：

- registry_version 維持 v0.2.0
- 在 HANDOFF.md 記錄原因

contract_validated 若語義不明：

- 維持既有值
- 透過 quality validation metadata 表達 artifact validation result
- 在 Open Questions 記錄是否需要重新定義 contract_validated

---

## 9. Catalog Update

更新 DATA_CATALOG.md。

至少反映：

- Universe Metadata 已有 first validated draft artifact
- artifact path
- source coverage
- validation status
- known gaps
- lifecycle status remains draft
- registered dataset count remains consistent

Catalog 仍為 human-readable derived view。

若 registry 與 catalog 不一致：

以 registry 為準，並修正 catalog。

---

## 10. Contract Update

檢查 DATA_CONTRACT.md 是否足以支援實際 ingestion。

至少評估：

- field definitions 是否足夠
- Null Policy 是否足夠
- status enum 是否足夠
- source coverage 是否需要表達
- provenance reference 是否需要補充
- contract_size 無明確 source 時如何處理

若 contract 需要修正：

- 最小修改
- 記錄原因
- 更新 docs/universe_metadata_dataset.md
- 更新 HANDOFF.md

若需要表達 coverage：

- 優先透過 manifest / provenance / registry quality metadata 表達
- 避免直接擴充 row status enum，除非有明確治理理由

---

## 11. Tests

建立 ingestion tests。

至少包含：

- deterministic instrument_id test
- normalization from raw fixture test
- manifest generation test
- checksum generation test
- validation integration test
- offline mode test
- idempotent offline rerun test
- raw snapshot naming test
- raw snapshot reuse test

建議 fixtures：

    tests/fixtures/ingestion/universe_metadata/

至少包含：

- minimal_exchange_info.json
- exchange_info_with_multiple_symbols.json
- exchange_info_missing_optional_fields.json
- archive_index_candidates.json

測試需使用 Python standard library。

若新增外部依賴：

- 記錄原因
- 更新 QUICKSTART.md
- 更新 docs

---

## 12. CLI Commands

建立或記錄以下 command。

至少支援：

    python -m datahub.ingestion.universe_metadata --fetch
    python -m datahub.ingestion.universe_metadata --normalize
    python -m datahub.ingestion.universe_metadata --all
    python -m datahub.ingestion.universe_metadata --offline --all

若採用不同 CLI：

需在 docs 中記錄。

CLI exit code：

    0 = success
    1 = ingestion or validation failed
    2 = invalid command or configuration error

Online command:

    python -m datahub.ingestion.universe_metadata --all

Offline deterministic command:

    python -m datahub.ingestion.universe_metadata --offline --all

Offline deterministic command must pass using committed raw snapshot or committed offline fixture.

---

## 13. Documentation

建立或更新：

- docs/universe_metadata_sources.md
- docs/universe_metadata_dataset.md
- docs/validation_framework.md
- QUICKSTART.md

內容至少包含：

- how to fetch source
- how to normalize
- how to validate artifact
- how to run offline
- source limitations
- coverage limitations
- artifact format
- raw snapshot policy
- data artifact commit policy
- idempotency policy
- final verification policy
- known gaps
- next steps

---

## 14. Repository Version Update

更新：

    VERSION

版本：

    v0.5.0

更新：

    CHANGELOG.md

記錄：

- Phase 4 Universe Metadata Ingestion MVP
- source authority review
- ingestion CLI
- raw snapshot
- normalized artifact
- manifest / checksum
- validation integration
- known gaps

---

## 15. Repository State Update

完成本階段後：

更新：

- AGENTS.md
- HANDOFF.md

反映：

- Current Phase
- Current Status
- Ingestion Decisions
- Source Decisions
- Artifact Locations
- Validation Result
- Known Coverage Gaps
- Open Questions
- Recommended Next Phase

HANDOFF.md 應記錄：

- ingestion architecture decision
- source authority decision
- instrument_id decision
- artifact format decision
- manifest decision
- registry update decision
- draft lifecycle decision
- coverage status decision
- idempotency decision
- raw snapshot decision
- data artifact commit decision
- final verification decision
- recommended Phase 5

---

# Completion Criteria

以下全部成立：

✓ Source Authority Review Completed

✓ Ingestion Architecture Implemented

✓ Ingestion CLI Created

✓ Raw Source Snapshot Created

✓ Raw Snapshot Policy Implemented

✓ Source Manifest Created

✓ Normalized Universe Metadata Artifact Created

✓ Normalized Artifact Format Aligned With Validator

✓ Deterministic Instrument ID Implemented

✓ Provenance Model Implemented

✓ Checksum Implemented

✓ Row Status And Coverage Status Kept Separate

✓ Universe Metadata Artifact Validates

✓ Registry Updated

✓ Catalog Updated

✓ Contract Reviewed

✓ Tests Added

✓ Offline Mode Works

✓ Offline Mode Is Idempotent

✓ Offline Re-run Does Not Create Unnecessary Git Diffs

✓ Data Artifact Commit Policy Documented

✓ Final Verification Policy Documented

✓ Documentation Updated

✓ VERSION Updated To v0.5.0

✓ CHANGELOG.md Updated

✓ AGENTS.md Updated

✓ HANDOFF.md Updated

✓ `python -m datahub.validation --all` Passes

✓ `python -m unittest discover tests` Passes

✓ Review Package Produced

---

# Required Verification Commands

Online command may be executed before final artifact freeze:

    python -m datahub.ingestion.universe_metadata --all

完成後必須執行 deterministic verification：

    python -m datahub.ingestion.universe_metadata --offline --all
    python -m datahub.validation --all
    python -m unittest discover tests

Final pre-commit verification should use deterministic offline commands.

After final artifact selection:

- run offline deterministic verification
- run validation
- run tests
- ensure git status contains only intended commit changes

The final committed state should be reproducible by offline verification.

若 online source 暫時不可用：

- 使用 offline snapshot 完成 deterministic verification
- 在 Risks 中記錄原因
- 保持 validation 與 tests 可通過

若 online command 成功：

- 記錄 source retrieval result
- 記錄 generated raw snapshot
- 記錄 normalized artifact summary
- 記錄 checksum
- 保持 offline deterministic verification 可重跑

---

# Output Requirement

完成後輸出：

## Completed

## Decisions

## Risks

## Open Questions

## Ingestion Result

## Validation Result

## Artifact Summary

## Recommended Phase 5

---

# Commit Requirement

完成後建立單一 commit。

建議 commit message：

    feat: Phase 4 — Universe Metadata Ingestion MVP (v0.5.0)

---

# Review Gate

Phase 4 完成後：

提交 Review Package。

等待 Review。

下一階段將於 Review 完成後定義。
