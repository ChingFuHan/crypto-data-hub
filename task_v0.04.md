# task_v0.04.md

請先完整理解本專案目前狀態。

目前：

- Phase 0 已完成
- Repository Foundation 已建立
- Phase 1 已完成
- Data Governance Foundation 已建立
- Phase 2 已完成
- Universe Metadata Dataset Design 已建立
- dataset_registry.json 已包含第一個 draft dataset entry
- DATA_CONTRACT.md 已包含 Universe Metadata Contract
- DATA_CATALOG.md 已包含 Universe Metadata Catalog Entry
- Universe Metadata dataset_id = reference.universe.metadata
- Universe Metadata 目前狀態為 draft
- repo version 目前為 v0.3.0

Phase 2 Review 發現：

- Governance 文件可定義規則
- Schema / Contract 可定義欄位形狀
- cross-field invariant 與 graph invariant 需要可執行驗證

本次任務：

Phase 3

Validation Foundation

---

# Mission

建立 Data Hub 的第一版可執行驗證基礎。

本階段目標：

將既有 Governance Rules 與 Universe Metadata Dataset Design 中的核心規則，轉化為可重複執行的 Validation Framework。

重點是建立：

- validation architecture
- validation result model
- validation CLI
- registry validation
- lifecycle validation
- naming validation
- Universe Metadata fixture-based validation
- first validation test skeleton

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

建立第一版 Validation Foundation。

驗證對象包含：

- dataset_registry.json
- Dataset Entry Schema
- Dataset Lifecycle Rules
- Naming Convention Rules
- Universe Metadata Contract
- Universe Metadata Quality Rules

本階段應讓 Data Hub 開始具備：

- machine-checkable governance
- repeatable validation command
- deterministic local validation
- fail-loud validation behavior
- clear validation report
- testable validation rules

驗證流程以 repo 內檔案與 test fixtures 為主要輸入。

資料匯入與外部資料下載作為 Phase 4 候選任務。

---

# Scope

本階段主要建立與更新：

- datahub/
- scripts/
- tests/
- docs/
- AGENTS.md
- HANDOFF.md
- CHANGELOG.md
- VERSION

可視需要更新：

- DATA_CONTRACT.md
- dataset_registry.json
- DATA_CATALOG.md

以補齊驗證所需的明確規則。

若 dataset_registry.json 的 registry contract 有實質變更：

- 評估 registry_version 是否需要更新
- 將原因寫入 HANDOFF.md

若僅新增 validation tooling：

- repo version 更新為 v0.4.0
- registry_version 可維持既有版本

---

# Deliverables

## 1. Validation Architecture

建立 Validation Framework 架構。

內容包含：

- validation module layout
- validation entry point
- validation result format
- validation error model
- validation report format
- validation rule organization

建議位置：

    datahub/validation/

Validation package 應支援 module execution。

主要執行入口：

    python -m datahub.validation

建議結構：

    datahub/
    ├── __init__.py
    └── validation/
        ├── __init__.py
        ├── __main__.py
        ├── cli.py
        ├── result.py
        ├── errors.py
        ├── registry.py
        ├── lifecycle.py
        ├── naming.py
        └── universe_metadata.py

Module execution 的基本要求：

- `datahub/__init__.py` 存在
- `datahub/validation/__init__.py` 存在
- `datahub/validation/__main__.py` 存在
- `python -m datahub.validation` 可從 repo root 執行
- module entry point 呼叫 validation CLI
- CLI return code 能正確傳回 shell

如採用不同結構：

需在 docs/validation_framework.md 說明原因。

---

## 2. Validation Result Model

建立標準 Validation Result Model。

至少包含：

- rule_id
- severity
- status
- message
- file
- dataset_id
- field
- location
- details

Severity 至少支援：

- error
- warning
- info

Status 至少支援：

- passed
- failed
- skipped

Validation Report 應能清楚呈現：

- total checks
- passed checks
- failed checks
- warning checks
- affected files
- affected dataset_id
- error summary

---

## 3. Registry Validation

建立 dataset_registry.json 驗證。

至少涵蓋：

- JSON validity
- registry_version format
- conventions presence
- dataset_entry_schema presence
- datasets array presence
- dataset_id naming pattern
- dataset version format
- required fields
- status enum
- owner format
- source format
- timezone format
- update_frequency format
- primary_key format
- schema_ref format
- created_at format
- updated_at format
- created_at <= updated_at

Registry validation 應以目前 registry standard 為依據。

若 registry standard 與實際 dataset_registry.json 存在差異：

- 記錄差異
- 補齊最小必要規則
- 將決策寫入 HANDOFF.md

---

## 4. Lifecycle Validation

建立 Dataset Lifecycle 驗證。

至少涵蓋：

- valid lifecycle state
- valid lifecycle transition rule
- draft dataset requirements
- active dataset requirements
- deprecated dataset requirements
- archived dataset requirements

Draft dataset validation 至少檢查：

- contract exists
- catalog entry exists
- metadata exists
- status = draft
- contract_validated field exists when applicable

Active dataset validation 至少設計檢查邏輯：

- contract_validated = true
- data location exists
- validation status available
- provenance available
- earliest_timestamp / latest_timestamp available when applicable

本階段以 draft dataset 作為主要可執行驗證案例。

active / deprecated / archived 可先建立 rule skeleton 與 fixture-based tests。

---

## 5. Naming Convention Validation

建立 dataset-related naming validation。

至少涵蓋：

- dataset_id naming
- dataset version naming
- registry version naming
- dataset file path naming
- metadata reference naming
- timestamp string format

命名檢查聚焦於 dataset-related artifacts。

例如：

- dataset_registry.json entries
- dataset_id
- schema_ref
- dataset path
- snapshot path
- metadata path

---

## 6. Universe Metadata Validation Rules

建立 Universe Metadata 專屬驗證規則。

至少涵蓋 Phase 2 定義的 Q1–Q6。

包含：

- required fields
- primary key uniqueness
- instrument_id uniqueness
- valid status enum
- listed_at timestamp validity
- delisted_at timestamp validity
- listed_at <= delisted_at when delisted_at exists
- active / delisted invariant
- renamed / merged successor_id requirement
- successor_id reference validity
- acyclic successor graph
- symbol-era point-in-time reconstruction requirement

Universe Metadata validation 應支援 fixture-based validation。

本階段以 test fixtures 驗證規則可執行。

---

## 7. Test Fixtures

建立 minimal validation fixtures。

建議位置：

    tests/fixtures/universe_metadata/

至少包含：

    valid_universe_metadata.json
    duplicate_instrument_id.json
    invalid_timestamp_order.json
    invalid_active_delisted_invariant.json
    invalid_successor_reference.json
    cyclic_successor_graph.json
    broken_point_in_time_reconstruction.json

Fixtures 應足以驗證：

- valid case passes
- invalid cases fail with expected rule_id
- graph invariant can detect cycles
- PIT reconstruction rule can detect broken symbol-era model

---

## 8. Validation CLI

建立可重複執行的驗證入口。

主要形式：

    python -m datahub.validation

CLI 應輸出：

- passed checks
- failed checks
- warning checks
- affected file
- affected dataset_id
- rule_id
- clear error message

CLI 應支援 exit code：

    0 = all checks passed
    1 = validation failed
    2 = validation command error / invalid invocation

建議支援：

    python -m datahub.validation --target registry
    python -m datahub.validation --target universe-metadata --fixture tests/fixtures/universe_metadata/valid_universe_metadata.json
    python -m datahub.validation --all

若實作不同 CLI 介面：

需在 docs/validation_framework.md 記錄。

---

## 9. Test Skeleton

建立第一版測試結構。

建議位置：

    tests/

至少包含：

- registry validation test
- lifecycle validation test
- naming validation test
- universe metadata validation test
- CLI exit code test

測試應覆蓋：

- valid registry passes
- invalid registry fails
- valid Universe Metadata fixture passes
- invalid Universe Metadata fixtures fail
- CLI success returns exit code 0
- CLI validation failure returns exit code 1

以 Python standard library 為預設。

若新增外部測試依賴：

- 記錄原因
- 更新 QUICKSTART.md 或 docs/validation_framework.md

---

## 10. Documentation

建立或更新：

    docs/validation_framework.md

內容至少包含：

- Validation Scope
- Validation Architecture
- Validation Result Model
- Validation Error Model
- How to Run Validation
- CLI Usage
- Exit Codes
- Test Fixtures
- Current Coverage
- Known Gaps
- Future Work

---

## 11. Repository Version Update

更新：

    VERSION

版本：

    v0.4.0

更新：

    CHANGELOG.md

記錄：

- Phase 3 Validation Foundation
- validation architecture
- validation CLI
- registry validation
- lifecycle validation
- naming validation
- Universe Metadata validation fixtures
- known gaps

---

## 12. Repository State Update

完成本階段後：

更新：

- AGENTS.md
- HANDOFF.md

反映：

- Current Phase
- Current Status
- Validation Decisions
- Validation Coverage
- Known Gaps
- Open Questions
- Recommended Next Phase

HANDOFF.md 應記錄：

- validation architecture decision
- CLI decision
- fixture strategy
- rule coverage
- remaining validation gaps
- recommended Phase 4

---

# Completion Criteria

以下全部成立：

✓ Validation Architecture Defined

✓ Validation Result Model Defined

✓ Registry Validation Implemented

✓ Lifecycle Validation Implemented

✓ Naming Convention Validation Implemented

✓ Universe Metadata Validation Rules Implemented For Fixtures

✓ Test Fixtures Created

✓ Validation CLI Created

✓ Module Execution Works With `python -m datahub.validation`

✓ CLI Exit Codes Implemented

✓ Test Skeleton Created

✓ Validation Documentation Updated

✓ VERSION Updated To v0.4.0

✓ CHANGELOG.md Updated

✓ AGENTS.md Updated

✓ HANDOFF.md Updated

✓ Validation Command Runs Successfully

✓ Valid Fixtures Pass

✓ Invalid Fixtures Fail With Expected Rule IDs

✓ Review Package Produced

---

# Output Requirement

完成後輸出：

## Completed

## Decisions

## Risks

## Open Questions

## Validation Result

## Recommended Phase 4

---

# Commit Requirement

完成後建立單一 commit。

建議 commit message：

    feat: Phase 3 — Validation Foundation (v0.4.0)

---

# Review Gate

Phase 3 完成後：

提交 Review Package。

等待 Review。

下一階段將於 Review 完成後定義。
