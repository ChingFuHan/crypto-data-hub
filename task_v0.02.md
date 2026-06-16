# task_v0.02.md

請先完整理解本專案目前狀態。

目前：

- Phase 0 已完成
- Repository Foundation 已建立
- ROOT.md 已建立
- AGENTS.md 已建立
- HANDOFF.md 已建立
- Repo Structure 已建立

本次任務：

Phase 1

Data Governance Foundation

---

# Mission

建立 Data Hub 的資料治理基礎規範。

本階段目標為建立未來所有 Dataset 共用的治理模型。

包含：

- Dataset Lifecycle
- Dataset Metadata
- Dataset Registry
- Dataset Contract
- Data Catalog

所有後續 Dataset 應遵循本階段建立之規範。

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

建立：

- Dataset Lifecycle Model
- Dataset Metadata Standard
- Dataset Registry Standard
- Authority Model
- Naming Convention Standard
- Dataset Contract Framework
- Data Catalog Framework

形成統一治理架構。

---

# Scope

本階段主要更新：

- DATA_CONTRACT.md
- DATA_CATALOG.md
- dataset_registry.json
- docs/

以及相關治理文件。

---

# Deliverables

## 1. Dataset Lifecycle Model

建立 Dataset 狀態模型。

定義：

- Lifecycle States
- State Transition Rules
- State Management Principles

---

## 2. Dataset Metadata Standard

建立統一 Metadata 規格。

至少涵蓋：

- dataset_id
- dataset_name
- version
- status
- owner
- source
- timezone
- update_frequency
- earliest_timestamp
- latest_timestamp
- created_at
- updated_at

以及：

- Data Lineage
- Provenance
- Upstream Relationship

定義：

- 欄位用途
- 欄位型別
- 必填規則

---

## 3. Dataset Registry Standard

正式定義：

dataset_registry.json

建立：

- Registry Structure
- Dataset Entry Structure
- Versioning Rules
- Dataset Discovery Rules

建立 Machine Readable Registry Contract。

---

## 4. Authority Model

建立資料治理權威模型。

定義：

- Authoritative Source
- Human Readable View
- Synchronization Responsibility
- Update Responsibility

建立 Registry、Catalog、Metadata 之間的治理關係。

---

## 5. Naming Convention Standard

建立統一命名規範。

涵蓋：

- Dataset Naming
- Dataset ID Naming
- Metadata Naming
- File Naming
- Directory Naming
- Version Naming

定義：

- Naming Style
- Allowed Characters
- Naming Consistency Rules

---

## 6. Dataset Contract Framework

建立：

DATA_CONTRACT.md

定義：

- Schema Definition Format
- Primary Key Rules
- Null Policy
- Timezone Policy
- Version Policy
- Validation Policy

建立統一 Dataset Contract Template。

---

## 7. Data Catalog Framework

建立：

DATA_CATALOG.md

定義：

每個 Dataset 應記錄：

- Name
- Description
- Owner
- Source
- Schema
- Update Frequency
- Known Issues
- Status

建立 Catalog Template。

---

## 8. Governance Documentation

更新：

docs/

建立：

- dataset_lifecycle.md
- metadata_standard.md
- registry_standard.md

以及治理架構所需文件。

---

# Repository State Update

完成本階段後：

更新：

- AGENTS.md
- HANDOFF.md

反映：

- Current Phase
- Current Status
- Governance Decisions
- Open Questions
- Recommended Next Phase

---

# Completion Criteria

以下全部成立：

✓ Dataset Lifecycle Model

✓ Dataset Metadata Standard

✓ Dataset Registry Standard

✓ Authority Model

✓ Naming Convention Standard

✓ Dataset Contract Framework

✓ Data Catalog Framework

✓ Governance Documentation

✓ AGENTS.md Updated

✓ HANDOFF.md Updated

---

# Output Requirement

完成後輸出：

## Completed

## Decisions

## Risks

## Open Questions

## Recommended Phase 2

---

# Review Gate

Phase 1 完成後：

提交 Review Package。

等待 Review。

下一階段將於 Review 完成後定義。
