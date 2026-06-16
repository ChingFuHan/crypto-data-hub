# task_v0.03.md

請先完整理解本專案目前狀態。

目前：

- Phase 0 已完成
- Repository Foundation 已建立
- Phase 1 已完成
- Data Governance Foundation 已建立
- Governance Documents 已建立
- Registry Standard 已建立
- Contract Framework 已建立
- Catalog Framework 已建立

本次任務：

Phase 2

First Dataset Design

---

# Mission

建立第一個正式 Dataset 的完整設計規格。

本階段目標：

驗證目前 Data Governance Framework 是否足以支撐真實 Dataset。

建立：

Universe Metadata Dataset Design

作為未來所有 Dataset 的參考範例。

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

完成：

Universe Metadata Dataset

之完整設計。

驗證：

- Dataset Contract
- Dataset Metadata
- Dataset Registry
- Data Catalog
- Lifecycle Model

是否可實際應用於真實 Dataset。

---

# Dataset Definition

本階段 Dataset：

Universe Metadata

用途：

描述可交易標的之生命週期資訊。

應支援：

- Active Symbol
- Delisted Symbol
- Renamed Symbol
- Merged Symbol

以及：

重建任意歷史時間點之 Universe。

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

## 1. Dataset Purpose

定義：

Universe Metadata Dataset

之用途。

至少包含：

- Dataset Goal
- Business Purpose
- Expected Consumer
- Expected Usage

---

## 2. Dataset Contract

建立：

Universe Metadata Contract

定義：

- Dataset Name
- Dataset ID
- Dataset Description
- Primary Key
- Field Definitions
- Data Types
- Null Policy

---

## 3. Schema Design

設計：

Universe Metadata Schema

至少評估：

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

Agent 可依需求增加欄位。

定義：

- 欄位用途
- 欄位型別
- 必填規則

---

## 4. Lifecycle Design

定義：

Universe Metadata Dataset

之生命週期。

包含：

- 建立
- 更新
- 廢棄
- 歸檔

以及：

Version Strategy

---

## 5. Metadata Design

建立：

Dataset Metadata

定義：

- dataset_id
- version
- owner
- source
- timezone
- update_frequency

以及其他必要 Metadata。

---

## 6. Registry Entry Design

建立：

Universe Metadata

之 Registry Entry。

更新：

dataset_registry.json

使其符合目前 Registry Standard。

---

## 7. Catalog Entry Design

建立：

Universe Metadata

之 Catalog Entry。

更新：

DATA_CATALOG.md

使其符合目前 Catalog Standard。

---

## 8. Data Quality Design

定義：

Universe Metadata

應具備之資料品質規則。

至少評估：

- Missing Value
- Duplicate Symbol
- Invalid Lifecycle
- Invalid Timestamp
- Invalid Contract Information

建立：

Dataset-specific Quality Rules。

---

## 9. Documentation

建立：

docs/universe_metadata_dataset.md

內容至少包含：

- Purpose
- Schema
- Lifecycle
- Metadata
- Registry Mapping
- Quality Rules

---

# Repository State Update

完成本階段後：

更新：

- AGENTS.md
- HANDOFF.md

反映：

- Current Phase
- Current Status
- Design Decisions
- Open Questions
- Recommended Next Phase

---

# Completion Criteria

以下全部成立：

✓ Universe Metadata Purpose Defined

✓ Universe Metadata Contract Defined

✓ Universe Metadata Schema Defined

✓ Universe Metadata Lifecycle Defined

✓ Universe Metadata Metadata Defined

✓ Universe Metadata Registry Entry Created

✓ Universe Metadata Catalog Entry Created

✓ Universe Metadata Quality Rules Defined

✓ Documentation Updated

✓ AGENTS.md Updated

✓ HANDOFF.md Updated

---

# Output Requirement

完成後輸出：

## Completed

## Decisions

## Risks

## Open Questions

## Recommended Phase 3

---

# Review Gate

Phase 2 完成後：

提交 Review Package。

等待 Review。

下一階段將於 Review 完成後定義。
