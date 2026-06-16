# task_v0.01.md

請先理解本專案目標。

本專案採用：

Greenfield Repository Strategy

目標：

建立一個全新的 Data Hub Repository。

Repo 名稱：

crypto-data-hub

請以：

- Maintainability
- Reproducibility
- Scalability
- Data Quality
- Automation

作為核心設計原則。

---

# Mission

建立一個可長期維護的資料平台 Repository。

Data Hub 提供：

- Dataset
- Metadata
- Registry
- Snapshot
- Documentation

作為統一資料基礎設施。

---

# Repository Strategy

採用：

- Architecture First
- MVP First
- Incremental Delivery
- Review Before Expansion

每個階段完成後停止。

等待 Review。

不得自行進入下一階段。

---

# Current Task

目前僅執行：

Phase 0

---

# Phase 0

Repository Foundation

建立：

- ROOT.md
- AGENTS.md
- HANDOFF.md
- README.md
- QUICKSTART.md
- VERSION

建立：

- DATA_CATALOG.md
- DATA_CONTRACT.md
- dataset_registry.json

之骨架檔案。

建立：

- Repo Structure
- Agent Onboarding Flow
- Repository Governance

---

# ROOT.md

ROOT.md 為本 Repo 最高優先級文件。

內容包含：

- Mission
- Core Principles
- Rule Priority
- Data Integrity Principles
- Snapshot Principles
- Handoff Principles

若發生規則衝突：

以 ROOT.md 為準。

---

# AGENTS.md

AGENTS.md 為 Agent Primary Entry Point。

內容保持精簡。

建議控制於：

200 行內。

內容包含：

- Current Phase
- Current Status
- Current Priorities
- Blocking Issues
- Recommended Next Actions
- Important Files
- Onboarding Order

---

# HANDOFF.md

HANDOFF.md 為交接文件。

內容包含：

- Architecture Overview
- Important Decisions
- Known Issues
- Pending Work
- Future Recommendations

---

# Agent Onboarding

閱讀順序：

1. ROOT.md
2. AGENTS.md
3. HANDOFF.md
4. README.md

---

# Version Format

採用 Semantic Versioning：

vMAJOR.MINOR.PATCH

初始版本：

v0.1.0

---

# Initial Repo Structure

crypto-data-hub/

├── ROOT.md
├── AGENTS.md
├── HANDOFF.md
├── README.md
├── QUICKSTART.md
├── VERSION

├── DATA_CATALOG.md
├── DATA_CONTRACT.md
├── CHANGELOG.md
├── dataset_registry.json

├── datahub/
├── scripts/
├── tests/
├── reports/
├── examples/
├── logs/
└── docs/

---

# Phase 0 Completion Criteria

✓ Repo Structure

✓ ROOT.md

✓ AGENTS.md

✓ HANDOFF.md

✓ README.md

✓ QUICKSTART.md

✓ VERSION

✓ DATA_CATALOG.md Skeleton

✓ DATA_CONTRACT.md Skeleton

✓ dataset_registry.json Skeleton

✓ Agent Onboarding Flow

---

# Output Requirement

Phase 0 完成後輸出：

## Completed

## Decisions

## Risks

## Open Questions

## Recommended Phase 1

完成後停止。

等待 Review。
