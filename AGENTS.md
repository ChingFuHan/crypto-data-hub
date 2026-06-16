# AGENTS.md

> **Agent primary entry point.** Read this after `ROOT.md`. Keep it short
> (target: under 200 lines). It reflects the **current** state of the repo.

---

## Current Phase

**Phase 2 — First Dataset Design (Universe Metadata)** (complete, awaiting review).

Delivery model: Architecture First → MVP First → Incremental Delivery →
Review Before Expansion. **Do not start Phase 3 without review approval.**

---

## Current Status

- Repo version: `v0.3.0`. `registry_version` stays `v0.2.0` (contract shape
  unchanged — only a dataset entry was added).
- Phases 0 (`v0.1.0`) and 1 (`v0.2.0`) complete.
- Phase 2 designed the **first concrete dataset**, Universe Metadata
  (`reference.universe.metadata`), to validate the Phase 1 governance framework
  against a real dataset:
  - Design doc — `docs/universe_metadata_dataset.md`
  - Contract — `DATA_CONTRACT.md` → *Contract: Universe Metadata*
  - Registry entry — `dataset_registry.json` `datasets[]` (`draft`, `v0.1.0`)
  - Catalog entry — `DATA_CATALOG.md` (count 0 → 1)
- **1 dataset registered**, in `draft` (`contract_validated = false`). No data
  ingested and no executable code yet (by design — this is a design phase).
- Awaiting Phase 2 review before any Phase 3 work begins.

---

## Current Priorities

1. Pass Phase 2 review.
2. Keep `dataset_registry.json` (authoritative) and `DATA_CATALOG.md` (derived)
   in sync — registry wins on any conflict.
3. Keep the Universe Metadata design consistent across its contract, registry
   entry, catalog entry, and design doc.

---

## Blocking Issues

- **None blocking Phase 2.**
- Phase 3 is intentionally blocked pending review (governance, not a defect).

---

## Recommended Next Actions

> Proposals only — execute **after** Phase 2 review approval.

1. Implement registry/contract **validation tooling** in `datahub/` + `scripts/`
   (enforce `dataset_entry_schema`, lifecycle transitions, naming patterns, and
   the Universe Metadata quality rules Q1–Q6).
2. Ingest Universe Metadata data, validate against its contract, and advance it
   `draft → active`.
3. Add a JSON Schema for `dataset_registry.json` and enforce it in CI.
4. Auto-generate `DATA_CATALOG.md` from the registry to remove drift.
5. Implement the snapshot mechanism per ROOT.md → *Snapshot Principles*.

---

## Important Files

| File | Role |
|------|------|
| `ROOT.md` | Highest-priority rules. Wins all conflicts. |
| `AGENTS.md` | This file. Current state + entry point. |
| `HANDOFF.md` | Architecture, decisions, known issues, pending work. |
| `README.md` | Project overview and structure. |
| `QUICKSTART.md` | Fast path to getting started. |
| `VERSION` | Current semantic version (`v0.3.0`). |
| `CHANGELOG.md` | Human-readable history of changes. |
| `DATA_CONTRACT.md` | Dataset Contract Framework — schema + quality rules. |
| `DATA_CATALOG.md` | Data Catalog Framework — derived human-readable view. |
| `dataset_registry.json` | Authoritative, machine-readable dataset registry. |
| `docs/dataset_lifecycle.md` | Lifecycle states + transition rules. |
| `docs/metadata_standard.md` | Metadata fields, types, required rules. |
| `docs/registry_standard.md` | Registry structure, versioning, discovery. |
| `docs/authority_model.md` | Authority/sync/update governance relationships. |
| `docs/naming_convention.md` | Naming rules for ids, fields, files, versions. |
| `docs/universe_metadata_dataset.md` | First dataset design (Universe Metadata). |

| Directory | Purpose |
|-----------|---------|
| `datahub/` | Core platform package (datasets, registry, snapshot logic). |
| `scripts/` | Automation and operational scripts. |
| `tests/` | Test suite. |
| `reports/` | Generated reports and quality outputs. |
| `examples/` | Usage examples. |
| `logs/` | Runtime logs (git-ignored content). |
| `docs/` | Extended governance documentation. |

---

## Onboarding Order

Core reading order (always):

1. `ROOT.md` — supreme rules and principles.
2. `AGENTS.md` — this file: current phase, status, next actions.
3. `HANDOFF.md` — architecture context and decisions.
4. `README.md` — project overview and structure.

Then, before touching data, read the governance set:
`DATA_CONTRACT.md` → `docs/metadata_standard.md` → `docs/registry_standard.md`
→ `docs/dataset_lifecycle.md` → `docs/authority_model.md` →
`docs/naming_convention.md` → `dataset_registry.json`.

---

## Operating Rules (quick reference)

- ROOT.md wins every conflict. Authority order:
  `ROOT.md` > `DATA_CONTRACT.md` > `dataset_registry.json` > `DATA_CATALOG.md`.
- Stop at each phase boundary. Wait for review. Never auto-advance.
- A dataset is not real until it is in `dataset_registry.json`.
- Validate against `DATA_CONTRACT.md` before trusting any data. Fail loud.
- The registry `status` MUST equal the dataset's true lifecycle state.
- Whoever updates the registry updates `DATA_CATALOG.md` in the same change.
- Keep this file current — it is the map every agent depends on.
