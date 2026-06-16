# AGENTS.md

> **Agent primary entry point.** Read this after `ROOT.md`. Keep it short
> (target: under 200 lines). It reflects the **current** state of the repo.

---

## Current Phase

**Phase 1 — Data Governance Foundation** (complete, awaiting review).

Delivery model: Architecture First → MVP First → Incremental Delivery →
Review Before Expansion. **Do not start Phase 2 without review approval.**

---

## Current Status

- Repo version: `v0.2.0`.
- Phase 0 (Repository Foundation) complete and reviewed; committed as `v0.1.0`.
- Phase 1 delivered the shared **data governance model** every future dataset
  must follow:
  - Dataset Lifecycle Model — `docs/dataset_lifecycle.md`
  - Dataset Metadata Standard — `docs/metadata_standard.md`
  - Dataset Registry Standard — `docs/registry_standard.md`
  - Authority Model — `docs/authority_model.md`
  - Naming Convention Standard — `docs/naming_convention.md`
  - Dataset Contract Framework — `DATA_CONTRACT.md`
  - Data Catalog Framework — `DATA_CATALOG.md`
  - Machine-readable registry contract — `dataset_registry.json`
    (`conventions` + `dataset_entry_schema`)
- Still **0 datasets registered** and no executable code (by design).
- Awaiting Phase 1 review before any Phase 2 work begins.

---

## Current Priorities

1. Pass Phase 1 review.
2. Keep `dataset_registry.json` (authoritative) and `DATA_CATALOG.md` (derived)
   in sync — registry wins on any conflict.
3. Keep the governance docs internally consistent: lifecycle states, metadata
   field names, and naming patterns must match across docs and the registry.

---

## Blocking Issues

- **None blocking Phase 1.**
- Phase 2 is intentionally blocked pending review (governance, not a defect).

---

## Recommended Next Actions

> Proposals only — execute **after** Phase 1 review approval.

1. Define the first concrete dataset end-to-end: a `DATA_CONTRACT.md` contract
   section + a real entry in `dataset_registry.json` + a catalog entry.
2. Implement registry/contract **validation tooling** in `datahub/` + `scripts/`
   (enforce `dataset_entry_schema`, lifecycle transitions, naming patterns).
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
| `VERSION` | Current semantic version (`v0.2.0`). |
| `CHANGELOG.md` | Human-readable history of changes. |
| `DATA_CONTRACT.md` | Dataset Contract Framework — schema + quality rules. |
| `DATA_CATALOG.md` | Data Catalog Framework — derived human-readable view. |
| `dataset_registry.json` | Authoritative, machine-readable dataset registry. |
| `docs/dataset_lifecycle.md` | Lifecycle states + transition rules. |
| `docs/metadata_standard.md` | Metadata fields, types, required rules. |
| `docs/registry_standard.md` | Registry structure, versioning, discovery. |
| `docs/authority_model.md` | Authority/sync/update governance relationships. |
| `docs/naming_convention.md` | Naming rules for ids, fields, files, versions. |

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
