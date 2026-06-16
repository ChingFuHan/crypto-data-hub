# AGENTS.md

> **Agent primary entry point.** Read this after `ROOT.md`. Keep it short
> (target: under 200 lines). It reflects the **current** state of the repo.

---

## Current Phase

**Phase 3 — Validation Foundation** (complete, awaiting review).

Delivery model: Architecture First → MVP First → Incremental Delivery →
Review Before Expansion. **Do not start Phase 4 without review approval.**

---

## Current Status

- Repo version: `v0.4.0`. `registry_version` stays `v0.2.0` because the
  registry contract shape did not change.
- Phases 0 (`v0.1.0`), 1 (`v0.2.0`), and 2 (`v0.3.0`) are complete.
- Phase 3 added the first executable validation foundation:
  - Package entry point — `python -m datahub.validation`
  - CLI implementation — `datahub/validation/cli.py`
  - Result/error model — `datahub/validation/result.py`, `errors.py`
  - Registry validation — `datahub/validation/registry.py`
  - Lifecycle validation — `datahub/validation/lifecycle.py`
  - Naming validation — `datahub/validation/naming.py`
  - Universe Metadata fixture validation — `datahub/validation/universe_metadata.py`
  - Test fixtures — `tests/fixtures/universe_metadata/`
  - Test skeleton — `tests/test_*.py`
  - Framework doc — `docs/validation_framework.md`
- Universe Metadata (`reference.universe.metadata`) remains `draft` with
  `quality.contract_validated = false`; no data was ingested in Phase 3.
- Module execution entry point is `python -m datahub.validation`; on hosts where
  the launcher is named only `python3`, use `python3 -m ...` for the same checks.
- Awaiting Phase 3 review before any Phase 4 work begins.

---

## Current Priorities

1. Pass Phase 3 review.
2. Keep validation commands green and deterministic.
3. Keep `dataset_registry.json` (authoritative) and `DATA_CATALOG.md` (derived)
   in sync — registry wins on any conflict.
4. Keep the Universe Metadata design consistent across its contract, registry
   entry, catalog entry, design doc, and validation fixtures.

---

## Blocking Issues

- **None blocking Phase 3 review.**
- Phase 4 is intentionally blocked pending review (governance, not a defect).

---

## Recommended Next Actions

> Proposals only — execute **after** Phase 3 review approval.

1. Ingest Universe Metadata data, validate against Q1–Q6, and advance it
   `draft → active` only after review.
2. Add a JSON Schema for `dataset_registry.json` and enforce it in CI.
3. Auto-generate `DATA_CATALOG.md` from the registry to remove drift.
4. Implement immutable, content-addressable snapshot creation.
5. Stand up generated validation reports under `reports/`.

---

## Important Files

| File | Role |
|------|------|
| `ROOT.md` | Highest-priority rules. Wins all conflicts. |
| `AGENTS.md` | This file. Current state + entry point. |
| `HANDOFF.md` | Architecture, decisions, known issues, pending work. |
| `README.md` | Project overview and structure. |
| `QUICKSTART.md` | Fast path to getting started. |
| `VERSION` | Current semantic version (`v0.4.0`). |
| `CHANGELOG.md` | Human-readable history of changes. |
| `DATA_CONTRACT.md` | Dataset Contract Framework — schema + quality rules. |
| `DATA_CATALOG.md` | Data Catalog Framework — derived human-readable view. |
| `dataset_registry.json` | Authoritative, machine-readable dataset registry. |
| `docs/validation_framework.md` | Phase 3 validation architecture and CLI docs. |
| `docs/dataset_lifecycle.md` | Lifecycle states + transition rules. |
| `docs/metadata_standard.md` | Metadata fields, types, required rules. |
| `docs/registry_standard.md` | Registry structure, versioning, discovery. |
| `docs/authority_model.md` | Authority/sync/update governance relationships. |
| `docs/naming_convention.md` | Naming rules for ids, fields, files, versions. |
| `docs/universe_metadata_dataset.md` | First dataset design (Universe Metadata). |

| Directory | Purpose |
|-----------|---------|
| `datahub/` | Core platform package. |
| `datahub/validation/` | Executable validation framework. |
| `scripts/` | Automation and operational scripts. |
| `tests/` | Test suite and fixtures. |
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

For validation work, also read `docs/validation_framework.md`.

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
