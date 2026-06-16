# AGENTS.md

> **Agent primary entry point.** Read this after `ROOT.md`. Keep it short
> (target: under 200 lines). It reflects the **current** state of the repo.

---

## Current Phase

**Phase 4 — Universe Metadata Ingestion MVP** (complete, awaiting review).

Delivery model: Architecture First → MVP First → Incremental Delivery →
Review Before Expansion. **Do not start Phase 5 without review approval.**

---

## Current Status

- Repo version: `v0.5.0`. `registry_version` stays `v0.2.0` because the
  registry contract shape did not change.
- Phases 0 (`v0.1.0`), 1 (`v0.2.0`), 2 (`v0.3.0`), and 3 (`v0.4.0`) are
  complete.
- Phase 4 added the first Universe Metadata ingestion MVP:
  - Source review — `docs/universe_metadata_sources.md`
  - Ingestion CLI — `python -m datahub.ingestion.universe_metadata`
  - Raw snapshot — `data/raw/reference/universe_metadata/`
  - Normalized artifact — `data/reference/universe_metadata/reference.universe.metadata.json`
  - Manifest — `data/manifests/reference/universe_metadata/manifest.json`
  - Ingestion tests/fixtures — `tests/test_universe_metadata_ingestion.py`,
    `tests/fixtures/ingestion/universe_metadata/`
- Universe Metadata (`reference.universe.metadata`) remains lifecycle `draft`.
  The Phase 4 artifact validates successfully, but
  `quality.contract_validated = false` remains unchanged to avoid implying
  `draft → active` promotion.
- Coverage is `active_current` only: Binance USD-M Futures current `TRADING`
  symbols from `exchangeInfo`.
- Module execution entry points:
  - `python -m datahub.ingestion.universe_metadata --offline --all`
  - `python -m datahub.validation --all`
  - `python -m unittest discover tests`
- Awaiting Phase 4 review before any Phase 5 work begins.

---

## Current Priorities

1. Pass Phase 4 review.
2. Keep offline ingestion idempotent and validation commands green.
3. Keep `dataset_registry.json` (authoritative) and `DATA_CATALOG.md` (derived)
   in sync — registry wins on any conflict.
4. Keep artifact/manifest/checksum references aligned across registry, catalog,
   handoff, and docs.

---

## Blocking Issues

- **None blocking Phase 4 review.**
- Phase 5 is intentionally blocked pending review (governance, not a defect).

---

## Recommended Next Actions

> Proposals only — execute **after** Phase 4 review approval.

1. Decide whether `contract_validated` should distinguish artifact validation
   from lifecycle promotion.
2. Expand Universe Metadata to historical delist / rename / merge evidence.
3. Add JSON Schema for `dataset_registry.json` and enforce validation in CI.
4. Auto-generate `DATA_CATALOG.md` from the registry.
5. Implement immutable, content-addressable snapshot publication.

---

## Important Files

| File | Role |
|------|------|
| `ROOT.md` | Highest-priority rules. Wins all conflicts. |
| `AGENTS.md` | This file. Current state + entry point. |
| `HANDOFF.md` | Architecture, decisions, known issues, pending work. |
| `README.md` | Project overview and structure. |
| `QUICKSTART.md` | Fast path to getting started. |
| `VERSION` | Current semantic version (`v0.5.0`). |
| `CHANGELOG.md` | Human-readable history of changes. |
| `DATA_CONTRACT.md` | Dataset Contract Framework — schema + quality rules. |
| `DATA_CATALOG.md` | Data Catalog Framework — derived human-readable view. |
| `dataset_registry.json` | Authoritative, machine-readable dataset registry. |
| `docs/universe_metadata_sources.md` | Source authority review. |
| `docs/validation_framework.md` | Validation architecture and CLI docs. |
| `docs/universe_metadata_dataset.md` | Universe Metadata design + Phase 4 artifact notes. |
| `data/manifests/reference/universe_metadata/manifest.json` | Phase 4 artifact manifest. |

| Directory | Purpose |
|-----------|---------|
| `datahub/ingestion/` | Universe Metadata ingestion workflow. |
| `datahub/validation/` | Executable validation framework. |
| `data/` | Small committed reference artifacts for offline validation. |
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

For Phase 4 work, also read `docs/universe_metadata_sources.md`,
`docs/universe_metadata_dataset.md`, and `docs/validation_framework.md`.

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
