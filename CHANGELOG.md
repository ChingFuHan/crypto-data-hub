# Changelog

All notable changes to this repository are documented here.

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/);
this project adheres to [Semantic Versioning](https://semver.org/) (`vMAJOR.MINOR.PATCH`).

---

## [v0.4.0] — 2026-06-16

### Added — Phase 3: Validation Foundation

- **Validation architecture** under `datahub/validation/`, including module
  execution with `python -m datahub.validation`.
- **Validation result model** with per-rule `rule_id`, `severity`, `status`,
  affected file, dataset id, field, location, and details.
- **Validation CLI** with registry, Universe Metadata fixture, and `--all`
  targets plus exit codes `0` / `1` / `2`.
- **Registry validation** for JSON validity, registry contract blocks, required
  fields, field types, naming patterns, status enum, timestamps, quality,
  provenance, and lineage references.
- **Lifecycle validation** for current draft datasets plus transition and
  active/deprecated/archived skeleton checks.
- **Naming validation** for dataset ids, versions, schema refs, timestamps, and
  dataset-related paths.
- **Universe Metadata fixture validation** for Q1-Q6 plus point-in-time
  symbol-era reconstruction (`UM-PIT`).
- **Synthetic test fixtures** for valid and invalid Universe Metadata cases.
- **Stdlib unittest skeleton** covering registry, lifecycle, naming, Universe
  Metadata fixtures, and CLI exit codes.
- **Validation documentation** in `docs/validation_framework.md`.

### Changed

- Bumped repo version `v0.3.0` → `v0.4.0`.
- Updated `AGENTS.md`, `HANDOFF.md`, README/quickstart status text, and related
  governance notes for Phase 3 review state.
- `registry_version` stays `v0.2.0` because the registry contract shape did not
  change.

### Known Gaps

- Universe Metadata remains `draft`; no external data was ingested.
- Active/deprecated/archived lifecycle checks are skeletons until those states
  exist in the registry.
- No JSON Schema, CI workflow, catalog generation, snapshot mechanism, or
  persisted report generation exists yet.

[v0.4.0]: #

---

## [v0.3.0] — 2026-06-16

### Added — Phase 2: First Dataset Design (Universe Metadata)

- **Universe Metadata dataset design** (`reference.universe.metadata`) — the
  first concrete dataset, designed to validate the Phase 1 governance framework
  against a real dataset (`docs/universe_metadata_dataset.md`).
- **Dataset contract** in `DATA_CONTRACT.md` (`Contract: Universe Metadata`):
  14-field schema, primary key `[instrument_id]`, null policy, and six
  dataset-specific quality rules (missing value, duplicate symbol, invalid
  lifecycle, invalid timestamp, invalid contract info, referential).
- **Registry entry** in `dataset_registry.json` `datasets[]` — registered as
  `draft` `v0.1.0`, `contract_validated = false` (design only; no ingestion).
- **Catalog entry** in `DATA_CATALOG.md` (count 0 → 1).
- Supports active / delisted / renamed / merged symbols and point-in-time
  universe reconstruction via `listed_at` / `delisted_at` intervals and
  `successor_id`.

### Changed

- Bumped repo version `v0.2.0` → `v0.3.0`; updated `AGENTS.md` and `HANDOFF.md`
  to reflect Phase 2. `registry_version` stays `v0.2.0` — the registry **contract
  shape** is unchanged; only a dataset entry was added (per decision D13).

### Notes

- Design only: no data ingested, no executable code. The dataset stays `draft`
  until a later phase ingests and validates data.
- Phased delivery: Phase 2 stops here and awaits review before Phase 3.

[v0.3.0]: #

---

## [v0.2.0] — 2026-06-16

### Added — Phase 1: Data Governance Foundation

- **Dataset Lifecycle Model** — states (`draft` → `active` → `deprecated` →
  `archived`), transition rules, and state-management principles
  (`docs/dataset_lifecycle.md`).
- **Dataset Metadata Standard** — unified metadata fields (id, name, version,
  status, owner, source, timezone, update_frequency, timestamps, lineage,
  provenance), with types and required rules (`docs/metadata_standard.md`).
- **Dataset Registry Standard** — formal registry structure, dataset entry
  structure, versioning and discovery rules; machine-readable registry contract
  (`docs/registry_standard.md`).
- **Authority Model** — registry as authoritative source, catalog as derived
  human-readable view, synchronization/update responsibilities
  (`docs/authority_model.md`).
- **Naming Convention Standard** — dataset / id / metadata / file / directory /
  version naming rules (`docs/naming_convention.md`).
- **Dataset Contract Framework** — schema format, primary-key, null, timezone,
  version, and validation policies, plus a contract template (`DATA_CONTRACT.md`).
- **Data Catalog Framework** — per-dataset catalog record and template
  (`DATA_CATALOG.md`).
- Expanded `dataset_registry.json` into a self-describing machine-readable
  contract (`conventions` + `dataset_entry_schema`).

### Changed

- Bumped version `v0.1.0` → `v0.2.0`; updated `AGENTS.md` and `HANDOFF.md` to
  reflect Phase 1 governance decisions and current state.

### Notes

- Governance standards only; still no datasets or executable code (by design).
- Phased delivery: Phase 1 stops here and awaits review before Phase 2.

[v0.2.0]: #

---

## [v0.1.0] — 2026-06-16

### Added — Phase 0: Repository Foundation

- Governance documents: `ROOT.md` (supreme rules), `AGENTS.md` (agent entry
  point), `HANDOFF.md` (architecture + decisions).
- Project docs: `README.md`, `QUICKSTART.md`.
- Versioning: `VERSION` (`v0.1.0`) and this `CHANGELOG.md`.
- Data-plane skeletons: `DATA_CATALOG.md`, `DATA_CONTRACT.md`,
  `dataset_registry.json` (authoritative index, empty).
- Repository structure: `datahub/`, `scripts/`, `tests/`, `reports/`,
  `examples/`, `logs/`, `docs/`.
- Defined core design principles (Maintainability, Reproducibility,
  Scalability, Data Quality, Automation) and the agent onboarding flow.

### Notes

- Skeleton-only foundation; no datasets, pipelines, or executable code yet.
- Phased delivery: Phase 0 stops here and awaits review before Phase 1.

[v0.1.0]: #
