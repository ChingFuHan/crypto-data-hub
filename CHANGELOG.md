# Changelog

All notable changes to this repository are documented here.

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/);
this project adheres to [Semantic Versioning](https://semver.org/) (`vMAJOR.MINOR.PATCH`).

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
