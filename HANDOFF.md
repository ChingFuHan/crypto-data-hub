# HANDOFF.md

> Handoff document. Read after `AGENTS.md`. Records architecture context and
> the **reasoning** behind decisions — the "why", not only the "what".

---

## Architecture Overview

`crypto-data-hub` is a **documentation-first, governance-driven** data platform.
Its purpose is to be the single unified infrastructure for crypto datasets:

```
ROOT.md                 supreme rules (conflict resolver)
  └─ AGENTS.md           current operating state (agent entry point)
      └─ HANDOFF.md      architecture + decision record (this file)
          └─ README.md   public-facing overview

Data plane
  DATA_CONTRACT.md       schema + quality rules every dataset must satisfy
  dataset_registry.json  authoritative, machine-readable index (source of truth)
  DATA_CATALOG.md        human-readable catalog derived from the registry

Governance docs
  dataset_lifecycle.md   states draft→active→deprecated→archived + transitions
  metadata_standard.md   metadata fields, types, rules
  registry_standard.md   registry structure, versioning, discovery
  authority_model.md     authority + sync + update responsibilities
  naming_convention.md   naming rules
  validation_framework.md validation architecture, CLI, coverage, gaps

Code plane
  datahub/validation/    registry, lifecycle, naming, and dataset validators
  scripts/validate.py    CLI wrapper for automation
  tests/                 unittest skeleton and fixtures
  reports/               future generated quality reports
```

Control flow for any dataset (target design): **source → validate against
DATA_CONTRACT → register in dataset_registry.json → snapshot → catalog/report**.

Phase 3 makes the first part executable: registry/lifecycle/naming checks and
Universe Metadata fixture checks now run locally and deterministically. No data
ingestion or external download exists yet.

---

## Important Decisions

| # | Decision | Why |
|---|----------|-----|
| D1 | ROOT.md is the single supreme document. | One unambiguous conflict resolver; prevents rule drift across docs. |
| D2 | `dataset_registry.json` is the authoritative source of truth. | Machine-readable single index enables automation + validation. |
| D3 | Documentation-first foundation before pipeline code. | Governance must exist before data, or quality erodes. |
| D4 | Strict phased delivery; stop-and-review at each boundary. | Prevents scope creep; keeps each increment reviewable. |
| D5 | Semantic Versioning starting at `v0.1.0`. | Pre-1.0 signals foundation stage; predictable version semantics. |
| D6 | Snapshots are immutable + content-addressable. | Guarantees reproducibility and verifiable provenance. |
| D7 | "Fail loud" on contract violations. | Silent coercion hides data-quality bugs. |
| D8 | Skeleton files first, real content per phase. | Establishes shape and onboarding flow without premature implementation. |

### Phase 1 — Data Governance Foundation (v0.2.0)

| # | Decision | Why |
|---|----------|-----|
| D9 | Lifecycle states fixed to `draft`/`active`/`deprecated`/`archived`. | Small closed state set keeps tooling simple and auditable. |
| D10 | Dataset metadata lives inside registry entries. | One source of truth; no metadata-vs-registry drift. |
| D11 | Registry carries `conventions` + `dataset_entry_schema`. | Makes the registry self-describing and machine-readable. |
| D12 | Catalog is a derived view. | Registry/catalog conflicts resolve predictably. |
| D13 | Dataset `version` and `registry_version` are separate axes. | Dataset evolution and registry-shape evolution are different concerns. |
| D14 | Active `dataset_id` is stable. | Stable ids protect lineage, snapshots, and reproducibility. |
| D15 | All timestamps stored UTC ISO 8601; source tz recorded in `timezone`. | Removes time ambiguity in crypto data. |

### Phase 2 — First Dataset Design (v0.3.0)

| # | Decision | Why |
|---|----------|-----|
| D16 | First dataset = Universe Metadata (`reference.universe.metadata`). | Reference data exercises the full governance framework. |
| D17 | Primary key is surrogate `instrument_id`, not `symbol`. | Tickers are reused/renamed/merged; stable surrogate preserves lineage. |
| D18 | Symbol-level `status` is separate from dataset lifecycle `status`. | Two lifecycle concepts must not be conflated. |
| D19 | Registered as `draft`, `contract_validated = false`, no ingestion. | Phase 2 was design; claiming validation without data would violate fail-loud. |
| D20 | Repo bumped to `v0.3.0`; `registry_version` stayed `v0.2.0`. | Adding a dataset entry did not change registry contract shape. |

### Phase 3 — Validation Foundation (v0.4.0)

| # | Decision | Why |
|---|----------|-----|
| D21 | Validation lives under `datahub/validation/` with `python -m datahub.validation`. | A package entry point is importable, testable, and automation-friendly. |
| D22 | Validation uses only Python standard library. | Keeps Phase 3 reproducible without dependency management overhead. |
| D23 | Result model is explicit per-check records (`rule_id`, `severity`, `status`, file, dataset, field, location, details). | Reports are machine-readable enough for future CI while still clear in CLI output. |
| D24 | `--target registry` composes registry, lifecycle, and naming checks. | The registry is the source of truth; these rule families are inseparable for governance validation. |
| D25 | Universe Metadata validation is fixture-based in Phase 3. | Cross-field and graph invariants become executable without pretending real ingestion exists. |
| D26 | `registry_version` remains `v0.2.0`. | Validation tooling adds code, tests, and docs but does not change `dataset_entry_schema` or `conventions`. |
| D27 | `quality.last_validated_at = null` is accepted when `contract_validated = false`. | Current draft registry state is honest; timestamp is required only after contract validation succeeds. |

---

## Validation Coverage

- Registry validation:
  JSON validity, top-level contract blocks, required fields, field types,
  dataset id uniqueness, status enum, owner/source/timezone/update frequency,
  primary key, schema ref, timestamps, quality, provenance, lineage upstream.
- Lifecycle validation:
  valid states, transition table, draft requirements executable against current
  registry, active/deprecated/archived requirement skeletons including future
  active data location / snapshot availability.
- Naming validation:
  dataset id, dataset version, registry version, schema refs, UTC timestamps,
  snapshot path naming skeleton, validation doc path naming.
- Universe Metadata validation:
  Q1 required fields, Q2 uniqueness, Q3 lifecycle invariants, Q4 timestamp
  validity/order/future guard, Q5 contract information, Q6 successor graph, and
  `UM-PIT` symbol-era overlap / rename handoff validation.
- Tests:
  stdlib `unittest` coverage for registry, lifecycle, naming, Universe fixtures,
  and CLI exit codes.

---

## Known Gaps

- Universe Metadata remains `draft`; no external exchange data has been ingested.
- Active/deprecated/archived lifecycle checks are implemented as skeleton logic
  and skipped for the current registry because no such dataset exists yet.
- No JSON Schema file or CI workflow exists yet.
- `DATA_CATALOG.md` is still hand-maintained; catalog generation is future work.
- Validation reports print to stdout; `reports/` generation is not implemented.
- Snapshot creation and checksum manifests are not implemented.

---

## Open Questions

- Which concrete exchange sources should Phase 4 use for the first Universe
  Metadata ingestion fixture or artifact?
- Where should published dataset artifacts live before snapshot storage is
  formalized?
- Should registry validation later become JSON Schema-first, with Python rules
  reserved for cross-field and graph invariants?

---

## Pending Work

- **Phase 4+ (post-review):**
  - Ingest Universe Metadata from approved sources, validate Q1–Q6, and move
    `draft → active` only after review.
  - Add JSON Schema for `dataset_registry.json`.
  - Add CI to run `python -m datahub.validation --all` and unittest.
  - Auto-generate `DATA_CATALOG.md` from `dataset_registry.json`.
  - Implement immutable, content-addressable snapshots.
  - Write generated validation reports under `reports/`.

---

## Future Recommendations

- Keep validation rules close to authoritative docs; update `HANDOFF.md` when a
  rule decision bridges an ambiguity.
- Prefer fixture-first tests for each new dataset before external ingestion.
- Revisit directory layout only at phase boundaries.
