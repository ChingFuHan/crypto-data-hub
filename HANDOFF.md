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

Data plane (governance model defined in Phase 1)
  DATA_CONTRACT.md       schema + quality rules every dataset must satisfy
  dataset_registry.json  authoritative, machine-readable index (source of truth)
  DATA_CATALOG.md        human-readable catalog derived from the registry

Governance docs (docs/, Phase 1)
  dataset_lifecycle.md   states draft→active→deprecated→archived + transitions
  metadata_standard.md   metadata fields, types, required rules
  registry_standard.md   registry structure, versioning, discovery
  authority_model.md     authority + sync + update responsibilities
  naming_convention.md   naming rules for ids, fields, files, versions

Code plane (future phases)
  datahub/   core package: registry access, validation, snapshot logic
  scripts/   automation entry points
  tests/     verification
  reports/   generated quality + usage reports
  examples/  worked usage examples
  logs/      runtime logs
```

Control flow for any dataset (target design): **source → validate against
DATA_CONTRACT → register in dataset_registry.json → snapshot → catalog/report**.

The Phase 1 governance model makes this concrete: the registry holds dataset
metadata inside each entry (`dataset_entry_schema`); the catalog is a derived
view; lifecycle `status` is single-sourced in the registry; validation gates the
`draft → active` transition.

---

## Important Decisions

| # | Decision | Why |
|---|----------|-----|
| D1 | ROOT.md is the single supreme document. | One unambiguous conflict resolver; prevents rule drift across docs. |
| D2 | `dataset_registry.json` is the authoritative source of truth. | Machine-readable single index enables automation + validation (Automation, Data Quality). |
| D3 | Documentation-first foundation before any pipeline code. | "Architecture First" — governance must exist before data does, or quality erodes. |
| D4 | Strict phased delivery; stop-and-review at each boundary. | Prevents scope creep; keeps each increment reviewable (Maintainability). |
| D5 | Semantic Versioning starting at `v0.1.0`. | Pre-1.0 signals foundation stage; predictable version semantics (Reproducibility). |
| D6 | Snapshots are immutable + content-addressable. | Guarantees reproducibility and verifiable provenance. |
| D7 | "Fail loud" on contract violations. | Silent coercion hides data-quality bugs; loud failure protects trust. |
| D8 | Skeleton files now, real content per phase. | Establishes shape and onboarding flow without premature implementation. |

### Phase 1 — Data Governance Foundation (v0.2.0)

| # | Decision | Why |
|---|----------|-----|
| D9 | Lifecycle states fixed to `draft`/`active`/`deprecated`/`archived`, forward-only with logged exceptions. | A small closed state set keeps lifecycle reasoning and tooling simple and auditable. |
| D10 | Dataset metadata lives **inside** registry entries, not in a separate store. | One source of truth; no metadata-vs-registry drift (Maintainability, Data Quality). |
| D11 | `dataset_registry.json` carries a self-describing contract (`conventions` + `dataset_entry_schema`). | Makes the registry machine-readable and self-validating without external schema yet. |
| D12 | Catalog is a **derived view**; conflict priority `ROOT > DATA_CONTRACT > registry > catalog`. | Removes ambiguity about which artifact wins; catalog can be regenerated safely. |
| D13 | Two independent version axes: dataset `version` vs `registry_version`. | Dataset evolution and registry-shape evolution are orthogonal; conflating them breaks SemVer meaning. |
| D14 | `dataset_id` is stable once active — deprecate + new id instead of rename. | Stable identifiers protect lineage, snapshots, and reproducibility. |
| D15 | All timestamps stored UTC ISO 8601; source tz recorded in `timezone`. | Eliminates ambiguity in time-series crypto data (Reproducibility, Data Quality). |

### Phase 2 — First Dataset Design (v0.3.0)

| # | Decision | Why |
|---|----------|-----|
| D16 | First dataset = **Universe Metadata** (`reference.universe.metadata`). | A reference dataset exercises every framework piece and is upstream of many future datasets — the strongest framework validation. |
| D17 | Primary key is a surrogate `instrument_id`, not `symbol`. | Tickers are reused/renamed/merged over time; a stable surrogate survives renames and enables point-in-time reconstruction (Reproducibility). |
| D18 | Symbol-level `status` column (`active`/`delisted`/`renamed`/`merged`) is **separate** from the dataset lifecycle `status`. | Two different lifecycles; conflating them would corrupt both the data model and governance. |
| D19 | Registered as `draft` with `contract_validated = false`; **no ingestion** in Phase 2. | Honest integrity — Phase 2 is design; claiming validation without data would violate "fail loud"/provenance. |
| D20 | Repo bumped to `v0.3.0` but `registry_version` stays `v0.2.0`. | Live proof of D13: adding a dataset entry does not change the registry contract shape. |

---

## Known Issues

- **1 dataset registered, in `draft`**: Universe Metadata is fully *designed* but
  has **no ingested data** and `contract_validated = false`. It cannot be trusted
  or moved to `active` until a later phase ingests and validates data.
- No executable code, tests, or CI exist yet (deferred to later phases) — so the
  Universe Metadata quality rules (Q1–Q6) are specified but not yet enforced.
- No JSON Schema file enforces `dataset_entry_schema` yet; the registry is
  self-describing but not machine-validated in CI.
- `DATA_CATALOG.md` is maintained by hand until catalog generation lands.

None of the above blocks Phase 2; they are the expected design-stage state.

---

## Pending Work

- **Phase 3+ (post-review):**
  - Build registry/contract **validation tooling** in `datahub/` + `scripts/`
    (schema, lifecycle transitions, naming patterns, quality rules Q1–Q6).
  - Ingest Universe Metadata data, validate, and advance `draft → active`.
  - Add a JSON Schema for `dataset_registry.json` and enforce in CI.
  - Auto-generate `DATA_CATALOG.md` from the registry.
  - Implement the snapshot mechanism (immutable, content-addressable).
  - Stand up automated data-quality reporting under `reports/`.

---

## Future Recommendations

- Wire **CI** early: lint docs, validate JSON, run schema checks on every push.
- Keep `DATA_CATALOG.md` **generated** from `dataset_registry.json` to avoid
  drift between the human catalog and the authoritative index.
- Add a `CONTRIBUTING.md` and a dataset-onboarding checklist when contributors
  beyond agents arrive.
- Introduce checksums/manifests for snapshots as soon as the first dataset lands.
- Revisit directory layout only at phase boundaries, never mid-phase.
