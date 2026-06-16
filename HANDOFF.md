# HANDOFF.md

> Handoff document. Read after `AGENTS.md`. Records architecture context and
> the **reasoning** behind decisions — the "why", not only the "what".

---

## Architecture Overview

`crypto-data-hub` is a documentation-first, governance-driven crypto data
platform.

```
ROOT.md                 supreme rules
  └─ AGENTS.md           current operating state
      └─ HANDOFF.md      decisions and rationale
          └─ README.md   project overview

Data plane
  DATA_CONTRACT.md       schema + quality rules
  dataset_registry.json  authoritative dataset metadata
  DATA_CATALOG.md        derived human-readable catalog
  data/                  small committed reference artifacts

Code plane
  datahub/validation/    registry, lifecycle, naming, dataset validation
  datahub/ingestion/     Universe Metadata ingestion MVP
  scripts/               automation wrappers
  tests/                 unittest suite and fixtures

Governance docs
  docs/universe_metadata_sources.md
  docs/universe_metadata_dataset.md
  docs/validation_framework.md
  docs/dataset_lifecycle.md
  docs/metadata_standard.md
  docs/registry_standard.md
  docs/authority_model.md
  docs/naming_convention.md
```

Current executable flow:

```text
Binance exchangeInfo → immutable raw snapshot → normalized Universe Metadata
artifact → manifest/checksums → Phase 3 validator → registry/catalog docs
```

Universe Metadata remains lifecycle `draft`; Phase 4 validates a draft artifact
only.

---

## Important Decisions

| # | Decision | Why |
|---|----------|-----|
| D1 | ROOT.md is the single supreme document. | One conflict resolver prevents rule drift. |
| D2 | `dataset_registry.json` is the authoritative source of truth. | Machine-readable source enables automation and validation. |
| D3 | Documentation-first foundation before pipeline code. | Governance before data protects quality. |
| D4 | Strict phased delivery with review gates. | Keeps each increment reviewable. |
| D5 | Semantic Versioning starting at `v0.1.0`. | Predictable version semantics. |
| D6 | Snapshots are immutable and content-addressable. | Supports reproducibility and provenance. |
| D7 | Fail loud on contract violations. | Silent coercion hides data-quality bugs. |
| D8 | Skeleton files first, real content per phase. | Establishes self-onboarding shape. |

### Phase 1 — Data Governance Foundation (v0.2.0)

| # | Decision | Why |
|---|----------|-----|
| D9 | Lifecycle states fixed to `draft`/`active`/`deprecated`/`archived`. | Small closed state set keeps tooling simple. |
| D10 | Dataset metadata lives inside registry entries. | One source of truth. |
| D11 | Registry carries `conventions` + `dataset_entry_schema`. | Self-describing machine-readable contract. |
| D12 | Catalog is a derived view. | Registry/catalog conflicts resolve predictably. |
| D13 | Dataset `version` and `registry_version` are separate. | Dataset evolution and registry shape are different axes. |
| D14 | Active `dataset_id` is stable. | Protects lineage and reproducibility. |
| D15 | All timestamps stored UTC ISO 8601. | Removes time ambiguity. |

### Phase 2 — First Dataset Design (v0.3.0)

| # | Decision | Why |
|---|----------|-----|
| D16 | First dataset = Universe Metadata. | Reference data exercises all governance pieces. |
| D17 | Primary key is surrogate `instrument_id`, not `symbol`. | Symbols are reused/renamed/merged. |
| D18 | Symbol `status` is separate from dataset lifecycle `status`. | Avoids lifecycle conflation. |
| D19 | Registered as draft with no ingestion. | Honest design-stage integrity. |
| D20 | `registry_version` stayed `v0.2.0`. | Dataset entry did not change registry contract shape. |

### Phase 3 — Validation Foundation (v0.4.0)

| # | Decision | Why |
|---|----------|-----|
| D21 | Validation lives under `datahub/validation/`. | Importable, testable, automation-friendly. |
| D22 | Validation uses only Python standard library. | No dependency overhead. |
| D23 | Result model is explicit per-check records. | CLI and future CI can share structure. |
| D24 | `--target registry` composes registry, lifecycle, naming checks. | Governance rules are coupled. |
| D25 | Universe Metadata validation is fixture-based first. | Cross-field/graph invariants become executable before ingestion. |
| D26 | `registry_version` remained `v0.2.0`. | Validation tooling did not change registry shape. |
| D27 | `quality.last_validated_at = null` accepted when `contract_validated = false`. | Draft registry state stays honest. |

### Phase 4 — Universe Metadata Ingestion MVP (v0.5.0)

| # | Decision | Why |
|---|----------|-----|
| D28 | Primary source = Binance USD-M Futures `exchangeInfo`. | Public, official, deterministic current active universe source. |
| D29 | Archive index and announcements are reviewed but not implemented as authoritative row sources. | They need extra evidence reconciliation before driving lifecycle rows. |
| D30 | Ingestion lives under `datahub/ingestion/universe_metadata.py`. | Keeps ingestion separate from validation but still module-executable. |
| D31 | Raw snapshots are immutable envelope JSON files with raw response checksum. | Enables offline deterministic replay and source provenance. |
| D32 | Normalized artifact is a JSON array at `data/reference/universe_metadata/reference.universe.metadata.json`. | Matches Phase 3 validator preferred input format. |
| D33 | Manifest lives at `data/manifests/reference/universe_metadata/manifest.json`. | Keeps provenance, checksums, coverage, and validation metadata out of row data. |
| D34 | Instrument ids use `binance.usd_m_futures.<market_type>.<symbol_lower>.<listed_yyyymmdd>`. | Human-readable, deterministic, symbol-era aware, not plain symbol. |
| D35 | Collision handling appends deterministic `.h<sha256_prefix>`. | Preserves reproducibility if source collisions appear. |
| D36 | `contract_size = 1` is a documented normalization convention for USD-M linear futures. | `exchangeInfo` lacks a separate field, but contract Q5 requires positive derivative contract size. |
| D37 | Coverage status is manifest/provenance metadata, not row `status`. | Prevents unsupported values from polluting contract enum. |
| D38 | `contract_validated` remains `false` while artifact validation passes. | Artifact validation is not lifecycle promotion; Phase 5/review must decide semantics. |
| D39 | `registry_version` remains `v0.2.0`. | Registry shape unchanged; artifact metadata fits existing provenance params. |
| D40 | Committed data artifacts are allowed because total size is small and supports offline validation. | Reproducibility beats avoiding small reference data. |

---

## Artifact Locations

- Raw snapshot:
  `data/raw/reference/universe_metadata/exchange_info_20260616T170138Z_d4d2d2ab1c6e.json`
- Normalized artifact:
  `data/reference/universe_metadata/reference.universe.metadata.json`
- Manifest:
  `data/manifests/reference/universe_metadata/manifest.json`
- Artifact checksum:
  `fcee6a125792598d19e4332c3acd848dd4c7e49551e1f1cef2ad09a73b533b39`
- Manifest checksum:
  `cd40840b48a46b1a844ce015e548d3ece82eba733ef2f4fea0ffba1adc9444f3`
- Rows: 671
- Source records: 792
- Coverage: `active_current`

---

## Validation Result

Required deterministic commands pass:

```bash
python -m datahub.ingestion.universe_metadata --offline --all
python -m datahub.validation --all
python -m unittest discover tests
```

The host may expose the launcher as `python3`; command form is otherwise the
same.

---

## Known Gaps

- Universe Metadata remains lifecycle `draft`.
- Coverage is Binance USD-M Futures current `TRADING` symbols only.
- Historical delisted, renamed, and merged lifecycle events are not ingested.
- Archive index candidates are not authoritative rows.
- Announcement parsing is not implemented.
- No JSON Schema or CI exists yet.
- `DATA_CATALOG.md` is still hand-maintained.
- Snapshot publication is not implemented.

---

## Open Questions

- Should `contract_validated` represent artifact validation, lifecycle promotion,
  or both via separate future fields?
- Which source combination should become authoritative for historical lifecycle
  events?
- Should coverage/confidence move into a formal registry schema in a future
  `registry_version` bump?
- Where should larger future source artifacts live if they exceed repo-reviewable
  size?

---

## Pending Work

- **Phase 5+ (post-review):**
  - Decide contract/artifact validation semantics.
  - Add historical delist/rename/merge source ingestion.
  - Add JSON Schema and CI.
  - Generate `DATA_CATALOG.md` from `dataset_registry.json`.
  - Implement immutable, content-addressable snapshots.
  - Emit validation reports under `reports/`.

---

## Future Recommendations

- Keep raw snapshot reuse by checksum; never overwrite raw data.
- Keep row data contract-clean; put coverage/provenance in manifest unless the
  registry contract is intentionally extended.
- Expand historical coverage with fixture-first tests before touching registry
  lifecycle state.
