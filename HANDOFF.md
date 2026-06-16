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
  dataset_registry.json  authoritative index of all datasets (source of truth)
  DATA_CATALOG.md        human-readable catalog derived from the registry

Code plane (future phases)
  datahub/   core package: registry access, validation, snapshot logic
  scripts/   automation entry points
  tests/     verification
  reports/   generated quality + usage reports
  examples/  worked usage examples
  logs/      runtime logs
  docs/      extended documentation
```

Control flow for any dataset (target design): **source → validate against
DATA_CONTRACT → register in dataset_registry.json → snapshot → catalog/report**.

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

---

## Known Issues

- All data documents (`DATA_CATALOG.md`, `DATA_CONTRACT.md`,
  `dataset_registry.json`) are **skeletons** with no real datasets yet.
- No executable code, tests, or CI exist yet (deferred to later phases).
- Empty directories are tracked via `.gitkeep` placeholders only.
- No JSON Schema validation is wired up for the registry yet (planned Phase 1).

None of the above blocks Phase 0; they are the expected starting state.

---

## Pending Work

- **Phase 1+ (post-review):**
  - Define the first real dataset end-to-end (contract entry → registry entry).
  - Build registry access + validation tooling in `datahub/` and `scripts/`.
  - Add JSON Schema for `dataset_registry.json` and enforce in CI.
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
