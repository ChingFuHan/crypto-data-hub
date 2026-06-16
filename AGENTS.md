# AGENTS.md

> **Agent primary entry point.** Read this after `ROOT.md`. Keep it short
> (target: under 200 lines). It reflects the **current** state of the repo.

---

## Current Phase

**Phase 0 — Repository Foundation** (complete, awaiting review).

Delivery model: Architecture First → MVP First → Incremental Delivery →
Review Before Expansion. **Do not start Phase 1 without review approval.**

---

## Current Status

- Repository skeleton created: governance docs, data docs, directory layout.
- All Phase 0 foundation files are in place as skeletons.
- No datasets, pipelines, or executable code exist yet (by design).
- Awaiting review before any Phase 1 work begins.

---

## Current Priorities

1. Pass Phase 0 review.
2. Keep `dataset_registry.json` and `DATA_CONTRACT.md` authoritative and in sync.
3. Preserve onboarding clarity — docs stay accurate and minimal.

---

## Blocking Issues

- **None blocking Phase 0.**
- Phase 1 is intentionally blocked pending review (governance, not a defect).

---

## Recommended Next Actions

> Proposals only — execute **after** Phase 0 review approval.

1. Define the first concrete dataset and its full entry in `DATA_CONTRACT.md`.
2. Implement registry read/validate tooling under `scripts/`.
3. Add schema validation for `dataset_registry.json` (CI-enforced).
4. Establish the snapshot mechanism per ROOT.md Snapshot Principles.

---

## Important Files

| File | Role |
|------|------|
| `ROOT.md` | Highest-priority rules. Wins all conflicts. |
| `AGENTS.md` | This file. Current state + entry point. |
| `HANDOFF.md` | Architecture, decisions, known issues, pending work. |
| `README.md` | Project overview and structure. |
| `QUICKSTART.md` | Fast path to getting started. |
| `VERSION` | Current semantic version (`v0.1.0`). |
| `CHANGELOG.md` | Human-readable history of changes. |
| `DATA_CATALOG.md` | Index of datasets (skeleton). |
| `DATA_CONTRACT.md` | Data schema + quality rules (skeleton). |
| `dataset_registry.json` | Authoritative dataset registry (skeleton). |

| Directory | Purpose |
|-----------|---------|
| `datahub/` | Core platform package (datasets, registry, snapshot logic). |
| `scripts/` | Automation and operational scripts. |
| `tests/` | Test suite. |
| `reports/` | Generated reports and quality outputs. |
| `examples/` | Usage examples. |
| `logs/` | Runtime logs (git-ignored content). |
| `docs/` | Extended documentation. |

---

## Onboarding Order

Read in this exact order:

1. `ROOT.md` — supreme rules and principles.
2. `AGENTS.md` — this file: current phase, status, next actions.
3. `HANDOFF.md` — architecture context and decisions.
4. `README.md` — project overview and structure.

Then, before touching data: `DATA_CONTRACT.md` and `dataset_registry.json`.

---

## Operating Rules (quick reference)

- ROOT.md wins every conflict. When unsure, re-read it.
- Stop at each phase boundary. Wait for review. Never auto-advance.
- A dataset is not real until it is in `dataset_registry.json`.
- Validate against `DATA_CONTRACT.md` before trusting any data. Fail loud.
- Keep this file current — it is the map every agent depends on.
