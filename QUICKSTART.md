# QUICKSTART

Fast path to becoming productive in `crypto-data-hub`.

> **Current phase:** Phase 3 (Validation Foundation) — executable validation
> exists; Universe Metadata remains `draft`; no data ingestion yet.

---

## 1. Read the docs in order

```
1. ROOT.md       # supreme rules — wins all conflicts
2. AGENTS.md     # current phase, status, next actions
3. HANDOFF.md    # architecture + decisions
4. README.md     # project overview
```

Then, before touching any data: `DATA_CONTRACT.md` and `dataset_registry.json`.

---

## 2. Understand the layout

```
Governance:  ROOT.md · AGENTS.md · HANDOFF.md · README.md · QUICKSTART.md
Versioning:  VERSION · CHANGELOG.md
Data plane:  DATA_CONTRACT.md · dataset_registry.json · DATA_CATALOG.md
Code plane:  datahub/ · scripts/ · tests/ · reports/ · examples/ · logs/ · docs/
```

The validation package now lives in `datahub/validation/`; data ingestion and
snapshot directories remain future work.

---

## 3. Check current state

- Version: see `VERSION` (currently `v0.4.0`).
- What's done / what's next: see `AGENTS.md`.
- Why things are the way they are: see `HANDOFF.md`.

---

## 4. Golden rules before you change anything

- **ROOT.md wins** every conflict. When unsure, re-read it.
- **Stop at phase boundaries.** Do not start the next phase without review.
- A dataset is not real until it is in `dataset_registry.json`.
- Validate against `DATA_CONTRACT.md` before trusting data. **Fail loud.**
- Keep `AGENTS.md` current after any state change.

---

## 5. Inspect the registry

The registry is plain JSON and validates with any standard tool, e.g.:

```bash
python -m json.tool dataset_registry.json
```

It currently holds **1 dataset** (`reference.universe.metadata`, `draft`) plus the
`conventions` and `dataset_entry_schema` blocks (the machine-readable registry
contract). See `docs/registry_standard.md` for how entries are structured and
discovered, and `docs/universe_metadata_dataset.md` for the dataset design.

## 6. Run validation

```bash
python -m datahub.validation --all
python -m unittest discover tests
```

If your environment exposes only `python3`, use `python3 -m ...`.
