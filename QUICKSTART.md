# QUICKSTART

Fast path to becoming productive in `crypto-data-hub`.

> **Current phase:** Phase 4 (Universe Metadata Ingestion MVP) — first validated
> draft artifact exists; Universe Metadata lifecycle remains `draft`.

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

The validation package lives in `datahub/validation/`; Universe Metadata
ingestion lives in `datahub/ingestion/`. Snapshot publication remains future
work.

---

## 3. Check current state

- Version: see `VERSION` (currently `v0.5.0`).
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
contract). The registry points to the Phase 4 draft artifact and manifest. See
`docs/registry_standard.md` for how entries are structured and discovered, and
`docs/universe_metadata_dataset.md` for the dataset design.

## 6. Run validation

```bash
python -m datahub.validation --all
python -m unittest discover tests
```

If your environment exposes only `python3`, use `python3 -m ...`.

## 7. Run Universe Metadata ingestion

Online source fetch + normalize + validate:

```bash
python -m datahub.ingestion.universe_metadata --all
```

Offline deterministic re-run from committed raw snapshot:

```bash
python -m datahub.ingestion.universe_metadata --offline --all
```

Individual commands:

```bash
python -m datahub.ingestion.universe_metadata --fetch
python -m datahub.ingestion.universe_metadata --normalize
```

Current artifact locations:

- Raw snapshot: `data/raw/reference/universe_metadata/`
- Normalized artifact: `data/reference/universe_metadata/reference.universe.metadata.json`
- Manifest: `data/manifests/reference/universe_metadata/manifest.json`

Committed data artifacts are intentionally small reference artifacts for offline
validation. Large market data remains out of scope.
