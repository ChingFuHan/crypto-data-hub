# DATA_CATALOG.md

> Part of the Phase 1 Data Governance Foundation. Subordinate to `ROOT.md`; in any conflict, `ROOT.md` wins.

The Data Catalog is the **human-readable view** of the datasets in crypto-data-hub. It is **DERIVED from `dataset_registry.json`** and exists for discovery and onboarding, not for authority.

---

## Authority and Scope

- `dataset_registry.json` is the **single source of truth**. This catalog is derived from it.
- When the catalog and the registry disagree, **the registry wins** — fix the catalog, never the registry from the catalog.
- The catalog **must never list an unregistered dataset**. A dataset is not real until it has an entry in `dataset_registry.json`.
- Conflict priority across governance docs: `ROOT.md` > `DATA_CONTRACT.md` > `dataset_registry.json` > `DATA_CATALOG.md`.
- **Synchronization responsibility:** whoever updates the registry MUST update this catalog in the same change. The target end-state is for this file to be generated automatically from the registry.

See also: `DATA_CONTRACT.md` (authoritative per-dataset schema and quality rules), `docs/metadata_standard.md` (metadata field definitions), and `docs/dataset_lifecycle.md` (lifecycle states).

---

## What Every Catalog Entry Records

Each registered dataset appears exactly once in the catalog. Every entry records the following fields, all sourced from the dataset's entry in `dataset_registry.json`:

| Field | Meaning | Registry source |
|-------|---------|-----------------|
| **Name** | Human-readable Title Case name. Discovery key is the `dataset_id`. | `dataset_name` (with `dataset_id`) |
| **Description** | One-line summary of the dataset's contents. | `description` |
| **Owner** | Responsible team or agent. | `owner` |
| **Source** | Where the data originates (type and reference). | `source.type`, `source.reference` |
| **Schema** | Reference into the dataset's schema section in `DATA_CONTRACT.md`. | `schema_ref` |
| **Update Frequency** | Cadence, e.g. `realtime`, `1h`, `daily`, `weekly`, `manual`. | `update_frequency` |
| **Known Issues** | Caveats, limitations, or open data-quality concerns. Empty if none. | derived (quality notes / lineage caveats) |
| **Status** | Lifecycle state: `draft`, `active`, `deprecated`, or `archived`. | `status` |

The `dataset_id` is the unique discovery key and must be shown with each entry. For full metadata (version, provenance, lineage, primary key, timestamps, snapshot), consult the registry entry directly.

---

## Catalog Entry Template

Copy this block when adding a dataset. Populate every field from the dataset's `dataset_registry.json` entry, then save the registry and catalog together.

```markdown
### <dataset_id>

- **Name:** <Human-Readable Title Case Name>
- **Description:** <one-line summary of contents>
- **Owner:** <team or agent>
- **Source:** <type: api|file|onchain|derived> — <endpoint, path, or upstream dataset_id>
- **Schema:** see DATA_CONTRACT.md#<schema_ref anchor>
- **Update Frequency:** <realtime | 1h | daily | weekly | manual>
- **Known Issues:** <caveats / limitations, or "None">
- **Status:** <draft | active | deprecated | archived>
```

---

## Datasets

_No datasets registered. **0 datasets** in `dataset_registry.json`._

Entries are added here only after a dataset is registered in `dataset_registry.json` and validated against `DATA_CONTRACT.md`. Each added entry must use the template above and stay in sync with its registry record.

---

## Catalog–Registry Contract

- Every catalog entry corresponds to exactly one entry in `dataset_registry.json`.
- The registry is the source of truth; the catalog never lists a dataset that is not registered.
- The `Status` shown here MUST equal the registry `status`, which MUST equal the dataset's true lifecycle state.
- Registered dataset count: **0**.
