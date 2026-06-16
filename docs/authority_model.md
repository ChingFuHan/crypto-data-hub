# Data Governance Authority Model

> Part of the Phase 1 Data Governance Foundation. Subordinate to `../ROOT.md`; if any statement here conflicts with ROOT.md, ROOT.md wins.

This document defines **who and what is authoritative** in crypto-data-hub: which artifact is the source of truth, which artifacts are derived from it, who is responsible for keeping them synchronized, and how conflicts are resolved. It is a standard, not a tutorial.

---

## Summary

| Artifact | Role | Authority |
| --- | --- | --- |
| `../dataset_registry.json` | Machine-readable index of all datasets; holds dataset metadata inside its entries | **Authoritative source of truth** |
| `../DATA_CONTRACT.md` | Per-dataset schema and quality rules | **Authoritative for data correctness** |
| `../DATA_CATALOG.md` | Human-readable catalog | **Derived view** (never authoritative) |
| Dataset metadata | The fields of each registry entry | Authoritative *because* it lives inside the registry |

A dataset is not "real" until it is registered in `../dataset_registry.json`. Nothing downstream may assert the existence of a dataset the registry does not list.

---

## Authoritative Source

`../dataset_registry.json` is the single, machine-readable source of truth and the discovery index that tools query.

- **Dataset metadata lives inside registry entries.** There is no separate authoritative metadata store. Each entry in `datasets[]` carries the full metadata for one dataset, structured according to the `dataset_entry_schema` in the registry and the metadata standard (`metadata_standard.md`).
- **`dataset_id` is the unique discovery key.** Datasets are discoverable by `dataset_id` (primary key) and filterable by `status`, `owner`, and `tags`.
- **The registry's `status` field MUST equal the dataset's true lifecycle state** (`draft`, `active`, `deprecated`, `archived`). See `dataset_lifecycle.md`.
- **The registry is updated only after `../DATA_CONTRACT.md` validation passes.** Validation precedes registration; on violation the process fails loud and the registry is not written.

`../DATA_CONTRACT.md` is authoritative for *per-dataset schema and quality rules*. The registry references it via each entry's `schema_ref`; the registry does not redefine field semantics that the contract owns.

---

## Human-Readable View

`../DATA_CATALOG.md` is a **derived, human-readable view** of the registry. It exists for human discovery and onboarding, not for machine authority.

- **The catalog is generated from the registry.** Target state: `../DATA_CATALOG.md` is produced from `../dataset_registry.json`, never authored independently.
- **The catalog MUST NEVER list an unregistered dataset.** Every catalogued dataset has a corresponding registry entry. The reverse direction (registry → catalog) is the synchronization obligation below.
- **On any disagreement, the registry wins.** If the catalog and the registry differ, the catalog is wrong and must be regenerated; the registry is never edited to match the catalog.

Per-dataset catalog fields (Name, Description, Owner, Source, Schema reference into `../DATA_CONTRACT.md`, Update Frequency, Known Issues, Status) are all projections of registry data.

---

## Governance Relationship

```
            ../ROOT.md   (supreme — overrides everything below)
                 |
        ../DATA_CONTRACT.md   (authoritative: schema + quality rules)
                 |  validates before registration
                 v
    ../dataset_registry.json   <-- SINGLE SOURCE OF TRUTH
       |  (dataset metadata lives INSIDE each entry)
       |
       |  generated from / derived
       v
      ../DATA_CATALOG.md   (human-readable view — never authoritative)
```

Read this diagram top-down for **authority** and along the arrows for **data flow**:

- Contract rules gate what may enter the registry.
- The registry is the one place metadata is authored and stored.
- The catalog is rendered out of the registry and adds no authority of its own.

---

## Conflict Priority

When artifacts disagree, resolve in this exact order (highest first):

1. `../ROOT.md`
2. `../DATA_CONTRACT.md`
3. `../dataset_registry.json`
4. `../DATA_CATALOG.md`

**Registry beats catalog.** The catalog must never list an unregistered dataset, and a catalog that contradicts the registry is defective and must be regenerated, not reconciled by editing the registry.

---

## Synchronization Responsibility

Synchronization is a **single-change obligation**, not a follow-up task.

- **Whoever updates `../dataset_registry.json` MUST update `../DATA_CATALOG.md` in the same change.** A registry change that leaves the catalog stale is incomplete.
- The catalog is the dependent artifact: it is brought into line with the registry, never the reverse.
- The standing target is full generation of the catalog from the registry, so that synchronization is mechanical and cannot drift.

---

## Update Responsibility

Updates follow a propose-then-gate model:

- **The dataset owner proposes** the metadata entry or change.
- **The governance / validation process gates lifecycle transitions.** No state change reaches the registry without passing the gate.
- **The registry is updated only after `../DATA_CONTRACT.md` validation passes.** On any contract violation the process fails loud (per ROOT.md's "fail loud" principle); the registry and catalog are left unchanged.
- Every lifecycle transition is logged, and `status` in the registry must continue to equal the dataset's true lifecycle state after the update.

---

## Cross-References

- `../ROOT.md` — supreme governance document.
- `../DATA_CONTRACT.md` — authoritative schema and quality rules; validation gate.
- `../dataset_registry.json` — authoritative machine-readable source of truth.
- `../DATA_CATALOG.md` — derived human-readable view.
- `dataset_lifecycle.md` — lifecycle states and allowed transitions.
- `metadata_standard.md` — metadata field definitions for registry entries.
