# DATA_CATALOG.md

> **Skeleton.** Human-readable index of datasets in the Data Hub. This catalog
> is a **view** of the authoritative `dataset_registry.json`; when they differ,
> the registry is correct. Target: generate this file from the registry to
> prevent drift (see `HANDOFF.md` → *Future Recommendations*).

---

## How to read this catalog

Each dataset appears once with a summary row plus a link to its full contract
section in `DATA_CONTRACT.md` and its entry in `dataset_registry.json`.

| Field | Meaning |
|-------|---------|
| `dataset_id` | Unique identifier (authoritative key). |
| `name` | Human-readable name. |
| `version` | Semantic version of the dataset. |
| `status` | `draft` \| `active` \| `deprecated`. |
| `owner` | Responsible team or agent. |
| `description` | One-line summary. |

---

## Datasets

_No datasets registered yet._

Phase 0 establishes the catalog structure only. Datasets are added in Phase 1+
after review, each one registered in `dataset_registry.json` and contracted in
`DATA_CONTRACT.md` first.

<!--
Template row (copy when adding a dataset):

| dataset_id | name | version | status | owner | description |
|------------|------|---------|--------|-------|-------------|
| `example.dataset` | Example Dataset | v0.1.0 | draft | data-team | One-line summary. |
-->

---

## Catalog ↔ Registry contract

- Every row here corresponds to exactly one entry in `dataset_registry.json`.
- The registry is the **source of truth**; this catalog must never list a
  dataset that is not registered.
- Counts: **0** datasets registered (Phase 0).
