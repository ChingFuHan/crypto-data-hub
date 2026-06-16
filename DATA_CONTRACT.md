# DATA_CONTRACT.md

> **Skeleton.** Defines the rules every dataset must satisfy to be trusted and
> registered. Second only to `ROOT.md` in the rule priority order. No concrete
> dataset contracts exist yet (Phase 0) — this defines the shape they take.

---

## Purpose

A **data contract** is the explicit, enforceable agreement about a dataset's
structure and quality. Data that fails its contract is **rejected**, never
silently coerced into compliance (ROOT.md → *Data Integrity Principles*).

A dataset is trusted only when:

1. It conforms to a defined schema (below), **and**
2. It passes all quality rules (below), **and**
3. It is registered in `dataset_registry.json`.

---

## Contract Sections (template per dataset)

Each registered dataset has a contract with these sections:

### 1. Identity

- `dataset_id` — unique, matches `^[a-z0-9]+(?:[._-][a-z0-9]+)*$`
- `name`, `description`, `version` (SemVer), `owner`

### 2. Schema

Field-by-field definition. Each field specifies:

| Attribute | Meaning |
|-----------|---------|
| `name` | Field name |
| `type` | Logical type (e.g. `string`, `integer`, `decimal`, `timestamp`, `bool`) |
| `nullable` | Whether null/missing is permitted |
| `unit` | Unit where applicable (e.g. USD, satoshi, seconds) |
| `constraints` | Range, enum, regex, or referential rules |
| `description` | What the field means |

### 3. Quality Rules

Explicit, checkable assertions. Examples of rule categories:

- **Completeness** — required fields are present and non-null.
- **Uniqueness** — primary key / identifier columns are unique.
- **Range / domain** — numeric bounds, allowed enum values.
- **Freshness** — data is no older than a stated threshold.
- **Referential** — foreign keys resolve to an existing registered dataset.
- **Consistency** — cross-field invariants hold (e.g. `high >= low`).

### 4. Provenance

- `source` (type + reference), `code_version`, generation `params`.
- Must be sufficient to **reproduce** the dataset (ROOT.md → Reproducibility).

### 5. Snapshot Policy

- Snapshot identity, immutability, and checksum requirements per ROOT.md
  → *Snapshot Principles*.

---

## Enforcement Principles

- **Fail loud** — a contract violation stops the pipeline with a clear error.
  Silent truncation, coercion, or `NaN`-filling is forbidden.
- **Validate before register** — data is validated against this contract
  *before* it is written to `dataset_registry.json`.
- **Contract is versioned** — changing a contract is a versioned, reviewed
  change; existing snapshots remain bound to the contract version they passed.

---

## Status

| Item | State |
|------|-------|
| Contract template defined | ✅ (this skeleton) |
| First concrete dataset contract | ⬜ (Phase 1+, after review) |
| Automated validation tooling | ⬜ (Phase 1+, after review) |
| Machine-readable schema (JSON Schema) | ⬜ (Phase 1+, after review) |
