# DATA_CONTRACT.md

> Part of the **Phase 1 Data Governance Foundation**, subordinate to `ROOT.md`.
> This document is the **Dataset Contract Framework**: the authoritative
> definition of the schema and quality rules every dataset must satisfy before
> it is trusted, published, and registered.

This document is **second only to `ROOT.md`** in the rule priority order
(`ROOT.md` > `DATA_CONTRACT.md` > `dataset_registry.json` > `DATA_CATALOG.md`).
A lower-priority document may add detail but must never contradict this one. If
it does, this document is correct and the lower one must be fixed. Where this
document and `ROOT.md` conflict, `ROOT.md` wins.

---

## Purpose

A **data contract** is the explicit, enforceable agreement about a dataset's
structure and quality. It is the gate between *candidate data* and *trusted
data*. Data that fails its contract is **rejected**, never silently coerced into
compliance (`ROOT.md` → *Data Integrity Principles*).

A dataset is trusted — and eligible to transition from `draft` to `active`
(see `docs/dataset_lifecycle.md`) — only when **all** of the following hold:

1. It conforms to a defined schema (see *Schema Definition Format*), **and**
2. It passes every declared quality rule (see *Validation Policy*), **and**
3. Its contract validation result is recorded
   (`quality.contract_validated = true`, `quality.last_validated_at` set), **and**
4. It is registered in `dataset_registry.json`.

The registry (`dataset_registry.json`) is the single source of truth for *which*
datasets exist and their metadata. This document is the single source of truth
for *what a dataset's data must look like and prove* to be considered valid.

---

## Schema Definition Format

Every dataset declares a schema as an ordered list of field definitions. Each
field is defined by exactly these six attributes:

| Attribute | Required | Meaning |
|-----------|----------|---------|
| `name` | yes | Field name. snake_case. Unique within the dataset. |
| `type` | yes | Logical field type from the vocabulary below. |
| `nullable` | yes | `true` or `false`. Whether null/missing is permitted for this field. Explicit — never assumed. |
| `unit` | yes | Unit of measure where applicable (e.g. `USD`, `satoshi`, `seconds`); use `null` (or `n/a`) when the field is unitless. |
| `constraints` | yes | Range, enum, regex, referential, or invariant rules the field must satisfy. Use `none` when there are no constraints beyond type and nullability. |
| `description` | yes | Plain-language meaning of the field. |

**Field type vocabulary** (use these names only; aligned with
`dataset_registry.json` → `dataset_entry_schema`):
`string`, `enum`, `object`, `array<string>`, `string|null`, `boolean`.
For numeric, temporal, or other physical quantities, declare the logical
`type` as appropriate to the dataset and pin exact behavior through `unit` and
`constraints` (e.g. a UTC instant is a `string` typed field carrying an
ISO 8601 value constrained to UTC offset; see *Timezone Policy*).

Schema rules:

- The schema is **complete**: every field present in the data must appear in the
  contract, and no field in the contract may be silently absent from the data.
- The schema is **ordered and stable**: field order is part of the contract.
- Adding a new field is a **MINOR** dataset version change; removing, renaming,
  or retyping a field is a **MAJOR** change (see *Version Policy*).
- A dataset's schema is referenced from its registry entry via `schema_ref`,
  which points to that dataset's section in this document.

---

## Primary Key Rules

- Every dataset **must** declare a primary key of **one or more fields**, mirrored
  in its registry entry's `primary_key` (`array<string>`).
- The primary key is **unique**: no two records may share the same primary-key
  tuple.
- Every primary-key field is **non-null**: primary-key fields must have
  `nullable = false` in the schema. A nullable field may never be part of a
  primary key.
- For time-series datasets, the primary key typically includes the UTC timestamp
  field (and any entity/instrument discriminator needed for uniqueness).
- Primary-key membership is part of the contract. Changing the primary key is a
  **MAJOR** dataset version change.

---

## Null Policy

- **Nullability is explicit per field** via the schema `nullable` attribute. A
  field is nullable **only** if its contract says so.
- **Required fields are never null.** Any field with `nullable = false` that is
  absent or null in the data is a contract violation.
- **No silent null-fill or coercion.** Missing or malformed values must not be
  imputed, defaulted, zero-filled, forward-filled, `NaN`-filled, or otherwise
  fabricated to pass validation. Such data **fails loud** (`ROOT.md` →
  *Fail loud*).
- A legitimately-absent value is permitted **only** where the contract declares
  `nullable = true`; the meaning of null for that field must be stated in its
  `description`.

---

## Timezone Policy

- **All timestamps are stored in UTC**, formatted as **ISO 8601 with an explicit
  offset** (e.g. `2026-06-16T00:00:00Z` / `+00:00`).
- The **source timezone semantics** of the data are recorded in the registry
  entry's `timezone` field (IANA name, e.g. `UTC`, `America/New_York`). Storage
  remains UTC regardless of the source timezone.
- Timestamps without a timezone offset are **invalid** and fail validation; no
  timezone is ever inferred or assumed silently.
- For time-series datasets, `earliest_timestamp` and `latest_timestamp` in the
  registry are required and likewise expressed as UTC ISO 8601.

---

## Version Policy

- A contract is **versioned together with its dataset** using SemVer
  (`vMAJOR.MINOR.PATCH`, v-prefixed; pattern `^v[0-9]+\.[0-9]+\.[0-9]+$`).
- Dataset version increments:
  - **PATCH** — data correction with no schema change.
  - **MINOR** — backward-compatible schema addition (e.g. a new nullable field).
  - **MAJOR** — breaking schema change (remove/rename/retype a field, change
    nullability of an existing field, or change the primary key).
- **Published snapshots stay bound to the contract version they passed.** A
  snapshot is immutable and references the exact contract under which it was
  validated; it is never retroactively re-bound to a newer contract.
- Changing a contract is a **reviewed, logged** change. Correct forward with a
  new version; never silently rewrite a published contract
  (`ROOT.md` → *Immutability of records*).
- The shape of *this framework itself* (the metadata/registry contract) is
  tracked by `registry_version` in `dataset_registry.json`, not by a dataset
  version.

---

## Validation Policy

- **Validate before register.** Data is validated against this contract
  **before** its registry entry is created or moved to `active`. The registry is
  updated **only after** validation passes.
- **Fail loud on any violation.** A schema, primary-key, null, timezone, or
  quality-rule violation stops the pipeline with a clear, actionable error.
  Silent truncation, coercion, or `NaN`-filling is forbidden.
- **Record the result.** On success, set `quality.contract_validated = true` and
  `quality.last_validated_at` to the validation timestamp (UTC ISO 8601) in the
  registry entry. A dataset with `contract_validated = false` is **not** trusted
  and must not be `active`.
- **Quality rule categories** every dataset contract draws from:

  | Category | Asserts that… |
  |----------|---------------|
  | Completeness | Required (`nullable = false`) fields are present and non-null. |
  | Uniqueness | Primary-key tuples are unique across all records. |
  | Range / domain | Numeric values fall in bounds; enum fields use allowed values only. |
  | Freshness | Data is no older than a stated threshold (for time-series / updating datasets). |
  | Referential | Foreign keys / upstream references resolve to an existing registered `dataset_id` (`lineage.upstream`). |
  | Consistency | Cross-field invariants hold (e.g. `high >= low`, `close > 0`). |

- **Provenance is mandatory.** Validation includes confirming the registry entry
  carries sufficient `provenance` (`code_version`, `params`, `generated_by`,
  `checksum`) to reproduce the dataset (`ROOT.md` → *Reproducibility*).

---

## Dataset Contract Template

Copy this skeleton into a per-dataset section of this document when defining a
new contract. Fill every placeholder; do not leave a section blank. The
`schema_ref` in the dataset's `dataset_registry.json` entry must point here.

```markdown
### Contract: <dataset_id>   <!-- e.g. market.btc_usd.ohlcv_1h -->

**Dataset name:** <Title Case Name>
**Contract version:** v<MAJOR.MINOR.PATCH>   <!-- moves with the dataset version -->
**Owner:** <team or agent>
**Status:** <draft | active | deprecated | archived>
**Source:** <type: api|file|onchain|derived> — <reference>
**Source timezone:** <IANA tz, e.g. UTC>   (storage: UTC, ISO 8601)
**Primary key:** [<field>, ...]   <!-- unique, non-null -->

#### Schema

| name | type | nullable | unit | constraints | description |
|------|------|----------|------|-------------|-------------|
| <field_1> | <type> | <true\|false> | <unit\|null> | <constraints\|none> | <meaning> |
| <field_2> | <type> | <true\|false> | <unit\|null> | <constraints\|none> | <meaning> |
| ...       |        |              |      |             |             |

#### Quality Rules

- **Completeness:** <which fields must always be present / non-null>
- **Uniqueness:** <primary-key uniqueness statement>
- **Range / domain:** <numeric bounds and enum allow-lists>
- **Freshness:** <max acceptable age, or "n/a (static)">
- **Referential:** <upstream dataset_ids that must resolve, or "none">
- **Consistency:** <cross-field invariants, e.g. high >= low>

#### Provenance

- **code_version:** <repo version that generated the data>
- **params:** <parameters needed to reproduce>
- **generated_by:** <process or agent>
- **checksum:** <content hash>

#### Snapshot Policy

- Snapshot identity = `<dataset_id>` + version + UTC timestamp.
- Immutable once published; checksum recorded; never modified in place
  (ROOT.md → Snapshot Principles).
- Bound to this contract version; re-validation under a new contract version
  produces a new snapshot, not a mutation of the old one.
```

---

## Status

| Item | State |
|------|-------|
| Contract framework defined (this document) | Done (v0.2.0) |
| Concrete dataset contracts | 1 defined — Universe Metadata (`reference.universe.metadata`) |
| Automated validation tooling | Initial foundation done (v0.4.0; registry/lifecycle/naming + Universe Metadata fixtures) |
| Machine-readable schema (JSON Schema) | Pending (later phase, after review) |

The framework and reusable template are defined above. The first concrete
contract — **Universe Metadata** — is defined below as a `draft`; Phase 4
produced a validated draft artifact, but the dataset is **not yet
contract-validated for lifecycle promotion** (`quality.contract_validated =
false`). A contract moves its dataset `draft → active` only after passing the
*Validation Policy* and review.

---

## Contract: Universe Metadata

> Concrete dataset contract. The registry entry for `reference.universe.metadata`
> references this section via `schema_ref`. Full design rationale and the
> point-in-time reconstruction model are in
> [docs/universe_metadata_dataset.md](docs/universe_metadata_dataset.md).

**Dataset name:** Universe Metadata
**Dataset ID:** `reference.universe.metadata`
**Contract version:** `v0.1.0`  (moves with the dataset version)
**Owner:** `data-platform`
**Status:** `draft`  (dataset lifecycle; not yet validated)
**Description:** Lifecycle and contract metadata for all tradable instruments,
supporting point-in-time reconstruction of the tradable universe.
**Source:** `api` — exchange instrument / `exchangeInfo` endpoints (aggregated)
**Source timezone:** `UTC`  (storage: UTC, ISO 8601)
**Primary key:** `[instrument_id]`  (unique, non-null)
**Secondary uniqueness:** `(exchange, symbol, listed_at)` unique.

> Per *Schema Definition Format*, data-field logical types include the
> domain-appropriate `timestamp` and `decimal` types pinned by `unit` and
> `constraints`. Timestamps are UTC ISO 8601 with offset.

### Schema

| name | type | nullable | unit | constraints | description |
|------|------|----------|------|-------------|-------------|
| `instrument_id` | string | false | n/a | matches `^[a-z0-9]+(?:[._-][a-z0-9]+)*$`; unique | Stable surrogate key for one tradable incarnation (symbol-era); primary key. A rename/merge produces a new incarnation with a new `instrument_id`, linked via the old row's `successor_id`. |
| `symbol` | string | false | n/a | non-empty | Exchange ticker for this incarnation (e.g. `BTCUSDT`). |
| `exchange` | string | false | n/a | lowercase venue code | Exchange / venue (e.g. `binance`). |
| `base_asset` | string | true | n/a | null if not applicable | Base asset (e.g. `BTC`). |
| `quote_asset` | string | true | n/a | null if not applicable | Quote asset (e.g. `USDT`). |
| `market_type` | enum | false | n/a | `spot` \| `futures` \| `perpetual` \| `option` | Instrument class. |
| `contract_type` | enum | true | n/a | futures/perpetual ⇒ `linear`\|`inverse`; option ⇒ `call`\|`put`; spot ⇒ null | Derivative contract type. |
| `status` | enum | false | n/a | `active` \| `delisted` \| `renamed` \| `merged` | Symbol lifecycle state (distinct from the dataset lifecycle). |
| `listed_at` | timestamp | false | UTC | ISO 8601 UTC | When the instrument became tradable. |
| `delisted_at` | timestamp | true | UTC | ISO 8601 UTC; `NULL` iff `status = active`; when present `>= listed_at` | When trading ceased; `NULL` only while active. |
| `successor_id` | string | true | n/a | resolves to an existing `instrument_id` (not self); `NOT NULL` iff `status ∈ {renamed, merged}` | Instrument this was renamed / merged into. |
| `tick_size` | decimal | true | quote currency | `> 0` when present | Minimum price increment. |
| `step_size` | decimal | true | base asset | `> 0` when present | Minimum quantity increment. |
| `contract_size` | decimal | true | base asset per contract | `> 0` for derivatives; null for spot | Units of underlying per contract. |

### Null Policy (dataset-specific)

Required (non-null): `instrument_id`, `symbol`, `exchange`, `market_type`,
`status`, `listed_at`. All other fields are nullable as marked. Nulls are never
imputed or coerced — a missing required value fails loud (`ROOT.md` → *Fail loud*).

### Quality Rules

| # | Rule | Category | Assertion |
|---|------|----------|-----------|
| Q1 | Missing Value | Completeness | Required fields are present and non-null. |
| Q2 | Duplicate Symbol | Uniqueness | `instrument_id` unique; `(exchange, symbol, listed_at)` unique; at most one `status = active` row per `(exchange, symbol)`. |
| Q3 | Invalid Lifecycle | Consistency | `delisted_at IS NULL` **iff** `status = active`; `successor_id IS NOT NULL` **iff** `status ∈ {renamed, merged}`. (So `delisted`/`renamed`/`merged` all carry `delisted_at`; `active`/`delisted` carry null `successor_id`.) |
| Q4 | Invalid Timestamp | Range / domain | `listed_at`/`delisted_at` valid UTC ISO 8601; when `delisted_at` present, `delisted_at >= listed_at`; neither future-dated beyond the ingestion instant. |
| Q5 | Invalid Contract Information | Range / domain | `spot` ⇒ `contract_type IS NULL`; `market_type ∈ {futures, perpetual}` ⇒ `contract_type ∈ {linear, inverse}`; `market_type = option` ⇒ `contract_type ∈ {call, put}`; derivatives ⇒ `contract_size > 0`; `tick_size`/`step_size > 0` when present. |
| Q6 | Referential | Referential | every non-null `successor_id` resolves to an existing `instrument_id`, is not self-referential (`!= instrument_id`), and the rename/merge successor graph is acyclic and terminates at an `active` or `delisted` row. |

> Phase 4 note: Binance USD-M Futures `exchangeInfo` does not expose a separate
> contract-size field. The MVP artifact uses `contract_size = 1` as a documented
> normalization convention for USD-M linear futures where the quantity unit is
> the base asset. This keeps Q5 executable without inventing source-derived
> lifecycle facts.

### Provenance & Snapshot

- **Provenance** (design stage): `code_version = v0.3.0`, `params` records the
  exchange set + `as_of`, `generated_by = design (no ingestion in Phase 2)`,
  `checksum` empty until first artifact.
- **Provenance** (Phase 4 draft artifact): `code_version = v0.5.0`, source =
  Binance USD-M Futures `exchangeInfo`, manifest =
  `data/manifests/reference/universe_metadata/manifest.json`, normalized
  artifact =
  `data/reference/universe_metadata/reference.universe.metadata.json`,
  checksum =
  `fcee6a125792598d19e4332c3acd848dd4c7e49551e1f1cef2ad09a73b533b39`.
- **Snapshot policy:** on first publication, snapshot identity =
  `reference.universe.metadata` + version + UTC timestamp; immutable, checksummed,
  bound to this contract version (`ROOT.md` → *Snapshot Principles*).
