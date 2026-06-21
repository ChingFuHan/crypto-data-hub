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
| Concrete dataset contracts | 2 defined — Universe Metadata (`reference.universe.metadata`), Binance USD-M Klines (`market.binance.um.klines`) |
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

---

## Contract: Binance USD-M Futures Klines

> Concrete dataset contract for the parameterized Binance USD-M Futures Kline
> family and its Parquet materialized interval datasets. The registry entries
> for `market.binance.um.klines` and
> `market.binance.um.klines.<INTERVAL>.parquet` reference this section via
> `schema_ref`. Full design is in
> [docs/binance_um_klines_dataset.md](docs/binance_um_klines_dataset.md) and
> [docs/binance_um_klines_parquet_materialization.md](docs/binance_um_klines_parquet_materialization.md).

**Dataset name:** Binance USD-M Futures Klines
**Dataset ID:** `market.binance.um.klines`  (raw family; materialized variants:
`market.binance.um.klines.<INTERVAL>.parquet`)
**Contract version:** `v0.1.0`  (moves with the dataset version)
**Owner:** `data-platform`
**Status:** `draft`  (raw and Parquet artifacts are validated draft outputs)
**Description:** Historical OHLCV Kline bars for Binance USD-M Futures, ingested
from the Binance Data Vision public archive and materialized by interval.
**Source:** `file` — Binance Data Vision public archive
(`https://data.binance.vision/data/futures/um/{monthly,daily}/klines/<SYMBOL>/<INTERVAL>/`)
**Source timezone:** `UTC`  (storage: UTC, ISO 8601 / epoch milliseconds)
**Primary key:** `[symbol, interval, open_time]`  (unique, non-null)
**Supported raw intervals:** `1d` · `4h` · `1h` · `15m` · `5m` · `3m` · `1m`
**Materialized Parquet intervals:** `1d` · `4h` · `1h` · `15m` · `5m` · `3m` · `1m`

> **Kline interval vs archive package source.** The Kline `interval` is the row
> period. The archive package source (`monthly` historical base / `daily` recent
> delta) is only how Binance packages files; it is **not** a Kline interval and
> is **not** part of the primary key.

### Schema (Parquet logical row as DuckDB exposes it)

| name | type | nullable | unit | constraints | description |
|------|------|----------|------|-------------|-------------|
| `symbol` | string | false | n/a | non-empty; Binance symbol | Trading symbol (e.g. `BTCUSDT`). Exposed from Hive partition path. |
| `interval` | enum | false | n/a | `1d`\|`4h`\|`1h`\|`15m`\|`5m`\|`3m`\|`1m` | Kline interval. From archive path, not a CSV column. |
| `open_time` | timestamp | false | ms epoch (UTC) | `>= 0`; aligned to interval | Bar open time. Part of primary key. |
| `open_time_utc` | timestamp | false | UTC wall-clock | derived from `open_time` | Bar open time as UTC timestamp. |
| `open_time_taipei` | timestamp | false | Asia/Taipei wall-clock | derived from `open_time` | Bar open time in Taipei wall-clock time. |
| `date` | string | false | Asia/Taipei date | `YYYY-MM-DD`; equals `CAST(open_time_taipei AS DATE)` | Date partition/grouping policy for research queries. |
| `year` | string | false | Asia/Taipei year | derived from `open_time_taipei`; exposed from Hive partition path | Year partition. |
| `month` | string | false | Asia/Taipei month | `1..12`; derived from `open_time_taipei` | Month grouping field. |
| `open` | decimal | false | quote | `> 0` | Open price. |
| `high` | decimal | false | quote | `>= open, close, low` | High price. |
| `low` | decimal | false | quote | `<= open, close, high` | Low price. |
| `close` | decimal | false | quote | `> 0` | Close price. |
| `volume` | decimal | false | base | `>= 0` | Base-asset volume. |
| `close_time` | timestamp | false | ms epoch (UTC) | `open_time + interval_ms - 1` | Bar close time. |
| `quote_volume` | decimal | false | quote | `>= 0` | Quote-asset volume. |
| `trade_count` | integer | false | n/a | `>= 0` | Number of trades. |
| `taker_buy_base_volume` | decimal | false | base | `>= 0, <= volume` | Taker buy base volume. |
| `taker_buy_quote_volume` | decimal | false | quote | `>= 0, <= quote_volume` | Taker buy quote volume. |
| `source_archive` | string | false | path | source zip path | Raw zip archive that supplied the retained row. |
| `archive_source` | enum | false | n/a | `monthly`\|`daily` | Binance archive package source. |
| `archive_period` | string | false | n/a | `YYYY-MM` or `YYYY-MM-DD` | Monthly or daily archive period. |

> The raw archive CSV also carries a trailing `ignore` column; it is a
> deprecated Binance field and is dropped from the normalized schema. Older
> archives have no header row; newer files may include one. Parsing is
> positional because Binance header names differ from the canonical Parquet
> names above.

### Quality Rules

| # | Rule | Category | Assertion |
|---|------|----------|-----------|
| K1 | Completeness | Completeness | All non-null fields present; no row dropped silently. |
| K2 | Uniqueness | Uniqueness | `(symbol, interval, open_time)` unique across all rows. |
| K3 | Range / domain | Range / domain | Prices `> 0`; volumes `>= 0`; `trade_count >= 0`; `interval` in the supported set. |
| K4 | OHLC consistency | Consistency | `high >= max(open, close)`; `low <= min(open, close)`; taker volumes `<=` totals. |
| K5 | Time consistency | Consistency | `date` equals the Taipei date of `open_time_taipei`; `open_time` is interval-aligned; `close_time = open_time + interval_ms - 1`. |
| K6 | Interval date policy | Consistency | `(symbol, date)` has at most `1d`: 1 row, `4h`: 6 rows, `1h`: 24 rows, `15m`: 96 rows, `5m`: 288 rows, `3m`: 480 rows, `1m`: 1440 rows. |
| K7 | Archive integrity | Completeness | Every downloaded archive zip matches its published `.CHECKSUM` (SHA-256). Mismatch fails loud. |
| K8 | Referential | Referential | `symbol` is cross-checkable against `reference.universe.metadata` but is **not** constrained to the current active universe (the archive includes delisted symbols). |

### Provenance & Snapshot

- **Raw provenance:** `generated_by = datahub.ingestion.binance_um_klines`;
  `params` records the interval, archive sources, daily-delta policy, and
  `local_root`. Per-file SHA-256 checksums are recorded in
  `local_data/binance_um_klines/interval=<INTERVAL>/manifests/`.
- **Parquet provenance:** `generated_by =
  datahub.materialization.binance_um_klines_parquet`; `params` records the
  interval, partition layout, DuckDB query engine, output root, manifest, and
  validation command. Full historical market data remains under `local_data/`
  and is uncommitted / machine-specific.
- **Snapshot policy:** snapshots are deferred until a future phase publishes an
  immutable, content-addressable materialization (`ROOT.md` -> *Snapshot
  Principles*); current raw and Parquet artifacts are local draft outputs.
