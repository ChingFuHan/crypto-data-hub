# Universe Metadata Dataset

> Part of the **Phase 2 First Dataset Design**. Subordinate to [ROOT.md](../ROOT.md);
> if any rule here conflicts with a higher-priority document, the higher-priority
> document wins. This is the reference design every future dataset follows.

This document is the complete design specification for the **Universe Metadata**
dataset (`reference.universe.metadata`). It is the first concrete dataset in
crypto-data-hub and exists to **validate that the Phase 1 governance framework
(contract, metadata, registry, catalog, lifecycle) actually supports a real
dataset.**

> **Status note:** the dataset is still registered as `draft` with
> `quality.contract_validated = false`. Phase 4 produced the first validated
> draft artifact for current Binance USD-M Futures symbols, but this does not
> promote the dataset lifecycle to `active`.

---

## 1. Purpose

- **Dataset Goal** — Provide authoritative lifecycle and contract information for
  every tradable instrument across supported exchanges, and enable
  **point-in-time reconstruction of the tradable universe** at any past instant.
- **Business Purpose** — Eliminate survivorship bias in backtests, drive correct
  universe selection, and make symbol lifecycle events (listing, delisting,
  rename, merge) explicit and queryable.
- **Expected Consumer** — Backtesting/research engines, data pipelines, risk and
  universe-selection systems, and **downstream datasets** that need a stable
  instrument reference (this is a foundational reference dataset, typically
  *upstream* of others).
- **Expected Usage**
  - "Which instruments were tradable on exchange *X* at time *T*?"
  - Resolve renames/merges to a surviving instrument.
  - Filter active vs. delisted instruments; trace an instrument's full lifecycle.

### Supported symbol cases

| Case | How it is represented |
|------|-----------------------|
| **Active Symbol** | `status = active`, `delisted_at = null` (the only state with null `delisted_at`). |
| **Delisted Symbol** | `status = delisted`, `delisted_at` set. |
| **Renamed Symbol** | `status = renamed`, `delisted_at` set to the rename instant, `successor_id` → the new instrument's `instrument_id` (a new row carries the new `symbol`, listed at the rename instant). |
| **Merged Symbol** | `status = merged`, `delisted_at` set to the merge instant, `successor_id` → the surviving instrument's `instrument_id`. |

**Point-in-time universe at time `T`** = all rows where
`listed_at <= T AND (delisted_at IS NULL OR delisted_at > T)`.

This is correct **because** rule Q3 guarantees `delisted_at IS NULL` iff
`status = active`: every non-active incarnation (delisted, renamed, merged)
carries its cease-trading instant in `delisted_at`. A renamed/merged old row is
therefore excluded for `T >= delisted_at`, while its successor — listed at that
same instant — takes over, so no instrument is double-counted at any `T`.

---

## 2. Schema

**Primary key:** `[instrument_id]` (unique, non-null).
**Secondary uniqueness:** `(exchange, symbol, listed_at)` must be unique — guards
against duplicate symbol-eras when a ticker is reused after delisting.

Per the *Schema Definition Format* in [DATA_CONTRACT.md](../DATA_CONTRACT.md),
data-field logical types may include domain-appropriate types
(`timestamp`, `decimal`) beyond the registry-metadata vocabulary, with exact
behavior pinned by `unit` and `constraints`. Timestamps are UTC ISO 8601.

| Field | Type | Nullable | Unit | Constraints | Description |
|-------|------|----------|------|-------------|-------------|
| `instrument_id` | string | false | n/a | matches `^[a-z0-9]+(?:[._-][a-z0-9]+)*$`; **unique** | Stable surrogate key for **one tradable incarnation** (symbol-era); primary key. A rename/merge produces a **new** incarnation with a new `instrument_id`, linked back via the old row's `successor_id`. |
| `symbol` | string | false | n/a | non-empty | Exchange ticker for this incarnation (e.g. `BTCUSDT`). |
| `exchange` | string | false | n/a | lowercase venue code | Exchange / venue (e.g. `binance`, `coinbase`). |
| `base_asset` | string | true | n/a | null if not applicable | Base asset (e.g. `BTC`). |
| `quote_asset` | string | true | n/a | null if not applicable | Quote asset (e.g. `USDT`). |
| `market_type` | enum | false | n/a | `spot` \| `futures` \| `perpetual` \| `option` | Instrument class. |
| `contract_type` | enum | true | n/a | futures/perpetual ⇒ `linear`\|`inverse`; option ⇒ `call`\|`put`; **spot ⇒ null** | Derivative contract type. |
| `status` | enum | false | n/a | `active` \| `delisted` \| `renamed` \| `merged` | **Symbol** lifecycle state — see note below. |
| `listed_at` | timestamp | false | UTC | ISO 8601 UTC | When the instrument became tradable. |
| `delisted_at` | timestamp | true | UTC | ISO 8601 UTC; `NULL` **iff** `status = active`; when present `>= listed_at` | When trading ceased; `NULL` only while active. |
| `successor_id` | string | true | n/a | resolves to an existing `instrument_id` (not self); `NOT NULL` **iff** `status ∈ {renamed, merged}` | Instrument this was renamed / merged into. |
| `tick_size` | decimal | true | quote currency | `> 0` when present | Minimum price increment. |
| `step_size` | decimal | true | base asset | `> 0` when present | Minimum quantity increment. |
| `contract_size` | decimal | true | base asset per contract | `> 0` for derivatives; null for spot | Units of underlying per contract. |

> **Two distinct "status" concepts — do not conflate:**
> - The **dataset** lifecycle (`draft`/`active`/`deprecated`/`archived`) lives in
>   the registry entry's `status` field — see [dataset_lifecycle.md](dataset_lifecycle.md).
> - This dataset's **`status` column** (`active`/`delisted`/`renamed`/`merged`)
>   describes an individual **symbol's** lifecycle inside the data.

Required (non-null) fields: `instrument_id`, `symbol`, `exchange`,
`market_type`, `status`, `listed_at`. All others are nullable per the table.

---

## 3. Lifecycle

This section covers the **dataset** lifecycle (the registry `status`), per
[dataset_lifecycle.md](dataset_lifecycle.md).

| Stage | Meaning for Universe Metadata |
|-------|-------------------------------|
| **建立 / Create** | Registered as `draft` with the schema above. Phase 4 adds a validated draft artifact. |
| **更新 / Update** | After full ingestion + contract validation + review, moves `draft → active`. Refreshed **daily** as instruments list/delist/rename/merge. |
| **廢棄 / Deprecate** | `active → deprecated` if superseded by a better universe source; existing consumers keep reading, no new adoption. |
| **歸檔 / Archive** | `deprecated → archived`; frozen and retained for reproducibility (terminal). |

### Version Strategy

The dataset uses its own SemVer axis (independent of `registry_version`):

| Bump | Trigger for this dataset |
|------|--------------------------|
| **PATCH** | Data correction to existing rows (e.g. fixing a wrong `delisted_at`). |
| **MINOR** | Backward-compatible schema addition (e.g. a new nullable field). |
| **MAJOR** | Breaking change: remove/rename/retype a field, or change the primary key. |

Initial design version: **`v0.1.0`**, `status = draft`. The first validated
publication will advance the lifecycle to `active`.

---

## 4. Metadata

The dataset's metadata lives **inside its `dataset_registry.json` entry** (see
[metadata_standard.md](metadata_standard.md)). Key values:

| Field | Value |
|-------|-------|
| `dataset_id` | `reference.universe.metadata` |
| `dataset_name` | `Universe Metadata` |
| `version` | `v0.1.0` |
| `status` | `draft` |
| `owner` | `data-platform` |
| `source` | `type: api` — Binance USD-M Futures `exchangeInfo` |
| `timezone` | `UTC` |
| `update_frequency` | `daily` |
| `primary_key` | `["instrument_id"]` |
| `schema_ref` | `DATA_CONTRACT.md#contract-universe-metadata` |
| `tags` | `["reference", "universe", "symbols", "lifecycle"]` |

Phase 4 registry timestamps describe the validated draft artifact:
`earliest_timestamp = MIN(listed_at)` and `latest_timestamp = retrieved_at`.
`provenance` points to the raw snapshot, normalized artifact, and manifest.
`contract_validated` remains `false` until review decides lifecycle promotion
semantics.

---

## 5. Registry Mapping

The full registry entry is in [dataset_registry.json](../dataset_registry.json)
under `datasets[]`. It conforms to `dataset_entry_schema`
(see [registry_standard.md](registry_standard.md)). Mapping highlights:

- `primary_key` in the entry = `["instrument_id"]`, matching the schema PK above.
- `schema_ref` points to this dataset's contract section in `DATA_CONTRACT.md`.
- `status = draft` and `quality.contract_validated = false` because no data has
  been validated for lifecycle promotion yet. The Phase 4 artifact itself has
  passed fixture validation.
- `lineage.upstream = []` — Universe Metadata is a **root** reference dataset; its
  sources are external exchange APIs, not other registered datasets.
- The human-readable [DATA_CATALOG.md](../DATA_CATALOG.md) entry is the derived
  view of this registry record and must stay in sync with it.

### Phase 4 Artifact

Current validated draft artifact:

- Raw snapshot:
  `data/raw/reference/universe_metadata/exchange_info_20260616T170138Z_d4d2d2ab1c6e.json`
- Normalized artifact:
  `data/reference/universe_metadata/reference.universe.metadata.json`
- Manifest:
  `data/manifests/reference/universe_metadata/manifest.json`
- Coverage: `active_current`
- Rows: 671
- Artifact checksum:
  `fcee6a125792598d19e4332c3acd848dd4c7e49551e1f1cef2ad09a73b533b39`

Coverage status is recorded in the manifest/provenance layer and is separate
from row `status`. Phase 4 emits only `active` rows derived from Binance
`status = TRADING`.

---

## 6. Quality Rules

Dataset-specific rules, drawn from the categories in
[DATA_CONTRACT.md](../DATA_CONTRACT.md) (*Validation Policy*). All violations
**fail loud**; nothing is silently coerced.

| # | Rule | Category | Assertion |
|---|------|----------|-----------|
| Q1 | **Missing Value** | Completeness | Required fields (`instrument_id`, `symbol`, `exchange`, `market_type`, `status`, `listed_at`) are present and non-null. |
| Q2 | **Duplicate Symbol** | Uniqueness | `instrument_id` is unique; `(exchange, symbol, listed_at)` is unique; **at most one** `status = active` row per `(exchange, symbol)`. |
| Q3 | **Invalid Lifecycle** | Consistency | `delisted_at IS NULL` **iff** `status = active`; `successor_id IS NOT NULL` **iff** `status ∈ {renamed, merged}` (and resolves to an existing `instrument_id`). So `delisted`/`renamed`/`merged` all carry `delisted_at`; `active`/`delisted` carry null `successor_id`. |
| Q4 | **Invalid Timestamp** | Range / domain | `listed_at`, `delisted_at` are valid UTC ISO 8601 with offset; when `delisted_at` is present, `delisted_at >= listed_at`; neither is future-dated beyond the ingestion instant. |
| Q5 | **Invalid Contract Information** | Range / domain | `spot` ⇒ `contract_type IS NULL`; `market_type ∈ {futures, perpetual}` ⇒ `contract_type ∈ {linear, inverse}`; `market_type = option` ⇒ `contract_type ∈ {call, put}`; derivatives ⇒ `contract_size > 0`; `tick_size`, `step_size > 0` when present. |
| Q6 | **Referential** | Referential | Every non-null `successor_id` resolves to an existing `instrument_id`, is not self-referential (`!= instrument_id`), and the rename/merge successor graph is acyclic and terminates at an `active` or `delisted` row. |

These rules are recorded in the dataset's contract section of
`DATA_CONTRACT.md` and are checked **before** the dataset may move
`draft → active`.

Phase 4 validation command:

```bash
python -m datahub.validation --target universe-metadata --fixture data/reference/universe_metadata/reference.universe.metadata.json
```

Offline deterministic verification:

```bash
python -m datahub.ingestion.universe_metadata --offline --all
python -m datahub.validation --all
python -m unittest discover tests
```

---

## Cross-References

- [ROOT.md](../ROOT.md) — supreme governance and data-integrity principles.
- [DATA_CONTRACT.md](../DATA_CONTRACT.md) — the Universe Metadata contract + framework.
- [dataset_registry.json](../dataset_registry.json) — authoritative registry entry.
- [DATA_CATALOG.md](../DATA_CATALOG.md) — derived human-readable catalog entry.
- [universe_metadata_sources.md](universe_metadata_sources.md) — source authority review and Phase 4 ingestion decisions.
- [metadata_standard.md](metadata_standard.md) · [registry_standard.md](registry_standard.md) · [dataset_lifecycle.md](dataset_lifecycle.md) · [naming_convention.md](naming_convention.md) — the governance standards this design exercises.
