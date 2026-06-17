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

**2 datasets** registered in `dataset_registry.json`.

Each entry mirrors its registry record (including lifecycle `status`, which may be
`draft`). Entries use the template above and stay in sync with the registry.

### reference.universe.metadata

- **Name:** Universe Metadata
- **Description:** Lifecycle and contract metadata for all tradable instruments, supporting point-in-time reconstruction of the tradable universe.
- **Owner:** data-platform
- **Source:** api — https://fapi.binance.com/fapi/v1/exchangeInfo (Binance USD-M Futures)
- **Schema:** see `DATA_CONTRACT.md#contract-universe-metadata`
- **Update Frequency:** daily
- **Artifact:** `data/reference/universe_metadata/reference.universe.metadata.json`
- **Manifest:** `data/manifests/reference/universe_metadata/manifest.json`
- **Validation:** first draft artifact validated successfully (`active_current` coverage, 671 rows, checksum `fcee6a125792598d19e4332c3acd848dd4c7e49551e1f1cef2ad09a73b533b39`).
- **Known Issues:** Lifecycle remains `draft` and `contract_validated = false`; Phase 4 covers only current Binance USD-M Futures `TRADING` symbols. Historical delisted, renamed, and merged events are not covered.
- **Status:** draft

Full design: [`docs/universe_metadata_dataset.md`](docs/universe_metadata_dataset.md).

### market.binance.um.klines

- **Name:** Binance USD-M Futures Klines
- **Description:** Historical OHLCV Kline bars for Binance USD-M Futures from the Binance Data Vision public archive, parameterized by Kline interval.
- **Owner:** data-platform
- **Source:** file — https://data.binance.vision/data/futures/um/{monthly,daily}/klines/<SYMBOL>/<INTERVAL>/ (Binance Data Vision public archive)
- **Schema:** see `DATA_CONTRACT.md#contract-binance-usd-m-futures-klines`
- **Update Frequency:** daily
- **Primary key:** `[symbol, interval, open_time]`
- **Supported intervals:** `1d` · `4h` · `1h` · `15m` · `5m` · `1m` (first production interval `1d`)
- **Local data:** `local_data/binance_um_klines/interval=<INTERVAL>/` — **not committed** (large market data; machine-specific). See `docs/market_data_storage_policy.md`.
- **Validation:** `python -m datahub.validation --target binance-um-klines --interval 1d --manifest local_data/binance_um_klines/interval=1d/manifests/manifest.json`
- **Known Issues:** Lifecycle remains `draft` and `contract_validated = false`. Phase 5 verifies raw archive inventory + checksums only; row-level normalization / Parquet materialization is deferred to Phase 6. Full historical data is uncommitted.
- **Status:** draft

Full design: [`docs/binance_um_klines_dataset.md`](docs/binance_um_klines_dataset.md).

### market.binance.um.klines.1d.parquet

- **Name:** Binance USD-M Futures Klines 1D (Parquet)
- **Description:** DuckDB-queryable Hive-partitioned Parquet materialization of the Binance USD-M Futures 1D Kline raw archive.
- **Owner:** data-platform
- **Source:** derived — `market.binance.um.klines` (Parquet materialization of the raw 1D zip archive)
- **Schema:** see `DATA_CONTRACT.md#contract-binance-usd-m-futures-klines` (logical DuckDB schema adds `open_time_utc`, `open_time_taipei`, `date`, `year`, `month`, `source_archive`, `archive_source`, `archive_period`; `symbol`/`year` come from Hive partitions)
- **Update Frequency:** daily
- **Primary key:** `[symbol, interval, open_time]` (`[symbol, date]` also unique for 1D)
- **Layers:** raw zip archive = immutable source; Parquet = query/materialized layer; **DuckDB** = standard query engine. CSV is transient (inside zips only); `generated_csv_file_count = 0`.
- **Local data:** `local_data/binance_um_klines/interval=1d/parquet/` — **not committed** (large market data; machine-specific). See `docs/market_data_storage_policy.md`.
- **Materialize:** `python -m datahub.materialization.binance_um_klines_parquet --interval 1d --all --workers 4 --resume`
- **Validation:** `python -m datahub.validation --target binance-um-klines-parquet --interval 1d --manifest local_data/binance_um_klines/interval=1d/parquet/manifests/materialization_manifest.json`
- **Known Issues:** Lifecycle remains `draft` and `contract_validated = false`. Only the `1d` interval is materialized in Phase 6. Parquet output is uncommitted (machine-specific).
- **Status:** draft

Full design: [`docs/binance_um_klines_parquet_materialization.md`](docs/binance_um_klines_parquet_materialization.md) · access: [`docs/klines_access.md`](docs/klines_access.md).

---

## Catalog–Registry Contract

- Every catalog entry corresponds to exactly one entry in `dataset_registry.json`.
- The registry is the source of truth; the catalog never lists a dataset that is not registered.
- The `Status` shown here MUST equal the registry `status`, which MUST equal the dataset's true lifecycle state.
- Registered dataset count: **3**.
