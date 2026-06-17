# Changelog

All notable changes to this repository are documented here.

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/);
this project adheres to [Semantic Versioning](https://semver.org/) (`vMAJOR.MINOR.PATCH`).

---

## [v0.8.0] — 2026-06-18

### Added — Phase 7: Binance UM 4H Kline Parquet Materialization

- **Interval-aware materializer** — `binance_um_klines_parquet.py` extended from
  1D-only to `--interval {1d,4h}`. Raw-root / manifest / output-root defaults are
  now derived from the interval, and `materialized_dataset_id` is
  `market.binance.um.klines.<interval>.parquet`. 1D behaviour is preserved
  (regression-tested).
- **4H time policy** — `open_time % 14_400_000 == 0` and
  `close_time = open_time + 14_399_999`, enforced at materialization
  (`find_time_rule_violations`, `--strict` fails) and re-checked in validation
  (`PQ-OPEN-TIME-ALIGNMENT`, `PQ-CLOSE-TIME-RULE`).
- **4H key policy** — unique key stays `(symbol, interval, open_time)`; `(symbol,
  date)` becomes a grouping column with **≤ 6 rows/day** (1D stays ≤ 1). Replaces
  the 1D-only duplicate-date check with a per-interval cardinality limit
  (`PQ-SYMBOL-DATE-LIMIT`).
- **4H Parquet dataset** at
  `local_data/binance_um_klines/interval=4h/parquet/` — `FULL_OUTPUT`,
  `symbol_count = 921`, `failed_symbol_count = 0`, `generated_csv_file_count = 0`,
  same Hive layout (`symbol=<S>/year=<Y>/part-000.parquet`).
- **Validation** — `binance-um-klines-parquet --interval 4h`; adds
  `PQ-FULL-SYMBOL-COVERAGE` and a `PQ-4H-ROWS-GT-1D` regression (4h row_count must
  exceed 1D) gated to production FULL_OUTPUT.
- **Dataset registration** — `market.binance.um.klines.4h.parquet` registered as
  `draft` with `DATA_CATALOG.md` entry.
- **Tests** — header/header-less 4H CSV, 4h alignment + close-time, ≤6 rows/day
  legal, >6 rows/day detected, strict conflict, resume skip, DuckDB read, and 1D
  regression. `82` tests pass.

---

## [v0.7.0] — 2026-06-17

### Added — Phase 6: Binance UM 1D Kline Parquet Materialization

- **Materialization pipeline** at
  `datahub/materialization/binance_um_klines_parquet.py` that transforms the
  immutable raw 1D zip archive into a DuckDB-queryable, Hive-partitioned Parquet
  dataset. CLI:
  `python -m datahub.materialization.binance_um_klines_parquet --interval 1d --all`
  with `--symbols`, `--resume`, `--overwrite`, `--workers`, `--strict`.
- **Layer model** — raw zip = immutable source; Parquet = query/materialized
  layer; DuckDB = standard query engine. CSV is transient (inside zips only); no
  persistent CSV is written (`generated_csv_file_count = 0`).
- **Positional CSV parsing** (header and header-less archives), fixed normalized
  schema, `trade_count` nullable integer, `ignore` column dropped.
- **Date policy** — `date` derived from the Asia/Taipei calendar date;
  `open_time_utc` / `open_time_taipei` stored as naive timestamps so
  `CAST(open_time_taipei AS DATE)` is session-timezone-independent.
- **Partition layout** — `symbol=<S>/year=<Y>/part-000.parquet`; `symbol` and
  `year` exposed via `hive_partitioning = true` and omitted from physical files
  to avoid duplicate/ambiguous columns.
- **Dedup / conflict policy** — key `(symbol, interval, open_time)`; daily wins
  over monthly on conflict; identical bars deduplicated; same-source
  inconsistencies and OHLC violations recorded; `--strict` fails on any.
- **Manifests + reports** — `materialization_manifest.json` (with
  `output_scope` FULL_OUTPUT vs SAMPLE_OUTPUT), `coverage_report.json`,
  `data_quality_report.json`, `duplicate_report.json`, `conflict_report.json`,
  and per-symbol resume sidecars — all under `local_data/` (uncommitted).
- **Validation target** `binance-um-klines-parquet` (`PQ-*` rules) querying the
  Parquet layer through DuckDB (logical schema, key/date uniqueness, null, OHLC,
  date policy, manifest-vs-actual counts, CSV absence). Requires an explicit
  `--manifest`; clone-safe `--all` validates it only when present.
- **Dataset registration** — `market.binance.um.klines.1d.parquet` registered as
  `draft` (derived from `market.binance.um.klines`) with a `DATA_CATALOG.md`
  entry.
- **Documentation** — `docs/binance_um_klines_parquet_materialization.md` and
  `docs/klines_access.md`.
- **Dependencies** — `duckdb` (query engine) and `pyarrow` (writer); the CLI
  emits a clear error and non-zero exit when a dependency is missing.

---

## [v0.6.0] — 2026-06-16

### Added — Phase 5: Binance USD-M Kline Historical Pipeline

- **Parameterized Kline ingestion pipeline** at
  `datahub/ingestion/binance_um_klines.py`, driven by Kline `interval`
  (`1d` / `4h` / `1h` / `15m` / `5m` / `1m`); first production interval `1d`,
  no interval hard-coded. CLI:
  `python -m datahub.ingestion.binance_um_klines --interval 1d --all`.
- **Archive discovery** from the Binance Data Vision public archive (S3 listing):
  full historical symbol discovery, monthly + daily archive package discovery,
  `catalog/archive_files.jsonl`, `symbols.json`, `discovery_summary.json`.
- **Downloader** with zip + `.CHECKSUM` download, SHA-256 verification,
  resume / skip-verified, transient retry, and failure recording. Checksum
  mismatch fails loud.
- **Daily recent-delta policy** — monthly is the canonical historical base;
  daily archives are skipped by default where a monthly package covers the same
  month (`required_delta` / `skipped_by_default` /
  `included_by_explicit_full_daily_history`), with an opt-in
  `--include-full-daily-history`.
- **Manifests and reports** — main manifest, per-file manifest, coverage summary,
  missing-files report, checksum-failures report, run summary, and a
  research-agent access manifest — all under `local_data/` (uncommitted).
- **Validation integration** — new `binance-um-klines` validation target
  (`KL-*` rules) requiring an explicit `--manifest`; clone-safe `--all` validates
  the default Kline manifest only when present and otherwise skips it.
- **Dataset registration** — `market.binance.um.klines` registered as `draft`
  with a `DATA_CONTRACT.md` contract section and `DATA_CATALOG.md` entry; primary
  key `[symbol, interval, open_time]`. No machine-specific local checksum stored
  in the registry.
- **Documentation** — `docs/binance_um_klines_dataset.md`,
  `docs/research_agent_klines_access.md`, `docs/market_data_storage_policy.md`;
  updated `docs/validation_framework.md`, `QUICKSTART.md`, and `README.md`.
- **Tests and fixtures** — `tests/test_binance_um_klines.py` and
  `tests/fixtures/ingestion/binance_um_klines/` covering interval handling,
  discovery parsing, daily-overlap classification, download / checksum / resume,
  manifest / coverage / research-access generation, validation integration,
  clone-safe `--all`, explicit-manifest requirement, and the `.gitignore` rule.

### Changed

- Bumped repo version `v0.5.0` → `v0.6.0`.
- `.gitignore` now excludes `local_data/`; large market data is never committed.
- Registered dataset count `1` → `2` in `dataset_registry.json` and
  `DATA_CATALOG.md`; `registry_version` stays `v0.2.0` (registry contract shape
  unchanged — only a dataset entry was added).
- Updated `AGENTS.md` and `HANDOFF.md` for Phase 5 decisions and state.

### Known Gaps

- Phase 5 verifies the **raw archive inventory + checksums** only; row-level
  normalization and Parquet materialization are deferred to **Phase 6**.
- Full historical market data is **uncommitted** (`local_data/`, machine-specific).
- `market.binance.um.klines` remains `draft`; `contract_validated = false`.
- The registry stores no single content checksum for the Kline family (no
  committed content); per-file checksums live in the run manifest.

[v0.6.0]: #

---

## [v0.5.0] — 2026-06-16

### Added — Phase 4: Universe Metadata Ingestion MVP

- **Source authority review** for Binance USD-M Futures `exchangeInfo`, Binance
  public archive index, and Binance announcements
  (`docs/universe_metadata_sources.md`).
- **Universe Metadata ingestion CLI** with online fetch, normalization, full run,
  and deterministic offline mode:
  `python -m datahub.ingestion.universe_metadata`.
- **Immutable raw source snapshot** under `data/raw/reference/universe_metadata/`
  with source metadata and raw response checksum.
- **Normalized draft artifact** at
  `data/reference/universe_metadata/reference.universe.metadata.json`.
- **Manifest and checksum metadata** at
  `data/manifests/reference/universe_metadata/manifest.json`.
- **Validation integration** for the normalized artifact through the Phase 3
  Universe Metadata validator.
- **Ingestion tests and fixtures** for deterministic ids, normalization,
  manifest/checksum generation, offline mode, idempotency, raw snapshot naming,
  and raw snapshot reuse.

### Changed

- Bumped repo version `v0.4.0` → `v0.5.0`.
- Updated `dataset_registry.json` and `DATA_CATALOG.md` to point to the first
  validated draft artifact while keeping dataset lifecycle `status = draft`.
- Kept `registry_version = v0.2.0`; no registry contract shape change.
- Updated `DATA_CONTRACT.md`, `docs/universe_metadata_dataset.md`,
  `docs/validation_framework.md`, `QUICKSTART.md`, `AGENTS.md`, and
  `HANDOFF.md` with Phase 4 decisions and validation commands.

### Known Gaps

- Coverage is `active_current` only for Binance USD-M Futures `TRADING` symbols.
- Historical delisted, renamed, and merged lifecycle events are not ingested.
- `contract_validated` remains `false`; artifact validation success does not
  imply lifecycle promotion.
- No snapshot publication, CI workflow, or catalog generation exists yet.

[v0.5.0]: #

---

## [v0.4.0] — 2026-06-16

### Added — Phase 3: Validation Foundation

- **Validation architecture** under `datahub/validation/`, including module
  execution with `python -m datahub.validation`.
- **Validation result model** with per-rule `rule_id`, `severity`, `status`,
  affected file, dataset id, field, location, and details.
- **Validation CLI** with registry, Universe Metadata fixture, and `--all`
  targets plus exit codes `0` / `1` / `2`.
- **Registry validation** for JSON validity, registry contract blocks, required
  fields, field types, naming patterns, status enum, timestamps, quality,
  provenance, and lineage references.
- **Lifecycle validation** for current draft datasets plus transition and
  active/deprecated/archived skeleton checks.
- **Naming validation** for dataset ids, versions, schema refs, timestamps, and
  dataset-related paths.
- **Universe Metadata fixture validation** for Q1-Q6 plus point-in-time
  symbol-era reconstruction (`UM-PIT`).
- **Synthetic test fixtures** for valid and invalid Universe Metadata cases.
- **Stdlib unittest skeleton** covering registry, lifecycle, naming, Universe
  Metadata fixtures, and CLI exit codes.
- **Validation documentation** in `docs/validation_framework.md`.

### Changed

- Bumped repo version `v0.3.0` → `v0.4.0`.
- Updated `AGENTS.md`, `HANDOFF.md`, README/quickstart status text, and related
  governance notes for Phase 3 review state.
- `registry_version` stays `v0.2.0` because the registry contract shape did not
  change.

### Known Gaps

- Universe Metadata remains `draft`; no external data was ingested.
- Active/deprecated/archived lifecycle checks are skeletons until those states
  exist in the registry.
- No JSON Schema, CI workflow, catalog generation, snapshot mechanism, or
  persisted report generation exists yet.

[v0.4.0]: #

---

## [v0.3.0] — 2026-06-16

### Added — Phase 2: First Dataset Design (Universe Metadata)

- **Universe Metadata dataset design** (`reference.universe.metadata`) — the
  first concrete dataset, designed to validate the Phase 1 governance framework
  against a real dataset (`docs/universe_metadata_dataset.md`).
- **Dataset contract** in `DATA_CONTRACT.md` (`Contract: Universe Metadata`):
  14-field schema, primary key `[instrument_id]`, null policy, and six
  dataset-specific quality rules (missing value, duplicate symbol, invalid
  lifecycle, invalid timestamp, invalid contract info, referential).
- **Registry entry** in `dataset_registry.json` `datasets[]` — registered as
  `draft` `v0.1.0`, `contract_validated = false` (design only; no ingestion).
- **Catalog entry** in `DATA_CATALOG.md` (count 0 → 1).
- Supports active / delisted / renamed / merged symbols and point-in-time
  universe reconstruction via `listed_at` / `delisted_at` intervals and
  `successor_id`.

### Changed

- Bumped repo version `v0.2.0` → `v0.3.0`; updated `AGENTS.md` and `HANDOFF.md`
  to reflect Phase 2. `registry_version` stays `v0.2.0` — the registry **contract
  shape** is unchanged; only a dataset entry was added (per decision D13).

### Notes

- Design only: no data ingested, no executable code. The dataset stays `draft`
  until a later phase ingests and validates data.
- Phased delivery: Phase 2 stops here and awaits review before Phase 3.

[v0.3.0]: #

---

## [v0.2.0] — 2026-06-16

### Added — Phase 1: Data Governance Foundation

- **Dataset Lifecycle Model** — states (`draft` → `active` → `deprecated` →
  `archived`), transition rules, and state-management principles
  (`docs/dataset_lifecycle.md`).
- **Dataset Metadata Standard** — unified metadata fields (id, name, version,
  status, owner, source, timezone, update_frequency, timestamps, lineage,
  provenance), with types and required rules (`docs/metadata_standard.md`).
- **Dataset Registry Standard** — formal registry structure, dataset entry
  structure, versioning and discovery rules; machine-readable registry contract
  (`docs/registry_standard.md`).
- **Authority Model** — registry as authoritative source, catalog as derived
  human-readable view, synchronization/update responsibilities
  (`docs/authority_model.md`).
- **Naming Convention Standard** — dataset / id / metadata / file / directory /
  version naming rules (`docs/naming_convention.md`).
- **Dataset Contract Framework** — schema format, primary-key, null, timezone,
  version, and validation policies, plus a contract template (`DATA_CONTRACT.md`).
- **Data Catalog Framework** — per-dataset catalog record and template
  (`DATA_CATALOG.md`).
- Expanded `dataset_registry.json` into a self-describing machine-readable
  contract (`conventions` + `dataset_entry_schema`).

### Changed

- Bumped version `v0.1.0` → `v0.2.0`; updated `AGENTS.md` and `HANDOFF.md` to
  reflect Phase 1 governance decisions and current state.

### Notes

- Governance standards only; still no datasets or executable code (by design).
- Phased delivery: Phase 1 stops here and awaits review before Phase 2.

[v0.2.0]: #

---

## [v0.1.0] — 2026-06-16

### Added — Phase 0: Repository Foundation

- Governance documents: `ROOT.md` (supreme rules), `AGENTS.md` (agent entry
  point), `HANDOFF.md` (architecture + decisions).
- Project docs: `README.md`, `QUICKSTART.md`.
- Versioning: `VERSION` (`v0.1.0`) and this `CHANGELOG.md`.
- Data-plane skeletons: `DATA_CATALOG.md`, `DATA_CONTRACT.md`,
  `dataset_registry.json` (authoritative index, empty).
- Repository structure: `datahub/`, `scripts/`, `tests/`, `reports/`,
  `examples/`, `logs/`, `docs/`.
- Defined core design principles (Maintainability, Reproducibility,
  Scalability, Data Quality, Automation) and the agent onboarding flow.

### Notes

- Skeleton-only foundation; no datasets, pipelines, or executable code yet.
- Phased delivery: Phase 0 stops here and awaits review before Phase 1.

[v0.1.0]: #
