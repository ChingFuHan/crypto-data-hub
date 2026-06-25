# Changelog

All notable changes to this repository are documented here.

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/);
this project adheres to [Semantic Versioning](https://semver.org/) (`vMAJOR.MINOR.PATCH`).

---

## [v0.14.0] — 2026-06-26

### Added — Live Update Phases 1–8: MVP primitives

> **Status note.** This version adds the live-update MVP primitives, CLI
> skeleton, and validation checks. The historical materialization pipeline
> (Phases 6–12) remains the stable main line and is unchanged. Live update is
> **not** a production-hardened long-running daemon yet — see *Notes* below.

- **Live update primitives (Phase 1)** — `KlineRecord` dataclass, path
  resolution, and base data structures for the live-update runtime
  (`datahub/live_update.py`).
- **Current historical dataset init + Parquet merge (Phase 2)** — initializes
  `local_data/binance_um_klines_current/interval=<INTERVAL>/parquet/` from the
  historical seed Parquet and merges live closed Kbars into the current
  dataset.
- **State + startup backfill planning (Phase 3)** — `live_update_state.json`
  tracking and state-driven startup gap/backfill planning.
- **REST backfill (Phase 4)** — REST fallback / startup backfill / gap repair
  against `https://fapi.binance.com/fapi/v1/klines` with 429/418/5xx/timeout
  backoff.
- **WebSocket primitives (Phase 5)** — WebSocket manager, combined stream
  parsing, stream batching, stale detection, and reconnect.
- **Webhook primitives (Phase 6)** — webhook server
  (`POST /webhook/kline`, `GET /healthz`) as an external bridge / agent
  entry point.
- **CLI modes (Phase 7)** — `scripts/live_update.py` (thin wrapper over
  `datahub.live_update.main`) with `--interval all|1m|3m|5m|15m|1h|4h|1d`,
  `--symbols`, `--symbols-file`, `--max-symbols`, `--once`, `--strict`,
  `--check-continuity`, `--describe-layout`, `--describe-websocket-connections`,
  `--describe-webhook-server`, and route-disable flags. `all` is a CLI
  expansion semantic only and is never sent to the Binance API.
- **Continuity / validation checks (Phase 8)** — `--check-continuity` reports
  duplicate / missing / misaligned `open_time` per symbol+interval; Kbar
  validation (OHLC, time alignment, volume/taker bounds) with `rejects`
  logging; live-update validation tests under `tests/`.
- **Current historical dataset path support** —
  `local_data/binance_um_klines_current/` is the research-agent default
  read-only entry point; runtime buffers / state / latest / closed_buffer /
  rejects live under `local_data/live_update/`. Both are git-ignored runtime
  data and are never committed.

### Notes

- **Live update is MVP primitives only.** Phases 1–8 deliver a tested CLI
  skeleton and validation checks. A production long-running, full-market,
  all-interval daemon has **not** been hardened — orchestration, retention,
  and long-running reliability work remains future work.
- **Small-scope validation first.** Before any full-market / all-interval
  long-running deployment, validate with a small symbol set (e.g.
  `BTCUSDT ETHUSDT`) and a single interval (e.g. `1m`) using the describe /
  check-continuity / once CLI modes.
- **Registry unchanged.** No new dataset was registered in
  `dataset_registry.json`. Whether
  `market.binance.um.klines.current` is registered as a formal derived dataset
  and whether `market.binance.um.klines.live_update` is a runtime operational
  namespace only are **pending governance decisions** (noted in
  `DATA_CONTRACT.md` and `AGENTS.md`). `registry_version` stays `v0.2.0`.

### Changed

- Bumped repo version `v0.13.0` → `v0.14.0`.
- Synced `README.md`, `AGENTS.md`, `HANDOFF.md`, `QUICKSTART.md`,
  `LIVE_UPDATE.md`, and `DATA_CONTRACT.md` to reflect live-update Phase 1–8
  status.

---

## [v0.13.0] — 2026-06-21

### Added — Phase 12: Binance UM 1M Kline Parquet Materialization

- **1M raw validation confirmed** — `binance-um-klines --interval 1m` passes
  against `local_data/binance_um_klines/interval=1m/manifests/manifest.json`
  with `13/13` checks passing.
- **Interval-aware materializer** — `binance_um_klines_parquet.py` extended to
  `--interval {1d,4h,1h,15m,5m,3m,1m}`. Adds the `1m` interval to
  `ALLOWED_INTERVALS`, `INTERVAL_MILLISECONDS` (`60_000`), and
  `ROWS_PER_SYMBOL_DATE_LIMIT` (`1_440`). `CODE_VERSION` bumped to `v0.13.0`.
  1D/4H/1H/15M/5M/3M behaviour is preserved (regression-tested).
- **1M time policy** — `open_time % 60_000 == 0` and
  `close_time = open_time + 59_999`, enforced at materialization
  (`find_time_rule_violations`, `--strict` fails) and re-checked in validation
  (`PQ-OPEN-TIME-ALIGNMENT`, `PQ-CLOSE-TIME-RULE`).
- **1M key policy** — unique key stays `(symbol, interval, open_time)`;
  `(symbol, date)` is a grouping column with **≤ 1440 rows/day**
  (`PQ-SYMBOL-DATE-LIMIT`).
- **1M Parquet dataset** at
  `local_data/binance_um_klines/interval=1m/parquet/` — `FULL_OUTPUT`,
  `raw_discovered_symbol_count = 922`, `symbol_count = 922`,
  `row_count = 918113837`, `file_count = 2667`, `failed_symbol_count = 0`,
  `generated_csv_file_count = 0`, same Hive layout
  (`symbol=<S>/year=<Y>/part-000.parquet`).
- **Validation** — `binance-um-klines-parquet --interval 1m`; adds
  `PQ-1M-ROWS-GT-3M` (1m row_count must exceed 3m, ratio `>= 2.5`), gated to
  production FULL_OUTPUT with `raw_discovered_symbol_count >= 921` and skipped
  for fixtures.
- **Dataset registration** — `market.binance.um.klines.1m.parquet` registered
  as `draft` with a `DATA_CATALOG.md` entry (registered dataset count -> 9).
- **Tests** — header/header-less 1M CSV, 1m alignment + close-time,
  ≤1440 rows/day legal, >1440 rows/day detected, DuckDB read + explicit 1m
  validation, and 1D/4H/1H/15M/5M/3M regression.

## [v0.12.0] — 2026-06-20

### Added — Phase 11: Binance UM 3M Kline Parquet Materialization

- **3M raw ingestion** — `binance_um_klines.py` adds `3m` to `ALLOWED_INTERVALS`,
  so `--interval 3m` discovers, downloads, verifies, and archives the immutable
  3m zip layer at `local_data/binance_um_klines/interval=3m/`. Raw validation
  (`binance-um-klines --interval 3m`) accepts the new interval too.
- **Interval-aware materializer** — `binance_um_klines_parquet.py` extended to
  `--interval {1d,4h,1h,15m,5m,3m}`. Adds the `3m` interval to
  `ALLOWED_INTERVALS`, `INTERVAL_MILLISECONDS` (`180_000`), and
  `ROWS_PER_SYMBOL_DATE_LIMIT` (`480`). `CODE_VERSION` bumped to `v0.12.0`.
  1D/4H/1H/15M/5M behaviour is preserved (regression-tested).
- **3M time policy** — `open_time % 180_000 == 0` and
  `close_time = open_time + 179_999`, enforced at materialization
  (`find_time_rule_violations`, `--strict` fails) and re-checked in validation
  (`PQ-OPEN-TIME-ALIGNMENT`, `PQ-CLOSE-TIME-RULE`).
- **3M key policy** — unique key stays `(symbol, interval, open_time)`; `(symbol,
  date)` is a grouping column with **≤ 480 rows/day** (`PQ-SYMBOL-DATE-LIMIT`).
- **3M Parquet dataset** at
  `local_data/binance_um_klines/interval=3m/parquet/` — `FULL_OUTPUT`,
  `symbol_count = 922` (`== raw_discovered_symbol_count`),
  `failed_symbol_count = 0`, `generated_csv_file_count = 0`, same Hive layout
  (`symbol=<S>/year=<Y>/part-000.parquet`).
- **New listing — cross-interval universe note.** The 3m raw discovery found
  **922** symbols vs the **921** baseline shared by 1D/4H/1H/15M/5M. The extra
  symbol is `REUSDT`, a perpetual **listed after the previous phases ran**
  (single daily archive `REUSDT-3m-2026-06-18.zip`). This is a new listing, not
  a data error: 3m is the universe as of its ingestion date. FULL_OUTPUT is
  therefore defined as `symbol_count == raw_discovered_symbol_count`
  (`>= 921`), not a hard `== 921`. Downstream cross-interval research should take
  the symbol intersection; the raw layer is never trimmed to force a count.
- **Validation** — `binance-um-klines-parquet --interval 3m`; adds
  `PQ-3M-ROWS-GT-5M` (3m row_count must exceed 5m, ratio `>= 1.5`), gated to
  production FULL_OUTPUT with `raw_discovered_symbol_count >= 921` (was a hard
  `== 921`) and skipped for fixtures.
- **Dataset registration** — `market.binance.um.klines.3m.parquet` registered as
  `draft` with a `DATA_CATALOG.md` entry (registered dataset count → 8).
- **Tests** — header/header-less 3M CSV, 3m alignment + close-time, ≤480 rows/day
  legal, >480 rows/day detected, DuckDB read + explicit 3m validation, and
  1D/4H/1H/15M/5M regression. `99` tests pass.

---

## [v0.11.0] — 2026-06-19

### Added — Phase 10: Binance UM 5M Kline Parquet Materialization

- **Interval-aware materializer** — `binance_um_klines_parquet.py` extended to
  `--interval {1d,4h,1h,15m,5m}`. Adds the `5m` interval to `ALLOWED_INTERVALS`,
  `INTERVAL_MILLISECONDS` (`300_000`), and `ROWS_PER_SYMBOL_DATE_LIMIT` (`288`).
  `CODE_VERSION` bumped to `v0.11.0`. 1D/4H/1H/15M behaviour is preserved
  (regression-tested).
- **5M time policy** — `open_time % 300_000 == 0` and
  `close_time = open_time + 299_999`, enforced at materialization
  (`find_time_rule_violations`, `--strict` fails) and re-checked in validation
  (`PQ-OPEN-TIME-ALIGNMENT`, `PQ-CLOSE-TIME-RULE`).
- **5M key policy** — unique key stays `(symbol, interval, open_time)`; `(symbol,
  date)` is a grouping column with **≤ 288 rows/day** (`PQ-SYMBOL-DATE-LIMIT`).
- **5M Parquet dataset** at
  `local_data/binance_um_klines/interval=5m/parquet/` — `FULL_OUTPUT`,
  `symbol_count = 921`, `row_count = 183166850`, `failed_symbol_count = 0`,
  `generated_csv_file_count = 0`, same Hive layout
  (`symbol=<S>/year=<Y>/part-000.parquet`).
- **Validation** — `binance-um-klines-parquet --interval 5m`; adds
  `PQ-5M-ROWS-GT-15M` (5m row_count must exceed 15m, ratio `>= 2.5`), gated to
  production FULL_OUTPUT and skipped for fixtures.
- **Dataset registration** — `market.binance.um.klines.5m.parquet` registered as
  `draft` with a `DATA_CATALOG.md` entry (registered dataset count → 7).
- **Tests** — header/header-less 5M CSV, 5m alignment + close-time, ≤288 rows/day
  legal, >288 rows/day detected, DuckDB read + explicit 5m validation, and
  1D/4H/1H/15M regression. `95` tests pass.

---

## [v0.10.0] — 2026-06-19

### Added — Phase 9: Binance UM 15M Kline Parquet Materialization

- **Interval-aware materializer** — `binance_um_klines_parquet.py` extended to
  `--interval {1d,4h,1h,15m}`. Adds the `15m` interval to `ALLOWED_INTERVALS`,
  `INTERVAL_MILLISECONDS` (`900_000`), and `ROWS_PER_SYMBOL_DATE_LIMIT` (`96`).
  `CODE_VERSION` bumped to `v0.10.0`. 1D/4H/1H behaviour is preserved
  (regression-tested).
- **15M time policy** — `open_time % 900_000 == 0` and
  `close_time = open_time + 899_999`, enforced at materialization
  (`find_time_rule_violations`, `--strict` fails) and re-checked in validation
  (`PQ-OPEN-TIME-ALIGNMENT`, `PQ-CLOSE-TIME-RULE`).
- **15M key policy** — unique key stays `(symbol, interval, open_time)`; `(symbol,
  date)` is a grouping column with **≤ 96 rows/day** (`PQ-SYMBOL-DATE-LIMIT`).
- **15M Parquet dataset** at
  `local_data/binance_um_klines/interval=15m/parquet/` — `FULL_OUTPUT`,
  `symbol_count = 921`, `row_count = 61055727`, `failed_symbol_count = 0`,
  `generated_csv_file_count = 0`, same Hive layout
  (`symbol=<S>/year=<Y>/part-000.parquet`).
- **Validation** — `binance-um-klines-parquet --interval 15m`; adds
  `PQ-15M-ROWS-GT-1H` (15m row_count must exceed 1h, ratio `>= 3.5`), gated to
  production FULL_OUTPUT and skipped for fixtures.
- **Dataset registration** — `market.binance.um.klines.15m.parquet` registered as
  `draft` with a `DATA_CATALOG.md` entry (registered dataset count → 6).
- **Tests** — header/header-less 15M CSV, 15m alignment + close-time, ≤96 rows/day
  legal, >96 rows/day detected, DuckDB read + explicit 15m validation, and 1D/4H/1H
  regression. `91` tests pass.

---

## [v0.9.0] — 2026-06-18

### Added — Phase 8: Binance UM 1H Kline Parquet Materialization

- **Interval-aware materializer** — `binance_um_klines_parquet.py` extended to
  `--interval {1d,4h,1h}`. Adds the `1h` interval to `ALLOWED_INTERVALS`,
  `INTERVAL_MILLISECONDS` (`3_600_000`), and `ROWS_PER_SYMBOL_DATE_LIMIT` (`24`).
  1D and 4H behaviour is preserved (regression-tested).
- **1H time policy** — `open_time % 3_600_000 == 0` and
  `close_time = open_time + 3_599_999`, enforced at materialization
  (`find_time_rule_violations`, `--strict` fails) and re-checked in validation
  (`PQ-OPEN-TIME-ALIGNMENT`, `PQ-CLOSE-TIME-RULE`).
- **1H key policy** — unique key stays `(symbol, interval, open_time)`; `(symbol,
  date)` is a grouping column with **≤ 24 rows/day** (`PQ-SYMBOL-DATE-LIMIT`).
- **Corrupt-bar quarantine** — bars failing the OHLC or time rules are now
  excluded from the materialized Parquet query layer and disclosed in
  `data_quality_report.json` (`quarantined_bar_count`), keeping the DuckDB layer
  clean; `--strict` still fails the symbol. One corrupt 1h source bar
  (`BTCUSDT_210326`, 2021-02-03, open/close above high) is quarantined. 1d/4h
  contain no such bars, so it is a no-op for them.
- **1H Parquet dataset** at
  `local_data/binance_um_klines/interval=1h/parquet/` — `FULL_OUTPUT`,
  `symbol_count = 921`, `failed_symbol_count = 0`, `generated_csv_file_count = 0`,
  same Hive layout (`symbol=<S>/year=<Y>/part-000.parquet`).
- **Validation** — `binance-um-klines-parquet --interval 1h`; generalizes the
  cross-interval row-count regression into `_validate_row_count_regression` and
  adds `PQ-1H-ROWS-GT-4H` (1h row_count must exceed 4h, ratio `>= 3.5`), gated to
  production FULL_OUTPUT and skipped for fixtures.
- **Dataset registration** — `market.binance.um.klines.1h.parquet` registered as
  `draft` with a `DATA_CATALOG.md` entry (registered dataset count → 5).
- **Tests** — header/header-less 1H CSV, 1h alignment + close-time, ≤24 rows/day
  legal, >24 rows/day detected, DuckDB read + explicit 1h validation, and 1D/4H
  regression, and corrupt-bar quarantine. `87` tests pass.

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
