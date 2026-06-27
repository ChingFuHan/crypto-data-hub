# HANDOFF.md

> Handoff document. Read after `AGENTS.md`. Records architecture context and
> the **reasoning** behind decisions — the "why", not only the "what".

---

## Current State (v0.14.0 — Phase 12 + Live Update Phases 1–8)

> **Read this first.** Everything from **Historical Context** onward is the
> original Phase 0–5 handoff, preserved verbatim as a decision record. This
> section is the authoritative summary of where the repo is now.

- Version `v0.14.0`; `registry_version` stays `v0.2.0` (registry contract shape
  unchanged — no dataset was registered in `v0.14.0`).
- The Binance USD-M Futures kline **historical** pipeline is complete for
  **every supported interval** (`1d`/`4h`/`1h`/`15m`/`5m`/`3m`/`1m`). Per
  interval the flow is: raw ingestion → raw validation → **Parquet
  materialization** → Parquet validation. Historical materialization
  (Phases 6–12, `v0.7.0`–`v0.13.0`) is the **stable main line** and is
  unchanged by live update.
- **Live Update Phases 1–8 are complete as MVP primitives** (`v0.14.0`) — an
  incremental live-update layer on top of the historical pipeline:
  WebSocket-first / REST-fallback / webhook primitives, a current historical
  dataset, state-driven startup backfill, CLI modes, and continuity /
  validation checks. This is a **tested CLI skeleton**, not a
  production-hardened long-running daemon.
  - Phase 1 — primitives (`KlineRecord`, path resolution, base data
    structures) in `datahub/live_update.py`.
  - Phase 2 — current historical dataset init from the seed Parquet + live
    Kbar Parquet merge at
    `local_data/binance_um_klines_current/interval=<INTERVAL>/parquet/`.
  - Phase 3 — `live_update_state.json` + state-driven startup backfill
    planning.
  - Phase 4 — REST backfill / fallback / gap repair against
    `https://fapi.binance.com/fapi/v1/klines` with 429/418/5xx/timeout
    backoff.
  - Phase 5 — WebSocket manager, combined stream parsing, stream batching,
    stale detection, reconnect.
  - Phase 6 — webhook server (`POST /webhook/kline`, `GET /healthz`).
  - Phase 7 — CLI modes via `scripts/live_update.py` (thin wrapper over
    `datahub.live_update.main`): `--interval all|1m|3m|5m|15m|1h|4h|1d`,
    `--symbols`, `--symbols-file`, `--once`, `--check-continuity`,
    `--describe-layout`, `--describe-websocket-connections`,
    `--describe-webhook-server`, route-disable flags. `all` is CLI expansion
    only; never sent to the Binance API.
  - Phase 8 — `--check-continuity` (duplicate / missing / misaligned
    `open_time`), shared Kbar validation (OHLC, time alignment, volume/taker
    bounds) with `rejects` logging, and live-update validation tests.
- **Data paths (live update):** current historical dataset (research-agent
  default read-only entry) at
  `local_data/binance_um_klines_current/`; runtime buffers / state / latest /
  closed_buffer / rejects at `local_data/live_update/`. Both are git-ignored
  runtime data, never committed.
- **Primary universe = Binance USDⓈ-M Futures, `PERPETUAL`, `quote_asset =
  USDT`, including delisted USDT perpetuals.** Binance UM is the venue, **not**
  the universe: USDⓈ-M Futures also lists USDC / BUSD quote pairs and delivery /
  settled / special symbols, which are **excluded** from normal primary
  ingestion / migration / trading research flow. Data HUB is the source of
  truth; research agents **read-only mount** it. Non-target or corrupt data is
  **inventoried / quarantined, never deleted or auto-fixed** — `KAITOUSDC` (USDC
  quote pair + unreadable source parquet) is a known quarantined symbol. Full
  definition: `DATA_CONTRACT.md` → *Primary Universe Policy*; acceptance
  checklist: `INIT_VERIFY.md`.
- **Live-update namespaces are NOT registered.**
  `market.binance.um.klines.current` (derived current dataset) and
  `market.binance.um.klines.live_update` (runtime operational namespace) are
  pending governance decisions; see `DATA_CONTRACT.md` → *Pending Governance
  Decisions*. Do not register them without a complete lifecycle design.
- Materialization CLI:
  `python -m datahub.materialization.binance_um_klines_parquet --interval <I>`
  (module `datahub/materialization/binance_um_klines_parquet.py`); Parquet
  validation runs under `python -m datahub.validation`. Large market data (raw
  archives **and** Parquet) lives under
  `local_data/binance_um_klines/interval=<I>/` and is never committed.
- Both registered datasets remain lifecycle `draft`; `market.binance.um.klines`
  now has validated raw **and** Parquet layers (`contract_validated = false`).
- **New machine / `local_data/` disaster recovery starts from `INIT.md`**, then
  `planning/tasks/task_rebuild_all_klines.md` (rebuild) and
  `planning/tasks/task_rebuild_all_klines_verify.md` (verify) — **not** by
  re-running historical phase tasks.
- **Live-update tasks start from `LIVE_UPDATE.md`** (then
  `docs/live_update/*.md`), not `INIT.md` or historical phase tasks.
- `python -m datahub.validation --all` is clone-safe global validation and does
  **not** prove every interval's `local_data/` is rebuilt; full all-interval
  validation follows the verify task above, interval by interval.

### Recommended next actions (post Live Update Phase 8)

> Proposals only — stop at the phase boundary and wait for review. The next
> step is **not** to keep adding features; it is to validate and harden.

1. **Small-scope validation** — single interval (`1m`) + few symbols
   (`BTCUSDT ETHUSDT`) via `--describe-*` / `--check-continuity` / `--once`
   before any full-market / all-interval deployment.
2. **Registry governance decision** — decide whether
   `market.binance.um.klines.current` is a registered derived dataset and
   whether `market.binance.um.klines.live_update` is a runtime operational
   namespace only; then update `dataset_registry.json`, `DATA_CATALOG.md`, and
   `DATA_CONTRACT.md` in the same change.
3. **Production long-running daemon hardening** — orchestration, retention
   manager, and long-running reliability (the MVP CLI skeleton is not a
   production daemon).
4. **CI validation for live-update paths.**
5. **Current dataset partial-initialization hardening remains pending:**
    when `current_root` has Parquet but the initialized marker is missing, the
    runtime must avoid falsely reporting `already_initialized`; it needs
    partial-copy detection, marker semantics, and a current-dataset integrity
    check.

---

## Historical Context — Phases 0–5 (preserved decision record)

> Everything below this line — Architecture Overview, the Phase 0–5 decision log
> (D1–D54), Artifact Locations, Validation Result, Known Gaps, Open Questions,
> Pending Work, and Future Recommendations — is the **original Phase 0–5
> handoff**, describing the repo as of `v0.6.0`. It is preserved as a historical
> decision record and intentionally **not** rewritten. Where it lists
> Parquet/normalization as future "Phase 6" work, that work has since shipped
> across Phases 6–12 (`v0.7.0`–`v0.13.0`); the **Current State** section above is
> authoritative.

---

## Architecture Overview

`crypto-data-hub` is a documentation-first, governance-driven crypto data
platform.

```
ROOT.md                 supreme rules
  └─ AGENTS.md           current operating state
      └─ HANDOFF.md      decisions and rationale
          └─ README.md   project overview

Data plane
  DATA_CONTRACT.md       schema + quality rules
  dataset_registry.json  authoritative dataset metadata
  DATA_CATALOG.md        derived human-readable catalog
  data/                  small committed reference artifacts

Code plane
  datahub/validation/    registry, lifecycle, naming, dataset, klines validation
  datahub/ingestion/     Universe Metadata + Binance Kline ingestion
  local_data/            large market data (Kline archives) — git-ignored
  scripts/               automation wrappers
  tests/                 unittest suite and fixtures

Governance docs
  docs/universe_metadata_sources.md
  docs/universe_metadata_dataset.md
  docs/validation_framework.md
  docs/dataset_lifecycle.md
  docs/metadata_standard.md
  docs/registry_standard.md
  docs/authority_model.md
  docs/naming_convention.md
```

Current executable flow:

```text
Binance exchangeInfo → immutable raw snapshot → normalized Universe Metadata
artifact → manifest/checksums → Phase 3 validator → registry/catalog docs
```

Universe Metadata remains lifecycle `draft`; Phase 4 validates a draft artifact
only.

Phase 5 adds a parameterized Binance USD-M Futures Kline pipeline:

```text
Binance Data Vision archive index → archive discovery (monthly base + daily
delta) → zip + .CHECKSUM download → SHA-256 verify / resume → local_data
manifest + coverage + research-access → binance-um-klines validator
```

`market.binance.um.klines` is registered `draft`; Phase 5 verifies raw archive
inventory + checksums only (row-level normalization / Parquet is Phase 6). Large
market data lives under `local_data/` and is never committed.

---

## Important Decisions

| # | Decision | Why |
|---|----------|-----|
| D1 | ROOT.md is the single supreme document. | One conflict resolver prevents rule drift. |
| D2 | `dataset_registry.json` is the authoritative source of truth. | Machine-readable source enables automation and validation. |
| D3 | Documentation-first foundation before pipeline code. | Governance before data protects quality. |
| D4 | Strict phased delivery with review gates. | Keeps each increment reviewable. |
| D5 | Semantic Versioning starting at `v0.1.0`. | Predictable version semantics. |
| D6 | Snapshots are immutable and content-addressable. | Supports reproducibility and provenance. |
| D7 | Fail loud on contract violations. | Silent coercion hides data-quality bugs. |
| D8 | Skeleton files first, real content per phase. | Establishes self-onboarding shape. |

### Phase 1 — Data Governance Foundation (v0.2.0)

| # | Decision | Why |
|---|----------|-----|
| D9 | Lifecycle states fixed to `draft`/`active`/`deprecated`/`archived`. | Small closed state set keeps tooling simple. |
| D10 | Dataset metadata lives inside registry entries. | One source of truth. |
| D11 | Registry carries `conventions` + `dataset_entry_schema`. | Self-describing machine-readable contract. |
| D12 | Catalog is a derived view. | Registry/catalog conflicts resolve predictably. |
| D13 | Dataset `version` and `registry_version` are separate. | Dataset evolution and registry shape are different axes. |
| D14 | Active `dataset_id` is stable. | Protects lineage and reproducibility. |
| D15 | All timestamps stored UTC ISO 8601. | Removes time ambiguity. |

### Phase 2 — First Dataset Design (v0.3.0)

| # | Decision | Why |
|---|----------|-----|
| D16 | First dataset = Universe Metadata. | Reference data exercises all governance pieces. |
| D17 | Primary key is surrogate `instrument_id`, not `symbol`. | Symbols are reused/renamed/merged. |
| D18 | Symbol `status` is separate from dataset lifecycle `status`. | Avoids lifecycle conflation. |
| D19 | Registered as draft with no ingestion. | Honest design-stage integrity. |
| D20 | `registry_version` stayed `v0.2.0`. | Dataset entry did not change registry contract shape. |

### Phase 3 — Validation Foundation (v0.4.0)

| # | Decision | Why |
|---|----------|-----|
| D21 | Validation lives under `datahub/validation/`. | Importable, testable, automation-friendly. |
| D22 | Validation uses only Python standard library. | No dependency overhead. |
| D23 | Result model is explicit per-check records. | CLI and future CI can share structure. |
| D24 | `--target registry` composes registry, lifecycle, naming checks. | Governance rules are coupled. |
| D25 | Universe Metadata validation is fixture-based first. | Cross-field/graph invariants become executable before ingestion. |
| D26 | `registry_version` remained `v0.2.0`. | Validation tooling did not change registry shape. |
| D27 | `quality.last_validated_at = null` accepted when `contract_validated = false`. | Draft registry state stays honest. |

### Phase 4 — Universe Metadata Ingestion MVP (v0.5.0)

| # | Decision | Why |
|---|----------|-----|
| D28 | Primary source = Binance USD-M Futures `exchangeInfo`. | Public, official, deterministic current active universe source. |
| D29 | Archive index and announcements are reviewed but not implemented as authoritative row sources. | They need extra evidence reconciliation before driving lifecycle rows. |
| D30 | Ingestion lives under `datahub/ingestion/universe_metadata.py`. | Keeps ingestion separate from validation but still module-executable. |
| D31 | Raw snapshots are immutable envelope JSON files with raw response checksum. | Enables offline deterministic replay and source provenance. |
| D32 | Normalized artifact is a JSON array at `data/reference/universe_metadata/reference.universe.metadata.json`. | Matches Phase 3 validator preferred input format. |
| D33 | Manifest lives at `data/manifests/reference/universe_metadata/manifest.json`. | Keeps provenance, checksums, coverage, and validation metadata out of row data. |
| D34 | Instrument ids use `binance.usd_m_futures.<market_type>.<symbol_lower>.<listed_yyyymmdd>`. | Human-readable, deterministic, symbol-era aware, not plain symbol. |
| D35 | Collision handling appends deterministic `.h<sha256_prefix>`. | Preserves reproducibility if source collisions appear. |
| D36 | `contract_size = 1` is a documented normalization convention for USD-M linear futures. | `exchangeInfo` lacks a separate field, but contract Q5 requires positive derivative contract size. |
| D37 | Coverage status is manifest/provenance metadata, not row `status`. | Prevents unsupported values from polluting contract enum. |
| D38 | `contract_validated` remains `false` while artifact validation passes. | Artifact validation is not lifecycle promotion; Phase 5/review must decide semantics. |
| D39 | `registry_version` remains `v0.2.0`. | Registry shape unchanged; artifact metadata fits existing provenance params. |
| D40 | Committed data artifacts are allowed because total size is small and supports offline validation. | Reproducibility beats avoiding small reference data. |

### Phase 5 — Binance USD-M Kline Historical Pipeline (v0.6.0)

| # | Decision | Why |
|---|----------|-----|
| D41 | **Base dataset_id = `market.binance.um.klines`.** | `market`.`exchange`.`product`.`entity` is stable and discoverable; klines are the first market dataset. |
| D42 | **Interval variant id = `market.binance.um.klines.<INTERVAL>`** (first `…​.1d`). | One family, many intervals; the variant id keeps interval explicit without a new dataset family per interval. |
| D43 | **Supported intervals are a single source-of-truth tuple** `1d/4h/1h/15m/5m/1m`; nothing hard-codes `1d`. | The task requires `1d` first but full parameterization; one constant prevents drift. Unsupported interval fails loud (exit 2 + allowed list). |
| D44 | **Primary key = `symbol + interval + open_time`.** | A Kline row is uniquely identified by its open time for a given symbol and interval. |
| D45 | **Source authority = Binance Data Vision public archive**; monthly = historical base, daily = recent delta; Universe Metadata is cross-check only. | The current active universe omits delisted symbols; the archive index is authoritative for the full historical symbol set. |
| D46 | **Kline interval and archive package source are kept strictly separate** everywhere (code, schema, docs). | `monthly`/`daily` are packaging, not row periods; conflating them corrupts the schema/PK. |
| D47 | **Daily recent-delta policy**: by default download monthly + only daily dates not covered by monthly; covered daily files are `skipped_by_default`. Full daily requires `--include-full-daily-history` (recorded in manifest). | Avoids downloading/duplicating data already covered by monthly while keeping a recent delta path. |
| D48 | **Large market data lives only under `local_data/`** with per-interval roots. | Keeps the repo small and clone-fast; intervals never collide on disk. |
| D49 | **No large market data is committed**; `.gitignore` excludes `local_data/`; check `git status --short` before commit. | ROOT *Maintainability* + repo must stay clone-safe; market data is machine-specific. |
| D50 | **`python -m datahub.validation --all` stays clone-safe**: it validates the Kline manifest only if present, else records `KL-MANIFEST-EXISTS` as skipped. The `binance-um-klines` target requires an explicit `--manifest` (else exit 2). | A fresh clone has no `local_data/`; default validation must not require it. Large-data validation is explicit. |
| D51 | **Manifests/reports/research-access live under `local_data/`** (main `manifest.json`, per-file `files.jsonl`, coverage, missing-files, checksum-failures, run summary, `research_access.json`). | Run state is machine-specific; it belongs with the data, not in VCS. |
| D52 | **Research-agent access is a generated manifest** plus `docs/research_agent_klines_access.md` with a minimal stdlib read example. | Agents must locate verified archives, inspect coverage, and distinguish interval vs package source without tribal knowledge. |
| D53 | **`market.binance.um.klines` is registered as `draft`** with a `DATA_CONTRACT.md` contract section and `DATA_CATALOG.md` entry, but **no machine-specific local checksum in the registry** (`provenance.checksum = ""`; per-file checksums live in the local manifest). | Governance says a dataset is not real until registered, but the registry must not store machine-specific local_data run checksums. `registry_version` stays `v0.2.0` (shape unchanged). |
| D54 | **Checksum mismatch fails loud** (recorded + non-zero exit); HTTP 404 is a genuine missing object (not retried); transient errors retry. | ROOT *Fail loud* + correct provenance. |

---

## Artifact Locations

- Raw snapshot:
  `data/raw/reference/universe_metadata/exchange_info_20260616T170138Z_d4d2d2ab1c6e.json`
- Normalized artifact:
  `data/reference/universe_metadata/reference.universe.metadata.json`
- Manifest:
  `data/manifests/reference/universe_metadata/manifest.json`
- Artifact checksum:
  `fcee6a125792598d19e4332c3acd848dd4c7e49551e1f1cef2ad09a73b533b39`
- Manifest checksum:
  `cd40840b48a46b1a844ce015e548d3ece82eba733ef2f4fea0ffba1adc9444f3`
- Rows: 671
- Source records: 792
- Coverage: `active_current`

---

## Validation Result

Required deterministic commands pass:

```bash
python -m datahub.ingestion.universe_metadata --offline --all
python -m datahub.validation --all
python -m unittest discover tests
```

The host may expose the launcher as `python3`; command form is otherwise the
same.

---

## Known Gaps

- Universe Metadata remains lifecycle `draft`.
- Coverage is Binance USD-M Futures current `TRADING` symbols only.
- Historical delisted, renamed, and merged lifecycle events are not ingested.
- Archive index candidates are not authoritative rows.
- Announcement parsing is not implemented.
- No JSON Schema or CI exists yet.
- `DATA_CATALOG.md` is still hand-maintained.
- Snapshot publication is not implemented.
- Klines (`market.binance.um.klines`): Phase 5 verifies raw archive inventory +
  checksums only; row-level normalization / Parquet materialization is Phase 6.
- Klines full historical market data is uncommitted (`local_data/`,
  machine-specific); the registry stores no single content checksum for it.

---

## Open Questions

- Should `contract_validated` represent artifact validation, lifecycle promotion,
  or both via separate future fields?
- Which source combination should become authoritative for historical lifecycle
  events?
- Should coverage/confidence move into a formal registry schema in a future
  `registry_version` bump?
- Where should larger future source artifacts live if they exceed repo-reviewable
  size?

---

## Pending Work

- **Phase 6 (post-review) — recommended:**
  - Normalize verified Kline archives into a primary-keyed
    (`symbol + interval + open_time`), partitioned Parquet materialization.
  - Make row-level Kline rules K1–K4 executable on the materialized rows.
  - Run remaining intervals (`4h`/`1h`/`15m`/`5m`/`1m`) through the pipeline.
  - Decide whether `--include-full-daily-history` materialization de-duplicates
    daily/monthly overlap by primary key.
- **Carried from earlier phases:**
  - Decide contract/artifact validation semantics (`contract_validated`).
  - Add historical delist/rename/merge source ingestion for Universe Metadata.
  - Add JSON Schema and CI; auto-generate `DATA_CATALOG.md` from the registry.
  - Implement immutable, content-addressable snapshots.

---

## Future Recommendations

- Never commit `local_data/`; confirm `git status --short` before every commit.
- Keep the Kline interval and the archive package source (monthly/daily)
  strictly separate in code, schema, and docs.
- Keep the registry free of machine-specific local_data run checksums; record
  per-file checksums in the local manifest instead.
- Keep raw snapshot reuse by checksum; never overwrite raw data.
- Expand historical coverage with fixture-first tests before touching registry
  lifecycle state.
