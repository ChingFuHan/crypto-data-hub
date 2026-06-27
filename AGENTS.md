# AGENTS.md

> **Agent primary entry point.** Read this after `ROOT.md`. Keep it short
> (target: under 200 lines). It reflects the **current** state of the repo.

---

## Current Phase

**Phase 12 complete — Binance UM Kline Parquet Materialization** (historical
main line, `v0.13.0`). **Live Update Phases 1–8 complete — MVP primitives**
(`v0.14.0`). Version `v0.14.0`.

The Binance USD-M Futures kline historical pipeline (raw ingestion → raw
validation → Parquet materialization → Parquet validation) is in place for
every supported interval: `1d`, `4h`, `1h`, `15m`, `5m`, `3m`, `1m`, and
remains the stable main line. **Live Update Phases 1–8** add an incremental
live-update layer on top of it: WebSocket-first / REST-fallback / webhook
primitives, a current historical dataset, state-driven startup backfill, CLI
modes, and continuity / validation checks. This is **MVP primitives + a tested
CLI skeleton**, not a production-hardened long-running daemon.

Delivery model: Architecture First → MVP First → Incremental Delivery →
Review Before Expansion. Each phase stops at its boundary for review; the repo
sits at a reviewed Phase 12 + Live Update Phase 8 boundary. The new-machine /
recovery entrypoint is `INIT.md` (historical rebuild) and the live-update task
entrypoint is `LIVE_UPDATE.md` — not a re-run of historical phase tasks.

---

## Current Status

- Repo version: `v0.14.0`. `registry_version` stays `v0.2.0` because the
  registry contract shape has not changed (no dataset was registered in
  `v0.14.0`; live-update namespaces are pending governance decisions).
- Phases 0 (`v0.1.0`) through 12 (`v0.13.0`) are complete. The Binance USD-M
  Futures Kline pipeline is fully built and materialized to Parquet for every
  supported interval (`1d`/`4h`/`1h`/`15m`/`5m`/`3m`/`1m`).
- **Live Update Phases 1–8 (`v0.14.0`) are complete as MVP primitives** —
  WebSocket-first / REST-fallback / webhook primitives, current historical
  dataset init + Parquet merge, state + startup backfill, CLI modes, and
  continuity / validation checks. See `LIVE_UPDATE.md` and
  `docs/live_update/*.md`. This is a tested CLI skeleton, **not** a
  production-hardened long-running daemon.
  - Live-update CLI — `scripts/live_update.py` (thin wrapper over
    `datahub.live_update.main`); supports `--interval all|1m|3m|5m|15m|1h|4h|1d`,
    `--symbols`, `--symbols-file`, `--once`, `--check-continuity`,
    `--describe-layout`, `--describe-websocket-connections`,
    `--describe-webhook-server`, and route-disable flags. `all` is a CLI
    expansion semantic only and is never sent to the Binance API.
  - Current historical dataset (research-agent default read-only entry point)
    lives at `local_data/binance_um_klines_current/interval=<INTERVAL>/parquet/`.
  - Runtime buffers / state / latest / closed_buffer / rejects live under
    `local_data/live_update/`. Both are git-ignored runtime data, never
    committed.
- Per interval the executable flow is: raw ingestion → raw validation →
  Parquet materialization → Parquet validation.
  - Raw ingestion CLI —
    `python -m datahub.ingestion.binance_um_klines --interval <I> --all`
  - Materialization CLI —
    `python -m datahub.materialization.binance_um_klines_parquet --interval <I>`
  - Supported intervals are a single source-of-truth tuple
    (`1d/4h/1h/15m/5m/3m/1m`); nothing hard-codes a single interval.
  - Source — Binance Data Vision public archive (monthly historical base + daily
    recent delta); archive discovery, zip + `.CHECKSUM` download, SHA-256 verify,
    resume / skip-verified.
  - Large market data (raw archives + Parquet) lives under
    `local_data/binance_um_klines/interval=<INTERVAL>/` and is
    **git-ignored / uncommitted**.
  - Manifests, coverage reports, materialization manifests, and a
    research-access manifest are written under `local_data/`.
  - Validation targets `binance-um-klines` (raw) and the Parquet layer take an
    explicit `--manifest`; clone-safe `--all` skips them when no `local_data`
    manifest exists.
- Both registered datasets remain lifecycle `draft`:
  - `reference.universe.metadata` — `active_current` coverage.
  - `market.binance.um.klines` — raw archive inventory/checksums **and**
    materialized Parquet are validated; `contract_validated = false`.
- **Live-update namespaces are NOT registered** —
  `market.binance.um.klines.current` (derived current dataset) and
  `market.binance.um.klines.live_update` (runtime operational namespace) are
  pending governance decisions; see `DATA_CONTRACT.md` → *Pending Governance
  Decisions* and the *Live Update Agent Guidance* section below.
- Module execution entry points:
  - `python -m datahub.ingestion.binance_um_klines --interval 1d --all`
  - `python -m datahub.materialization.binance_um_klines_parquet --interval 1d`
  - `python -m datahub.ingestion.universe_metadata --offline --all`
  - `python -m datahub.validation --all`
  - `python -m unittest discover tests`
  - Live update: `.venv/bin/python scripts/live_update.py --interval <I> ...`
    (see `LIVE_UPDATE.md`)

---

## New-machine / Maintenance Entrypoint

A new machine, fresh clone, or `local_data/` disaster recovery starts from
`INIT.md` — **not** from re-running historical phase tasks.

- Init guide: `INIT.md`
- Rebuild task: `planning/tasks/task_rebuild_all_klines.md`
- Verify task: `planning/tasks/task_rebuild_all_klines_verify.md`

Historical phase tasks (`planning/tasks/task_v0.07.md` … `task_v0.13.md`) record
how each interval was delivered; they are **not** the new-machine recovery
entrypoint. To rebuild `local_data/` on a new machine, follow
`INIT.md` → rebuild task → verify task.

For **live-update** tasks, the entrypoint is `LIVE_UPDATE.md` (see *Live Update
Agent Guidance* below), not `INIT.md` or historical phase tasks.

---

## Validation Scope (important)

`python -m datahub.validation --all` is the **clone-safe global validation**: it
checks the registry/governance and validates a kline manifest only if one is
present locally. It does **not** prove that every interval's `local_data/` has
been rebuilt and validated.

Full all-interval `local_data/` validation must follow
`planning/tasks/task_rebuild_all_klines_verify.md`, which checks each interval
(raw + Parquet) explicitly, interval by interval.

---

## Live Update Agent Guidance

> **Read this before any live-update task.** Live update has its own entrypoint
> and per-module specs; it is not covered by the historical-pipeline docs.

- **Task entrypoint:** `LIVE_UPDATE.md` is the live-update task entrypoint. Any
  task that touches live update **must** read `LIVE_UPDATE.md` first — reading
  only `README.md` or `AGENTS.md` is insufficient.
- **Per-module specs:** `docs/live_update/*.md`
  (`00_OVERVIEW.md` … `09_RUNBOOK.md`) are the live-update分卷規格. Read the
  relevant volume for the module you are changing; do not guess from the
  overview alone.
- **Current completion:** Live Update Phases 1–8 are complete as MVP
  primitives (CLI skeleton + validation checks). Production long-running
  daemon hardening is **pending** — do not claim live update is
  production-ready.
- **Historical materialization is the stable main line.** Live update is an
  incremental layer on top of it; do not break the historical pipeline.

### Data paths (live update)

- **Current historical dataset** (research-agent default read-only entry
  point): `local_data/binance_um_klines_current/interval=<INTERVAL>/parquet/`.
- **Runtime buffers / state / latest / closed_buffer / rejects:**
  `local_data/live_update/`.
- **Historical seed Parquet:** `local_data/binance_um_klines/interval=<INTERVAL>/parquet/`.

### Hard rules (live update)

- **Do not commit** `local_data/`, Parquet, or JSONL runtime artifacts. They
  are git-ignored runtime data. Confirm `git status --short` before every
  commit.
- **Do not treat `--interval all` as a Binance API interval.** `all` is a CLI
  expansion semantic only; expand it to the supported interval tuple before
  any REST / WebSocket call. Never send `all` to Binance.
- **No unclosed Kbar may enter `closed_buffer` or the current dataset.**
  Unclosed bars update `latest` / buffers only; only closed Kbars
  (`is_closed = true`) are written to `closed_buffer` and the partition write
  queue.
- **WebSocket / REST / webhook share one Kbar validation path.** All three
  live routes must pass the same Kbar validation (OHLC, time alignment,
  volume/taker bounds); failures go to `rejects`.
- **`state.last_closed_open_time` is updated only after a successful current
  dataset flush**, never after merely buffering a closed Kbar.
- **Research agents are read-only** against the current historical dataset by
  default; they must not write to `local_data/live_update/` or the historical
  seed Parquet unless a task explicitly requires audit / debug / replay.

### Primary universe (data scope)

> **Data HUB is the source of truth.** Research agents **read-only mount** the
> Data HUB; they must not write the current dataset, live-update runtime, or the
> historical seed (except an explicit audit / debug / replay task).

- **Primary research / trading universe** = Binance **USDⓈ-M Futures**,
  `PERPETUAL`, `quote_asset = USDT`, **including delisted USDT perpetual
  contracts** for historical research.
- **Binance UM ≠ USDT pairs only.** USDⓈ-M Futures also lists USDC / BUSD quote
  pairs and delivery / settled / special symbols. **Do not treat all Binance UM
  symbols as the primary universe.**
- **Excluded from normal migration / trading research flow:** `quote_asset !=
  USDT`, USDC quote pairs (e.g. `KAITOUSDC`, `BTCUSDC`), BUSD quote pairs,
  delivery contracts (e.g. `BTCUSDT_230630`), SETTLED symbols (e.g.
  `CVXUSDTSETTLED`), non-ASCII symbols (e.g. `龙虾USDT`).
- **Corrupt parquet is noted / quarantined, not auto-fixed or deleted.**
  `KAITOUSDC` is a known quarantined symbol (USDC quote pair + unreadable source
  parquet). Migration must not proceed past a source-parquet readability
  failure.
- Full definition: `DATA_CONTRACT.md` → *Primary Universe Policy*. Acceptance
  checklist: `INIT_VERIFY.md`.

### Pending governance decisions (live update)

- `market.binance.um.klines.current` — whether to register as a formal derived
  dataset is pending (see `DATA_CONTRACT.md` → *Pending Governance Decisions*).
- `market.binance.um.klines.live_update` — whether it is only a runtime
  operational namespace or a registered dataset is pending.
- **Do not register either in `dataset_registry.json` until the governance
  decision is made**, and then update the registry, `DATA_CATALOG.md`, and
  `DATA_CONTRACT.md` in the same change.

---

## Current Priorities

1. Keep `python -m datahub.validation --all` and `unittest discover tests`
   green and **clone-safe** (no `local_data/` required).
2. Keep `local_data/` rebuildable from `INIT.md` → rebuild task → verify task.
3. Never commit `local_data/`; confirm `git status --short` before every commit.
4. Keep `dataset_registry.json` (authoritative) and `DATA_CATALOG.md` (derived)
   in sync — registry wins on any conflict.
5. Do not break the historical materialization main line when touching live
   update.

---

## Blocking Issues

- **None.**

---

## Recommended Next Actions

> Proposals only — stop at the phase boundary and wait for review before
> starting new scope.

1. Decide whether `contract_validated` should distinguish artifact validation
   from lifecycle promotion (applies to both datasets).
2. Add JSON Schema for `dataset_registry.json` and enforce validation in CI.
3. Auto-generate `DATA_CATALOG.md` from the registry instead of hand-maintaining.
4. Implement immutable, content-addressable snapshot publication.
5. Add historical delist/rename/merge source ingestion for Universe Metadata.
6. **Live update (post Phase 8 review):**
   1. Small-scope validation (single interval + few symbols) before any
      full-market deployment.
   2. Decide the `market.binance.um.klines.current` /
      `market.binance.um.klines.live_update` registry governance question.
   3. Production long-running daemon hardening (orchestration, retention,
      long-running reliability).
   4. CI validation for live-update paths.
   5. Current dataset partial-initialization hardening — when `current_root`
      has Parquet but the initialized marker is missing, avoid falsely
      reporting `already_initialized`; needs partial-copy detection, marker
      semantics, and a current-dataset integrity check.

---

## Important Files

| File | Role |
|------|------|
| `ROOT.md` | Highest-priority rules. Wins all conflicts. |
| `AGENTS.md` | This file. Current state + entry point. |
| `HANDOFF.md` | Architecture, decisions, known issues, pending work. |
| `README.md` | Project overview and structure. |
| `QUICKSTART.md` | Fast path to getting started. |
| `VERSION` | Current semantic version (`v0.14.0`). |
| `INIT.md` | New-machine / disaster-recovery entrypoint (3-layer init + rebuild). |
| `INIT_VERIFY.md` | Mandatory post-init acceptance checklist (universe + git + migration). |
| `LIVE_UPDATE.md` | Live-update task entrypoint (Phases 1–8 MVP primitives). |
| `CHANGELOG.md` | Human-readable history of changes. |
| `DATA_CONTRACT.md` | Dataset Contract Framework — schema + quality rules. |
| `DATA_CATALOG.md` | Data Catalog Framework — derived human-readable view. |
| `dataset_registry.json` | Authoritative, machine-readable dataset registry. |
| `docs/universe_metadata_sources.md` | Source authority review. |
| `docs/validation_framework.md` | Validation architecture and CLI docs. |
| `docs/universe_metadata_dataset.md` | Universe Metadata design + Phase 4 artifact notes. |
| `docs/binance_um_klines_dataset.md` | Binance Kline pipeline design + CLI. |
| `docs/research_agent_klines_access.md` | How a research agent reads Kline archives. |
| `docs/market_data_storage_policy.md` | `local_data/` storage + no-large-data-commit policy. |
| `data/manifests/reference/universe_metadata/manifest.json` | Phase 4 artifact manifest. |

| Directory | Purpose |
|-----------|---------|
| `datahub/ingestion/` | Universe Metadata + Binance Kline ingestion. |
| `datahub/materialization/` | Binance Kline → partitioned Parquet materialization. |
| `datahub/live_update.py` | Live update runtime (Phases 1–8 MVP primitives). |
| `datahub/validation/` | Executable validation framework. |
| `planning/tasks/` | Rebuild + verify tasks and historical phase task specs. |
| `data/` | Small committed reference artifacts for offline validation. |
| `local_data/` | Large market data + live-update runtime — **git-ignored, never committed**. |
| `scripts/` | Automation and operational scripts (incl. `live_update.py` wrapper). |
| `tests/` | Test suite and fixtures. |
| `reports/` | Generated reports and quality outputs. |
| `examples/` | Usage examples. |
| `logs/` | Runtime logs (git-ignored content). |
| `docs/` | Extended governance documentation. |
| `docs/live_update/` | Live-update per-module specs (`00`…`09`). |

---

## Onboarding Order

Core reading order (always):

1. `ROOT.md` — supreme rules and principles.
2. `AGENTS.md` — this file: current phase, status, next actions.
3. `HANDOFF.md` — architecture context and decisions.
4. `README.md` — project overview and structure.

Then, before touching data, read the governance set:
`DATA_CONTRACT.md` → `docs/metadata_standard.md` → `docs/registry_standard.md`
→ `docs/dataset_lifecycle.md` → `docs/authority_model.md` →
`docs/naming_convention.md` → `dataset_registry.json`.

For Phase 4 work, also read `docs/universe_metadata_sources.md`,
`docs/universe_metadata_dataset.md`, and `docs/validation_framework.md`.

---

## Operating Rules (quick reference)

- ROOT.md wins every conflict. Authority order:
  `ROOT.md` > `DATA_CONTRACT.md` > `dataset_registry.json` > `DATA_CATALOG.md`.
- Stop at each phase boundary. Wait for review. Never auto-advance.
- A dataset is not real until it is in `dataset_registry.json`.
- Validate against `DATA_CONTRACT.md` before trusting any data. Fail loud.
- The registry `status` MUST equal the dataset's true lifecycle state.
- Whoever updates the registry updates `DATA_CATALOG.md` in the same change.
- Keep this file current — it is the map every agent depends on.
