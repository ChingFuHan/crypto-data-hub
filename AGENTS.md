# AGENTS.md

> **Agent primary entry point.** Read this after `ROOT.md`. Keep it short
> (target: under 200 lines). It reflects the **current** state of the repo.

---

## Current Phase

**Phase 12 complete — Binance UM Kline Parquet Materialization.** Version
`v0.13.0`.

The Binance USD-M Futures kline pipeline (raw ingestion → raw validation →
Parquet materialization → Parquet validation) is in place for every supported
interval: `1d`, `4h`, `1h`, `15m`, `5m`, `3m`, `1m`.

Delivery model: Architecture First → MVP First → Incremental Delivery →
Review Before Expansion. Each phase stops at its boundary for review; the repo
sits at a reviewed Phase 12 boundary. The new-machine / recovery entrypoint is
`INIT.md` (see below) — not a re-run of historical phase tasks.

---

## Current Status

- Repo version: `v0.13.0`. `registry_version` stays `v0.2.0` because the
  registry contract shape has not changed (dataset entries were added, not new
  fields).
- Phases 0 (`v0.1.0`) through 12 (`v0.13.0`) are complete. The Binance USD-M
  Futures Kline pipeline is fully built and materialized to Parquet for every
  supported interval (`1d`/`4h`/`1h`/`15m`/`5m`/`3m`/`1m`).
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
- Both datasets remain lifecycle `draft`:
  - `reference.universe.metadata` — `active_current` coverage.
  - `market.binance.um.klines` — raw archive inventory/checksums **and**
    materialized Parquet are validated; `contract_validated = false`.
- Module execution entry points:
  - `python -m datahub.ingestion.binance_um_klines --interval 1d --all`
  - `python -m datahub.materialization.binance_um_klines_parquet --interval 1d`
  - `python -m datahub.ingestion.universe_metadata --offline --all`
  - `python -m datahub.validation --all`
  - `python -m unittest discover tests`

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

## Current Priorities

1. Keep `python -m datahub.validation --all` and `unittest discover tests`
   green and **clone-safe** (no `local_data/` required).
2. Keep `local_data/` rebuildable from `INIT.md` → rebuild task → verify task.
3. Never commit `local_data/`; confirm `git status --short` before every commit.
4. Keep `dataset_registry.json` (authoritative) and `DATA_CATALOG.md` (derived)
   in sync — registry wins on any conflict.

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

---

## Important Files

| File | Role |
|------|------|
| `ROOT.md` | Highest-priority rules. Wins all conflicts. |
| `AGENTS.md` | This file. Current state + entry point. |
| `HANDOFF.md` | Architecture, decisions, known issues, pending work. |
| `README.md` | Project overview and structure. |
| `QUICKSTART.md` | Fast path to getting started. |
| `VERSION` | Current semantic version (`v0.13.0`). |
| `INIT.md` | New-machine / disaster-recovery entrypoint (rebuild + verify). |
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
| `datahub/validation/` | Executable validation framework. |
| `planning/tasks/` | Rebuild + verify tasks and historical phase task specs. |
| `data/` | Small committed reference artifacts for offline validation. |
| `local_data/` | Large market data (Kline archives) — **git-ignored, never committed**. |
| `scripts/` | Automation and operational scripts. |
| `tests/` | Test suite and fixtures. |
| `reports/` | Generated reports and quality outputs. |
| `examples/` | Usage examples. |
| `logs/` | Runtime logs (git-ignored content). |
| `docs/` | Extended governance documentation. |

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
