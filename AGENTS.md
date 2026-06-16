# AGENTS.md

> **Agent primary entry point.** Read this after `ROOT.md`. Keep it short
> (target: under 200 lines). It reflects the **current** state of the repo.

---

## Current Phase

**Phase 5 — Binance USD-M Kline Historical Pipeline** (complete, awaiting review).

Delivery model: Architecture First → MVP First → Incremental Delivery →
Review Before Expansion. **Do not start Phase 6 without review approval.**

---

## Current Status

- Repo version: `v0.6.0`. `registry_version` stays `v0.2.0` because the
  registry contract shape did not change (a dataset entry was added, not a new
  field).
- Phases 0 (`v0.1.0`) through 4 (`v0.5.0`) are complete.
- Phase 5 added a parameterized Binance USD-M Futures Kline ingestion pipeline:
  - Ingestion CLI — `python -m datahub.ingestion.binance_um_klines --interval 1d --all`
  - Interval-parameterized (`1d`/`4h`/`1h`/`15m`/`5m`/`1m`); first interval `1d`,
    nothing hard-codes `1d`.
  - Source — Binance Data Vision public archive (monthly historical base + daily
    recent delta); archive discovery, zip + `.CHECKSUM` download, SHA-256 verify,
    resume / skip-verified.
  - Large market data lives under `local_data/binance_um_klines/interval=<INTERVAL>/`
    and is **git-ignored / uncommitted**.
  - Manifests, coverage reports, and a research-access manifest are written under
    `local_data/`.
  - Validation target `binance-um-klines` (explicit `--manifest`); clone-safe
    `--all` skips it when no `local_data` manifest exists.
  - Tests/fixtures — `tests/test_binance_um_klines.py`,
    `tests/fixtures/ingestion/binance_um_klines/`.
- Both datasets remain lifecycle `draft`:
  - `reference.universe.metadata` — `active_current` coverage (Phase 4).
  - `market.binance.um.klines` — Phase 5 verifies raw archive inventory +
    checksums only; row-level normalization / Parquet is deferred to Phase 6.
    `contract_validated = false`.
- Module execution entry points:
  - `python -m datahub.ingestion.binance_um_klines --interval 1d --all`
  - `python -m datahub.ingestion.universe_metadata --offline --all`
  - `python -m datahub.validation --all`
  - `python -m unittest discover tests`
- Awaiting Phase 5 review before any Phase 6 work begins.

---

## Current Priorities

1. Pass Phase 5 review.
2. Keep `python -m datahub.validation --all` and `unittest discover tests`
   green and **clone-safe** (no `local_data/` required).
3. Never commit `local_data/`; confirm `git status --short` before every commit.
4. Keep `dataset_registry.json` (authoritative) and `DATA_CATALOG.md` (derived)
   in sync — registry wins on any conflict.

---

## Blocking Issues

- **None blocking Phase 5 review.**
- Phase 6 is intentionally blocked pending review (governance, not a defect).

---

## Recommended Next Actions

> Proposals only — execute **after** Phase 5 review approval.

1. Phase 6: normalize verified Kline archives into a primary-keyed, partitioned
   Parquet materialization; make row-level rules K1–K4 executable.
2. Run remaining intervals (`4h`/`1h`/`15m`/`5m`/`1m`) through the same pipeline.
3. Decide whether `contract_validated` should distinguish artifact validation
   from lifecycle promotion (applies to both datasets).
4. Add JSON Schema for `dataset_registry.json` and enforce validation in CI.
5. Implement immutable, content-addressable snapshot publication.

---

## Important Files

| File | Role |
|------|------|
| `ROOT.md` | Highest-priority rules. Wins all conflicts. |
| `AGENTS.md` | This file. Current state + entry point. |
| `HANDOFF.md` | Architecture, decisions, known issues, pending work. |
| `README.md` | Project overview and structure. |
| `QUICKSTART.md` | Fast path to getting started. |
| `VERSION` | Current semantic version (`v0.6.0`). |
| `CHANGELOG.md` | Human-readable history of changes. |
| `DATA_CONTRACT.md` | Dataset Contract Framework — schema + quality rules. |
| `DATA_CATALOG.md` | Data Catalog Framework — derived human-readable view. |
| `dataset_registry.json` | Authoritative, machine-readable dataset registry. |
| `docs/universe_metadata_sources.md` | Source authority review. |
| `docs/validation_framework.md` | Validation architecture and CLI docs. |
| `docs/universe_metadata_dataset.md` | Universe Metadata design + Phase 4 artifact notes. |
| `docs/binance_um_klines_dataset.md` | Binance Kline pipeline design + CLI (Phase 5). |
| `docs/research_agent_klines_access.md` | How a research agent reads Kline archives. |
| `docs/market_data_storage_policy.md` | `local_data/` storage + no-large-data-commit policy. |
| `data/manifests/reference/universe_metadata/manifest.json` | Phase 4 artifact manifest. |

| Directory | Purpose |
|-----------|---------|
| `datahub/ingestion/` | Universe Metadata + Binance Kline ingestion. |
| `datahub/validation/` | Executable validation framework. |
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
