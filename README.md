# crypto-data-hub

A long-term maintainable **crypto data platform repository** — the single,
unified data infrastructure providing **Dataset**, **Metadata**, **Registry**,
**Snapshot**, and **Documentation**.

> **Status:** Phase 12 complete — Binance UM Kline Parquet materialization for
> every supported interval (`1d`/`4h`/`1h`/`15m`/`5m`/`3m`/`1m`). **Live Update
> Phases 1–8** (MVP primitives) complete — see
> [Live Update](#live-update-mvp-primitives) below.
> **Version:** `v0.14.0` (see [`VERSION`](VERSION) / [`CHANGELOG.md`](CHANGELOG.md)).

---

## Core Design Principles

- **Maintainability** — simple, readable, discoverable.
- **Reproducibility** — anything can be regenerated from recorded inputs + code.
- **Scalability** — grows in datasets, volume, and contributors without rework.
- **Data Quality** — validated against an explicit contract; bad data fails loud.
- **Automation** — validation, registry, snapshots, and reporting are scripted.

Full rules live in [`ROOT.md`](ROOT.md), the highest-priority document.

---

## Repository Structure

```
crypto-data-hub/
├── ROOT.md                 # Supreme rules — wins all conflicts
├── AGENTS.md               # Agent entry point — current state
├── HANDOFF.md              # Architecture + decisions
├── README.md               # This file
├── QUICKSTART.md           # Fast path to getting started
├── VERSION                 # Semantic version (v0.14.0)
├── INIT.md                 # New-machine / disaster-recovery entrypoint
├── LIVE_UPDATE.md          # Live update task entrypoint (Phases 1–8 MVP)
├── CHANGELOG.md            # Human-readable change history
│
├── DATA_CATALOG.md         # Data Catalog Framework — derived view
├── DATA_CONTRACT.md        # Dataset Contract Framework — schema + quality rules
├── dataset_registry.json   # Authoritative, machine-readable registry
│
├── docs/                   # Governance standards
│   ├── dataset_lifecycle.md    # Lifecycle states + transitions
│   ├── metadata_standard.md    # Metadata fields, types, rules
│   ├── registry_standard.md    # Registry structure, versioning, discovery
│   ├── authority_model.md      # Authority + sync + update model
│   ├── naming_convention.md    # Naming rules
│   ├── universe_metadata_dataset.md  # First dataset design (Phase 2)
│   ├── validation_framework.md       # Validation framework (Phase 3)
│   └── live_update/            # Live update per-module specs (Phases 1–8)
│
├── datahub/                # Core platform package
│   ├── ingestion/          # Universe Metadata + Binance Kline ingestion
│   ├── materialization/    # Binance Kline → partitioned Parquet
│   ├── live_update.py      # Live update runtime (Phases 1–8 MVP)
│   └── validation/         # Executable validation framework
├── data/                   # Small committed reference artifacts
├── local_data/             # Large market data + live runtime — git-ignored
├── scripts/                # Automation scripts (incl. live_update.py wrapper)
├── tests/                  # Test suite and fixtures
├── reports/                # Generated reports (future)
├── examples/               # Usage examples (future)
└── logs/                   # Runtime logs
```

Large market data (Binance Kline archives) lives under `local_data/` and is
**never committed** — see [`docs/market_data_storage_policy.md`](docs/market_data_storage_policy.md).

---

## Live Update MVP Primitives

> **Historical materialization is the stable main line** (Phases 6–12,
> unchanged). Live update is an incremental layer on top of it.

**Live Update Phases 1–8 are complete as MVP primitives.** This delivers a
tested CLI skeleton and validation checks — **not** a production-hardened
long-running daemon.

- **Task entrypoint:** [`LIVE_UPDATE.md`](LIVE_UPDATE.md) (read this first for
  any live-update task; do not rely on README/AGENTS alone).
- **Per-module specs:** [`docs/live_update/`](docs/live_update/)
  (`00_OVERVIEW.md` … `09_RUNBOOK.md`).
- **CLI:** `scripts/live_update.py` (thin wrapper over `datahub.live_update`).
- **Current historical dataset** (research-agent default read-only entry
  point): `local_data/binance_um_klines_current/interval=<INTERVAL>/parquet/`.
- **Runtime buffers / state / latest / closed_buffer / rejects:**
  `local_data/live_update/`.
- **Phase 1–8 scope:** primitives → current dataset init + Parquet merge →
  state + startup backfill planning → REST backfill → WebSocket primitives →
  webhook primitives → CLI modes → continuity / validation checks.

**Production long-running, full-market, all-interval daemon hardening is
pending.** Orchestration, retention manager, and long-running reliability work
remains future work. Do **not** deploy a full-market / all-interval
long-running daemon without first completing small-scope validation.

### Small-scope validation (recommended first run)

Use a single interval (`1m`) and a small symbol set (`BTCUSDT ETHUSDT`). All
output is git-ignored runtime data under `local_data/` — never commit it.

```bash
.venv/bin/python scripts/live_update.py --interval 1m --symbols BTCUSDT ETHUSDT --describe-layout
.venv/bin/python scripts/live_update.py --interval 1m --symbols BTCUSDT ETHUSDT --describe-websocket-connections
.venv/bin/python scripts/live_update.py --interval 1m --symbols BTCUSDT ETHUSDT --describe-webhook-server
.venv/bin/python scripts/live_update.py --interval 1m --symbols BTCUSDT ETHUSDT --check-continuity
.venv/bin/python scripts/live_update.py --interval 1m --symbols BTCUSDT ETHUSDT --once
```

> `--interval all` is a CLI expansion semantic only and is **never** sent to
> the Binance API. Unclosed Kbars must never enter `closed_buffer` or the
> current dataset. WebSocket / REST / webhook share one Kbar validation path.

> **Registry note:** `market.binance.um.klines.current` and
> `market.binance.um.klines.live_update` are **not yet registered** in
> `dataset_registry.json` — pending governance decisions (see
> [`DATA_CONTRACT.md`](DATA_CONTRACT.md) → *Pending Governance Decisions*).

---

## Getting Started

New here? Read in this order:

1. [`ROOT.md`](ROOT.md) — supreme rules and principles
2. [`AGENTS.md`](AGENTS.md) — current phase, status, next actions
3. [`HANDOFF.md`](HANDOFF.md) — architecture and decisions
4. [`README.md`](README.md) — this overview

Then see [`QUICKSTART.md`](QUICKSTART.md). Before touching data, read the
governance set: [`DATA_CONTRACT.md`](DATA_CONTRACT.md),
[`docs/metadata_standard.md`](docs/metadata_standard.md),
[`docs/registry_standard.md`](docs/registry_standard.md),
[`docs/dataset_lifecycle.md`](docs/dataset_lifecycle.md),
[`docs/authority_model.md`](docs/authority_model.md),
[`docs/naming_convention.md`](docs/naming_convention.md), and
[`dataset_registry.json`](dataset_registry.json).

Run current validation checks from repo root:

```bash
python -m datahub.validation --all
python -m datahub.ingestion.universe_metadata --offline --all
python -m unittest discover tests
```

> **Validation scope:** `python -m datahub.validation --all` is **clone-safe
> global validation** (registry/governance + any local kline manifest). It does
> **not** prove that every interval's `local_data/` has been rebuilt and
> validated. Full all-interval `local_data/` validation follows
> [`planning/tasks/task_rebuild_all_klines_verify.md`](planning/tasks/task_rebuild_all_klines_verify.md).

**New machine or `local_data/` recovery?** Start from [`INIT.md`](INIT.md), then
[`planning/tasks/task_rebuild_all_klines.md`](planning/tasks/task_rebuild_all_klines.md)
(rebuild) and
[`planning/tasks/task_rebuild_all_klines_verify.md`](planning/tasks/task_rebuild_all_klines_verify.md)
(verify) — do **not** re-run historical phase tasks.

---

## Governance

This repo uses **Architecture First → MVP First → Incremental Delivery →
Review Before Expansion**. Work proceeds in numbered phases; each phase stops on
completion and waits for review. Agents must not auto-advance to the next phase.
See [`ROOT.md`](ROOT.md) → *Phased Delivery Governance*.
