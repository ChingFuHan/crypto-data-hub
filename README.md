# crypto-data-hub

A long-term maintainable **crypto data platform repository** — the single,
unified data infrastructure providing **Dataset**, **Metadata**, **Registry**,
**Snapshot**, and **Documentation**.

> **Status:** Phase 12 complete — Binance UM Kline Parquet materialization for
> every supported interval (`1d`/`4h`/`1h`/`15m`/`5m`/`3m`/`1m`).
> **Version:** `v0.13.0` (see [`VERSION`](VERSION) / [`CHANGELOG.md`](CHANGELOG.md)).

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
├── VERSION                 # Semantic version (v0.13.0)
├── INIT.md                 # New-machine / disaster-recovery entrypoint
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
│   └── validation_framework.md       # Validation framework (Phase 3)
│
├── datahub/                # Core platform package
│   ├── ingestion/          # Universe Metadata + Binance Kline ingestion
│   ├── materialization/    # Binance Kline → partitioned Parquet
│   └── validation/         # Executable validation framework
├── data/                   # Small committed reference artifacts
├── local_data/             # Large market data (Kline archives) — git-ignored
├── scripts/                # Automation scripts
├── tests/                  # Test suite and fixtures
├── reports/                # Generated reports (future)
├── examples/               # Usage examples (future)
└── logs/                   # Runtime logs
```

Large market data (Binance Kline archives) lives under `local_data/` and is
**never committed** — see [`docs/market_data_storage_policy.md`](docs/market_data_storage_policy.md).

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
