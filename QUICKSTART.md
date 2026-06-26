# QUICKSTART

Fast path to becoming productive in `crypto-data-hub`.

> **Current state:** Phase 12 complete (`v0.14.0`). The Binance USD-M Futures
> kline pipeline (raw ingestion → raw validation → Parquet materialization →
> Parquet validation) is in place for every supported interval
> (`1d`/`4h`/`1h`/`15m`/`5m`/`3m`/`1m`) — this is the stable main line. **Live
> Update Phases 1–8** (MVP primitives) are complete as a tested CLI skeleton
> (not a production daemon). Large market data + live runtime live under
> `local_data/` (uncommitted). Both registered datasets remain lifecycle
> `draft`.
>
> **New machine / `local_data/` recovery?** Start from `INIT.md`, then
> `planning/tasks/task_rebuild_all_klines.md` and
> `planning/tasks/task_rebuild_all_klines_verify.md` — do **not** re-run
> historical phase tasks (`task_v0.07` … `task_v0.13`).
>
> **Live-update task?** Read `LIVE_UPDATE.md` first (then
> `docs/live_update/*.md`), not this file.

---

## 1. Read the docs in order

```
1. ROOT.md       # supreme rules — wins all conflicts
2. AGENTS.md     # current phase, status, next actions
3. HANDOFF.md    # architecture + decisions
4. README.md     # project overview
```

Then, before touching any data: `DATA_CONTRACT.md` and `dataset_registry.json`.

---

## 2. Understand the layout

```
Governance:  ROOT.md · AGENTS.md · HANDOFF.md · README.md · QUICKSTART.md
Versioning:  VERSION · CHANGELOG.md
Data plane:  DATA_CONTRACT.md · dataset_registry.json · DATA_CATALOG.md
Code plane:  datahub/ · scripts/ · tests/ · reports/ · examples/ · logs/ · docs/
```

The validation package lives in `datahub/validation/`; Universe Metadata
ingestion lives in `datahub/ingestion/`. Snapshot publication remains future
work.

---

## 3. Check current state

- Version: see `VERSION` (currently `v0.14.0`).
- What's done / what's next: see `AGENTS.md`.
- Why things are the way they are: see `HANDOFF.md`.

---

## 4. Golden rules before you change anything

- **ROOT.md wins** every conflict. When unsure, re-read it.
- **Stop at phase boundaries.** Do not start the next phase without review.
- A dataset is not real until it is in `dataset_registry.json`.
- Validate against `DATA_CONTRACT.md` before trusting data. **Fail loud.**
- Keep `AGENTS.md` current after any state change.

---

## 5. Inspect the registry

The registry is plain JSON and validates with any standard tool, e.g.:

```bash
python -m json.tool dataset_registry.json
```

It currently holds **2 datasets** (`reference.universe.metadata` and
`market.binance.um.klines`, both `draft`) plus the `conventions` and
`dataset_entry_schema` blocks (the machine-readable registry contract). See
`docs/registry_standard.md` for how entries are structured and discovered, and
`docs/universe_metadata_dataset.md` / `docs/binance_um_klines_dataset.md` for the
dataset designs.

## 6. Run validation

```bash
python -m datahub.validation --all
python -m unittest discover tests
```

If your environment exposes only `python3`, use `python3 -m ...`.

> **Validation scope:** `python -m datahub.validation --all` is **clone-safe
> global validation** (registry/governance + any local kline manifest). It is
> **not** proof that every interval's `local_data/` has been rebuilt and
> validated. Full all-interval `local_data/` validation must follow
> `planning/tasks/task_rebuild_all_klines_verify.md`, which checks each interval
> (raw + Parquet) explicitly.

## 7. Run Universe Metadata ingestion

Online source fetch + normalize + validate:

```bash
python -m datahub.ingestion.universe_metadata --all
```

Offline deterministic re-run from committed raw snapshot:

```bash
python -m datahub.ingestion.universe_metadata --offline --all
```

Individual commands:

```bash
python -m datahub.ingestion.universe_metadata --fetch
python -m datahub.ingestion.universe_metadata --normalize
```

Current artifact locations:

- Raw snapshot: `data/raw/reference/universe_metadata/`
- Normalized artifact: `data/reference/universe_metadata/reference.universe.metadata.json`
- Manifest: `data/manifests/reference/universe_metadata/manifest.json`

Committed data artifacts are intentionally small reference artifacts for offline
validation.

## 8. Run the Binance USD-M Kline pipeline

Parameterized by Kline interval (`1d`/`4h`/`1h`/`15m`/`5m`/`3m`/`1m`); nothing
hard-codes a single interval. Large market data (raw archives + Parquet) goes to
`local_data/` (**uncommitted**). Per interval the flow is raw ingestion → raw
validation → Parquet materialization → Parquet validation.

```bash
# 1. inspect, then download raw archives (resumable):
python -m datahub.ingestion.binance_um_klines --interval 1d --discover
python -m datahub.ingestion.binance_um_klines --interval 1d --dry-run
python -m datahub.ingestion.binance_um_klines --interval 1d --all --workers 16
python -m datahub.ingestion.binance_um_klines --interval 1d --resume --all

# 2. validate the raw run manifest (explicit; not part of clone-safe --all):
python -m datahub.validation --target binance-um-klines --interval 1d \
  --manifest local_data/binance_um_klines/interval=1d/manifests/manifest.json

# 3. materialize verified archives into partitioned Parquet:
python -m datahub.materialization.binance_um_klines_parquet --interval 1d
```

> For a full new-machine rebuild across **all** intervals, follow `INIT.md` and
> `planning/tasks/task_rebuild_all_klines.md` rather than running intervals by
> hand, then verify with `planning/tasks/task_rebuild_all_klines_verify.md`.

See `docs/binance_um_klines_dataset.md` (pipeline + CLI),
`docs/research_agent_klines_access.md` (how to read the data), and
`docs/market_data_storage_policy.md` (storage / commit rules). The full archive
is large: prefer `--discover` → `--dry-run` → `--all`, and rely on resume.

---

## 9. Run the Live Update MVP (small-scope first)

> **Read `LIVE_UPDATE.md` and `docs/live_update/*.md` before any live-update
> work.** This is a quickstart, not the spec. Live Update Phases 1–8 are MVP
> primitives — a tested CLI skeleton, **not** a production long-running daemon.
> The historical materialization pipeline above remains the stable main line.

**Do not start with full-market / all-interval.** Validate small first: one
interval (`1m`) + a few symbols (`BTCUSDT ETHUSDT`). All output is git-ignored
runtime data under `local_data/` — **never commit it** (`git status --short`
before any commit).

```bash
# 1. inspect the planned data layout (no network, no writes):
.venv/bin/python scripts/live_update.py --interval 1m --symbols BTCUSDT ETHUSDT --describe-layout

# 2. inspect planned WebSocket connections:
.venv/bin/python scripts/live_update.py --interval 1m --symbols BTCUSDT ETHUSDT --describe-websocket-connections

# 3. inspect the planned webhook server:
.venv/bin/python scripts/live_update.py --interval 1m --symbols BTCUSDT ETHUSDT --describe-webhook-server

# 4. check data continuity on the current dataset (no long-running daemon):
.venv/bin/python scripts/live_update.py --interval 1m --symbols BTCUSDT ETHUSDT --check-continuity

# 5. run one update cycle (startup backfill + recent bars + forced flush), then exit:
.venv/bin/python scripts/live_update.py --interval 1m --symbols BTCUSDT ETHUSDT --once
```

`--symbols` accepts these equivalent small-scope forms:

```bash
--symbols BTCUSDT ETHUSDT
--symbols "BTCUSDT ETHUSDT"
--symbols BTCUSDT,ETHUSDT
```

Whole market (Binance USD-M USDT perpetuals, resolved via
`/fapi/v1/exchangeInfo`):

```bash
--symbols all
```

Whole-market smoke test (resolve all, keep the first 5):

```bash
--symbols all --max-symbols 5
```

> ⚠️ `--symbols all` increases REST / WebSocket / IO pressure. Don't combine it
> with `--interval all` on a first run — validate a small scope first, then
> widen. Data-writing modes refuse to run without `--symbols`; they never
> default to the whole market.

What gets written (all git-ignored):

- Current dataset: `local_data/binance_um_klines_current/interval=1m/parquet/`
- Runtime: `local_data/live_update/` (state, latest, buffers, `closed_buffer`,
  `rejects`)

Rules:

- `--interval all` is a CLI expansion semantic only — never sent to the
  Binance API.
- Unclosed Kbars never enter `closed_buffer` or the current dataset.
- WebSocket / REST / webhook share one Kbar validation path; failures go to
  `rejects`.
- `market.binance.um.klines.current` and `market.binance.um.klines.live_update`
  are **not yet registered** — pending governance decisions
  (`DATA_CONTRACT.md`).

Production long-running, full-market, all-interval daemon hardening is
**pending** — expand scope only after small-scope validation passes. See
`LIVE_UPDATE.md`, `docs/live_update/09_RUNBOOK.md`, and the live-update
validation tests under `tests/`.
