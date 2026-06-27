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

# 5. run one complete live update cycle (ensure current symbols from seed +
#    backfill/REST gap repair, merge to current parquet, update state), then exit:
.venv/bin/python scripts/live_update.py --interval 1m --symbols BTCUSDT ETHUSDT --once
```

`--once` = **run one complete live update cycle and exit** — it catches up to the
latest closed bars (writing closed_buffer, merging into current parquet, updating
state only after a successful merge) and emits a machine-readable `once_update`
JSON. It shares the same core flow as `--run-startup-backfill-once`; `--once` is
the user-facing shorthand, `--run-startup-backfill-once` is the explicit
startup-backfill one-shot mode. Both require `--symbols`; a seed-missing symbol
stays `bootstrap_required` (no REST, no zero-history rebuild).

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

### New-machine / partial current symbol flow

1. Build the historical seed with `INIT.md` (`local_data/binance_um_klines/...`).
2. Initialize the current dataset for the symbols you want, e.g. one symbol:

   ```bash
   .venv/bin/python scripts/live_update.py \
     --interval 1m --symbols ETHUSDT --initialize-current-dataset
   ```

3. Then catch up the latest gap:

   ```bash
   .venv/bin/python scripts/live_update.py \
     --interval 1m --symbols ETHUSDT --run-startup-backfill-once
   ```

**Partial current symbol missing:** if the seed has a symbol but
`local_data/binance_um_klines_current/.../symbol=ETHUSDT` is missing, that is a
*partial current symbol missing*, **not** a historical bootstrap. Repair it with
`--initialize-current-dataset --symbols ETHUSDT` — it copies only that symbol's
seed parquet into current (temp dir then rename; never overwrites, never deletes,
never touches the seed). Statuses: `initialized_current_symbol_from_seed`,
`already_available`, or `bootstrap_required` (seed genuinely missing). Don't
initialize the whole market up front — validate a small scope first.

**Current dataset layout = year/month.** The canonical current layout is
`symbol=<S>/year=<YYYY>/month=<MM>/part-000.parquet`. The historical seed may
still be year-only (`symbol=<S>/year=<YYYY>/part-000.parquet`); initialization
from seed **converts** it to year/month (re-derived from `open_time`), so it does
not copy the year-only layout into current. If a symbol ends up with both
year-only and year/month parquet in current, that's a *mixed layout* (DuckDB
`hive_partitioning=true` will error). Audit it read-only — writes nothing, no
auto-migration:

```bash
.venv/bin/python scripts/live_update.py --interval 1m \
  --symbols BTCUSDT ETHUSDT --audit-current-layout
```

For a per-symbol migration **dry-run precheck** (status, expected canonical
year/month partitions, row/duplicate counts, recommended action) — also
read-only, no writes, no Binance call, local current-dataset discovery when
symbols are omitted:

```bash
.venv/bin/python scripts/live_update.py --interval 1m \
  --symbols BTCUSDT ETHUSDT --plan-current-layout-migration
```

Neither command migrates data. Actual mixed-layout migration must be run
separately and verified by row count / duplicates / continuity. (1m full-market
precheck reads many parquet files and can be slow — scope it with `--symbols`.)

To pick the next safe batch, use the read-only planner (writes nothing, no
Binance, local current-dataset only). It defaults to `year_only_needs_migration`,
excludes mixed/canonical, and ranks no-duplicates + small row_count first:

```bash
# JSON with ranked candidates
.venv/bin/python scripts/live_update.py --interval 1m \
  --list-current-layout-migration-candidates --limit 10 --max-row-count 300000

# just the symbols, ready to paste into --symbols
.venv/bin/python scripts/live_update.py --interval 1m \
  --list-current-layout-migration-candidates --limit 10 --max-row-count 300000 \
  --output-symbols-only
```

Keep batches small (`--limit 10` / `--max-row-count`), migrate, verify, repeat.
`BTCUSDT` / `ETHUSDT` (mixed) are excluded by default — handle them later with
`--include-mixed` once year-only migration is proven.

To auto-slice candidates into batches instead of pasting them by hand, use the
**controlled batch planner** `--plan-current-layout-migration-batches`. This first
version is **plan / dry-run only — it never executes a migration**, never writes
parquet / stage / backup / jsonl / state / registry, and never contacts Binance
(it does not touch `dataset_registry.json`). It reuses the candidate planner, pulls
a large ranked pool, applies the exclude filters, then slices the survivors into
batches:

```bash
.venv/bin/python scripts/live_update.py --interval 1m \
  --plan-current-layout-migration-batches \
  --batch-size 10 --max-row-count 300000 --max-batches 2 \
  --quote-assets USDT \
  --exclude-delivery-contracts --exclude-settled --exclude-non-ascii \
  --exclude-symbols BTCUSDT ETHUSDT KAITOUSDC \
  --dry-run-batches
```

> **Primary universe = USDT quote perpetual.** Binance UM (USDⓈ-M Futures) is the
> venue, not the universe: it also lists USDC / BUSD quote pairs and delivery /
> SETTLED / non-ASCII symbols, which are **not** part of the primary universe.
> `--quote-assets USDT` is implemented and affects only batch planner candidate
> filtering, not the live daemon, `--once`, or startup backfill. It supports
> `--quote-assets USDT`, `--quote-assets USDT,USDC`, and quoted lists such as
> `--quote-assets "USDT USDC"`; quote asset is detected by suffix (`USDT` /
> `USDC` / `BUSD`). Delivery contracts still require
> `--exclude-delivery-contracts`; quote mismatches appear in
> `excluded.quote_asset_mismatch`, and active filters appear in
> `filters.quote_assets`. `KAITOUSDC` is additionally a **quarantined** symbol
> (unreadable source parquet) — do not re-run its migration, auto-fix, or delete
> it. See `DATA_CONTRACT.md` → *Primary Universe Policy* and `INIT_VERIFY.md`.

Defaults: only `year_only_needs_migration`, excludes mixed / canonical /
source_missing, and excludes `BTCUSDT` / `ETHUSDT` by default (migrate those mixed
symbols last, on their own). `--dry-run-batches` runs each symbol through
`--migrate-current-layout` with `execute=False` — still writing nothing. The
emitted `commands.execute` strings are reference only; **this CLI does not execute
them** — there is no `--execute-batches`. Copy the per-batch execute command
yourself, run it, verify, repeat. It is an independent mode: it rejects `--symbols`,
`--interval all`, invalid numeric args, and any other current-layout / `--once` /
backfill / init mode (fail fast). Keep `--batch-size` at 10 for plain symbols (20
only once proven); split symbols with `row_count > 300000` into smaller batches or
one at a time; never run the whole market at once.

To actually migrate one symbol to canonical year/month, use
`--migrate-current-layout` (dry-run by default; add `--execute` to write). It
requires explicit `--symbols` and a single concrete `--interval` (`all` rejected
in both), stages + verifies the rewrite, backs up the original under
`interval=<I>/_layout_migration_backup/<ts>/` (outside the parquet root, so audit
/ discovery ignore it), then promotes the stage; on verification failure the
original is left untouched. Start with a small symbol like `URNMUSDT`:

```bash
# dry-run plan (writes nothing)
.venv/bin/python scripts/live_update.py --interval 1m \
  --symbols URNMUSDT --migrate-current-layout

# execute (writes + replaces; leaves a __backup_migrate_<ts> dir)
.venv/bin/python scripts/live_update.py --interval 1m \
  --symbols URNMUSDT --migrate-current-layout --execute
```

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
