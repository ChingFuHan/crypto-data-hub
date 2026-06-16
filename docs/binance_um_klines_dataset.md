# Binance USD-M Futures Klines Dataset

> Phase 5 dataset design and pipeline reference. Subordinate to
> [ROOT.md](../ROOT.md) and [DATA_CONTRACT.md](../DATA_CONTRACT.md). Contract:
> [`DATA_CONTRACT.md#contract-binance-usd-m-futures-klines`](../DATA_CONTRACT.md).

---

## Summary

| Item | Value |
|------|-------|
| Base dataset_id | `market.binance.um.klines` |
| Interval variant | `market.binance.um.klines.<INTERVAL>` (first: `market.binance.um.klines.1d`) |
| Primary key | `symbol + interval + open_time` |
| Supported intervals | `1d` · `4h` · `1h` · `15m` · `5m` · `1m` |
| First production interval | `1d` |
| Lifecycle status | `draft` |
| Source | Binance Data Vision public archive |
| Local data root | `local_data/binance_um_klines/interval=<INTERVAL>/` (uncommitted) |
| Module | `datahub/ingestion/binance_um_klines.py` |

---

## Source authority

Primary source is the **Binance Data Vision public archive**:

- monthly archive package — **historical base**
- daily archive package — **recent delta**
- `reference.universe.metadata` — reference / cross-check **only**

Universe Metadata must **not** be the sole symbol source: the current active
universe omits delisted symbols. The archive index is authoritative for the full
historical symbol set (including delisted symbols).

Private API keys, account endpoints, and trading endpoints are **out of scope**
for this phase.

### Archive paths

```
data/futures/um/monthly/klines/<SYMBOL>/<INTERVAL>/<SYMBOL>-<INTERVAL>-<YYYY-MM>.zip
data/futures/um/daily/klines/<SYMBOL>/<INTERVAL>/<SYMBOL>-<INTERVAL>-<YYYY-MM-DD>.zip
```

Each `.zip` has a sibling `.zip.CHECKSUM` (`<sha256>  <zip_name>`). Download base
is `https://data.binance.vision/`; the symbol/file index is the S3 listing at
`https://s3-ap-northeast-1.amazonaws.com/data.binance.vision`.

---

## Kline interval vs archive package source

This distinction is load-bearing and kept separate everywhere:

- **Kline interval** — the trading-data period of each row: `1d`, `4h`, `1h`,
  `15m`, `5m`, `1m`.
- **Archive package source** — how Binance *packages* files on disk: `monthly`
  (historical base) or `daily` (recent delta).

`monthly` / `daily` are **not** Kline intervals. They are never part of the
schema or the primary key.

---

## Daily recent-delta policy

Default behavior (`--archive-source both`, the default):

- download every monthly archive package as the historical base;
- download a daily archive package **only** for dates not covered by an
  available monthly package (the recent delta);
- a daily file whose month is already covered by a monthly package is marked
  `skipped_by_default` and **not** downloaded.

Each daily archive file is classified as one of:

- `required_delta` — not covered by monthly → downloaded
- `skipped_by_default` — covered by monthly, default policy → skipped
- `included_by_explicit_full_daily_history` — covered by monthly but
  `--include-full-daily-history` was set → downloaded

`--include-full-daily-history` records the decision, the extra file count, and
the overlap in the manifest, and preserves the monthly package as the canonical
historical coverage.

---

## CLI

```
python -m datahub.ingestion.binance_um_klines --interval 1d --all
```

Flags:

| Flag | Meaning |
|------|---------|
| `--interval <INTERVAL>` | Kline interval (default `1d`). Unsupported → exit 2 + allowed list. |
| `--discover` | list archive files → `catalog/` |
| `--download` | download + verify zips and checksums |
| `--verify` | re-verify local files against stored checksums |
| `--report` | (re)write coverage reports + manifest |
| `--all` | discover + download + verify + report |
| `--resume` | skip already-verified files (downloads are idempotent regardless) |
| `--dry-run` | discover + plan only; no downloads, no writes |
| `--workers <N>` | concurrent workers (default 8) |
| `--timeout <SECONDS>` | per-request timeout (default 30) |
| `--retries <N>` | transient-error retries (default 3) |
| `--archive-source monthly\|daily\|both` | archive package source (default `both`) |
| `--symbols-file <PATH>` | symbol allow-list (txt one-per-line, or JSON array) |
| `--max-symbols <N>` | limit symbol count (sampling / testing) |
| `--local-root <PATH>` | local_data root (default `local_data/binance_um_klines`) |
| `--include-full-daily-history` | also download daily archives covered by monthly |
| `--archive-root <PATH>` | offline/testing: read archive from a local directory tree instead of HTTP |

Allowed intervals are defined once in `ALLOWED_INTERVALS`; nothing hard-codes
`1d`.

### Full local Kline archive command

Run the entire historical 1d archive for every symbol (monthly base + daily
delta), resumable:

```bash
python -m datahub.ingestion.binance_um_klines --interval 1d --all --workers 16
# interrupt-safe; re-run to resume:
python -m datahub.ingestion.binance_um_klines --interval 1d --resume --all --workers 16
```

Switch interval by changing `--interval` (e.g. `--interval 1h`). Because the
full archive is large, prefer: `--discover` → `--dry-run` → `--all`, and rely on
resume across runs.

---

## Resume & checksum behavior

- A file already present locally with a matching `.CHECKSUM` is re-verified and
  skipped (`skipped_existing_verified`) — runs are idempotent.
- Each downloaded zip is verified against its published SHA-256 `.CHECKSUM`.
- A checksum mismatch is an **error**: it is recorded in
  `reports/checksum_failures.jsonl`, raises `checksum_failed_count`, and the CLI
  exits non-zero (fail loud).
- Transient network errors are retried up to `--retries`; HTTP 404 is treated as
  a genuinely missing object (`missing_zip` / `missing_checksum`), not retried.

`checksum_status` ∈ `passed` · `failed` · `missing_checksum` · `missing_zip` ·
`skipped_existing_verified` · `download_failed` · `not_attempted`.

---

## Outputs

Discovery (`catalog/`): `archive_files.jsonl`, `symbols.json`,
`discovery_summary.json`, `research_access.json`.

Manifest (`manifests/`): `manifest.json` (run-level), `files.jsonl` (per-file).

Reports (`reports/`): `coverage_summary.json`, `missing_files.jsonl`,
`checksum_failures.jsonl`, `run_summary.md`.

See [docs/research_agent_klines_access.md](research_agent_klines_access.md) for
the full manifest field list and a minimal read example, and
[docs/market_data_storage_policy.md](market_data_storage_policy.md) for the
storage / commit policy.

---

## Schema & primary key

The normalized row schema and quality rules are defined in
[`DATA_CONTRACT.md#contract-binance-usd-m-futures-klines`](../DATA_CONTRACT.md).
The raw archive CSV columns are:

```
open_time, open, high, low, close, volume, close_time,
quote_asset_volume, count, taker_buy_volume, taker_buy_quote_volume, ignore
```

`open_time` / `close_time` are epoch milliseconds (UTC). `symbol` and `interval`
come from the archive path, not the CSV. Older archives have no header row; newer
files may include one. The trailing `ignore` column (always `0`) is a deprecated
Binance field.

Primary key: `(symbol, interval, open_time)` — a Kline row is uniquely
identified by its open time for a given symbol and interval.

---

## Known gaps

- Phase 5 verifies the **raw archive inventory + checksums** only. Row-level
  normalization and Parquet materialization are deferred to **Phase 6**.
- Full historical market data is **uncommitted** (lives under `local_data/`).
- The dataset remains lifecycle `draft`; `contract_validated = false`.
- Snapshot publication is deferred to a later phase.
