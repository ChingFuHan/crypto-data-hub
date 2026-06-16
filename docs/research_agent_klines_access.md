# Research Agent — Binance Klines Access

> How a research agent locates and reads verified Binance USD-M Futures Kline
> archives produced by `datahub.ingestion.binance_um_klines`. Subordinate to
> [ROOT.md](../ROOT.md).

---

## Access summary

| Item | Value |
|------|-------|
| dataset_id | `market.binance.um.klines` |
| interval variant | `market.binance.um.klines.<INTERVAL>` (e.g. `.1d`) |
| primary key | `symbol + interval + open_time` |
| local_data path | `local_data/binance_um_klines/interval=<INTERVAL>/` |
| manifest | `…/manifests/manifest.json` |
| file manifest | `…/manifests/files.jsonl` |
| catalog inventory | `…/catalog/archive_files.jsonl` |
| coverage report | `…/reports/coverage_summary.json` |
| machine-generated access metadata | `…/catalog/research_access.json` |

> ⚠️ **`local_data/` is NOT committed to Git.** These files exist only on the
> machine that ran the pipeline. A fresh clone has no Kline data until the
> pipeline runs.

---

## Raw layout

```
local_data/binance_um_klines/interval=<INTERVAL>/
├── raw/monthly/<SYMBOL>/<SYMBOL>-<INTERVAL>-<YYYY-MM>.zip
├── raw/daily/<SYMBOL>/<SYMBOL>-<INTERVAL>-<YYYY-MM-DD>.zip
└── checksums/<SOURCE>/<SYMBOL>/<...>.zip.CHECKSUM
```

---

## Schema (raw archive CSV)

```
open_time, open, high, low, close, volume, close_time,
quote_asset_volume, count, taker_buy_volume, taker_buy_quote_volume, ignore
```

- `open_time` / `close_time` — epoch **milliseconds**, UTC.
- `open/high/low/close` — prices in the quote asset.
- `volume` — base-asset volume; `quote_asset_volume` — quote-asset volume.
- `count` — number of trades.
- `taker_buy_volume` / `taker_buy_quote_volume` — taker-side volumes.
- `ignore` — deprecated Binance field (always `0`).
- `symbol` and `interval` are **not** CSV columns — read them from the archive
  path. Older archives have no header row; newer files may include one.

Primary key: `(symbol, interval, open_time)`.

---

## Kline interval vs archive package source

- **Kline interval** (`1d/4h/1h/15m/5m/1m`) = the row period.
- **Archive package source** (`monthly`/`daily`) = how Binance packages files —
  `monthly` is the historical base, `daily` is the recent delta. It is **not** a
  Kline interval.

A research agent must not assume the **current active universe equals the
historical universe**: the archive includes delisted symbols. Use the catalog /
file manifest, not `reference.universe.metadata`, to enumerate historical
symbols.

---

## How to locate files

- **By symbol** — filter `manifests/files.jsonl` (or `catalog/archive_files.jsonl`)
  by `symbol`; zips live at `raw/<source>/<symbol>/`.
- **By interval** — each interval has its own root:
  `local_data/binance_um_klines/interval=<INTERVAL>/`.
- **Check coverage** — read `reports/coverage_summary.json`
  (`discovered_symbol_count`, `verified_file_count`, `date_min`, `date_max`,
  `known_gaps`).
- **Check checksum status** — read `manifests/files.jsonl` `checksum_status`.
  Only `passed` and `skipped_existing_verified` are verified-good.

---

## Manifest fields

`manifests/manifest.json` (run-level): `dataset_id`, `dataset_variant_id`,
`interval`, `local_root`, `run_id`, `symbol_count`, `file_count`,
`downloaded_count`, `verified_count`, `skipped_count`, `failed_count`,
`checksum_failed_count`, `missing_count`, `skipped_daily_overlap_count`,
`total_bytes`, `date_min`, `date_max`, `archive_package_sources`,
`include_full_daily_history`, `daily_delta_policy`, `primary_key`,
`validation_summary`.

`manifests/files.jsonl` (per-file): `symbol`, `interval`,
`archive_package_source`, `archive_period`, `zip_name`, `checksum_name`,
`source_path`, `local_zip_path`, `local_checksum_path`, `checksum_status`,
`download_status`, `file_size`, `retry_count`, `skip_reason`.

---

## Minimal read example

Read one symbol's first archive, verified, and print the first rows:

```python
import csv, io, json, zipfile
from pathlib import Path

INTERVAL = "1d"
SYMBOL = "BTCUSDT"
root = Path(f"local_data/binance_um_klines/interval={INTERVAL}")

# 1. locate verified archives for the symbol via the file manifest
records = [
    json.loads(line)
    for line in (root / "manifests" / "files.jsonl").read_text().splitlines()
    if line.strip()
]
verified = [
    r for r in records
    if r["symbol"] == SYMBOL
    and r["checksum_status"] in ("passed", "skipped_existing_verified")
]
first = sorted(verified, key=lambda r: r["archive_period"])[0]

# 2. read one archive zip / csv
zip_path = Path(first["local_zip_path"])
with zipfile.ZipFile(zip_path) as zf:
    raw = zf.read(zf.namelist()[0]).decode("utf-8")

# 3. print the first rows (skip a header row if present)
rows = list(csv.reader(io.StringIO(raw)))
if rows and rows[0][0] == "open_time":   # newer files carry a header
    rows = rows[1:]
columns = [
    "open_time", "open", "high", "low", "close", "volume", "close_time",
    "quote_asset_volume", "count", "taker_buy_volume",
    "taker_buy_quote_volume", "ignore",
]
print(columns)
for row in rows[:3]:
    print(dict(zip(columns, row)))
# open_time / close_time are epoch milliseconds (UTC); symbol+interval come
# from the path, not the CSV.
```

---

## Research-agent-ready checklist

- ✅ Agent can locate verified raw Kline archives (file manifest + `local_zip_path`).
- ✅ Agent can inspect coverage / missing files (`reports/`).
- ✅ Agent can distinguish Kline interval from archive package source.
- ✅ Agent does not assume the current active universe equals the historical universe.

Full Parquet materialization remains a **Phase 6** candidate.
