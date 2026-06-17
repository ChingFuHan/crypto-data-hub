# Binance UM 1D Kline — Parquet Materialization (Phase 6)

> How the immutable raw zip archive produced by
> `datahub.ingestion.binance_um_klines` is materialized into a
> DuckDB-queryable Parquet dataset. Subordinate to [ROOT.md](../ROOT.md).

---

## Layer model

| Layer | Path | Role |
|-------|------|------|
| **raw** | `local_data/binance_um_klines/interval=1d/raw/` | Immutable **source** layer (zip archives, Phase 5). |
| **parquet** | `local_data/binance_um_klines/interval=1d/parquet/` | **Query / materialized** layer (this phase). |
| query engine | — | **DuckDB** is the standard reader. |

- **CSV is only a transient format inside the zip parsing flow.** No persistent
  `.csv` is ever written under the materialized tree
  (`generated_csv_file_count == 0`).
- The query layer is **Parquet**; the standard read path is **DuckDB**.

> ⚠️ `local_data/` is **not** committed to Git. The Parquet layer exists only on
> the machine that ran materialization. A fresh clone has no data until the
> ingestion + materialization pipelines run.

---

## CLI

```bash
# Sample (subset -> SAMPLE_OUTPUT)
python -m datahub.materialization.binance_um_klines_parquet \
  --interval 1d --symbols BTCUSDT ETHUSDT --workers 2 --overwrite

# Full (all verified symbols -> FULL_OUTPUT)
python -m datahub.materialization.binance_um_klines_parquet \
  --interval 1d --all --workers 4 --resume
```

| Flag | Meaning |
|------|---------|
| `--interval 1d` | Only `1d` is supported in Phase 6. |
| `--all` | Materialize every verified symbol in the manifest. |
| `--symbols S1 S2` | Materialize only these symbols (yields `SAMPLE_OUTPUT`). |
| `--raw-root` | Raw archive root (default `…/interval=1d/raw`). |
| `--manifest` | Phase 5 run manifest (default `…/manifests/manifest.json`). |
| `--output-root` | Parquet output root (default `…/interval=1d/parquet`). |
| `--resume` | Skip symbols already materialized and verifiable on disk. |
| `--overwrite` | Rebuild existing output (per symbol). |
| `--workers N` | Parallel symbol workers (default 4). |
| `--strict` | Fail on any conflict / data-quality issue. |

Input is the per-file manifest (`files.jsonl`, referenced by the run
manifest's `file_manifest`); only `downloaded` + checksum-`passed` records are
materialized. Severe errors return a non-zero exit code.

### Resume

`--resume` makes a full run idempotent. Each symbol writes a sidecar at
`parquet/manifests/symbols/<SYMBOL>.json` recording its files and counts. On a
resumed run a symbol is skipped iff its sidecar is `ok` and every listed parquet
file still exists; otherwise it is rebuilt. Re-running
`--all --workers 4 --resume` after a complete run is a no-op materialization.

---

## Partition layout

Hive-style, one part file per `(symbol, year)`:

```
local_data/binance_um_klines/interval=1d/parquet/
  symbol=BTCUSDT/
    year=2019/part-000.parquet
    year=2020/part-000.parquet
  symbol=ETHUSDT/
    year=2019/part-000.parquet
  manifests/
    materialization_manifest.json
    symbols/<SYMBOL>.json          # per-symbol resume sidecars
  reports/
    coverage_report.json
    data_quality_report.json
    duplicate_report.json
    conflict_report.json
```

### Partition columns policy

- The logical DuckDB result **must** expose `symbol` and `year`.
- Physical parquet files **omit** `symbol` and `year`; DuckDB supplies them from
  the Hive path via `hive_partitioning = true`. This avoids duplicate/ambiguous
  `symbol`/`year` columns when querying.
- Validation checks the **logical DuckDB schema**, not only the physical file
  schema.

---

## Schema (logical, as DuckDB exposes it)

| Column | Type | Notes |
|--------|------|-------|
| `symbol` | VARCHAR | Hive partition column. |
| `interval` | VARCHAR | Fixed `"1d"`. |
| `open_time` | BIGINT | Millisecond epoch (UTC). |
| `open_time_utc` | TIMESTAMP | Naive UTC wall-clock. |
| `open_time_taipei` | TIMESTAMP | Naive Asia/Taipei wall-clock (UTC+8). |
| `date` | VARCHAR | `YYYY-MM-DD`, derived from `open_time_taipei`. |
| `year` | BIGINT | Hive partition column. |
| `month` | INTEGER | Taipei calendar month. |
| `open`/`high`/`low`/`close` | DOUBLE | OHLC prices. |
| `volume` | DOUBLE | Base-asset volume. |
| `close_time` | BIGINT | Millisecond epoch (UTC). |
| `quote_volume` | DOUBLE | Quote-asset volume. |
| `trade_count` | BIGINT | Number of trades. |
| `taker_buy_base_volume` | DOUBLE | Taker buy base volume. |
| `taker_buy_quote_volume` | DOUBLE | Taker buy quote volume. |
| `source_archive` | VARCHAR | Relative path to the source zip. |
| `archive_source` | VARCHAR | `monthly` or `daily`. |
| `archive_period` | VARCHAR | `YYYY-MM` (monthly) or `YYYY-MM-DD` (daily). |

The Binance `ignore` column is **dropped**. Raw CSV columns are interpreted
**positionally** — newer archives carry a header (`…,count,taker_buy_volume,…`)
whose names differ from the canonical names above, and older archives carry no
header; position is the stable contract.

---

## Date / timezone policy

- `date` is the **Asia/Taipei calendar date** derived from `open_time`.
- Both `open_time_utc` and `open_time_taipei` are materialized as **naive
  timestamps** (UTC wall-clock and UTC+8 wall-clock). This makes
  `CAST(open_time_taipei AS DATE)` independent of the DuckDB session timezone,
  so it always equals `date`.
- For 1D bars (open at 00:00 UTC = 08:00 Taipei) the Taipei calendar date equals
  the UTC calendar date.

---

## Dedup / conflict policy

Dedup key = `(symbol, interval, open_time)`.

| Situation | Action |
|-----------|--------|
| monthly & daily agree on a bar | keep one row (counted as **duplicate**) |
| monthly & daily disagree | **daily wins**, recorded as **conflict** |
| same source duplicated, consistent | keep one row (**duplicate**) |
| same source duplicated, inconsistent | keep first, recorded as **data-quality issue** |

`--strict` turns any conflict or data-quality issue (including OHLC-rule
violations and duplicate dates) into a symbol failure and a non-zero exit.

Within a symbol, rows are unique by `open_time`, sorted by `open_time`, and the
1D `date` is unique.

---

## Reports & manifest

`manifests/materialization_manifest.json` includes:
`materialized_dataset_id` (`market.binance.um.klines.1d.parquet`), `interval`,
`input_manifest`, `output_root`, `raw_discovered_symbol_count`, `symbol_count`,
`row_count`, `file_count`, `date_min`, `date_max`, `duplicate_count`,
`conflict_count`, `failed_symbol_count`, `failed_symbols`,
`generated_csv_file_count` (=0), `query_engine` (`duckdb`), `output_format`
(`parquet`), `output_scope`, `git_commit`, `created_at_utc`.

Reports: `coverage_report.json`, `data_quality_report.json`,
`duplicate_report.json`, `conflict_report.json`.

### FULL_OUTPUT vs SAMPLE_OUTPUT

- **FULL_OUTPUT** — `--all`, no failed symbols, and the materialized symbol set
  equals the raw 1D universe (≈ 921 symbols) from the Phase 5 manifest.
- **SAMPLE_OUTPUT** — only a subset is materialized (e.g. a `--symbols` run). A
  BTCUSDT/ETHUSDT run is `SAMPLE_OUTPUT`, **not** full completion.

---

## Validation

```bash
python -m datahub.validation \
  --target binance-um-klines-parquet \
  --interval 1d \
  --manifest local_data/binance_um_klines/interval=1d/parquet/manifests/materialization_manifest.json
```

Checks: manifest fixed fields; `output_root` exists; positive symbol/row counts;
`failed_symbol_count == 0`; report files exist & non-empty; no `.csv` under the
tree; parquet files exist; manifest `file_count` matches actual; DuckDB reads the
dataset; logical schema has all required columns; `(symbol, interval, open_time)`
unique; `(symbol, date)` unique; required fields non-null; OHLC rule; date policy
(`CAST(open_time_taipei AS DATE) == CAST(date AS DATE)`); manifest `row_count`
matches DuckDB `COUNT(*)`; `output_scope` classification (SAMPLE separated from
FULL).

`python -m datahub.validation --all` stays **clone-safe**: the parquet layer is
validated only when its manifest is present; otherwise the check is skipped.

---

## DuckDB example

```python
import duckdb

root = "local_data/binance_um_klines/interval=1d/parquet"

df = duckdb.sql(f"""
    SELECT symbol, MIN(date) AS date_min, MAX(date) AS date_max,
           COUNT(*) AS row_count
    FROM read_parquet('{root}/**/*.parquet', hive_partitioning = true)
    WHERE symbol IN ('BTCUSDT', 'ETHUSDT', 'SOLUSDT')
    GROUP BY symbol ORDER BY symbol
""").df()
print(df)
```

---

## Dependencies

`duckdb` (query engine) and `pyarrow` (parquet writer) are required. When a
dependency is missing the CLI prints a clear error and exits non-zero
(`dependency error: …`, exit 3). Install with `pip install duckdb pyarrow`.
