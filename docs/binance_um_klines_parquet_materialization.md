# Binance UM Kline Parquet Materialization (Phases 6–12)

> How the immutable raw zip archive produced by
> `datahub.ingestion.binance_um_klines` is materialized into
> DuckDB-queryable Parquet datasets. Subordinate to [ROOT.md](../ROOT.md).

---

## Layer Model

| Layer | Path | Role |
|-------|------|------|
| raw | `local_data/binance_um_klines/interval=<INTERVAL>/raw/` | Immutable source layer (zip archives). |
| parquet | `local_data/binance_um_klines/interval=<INTERVAL>/parquet/` | Query / materialized layer. |
| query engine | DuckDB | Standard reader for the Parquet layer. |

CSV is only a transient format inside zip parsing. No persistent `.csv` is
written under the materialized tree (`generated_csv_file_count = 0`).

`local_data/` is not committed to Git. A fresh clone has no raw or Parquet
market data until the ingestion and materialization pipelines run.

---

## Supported Intervals

The materializer currently supports:

- `1d` from Phase 6.
- `4h` from Phase 7.
- `1h` from Phase 8.
- `15m` from Phase 9.
- `5m` from Phase 10.
- `3m` from Phase 11.
- `1m` from Phase 12.

Materialized dataset IDs:

- `market.binance.um.klines.1d.parquet`
- `market.binance.um.klines.4h.parquet`
- `market.binance.um.klines.1h.parquet`
- `market.binance.um.klines.15m.parquet`
- `market.binance.um.klines.5m.parquet`
- `market.binance.um.klines.3m.parquet`
- `market.binance.um.klines.1m.parquet`

Phase 12 produced the 1m Parquet materialization at
`local_data/binance_um_klines/interval=1m/parquet/` with `FULL_OUTPUT`,
`raw_discovered_symbol_count = 922`, `symbol_count = 922`,
`row_count = 918113837`, `file_count = 2667`, `failed_symbol_count = 0`, and
`generated_csv_file_count = 0`.

---

## CLI

```bash
# Sample output
python -m datahub.materialization.binance_um_klines_parquet \
  --interval 1h --symbols BTCUSDT ETHUSDT --workers 2 --overwrite

# Full output
python -m datahub.materialization.binance_um_klines_parquet \
  --interval 1h --all --workers 4 --resume
```

| Flag | Meaning |
|------|---------|
| `--interval 1d|4h|1h|15m|5m|3m|1m` | Materialized interval. Defaults to `1d`. |
| `--all` | Materialize every verified symbol in the raw manifest. |
| `--symbols S1 S2` | Materialize only selected symbols (`SAMPLE_OUTPUT`). |
| `--raw-root` | Raw archive root. Default is `local_data/.../interval=<INTERVAL>/raw`. |
| `--manifest` | Raw run manifest. Default is `local_data/.../interval=<INTERVAL>/manifests/manifest.json`. |
| `--output-root` | Parquet output root. Default is `local_data/.../interval=<INTERVAL>/parquet`. |
| `--resume` | Skip symbols whose sidecar is complete and whose parquet files still exist. |
| `--overwrite` | Rebuild selected symbol output. |
| `--workers N` | Parallel symbol workers. |
| `--strict` | Return non-zero on conflicts or data-quality issues. |

Input is the raw run manifest's `file_manifest`; only downloaded and
checksum-verified records are materialized.

---

## Partition Layout

Hive-style, one part file per `(symbol, year)`:

```text
local_data/binance_um_klines/interval=4h/parquet/
  symbol=BTCUSDT/
    year=2019/part-000.parquet
    year=2020/part-000.parquet
  manifests/
    materialization_manifest.json
    symbols/<SYMBOL>.json
  reports/
    coverage_report.json
    data_quality_report.json
    duplicate_report.json
    conflict_report.json
```

Physical parquet files omit `symbol` and `year`; DuckDB supplies them from the
Hive path via `hive_partitioning = true`. Validation checks the DuckDB logical
schema, not only the physical file schema.

---

## Logical Schema

DuckDB exposes:

`symbol`, `interval`, `open_time`, `open_time_utc`, `open_time_taipei`, `date`,
`year`, `month`, `open`, `high`, `low`, `close`, `volume`, `close_time`,
`quote_volume`, `trade_count`, `taker_buy_base_volume`,
`taker_buy_quote_volume`, `source_archive`, `archive_source`, `archive_period`.

The Binance `ignore` column is dropped. Raw CSV columns are interpreted
positionally, supporting both header and header-less archives.

---

## Date And Key Policy

`date` is the Asia/Taipei calendar date derived from `open_time_taipei`.
`open_time_utc` and `open_time_taipei` are stored as naive wall-clock
timestamps, making `CAST(open_time_taipei AS DATE) = CAST(date AS DATE)`
independent of the DuckDB session timezone.

Primary key for all materialized intervals:

```text
(symbol, interval, open_time)
```

Interval-specific rules:

| Interval | Time rule | `(symbol, date)` rule |
|----------|-----------|------------------------|
| `1d` | `open_time % 86400000 == 0`; `close_time = open_time + 86399999` | Unique. |
| `4h` | `open_time % 14400000 == 0`; `close_time = open_time + 14399999` | At most 6 rows. |
| `1h` | `open_time % 3600000 == 0`; `close_time = open_time + 3599999` | At most 24 rows. |
| `15m` | `open_time % 900000 == 0`; `close_time = open_time + 899999` | At most 96 rows. |
| `5m` | `open_time % 300000 == 0`; `close_time = open_time + 299999` | At most 288 rows. |
| `3m` | `open_time % 180000 == 0`; `close_time = open_time + 179999` | At most 480 rows. |
| `1m` | `open_time % 60000 == 0`; `close_time = open_time + 59999` | At most 1440 rows. |

`(symbol, date)` is a grouping field for `4h`, `1h`, `15m`, `5m`, `3m`, and
`1m`, not a unique key.

---

## Dedup And Conflict Policy

Dedup key = `(symbol, interval, open_time)`.

| Situation | Action |
|-----------|--------|
| monthly and daily agree on a bar | Keep one row; record a duplicate. |
| monthly and daily disagree | Daily wins; record a conflict. |
| same source duplicated and consistent | Keep one row; record a duplicate. |
| same source duplicated and inconsistent | Keep first; record a data-quality issue. |

`--strict` turns any conflict or quality issue into a symbol failure and a
non-zero command exit.

---

## Manifest And Reports

`manifests/materialization_manifest.json` includes:

`materialized_dataset_id`, `interval`, `input_manifest`, `output_root`,
`raw_discovered_symbol_count`, `symbol_count`, `row_count`, `file_count`,
`date_min`, `date_max`, `duplicate_count`, `conflict_count`,
`failed_symbol_count`, `failed_symbols`, `generated_csv_file_count`,
`query_engine`, `output_format`, `output_scope`, `git_commit`, `code_version`,
`created_at_utc`.

Reports:

- `reports/coverage_report.json`
- `reports/data_quality_report.json`
- `reports/duplicate_report.json`
- `reports/conflict_report.json`

`FULL_OUTPUT` means `--all`, no failed symbols, and materialized symbols equal
the raw interval universe. `SAMPLE_OUTPUT` means a subset, such as
`--symbols BTCUSDT ETHUSDT`.

---

## Validation

```bash
python -m datahub.validation \
  --target binance-um-klines-parquet \
  --interval 1h \
  --manifest local_data/binance_um_klines/interval=1h/parquet/manifests/materialization_manifest.json
```

Checks include manifest fixed fields, output root, reports, no `.csv` files,
actual parquet file count, DuckDB readability, logical schema, required
non-null fields, `(symbol, interval, open_time)` uniqueness, per-interval
`(symbol, date)` row limit, OHLC rules, date policy, open-time alignment,
close-time delta, and manifest row/file counts matching actual output.

For production `FULL_OUTPUT` runs the validator also enforces a cross-interval
row-count regression against the coarser interval's manifest:

- `4h` row_count must exceed `1d` row_count.
- `1h` row_count must exceed `4h` row_count, and `1h / 4h >= 3.5`.
- `15m` row_count must exceed `1h` row_count, and `15m / 1h >= 3.5`.
- `5m` row_count must exceed `15m` row_count, and `5m / 15m >= 2.5`.
- `3m` row_count must exceed `5m` row_count, and `3m / 5m >= 1.5`.
- `1m` row_count must exceed `3m` row_count, and `1m / 3m >= 2.5`.

These regression checks are skipped for sample fixtures and when the baseline
interval manifest is absent. The production gate is
`raw_discovered_symbol_count >= 921` (the established baseline plus any later
listings), not a hard `== 921`.

### Symbol universe (new listings)

`FULL_OUTPUT` means `symbol_count == raw_discovered_symbol_count` — the
materialized set equals the raw universe discovered at ingestion time, **not** a
fixed number. Phase 11's 3m ingestion discovered **922** symbols versus the
**921** baseline shared by 1D/4H/1H/15M/5M. The extra symbol is `REUSDT`, a
perpetual **listed after the earlier phases ran** (first 3m archive
`REUSDT-3m-2026-06-18.zip`). This is a new listing, not a data error: each
interval's universe reflects its own ingestion date. The raw layer is never
trimmed to force a count; cross-interval research should intersect symbols until
the coarser intervals are re-ingested.

`python -m datahub.validation --all` remains clone-safe: raw and Parquet
`local_data` layers are validated only when their manifests are present.

---

## DuckDB Example

```python
import duckdb

root = "local_data/binance_um_klines/interval=4h/parquet"

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

## Future Phases

PostgreSQL serving, live API updates, strategy / trading layers, and research
workspace integration are outside Phase 12.

Dependencies: `duckdb` for validation/querying and `pyarrow` for writing.
