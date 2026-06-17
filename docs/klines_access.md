# Klines Access — Raw Archive vs Parquet Query Layer

> How to read Binance USD-M Futures Kline data from the two storage layers.
> Subordinate to [ROOT.md](../ROOT.md). For raw-archive enumeration details see
> [research_agent_klines_access.md](research_agent_klines_access.md); for the
> materialization design see
> [binance_um_klines_parquet_materialization.md](binance_um_klines_parquet_materialization.md).

---

## Two layers

| Layer | Path | Format | How to read |
|-------|------|--------|-------------|
| **raw** (source) | `local_data/binance_um_klines/interval=1d/raw/` | zip (CSV inside) | unzip + positional CSV parse |
| **parquet** (query) | `local_data/binance_um_klines/interval=1d/parquet/` | Parquet (Hive) | **DuckDB** `read_parquet(...)` |

- The **raw zip archive is the immutable source layer**. CSV exists only inside
  the zips; it is the transient parse format, never a persistent output.
- The **query layer is Parquet** and the **standard read path is DuckDB**.
- `local_data/` is **not** committed to Git — both layers are machine-specific.

---

## Reading the Parquet query layer (DuckDB)

```python
import duckdb

root = "local_data/binance_um_klines/interval=1d/parquet"
con = duckdb.connect()

# hive_partitioning=true exposes the symbol and year partition columns.
df = con.sql(f"""
    SELECT symbol, MIN(date) AS date_min, MAX(date) AS date_max,
           COUNT(*) AS row_count
    FROM read_parquet('{root}/**/*.parquet', hive_partitioning = true)
    WHERE symbol IN ('BTCUSDT', 'ETHUSDT', 'SOLUSDT')
    GROUP BY symbol ORDER BY symbol
""").df()
print(df)
```

Always pass `hive_partitioning = true` and use a recursive glob
(`**/*.parquet`). `symbol` and `year` come from the Hive path; they are **not**
stored as physical columns (this avoids duplicate/ambiguous columns).

### Logical schema

`symbol`, `interval`, `open_time`, `open_time_utc`, `open_time_taipei`, `date`,
`year`, `month`, `open`, `high`, `low`, `close`, `volume`, `close_time`,
`quote_volume`, `trade_count`, `taker_buy_base_volume`,
`taker_buy_quote_volume`, `source_archive`, `archive_source`, `archive_period`.

- `open_time` / `close_time` — epoch **milliseconds** (UTC).
- `open_time_utc` / `open_time_taipei` — naive timestamps (UTC and UTC+8
  wall-clock).
- `date` — `YYYY-MM-DD`, the **Asia/Taipei** calendar date of the bar.
- Primary key: `(symbol, interval, open_time)`; for 1D, `(symbol, date)` is also
  unique.

---

## Date / timezone policy

`date` is derived from `open_time_taipei` (Asia/Taipei). Because the timestamps
are stored as naive wall-clock values, `CAST(open_time_taipei AS DATE)` equals
`date` regardless of the DuckDB session timezone. For 1D bars (00:00 UTC =
08:00 Taipei) the Taipei date equals the UTC date.

---

## Duplicate / conflict policy

Bars are deduplicated on `(symbol, interval, open_time)`. When `monthly` and
`daily` archives disagree on a bar, **daily wins** and the disagreement is
recorded in `reports/conflict_report.json`. Identical cross-archive bars are
deduplicated and counted in `reports/duplicate_report.json`. Same-source
inconsistencies and OHLC-rule violations are recorded in
`reports/data_quality_report.json`.

---

## FULL_OUTPUT vs SAMPLE_OUTPUT

`manifests/materialization_manifest.json` carries `output_scope`:

- **FULL_OUTPUT** — the materialized symbol set covers the raw 1D universe
  (≈ 921 symbols).
- **SAMPLE_OUTPUT** — only a subset (e.g. a `BTCUSDT`/`ETHUSDT` run). Not full
  completion.

---

## Reading the raw layer (when you need the source)

See [research_agent_klines_access.md](research_agent_klines_access.md). In
short: enumerate verified zips from `manifests/files.jsonl`
(`checksum_status in {passed, skipped_existing_verified}`), unzip, and parse the
CSV **positionally** (newer files have a header, older files do not).

---

## Validation

```bash
python -m datahub.validation \
  --target binance-um-klines-parquet \
  --interval 1d \
  --manifest local_data/binance_um_klines/interval=1d/parquet/manifests/materialization_manifest.json
```

`python -m datahub.validation --all` validates both layers when present and
stays clone-safe (skips absent `local_data` layers).
