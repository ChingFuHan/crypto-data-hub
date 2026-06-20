# Klines Access — Raw Archive Vs Parquet Query Layer

> How to read Binance USD-M Futures Kline data from the raw and Parquet
> storage layers. Subordinate to [ROOT.md](../ROOT.md). For raw-archive
> enumeration details see [research_agent_klines_access.md](research_agent_klines_access.md);
> for materialization design see
> [binance_um_klines_parquet_materialization.md](binance_um_klines_parquet_materialization.md).

---

## Layers

| Layer | Path | Format | How to read |
|-------|------|--------|-------------|
| raw source | `local_data/binance_um_klines/interval=<INTERVAL>/raw/` | zip (CSV inside) | unzip and positional CSV parse |
| parquet query | `local_data/binance_um_klines/interval=<INTERVAL>/parquet/` | Parquet (Hive) | DuckDB `read_parquet(...)` |

CSV exists only inside raw zip archives and the transient parser. Persistent CSV
file count under the Parquet tree must remain zero.

`local_data/` is machine-specific and not committed.

---

## DuckDB Read Path

```python
import duckdb

root = "local_data/binance_um_klines/interval=4h/parquet"
con = duckdb.connect()

df = con.sql(f"""
    SELECT symbol, MIN(date) AS date_min, MAX(date) AS date_max,
           COUNT(*) AS row_count
    FROM read_parquet('{root}/**/*.parquet', hive_partitioning = true)
    WHERE symbol IN ('BTCUSDT', 'ETHUSDT', 'SOLUSDT')
    GROUP BY symbol ORDER BY symbol
""").df()
print(df)
```

Always use:

```text
read_parquet('local_data/binance_um_klines/interval=<INTERVAL>/parquet/**/*.parquet', hive_partitioning=true)
```

`symbol` and `year` come from the Hive path; they are not stored as physical
columns in the parquet files.

---

## Logical Schema

DuckDB exposes:

`symbol`, `interval`, `open_time`, `open_time_utc`, `open_time_taipei`, `date`,
`year`, `month`, `open`, `high`, `low`, `close`, `volume`, `close_time`,
`quote_volume`, `trade_count`, `taker_buy_base_volume`,
`taker_buy_quote_volume`, `source_archive`, `archive_source`, `archive_period`.

`open_time` and `close_time` are epoch milliseconds. `open_time_utc` and
`open_time_taipei` are naive wall-clock timestamps. `date` is the Asia/Taipei
calendar date of `open_time_taipei`.

---

## Key And Date Policy

Primary key:

```text
(symbol, interval, open_time)
```

Interval rules:

| Interval | Dataset ID | Date policy |
|----------|------------|-------------|
| `1d` | `market.binance.um.klines.1d.parquet` | `(symbol, date)` is unique. |
| `4h` | `market.binance.um.klines.4h.parquet` | `(symbol, date)` is a grouping field with at most 6 rows. |
| `1h` | `market.binance.um.klines.1h.parquet` | `(symbol, date)` is a grouping field with at most 24 rows. |
| `15m` | `market.binance.um.klines.15m.parquet` | `(symbol, date)` is a grouping field with at most 96 rows. |
| `5m` | `market.binance.um.klines.5m.parquet` | `(symbol, date)` is a grouping field with at most 288 rows. |
| `3m` | `market.binance.um.klines.3m.parquet` | `(symbol, date)` is a grouping field with at most 480 rows. |

For `4h`, `1h`, `15m`, `5m`, and `3m`, do not join or upsert by `(symbol, date)` alone.

---

## FULL_OUTPUT Vs SAMPLE_OUTPUT

The materialization manifest records `output_scope`:

- `FULL_OUTPUT`: `--all`, zero failed symbols, materialized symbols equal the raw
  interval universe.
- `SAMPLE_OUTPUT`: selected symbols only, such as `BTCUSDT ETHUSDT`.

Phase 7 completion requires the 4h Parquet manifest to be `FULL_OUTPUT` with
the full raw 4h symbol set. Phase 8 adds the same requirement for the 1h
Parquet manifest, with `1h` row_count exceeding `4h` row_count (ratio `>= 3.5`).
Phase 9 adds the same requirement for the 15m Parquet manifest, with `15m`
row_count exceeding `1h` row_count (ratio `>= 3.5`). Phase 10 adds the same
requirement for the 5m Parquet manifest, with `5m` row_count exceeding `15m`
row_count (ratio `>= 2.5`). Phase 11 adds the same requirement for the 3m
Parquet manifest, with `3m` row_count exceeding `5m` row_count (ratio `>= 1.5`).

---

## Reading Raw Archives

See [research_agent_klines_access.md](research_agent_klines_access.md). In
short: enumerate verified zips from `manifests/files.jsonl`
(`checksum_status in {passed, skipped_existing_verified}`), unzip, and parse
CSV positionally. Newer files may have a header; older files may not.

---

## Validation

```bash
python -m datahub.validation \
  --target binance-um-klines-parquet \
  --interval 4h \
  --manifest local_data/binance_um_klines/interval=4h/parquet/manifests/materialization_manifest.json
```

`python -m datahub.validation --all` validates local raw and Parquet layers only
when their manifests exist, so a fresh clone remains clone-safe.
