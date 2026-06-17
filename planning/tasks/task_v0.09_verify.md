# task_v0.09_verify.md

# Verify Phase 8 — Binance UM Kline 1H Parquet Materialization

你是本專案的 Verification Agent。

任務：驗證 Phase 8 的 1H Parquet 是否真的可用。

本階段驗證範圍：

    1H raw archive
    1H Parquet
    DuckDB read
    validation
    1D regression
    4H regression
    Git safety

PostgreSQL、live API、策略、交易層由後續 Phase 驗證。

---

## 1. 驗證目標

確認：

    1H raw zip -> 1H Parquet
    DuckDB 可讀
    output_scope=FULL_OUTPUT
    symbol_count=921
    row_count > 4H row_count
    row_count / 4H row_count >= 3.5
    1H time policy 正確
    1H key policy 正確
    1D regression 通過
    4H regression 通過
    persistent CSV count = 0
    validation 可重跑
    git tracked local_data count = 0

---

## 2. 路徑

Repo：

    ~/work/crypto-data-hub

1H raw manifest：

    local_data/binance_um_klines/interval=1h/manifests/manifest.json

1H parquet root：

    local_data/binance_um_klines/interval=1h/parquet/

1H materialization manifest：

    local_data/binance_um_klines/interval=1h/parquet/manifests/materialization_manifest.json

1D regression manifest：

    local_data/binance_um_klines/interval=1d/parquet/manifests/materialization_manifest.json

4H regression manifest：

    local_data/binance_um_klines/interval=4h/parquet/manifests/materialization_manifest.json

---

## 3. 預期值

1H raw：

    interval=1h
    symbol_count=921
    downloaded_count=33447
    verified_count=33447
    failed_count=0
    checksum_failed_count=0
    missing_count=0
    date_min=2019-12-31
    date_max=2026-06-16

1H parquet：

    materialized_dataset_id=market.binance.um.klines.1h.parquet
    interval=1h
    output_scope=FULL_OUTPUT
    symbol_count=921
    failed_symbol_count=0
    generated_csv_file_count=0

Key policy：

    unique key = (symbol, interval, open_time)
    max rows per (symbol, date) = 24

Time policy：

    open_time % 3600000 == 0
    close_time = open_time + 3599999
    date = open_time_taipei 的日期

Regression：

    1D parquet validation pass
    4H parquet validation pass

---

## 4. 必跑指令

進 repo：

    cd ~/work/crypto-data-hub

Repo：

    git log --oneline -8
    git status --short
    cat VERSION

CLI：

    python -m datahub.materialization.binance_um_klines_parquet --help

Tests：

    python -m unittest discover tests

Validation all：

    python -m datahub.validation --all

Raw 1H validation：

    python -m datahub.validation \
      --target binance-um-klines \
      --interval 1h \
      --manifest local_data/binance_um_klines/interval=1h/manifests/manifest.json

Explicit 1H validation：

    python -m datahub.validation \
      --target binance-um-klines-parquet \
      --interval 1h \
      --manifest local_data/binance_um_klines/interval=1h/parquet/manifests/materialization_manifest.json

1D regression validation：

    python -m datahub.validation \
      --target binance-um-klines-parquet \
      --interval 1d \
      --manifest local_data/binance_um_klines/interval=1d/parquet/manifests/materialization_manifest.json

4H regression validation：

    python -m datahub.validation \
      --target binance-um-klines-parquet \
      --interval 4h \
      --manifest local_data/binance_um_klines/interval=4h/parquet/manifests/materialization_manifest.json

Resume idempotency：

    python -m datahub.materialization.binance_um_klines_parquet \
      --interval 1h \
      --all \
      --workers 4 \
      --resume

Resume 成功標準：

    output_scope=FULL_OUTPUT
    symbol_count=921
    row_count 維持一致
    file_count 維持一致
    failed_symbol_count=0

---

## 5. Raw Manifest 檢查

```bash
python - <<'PY'
import json
from pathlib import Path

p = Path("local_data/binance_um_klines/interval=1h/manifests/manifest.json")
print("exists:", p.exists())

data = json.loads(p.read_text())

for k in [
    "dataset_id",
    "dataset_variant_id",
    "interval",
    "archive_package_sources",
    "symbol_count",
    "file_count",
    "downloaded_count",
    "verified_count",
    "failed_count",
    "checksum_failed_count",
    "missing_count",
    "total_bytes",
    "date_min",
    "date_max",
]:
    print(f"{k}: {data.get(k)}")
PY
```

通過標準：

    interval=1h
    symbol_count=921
    downloaded_count=verified_count=33447
    failed_count=0
    checksum_failed_count=0
    missing_count=0

---

## 6. Materialization Manifest 檢查

```bash
python - <<'PY'
import json
from pathlib import Path

raw_p = Path("local_data/binance_um_klines/interval=1h/manifests/manifest.json")
mat_p = Path("local_data/binance_um_klines/interval=1h/parquet/manifests/materialization_manifest.json")

print("raw_exists:", raw_p.exists())
print("mat_exists:", mat_p.exists())

raw = json.loads(raw_p.read_text())
mat = json.loads(mat_p.read_text())

raw_symbols = raw.get("symbol_count")
mat_symbols = mat.get("symbol_count")

for k in [
    "materialized_dataset_id",
    "interval",
    "output_scope",
    "raw_discovered_symbol_count",
    "symbol_count",
    "row_count",
    "file_count",
    "date_min",
    "date_max",
    "duplicate_count",
    "conflict_count",
    "failed_symbol_count",
    "generated_csv_file_count",
    "query_engine",
    "output_format",
    "git_commit",
]:
    print(f"{k}: {mat.get(k)}")

print("symbol_count_delta:", raw_symbols - mat_symbols)
PY
```

通過標準：

    materialized_dataset_id=market.binance.um.klines.1h.parquet
    interval=1h
    output_scope=FULL_OUTPUT
    raw_discovered_symbol_count=921
    symbol_count=921
    symbol_count_delta=0
    row_count > 0
    file_count > 0
    failed_symbol_count=0
    generated_csv_file_count=0
    query_engine=duckdb
    output_format=parquet

---

## 7. Report 檢查

```bash
python - <<'PY'
from pathlib import Path

base = Path("local_data/binance_um_klines/interval=1h/parquet/reports")

for name in [
    "coverage_report.json",
    "data_quality_report.json",
    "duplicate_report.json",
    "conflict_report.json",
]:
    p = base / name
    print(name, "exists:", p.exists(), "size:", p.stat().st_size if p.exists() else None)
PY
```

通過標準：

    全部存在
    全部 size > 0

---

## 8. CSV 檢查

```bash
find local_data/binance_um_klines/interval=1h/parquet -type f -name '*.csv' | wc -l
```

通過標準：

    0

---

## 9. Parquet 檔案數

```bash
find local_data/binance_um_klines/interval=1h/parquet -type f -name '*.parquet' | wc -l
```

通過標準：

    > 0

---

## 10. Manifest vs 實體資料

```bash
python - <<'PY'
import json
from pathlib import Path
import duckdb

manifest = Path("local_data/binance_um_klines/interval=1h/parquet/manifests/materialization_manifest.json")
data = json.loads(manifest.read_text())

root = "local_data/binance_um_klines/interval=1h/parquet"

actual_files = len(list(Path(root).rglob("*.parquet")))
actual_rows = duckdb.sql(f"""
    SELECT COUNT(*)
    FROM read_parquet('{root}/**/*.parquet', hive_partitioning=true)
""").fetchone()[0]

print("manifest_file_count:", data.get("file_count"))
print("actual_file_count:", actual_files)
print("manifest_row_count:", data.get("row_count"))
print("actual_row_count:", actual_rows)
PY
```

通過標準：

    manifest_file_count == actual_file_count
    manifest_row_count == actual_row_count

---

## 11. DuckDB Smoke Query

```bash
python - <<'PY'
import duckdb

root = "local_data/binance_um_klines/interval=1h/parquet"

df = duckdb.sql(f"""
    SELECT
        symbol,
        MIN(date) AS date_min,
        MAX(date) AS date_max,
        COUNT(*) AS row_count
    FROM read_parquet('{root}/**/*.parquet', hive_partitioning=true)
    WHERE symbol IN ('BTCUSDT', 'ETHUSDT')
    GROUP BY symbol
    ORDER BY symbol
""").df()

print(df)
PY
```

通過標準：

    BTCUSDT row_count > 0
    ETHUSDT row_count > 0

---

## 12. Schema 檢查

```bash
python - <<'PY'
import duckdb

root = "local_data/binance_um_klines/interval=1h/parquet"

df = duckdb.sql(f"""
    DESCRIBE
    SELECT *
    FROM read_parquet('{root}/**/*.parquet', hive_partitioning=true)
    LIMIT 1
""").df()

print(df)
PY
```

必須包含：

    symbol
    interval
    open_time
    open_time_utc
    open_time_taipei
    date
    year
    month
    open
    high
    low
    close
    volume
    close_time
    quote_volume
    trade_count
    taker_buy_base_volume
    taker_buy_quote_volume
    source_archive
    archive_source
    archive_period

---

## 13. NULL 檢查

```bash
python - <<'PY'
import duckdb

root = "local_data/binance_um_klines/interval=1h/parquet"

df = duckdb.sql(f"""
    SELECT COUNT(*) AS bad_null_rows
    FROM read_parquet('{root}/**/*.parquet', hive_partitioning=true)
    WHERE symbol IS NULL
       OR interval IS NULL
       OR open_time IS NULL
       OR open_time_utc IS NULL
       OR open_time_taipei IS NULL
       OR date IS NULL
       OR open IS NULL
       OR high IS NULL
       OR low IS NULL
       OR close IS NULL
       OR volume IS NULL
       OR close_time IS NULL
       OR quote_volume IS NULL
       OR trade_count IS NULL
       OR taker_buy_base_volume IS NULL
       OR taker_buy_quote_volume IS NULL
""").df()

print(df)
PY
```

通過標準：

    bad_null_rows=0

---

## 14. Duplicate Key 檢查

```bash
python - <<'PY'
import duckdb

root = "local_data/binance_um_klines/interval=1h/parquet"

df = duckdb.sql(f"""
    SELECT symbol, interval, open_time, COUNT(*) AS n
    FROM read_parquet('{root}/**/*.parquet', hive_partitioning=true)
    GROUP BY symbol, interval, open_time
    HAVING COUNT(*) > 1
    LIMIT 20
""").df()

print(df)
print("duplicate_rows_found:", len(df))
PY
```

通過標準：

    duplicate_rows_found=0

---

## 15. 1H 同日筆數檢查

```bash
python - <<'PY'
import duckdb

root = "local_data/binance_um_klines/interval=1h/parquet"

df = duckdb.sql(f"""
    SELECT symbol, date, COUNT(*) AS n
    FROM read_parquet('{root}/**/*.parquet', hive_partitioning=true)
    GROUP BY symbol, date
    HAVING COUNT(*) > 24
    LIMIT 20
""").df()

print(df)
print("symbol_date_rows_gt_24_found:", len(df))
PY
```

通過標準：

    symbol_date_rows_gt_24_found=0

---

## 16. 1H open_time 對齊

```bash
python - <<'PY'
import duckdb

root = "local_data/binance_um_klines/interval=1h/parquet"

df = duckdb.sql(f"""
    SELECT COUNT(*) AS bad_alignment_rows
    FROM read_parquet('{root}/**/*.parquet', hive_partitioning=true)
    WHERE open_time % 3600000 != 0
""").df()

print(df)
PY
```

通過標準：

    bad_alignment_rows=0

---

## 17. close_time 檢查

```bash
python - <<'PY'
import duckdb

root = "local_data/binance_um_klines/interval=1h/parquet"

df = duckdb.sql(f"""
    SELECT COUNT(*) AS bad_close_time_rows
    FROM read_parquet('{root}/**/*.parquet', hive_partitioning=true)
    WHERE close_time != open_time + 3599999
""").df()

print(df)
PY
```

通過標準：

    bad_close_time_rows=0

---

## 18. OHLC 檢查

```bash
python - <<'PY'
import duckdb

root = "local_data/binance_um_klines/interval=1h/parquet"

df = duckdb.sql(f"""
    SELECT COUNT(*) AS bad_rows
    FROM read_parquet('{root}/**/*.parquet', hive_partitioning=true)
    WHERE
        high IS NULL
        OR low IS NULL
        OR open IS NULL
        OR close IS NULL
        OR volume IS NULL
        OR quote_volume IS NULL
        OR trade_count IS NULL
        OR NOT (
            high >= low
            AND high >= open
            AND high >= close
            AND low <= open
            AND low <= close
            AND volume >= 0
            AND quote_volume >= 0
            AND trade_count >= 0
        )
""").df()

print(df)
PY
```

通過標準：

    bad_rows=0

---

## 19. Date Policy 檢查

```bash
python - <<'PY'
import duckdb

root = "local_data/binance_um_klines/interval=1h/parquet"

df = duckdb.sql(f"""
    SELECT COUNT(*) AS bad_date_rows
    FROM read_parquet('{root}/**/*.parquet', hive_partitioning=true)
    WHERE CAST(open_time_taipei AS DATE) != CAST(date AS DATE)
""").df()

print(df)
PY
```

通過標準：

    bad_date_rows=0

---

## 20. 1H vs 4H Row Count

```bash
python - <<'PY'
import duckdb

root_4h = "local_data/binance_um_klines/interval=4h/parquet"
root_1h = "local_data/binance_um_klines/interval=1h/parquet"

rows_4h = duckdb.sql(f"""
    SELECT COUNT(*)
    FROM read_parquet('{root_4h}/**/*.parquet', hive_partitioning=true)
""").fetchone()[0]

rows_1h = duckdb.sql(f"""
    SELECT COUNT(*)
    FROM read_parquet('{root_1h}/**/*.parquet', hive_partitioning=true)
""").fetchone()[0]

ratio = rows_1h / rows_4h if rows_4h else None

print("rows_4h:", rows_4h)
print("rows_1h:", rows_1h)
print("ratio:", ratio)
PY
```

通過標準：

    rows_1h > rows_4h
    ratio >= 3.5

---

## 21. Git Safety

```bash
git status --short
git ls-files local_data | wc -l
git status --short | grep 'local_data/' || true
```

通過標準：

    git ls-files local_data = 0
    local_data staged count = 0
    local_data tracked count = 0

---

## 22. Docs / Registry

```bash
grep -RIn "1h" docs DATA_CATALOG.md dataset_registry.json 2>/dev/null | head -50
ls -lh planning/tasks/task_v0.09.md planning/tasks/task_v0.09_verify.md
```

通過標準：

- docs 有 1H Parquet 使用說明
- DATA_CATALOG.md 有 1H materialized layer
- dataset_registry.json 有 `market.binance.um.klines.1h.parquet`
- task files 存在

---

## 23. 最終回報格式

請用以下格式：

Summary
- PASS / FAIL / PASS_WITH_WARNINGS
- 核心結論

Implementation
- CLI exists
- interval=1h supported
- interval=1d regression preserved
- interval=4h regression preserved
- validation target exists
- docs updated
- tests added

Raw 1H
- symbol_count
- downloaded_count
- verified_count
- failed_count
- checksum_failed_count
- missing_count
- date_min/date_max

Materialization
- output_scope
- symbol_count
- row_count
- file_count
- actual_duckdb_row_count
- row_count ratio vs 4H
- failed_symbol_count
- generated_csv_file_count

DuckDB Checks
- smoke query
- schema
- null
- duplicate key
- symbol/date max rows
- open_time alignment
- close_time
- OHLC
- date policy
- manifest vs actual

Validation
- unittest
- raw 1H validation
- explicit 1H validation
- explicit 1D regression
- explicit 4H regression
- validation --all
- resume idempotency

Git
- git status
- local_data tracked count
- commit hash
- VERSION

Problems
- 問題
- 證據
- 最小修法

Final Decision
- ACCEPT
- REJECT
- ACCEPT_WITH_WARNINGS

---

## 24. 判定規則

ACCEPT 條件：

    output_scope=FULL_OUTPUT
    symbol_count=921
    row_count > 4H row_count
    row_count / 4H row_count >= 3.5
    failed_symbol_count=0
    generated_csv_file_count=0
    DuckDB checks 全過
    explicit 1H validation 通過
    explicit 1D regression 通過
    explicit 4H regression 通過
    validation --all 通過
    unit tests 通過
    git tracked local_data count = 0
    working tree clean

REJECT 條件：

    SAMPLE_OUTPUT
    symbol_count 不足
    row_count <= 4H row_count
    row_count / 4H row_count < 3.5
    DuckDB 讀不到
    duplicate key count > 0
    required fields NULL count > 0
    OHLC fail
    open_time alignment fail
    close_time fail
    manifest count 對不上
    1D regression fail
    4H regression fail
    git tracked local_data count > 0
    validation fail

ACCEPT_WITH_WARNINGS 條件：

    功能正確
    資料完整
    問題只影響 metadata 或文件
    無資料完整性風險
