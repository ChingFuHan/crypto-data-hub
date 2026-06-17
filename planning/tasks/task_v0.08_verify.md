# task_v0.08_verify.md

# Verify Phase 7 — Binance UM Kline 4H Parquet Materialization

你是本專案的 Verification Agent。

任務：驗證 Phase 7 的 4h Parquet 是否真的可用。

本階段驗證範圍：

    4h raw archive
    4h Parquet
    DuckDB read
    validation
    1D regression
    Git safety

PostgreSQL、live API、策略、交易層由後續 Phase 驗證。

---

## 1. 驗證目標

確認：

    4h raw zip -> 4h Parquet
    DuckDB 可讀
    output_scope=FULL_OUTPUT
    symbol_count=921
    4h time policy 正確
    4h key policy 正確
    1D regression 通過
    persistent CSV count = 0
    validation 可重跑
    git tracked local_data count = 0

---

## 2. 路徑

Repo：

    ~/work/crypto-data-hub

4h raw manifest：

    local_data/binance_um_klines/interval=4h/manifests/manifest.json

4h parquet root：

    local_data/binance_um_klines/interval=4h/parquet/

4h materialization manifest：

    local_data/binance_um_klines/interval=4h/parquet/manifests/materialization_manifest.json

1D regression manifest：

    local_data/binance_um_klines/interval=1d/parquet/manifests/materialization_manifest.json

---

## 3. 預期值

4h raw：

    interval=4h
    symbol_count=921
    downloaded_count=32656
    downloaded_count=32656
    verified_count=32656
    failed_count=0
    checksum_failed_count=0
    missing_count=0
    date_min=2019-12-31
    date_max=2026-06-15

4h parquet：

    materialized_dataset_id=market.binance.um.klines.4h.parquet
    interval=4h
    output_scope=FULL_OUTPUT
    symbol_count=921
    failed_symbol_count=0
    generated_csv_file_count=0

Key policy：

    unique key = (symbol, interval, open_time)
    max rows per (symbol, date) = 6

Time policy：

    open_time % 14400000 == 0
    close_time = open_time + 14399999
    date = open_time_taipei 的日期

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

Raw 4h validation：

    python -m datahub.validation \
      --target binance-um-klines \
      --interval 4h \
      --manifest local_data/binance_um_klines/interval=4h/manifests/manifest.json

Explicit 4h validation：

    python -m datahub.validation \
      --target binance-um-klines-parquet \
      --interval 4h \
      --manifest local_data/binance_um_klines/interval=4h/parquet/manifests/materialization_manifest.json

1D regression validation：

    python -m datahub.validation \
      --target binance-um-klines-parquet \
      --interval 1d \
      --manifest local_data/binance_um_klines/interval=1d/parquet/manifests/materialization_manifest.json

Resume idempotency：

    python -m datahub.materialization.binance_um_klines_parquet \
      --interval 4h \
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

p = Path("local_data/binance_um_klines/interval=4h/manifests/manifest.json")
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
