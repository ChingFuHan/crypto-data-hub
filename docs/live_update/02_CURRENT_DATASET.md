# 02_CURRENT_DATASET.md

# Current Historical Dataset 規格

本文件定義 current historical dataset 的初始化、schema、partition、micro-batch flush 與 merge 規則。

---

## 1. 角色

current historical dataset 是研究 agent 的唯一預設資料入口。

路徑：

```text
local_data/binance_um_klines_current/interval=<INTERVAL>/parquet/
```

它包含：

```text
第一次歷史資料建置產生的歷史 KBar
+
live update 後續補上的完整 KBar
```

---

## 2. 初始化來源

來源：

```text
local_data/binance_um_klines/interval=<INTERVAL>/parquet/
```

目標：

```text
local_data/binance_um_klines_current/interval=<INTERVAL>/parquet/
```

初始化規則：

```text
1. 啟動時檢查 current historical dataset 是否存在。
2. 若不存在，從 historical seed parquet 複製或 materialize 一份 current dataset。
3. 複製時保留 schema、partition、排序。
4. current dataset 建立完成後，live update 只 merge 新完整 KBar 到 current dataset。
5. 若 seed parquet 不存在，回報 bootstrap_required。
6. 若 current dataset 已存在，不得每次覆蓋。
7. 若 current dataset 部分存在，缺的 interval 初始化，已存在的 interval 不覆蓋。
```

當 `--interval all` 時，每個 interval 都需要獨立初始化。

若某個 interval 不存在，不應阻斷其他 interval。

### 2.1 Partial current symbol missing（per-symbol 修復）

interval 層級的 marker / parquet 存在，**不代表**每個 symbol 都存在。會出現：

```text
seed   symbol=ETHUSDT 存在
current symbol=ETHUSDT 不存在
```

這是 **partial current dataset symbol missing**，不是 historical bootstrap
missing。診斷必須區分：

```text
A. seed symbol 缺 + current symbol 缺      -> bootstrap_required
B. seed symbol 在 + current symbol 缺      -> initialized_current_symbol_from_seed（copy 修復）
C. current symbol 在                       -> already_available（不覆蓋、不重 copy）
```

修復方式（只處理明確指定的 symbols，不做整個 interval / 全市場隱式搬運）：

```bash
.venv/bin/python scripts/live_update.py \
  --interval 1m \
  --symbols ETHUSDT \
  --initialize-current-dataset
```

行為：

- 只 copy 指定 symbol 的 seed parquet 到 current dataset。
- 先 copy 到 temp dir，再 rename 到 target；失敗不留半成品（不會被誤認完整）。
- 不覆蓋已存在的 current symbol，不刪資料，不改 seed，不寫 dataset_registry.json。

`--run-startup-backfill-once` 在做 REST gap backfill 前，會先對指定 symbols
做上述修復：seed 在但 current 缺時先 copy，再依 current max_open_time 補到
latest closed KBar；seed 缺時仍回 bootstrap_required，不從 0 重建歷史。

---

## 3. 初始化 Marker

初始化完成後，需建立 marker file：

```text
local_data/binance_um_klines_current/interval=<INTERVAL>/_current_dataset_initialized.json
```

格式：

```json
{
  "initialized_at_utc": "2026-06-26T00:00:00Z",
  "seed_root": "local_data/binance_um_klines/interval=1m/parquet",
  "current_root": "local_data/binance_um_klines_current/interval=1m/parquet",
  "interval": "1m",
  "method": "copy",
  "schema_version": 1,
  "dataset_version": "current-v1"
}
```

若 marker 存在且 current parquet 存在，啟動時不得重複全量複製。

---

## 4. Partition 規則

路徑：

```text
local_data/binance_um_klines_current/interval=<INTERVAL>/parquet/
  symbol=<SYMBOL>/
    year=<YYYY>/
      month=<MM>/
        part-000.parquet
```

範例：

```text
local_data/binance_um_klines_current/interval=1m/parquet/
  symbol=BTCUSDT/
    year=2026/
      month=06/
        part-000.parquet
```

### 4.1 Canonical layout governance（year/month）

current dataset 的 **canonical partition layout 是 year/month**：

```text
symbol=<SYMBOL>/year=<YYYY>/month=<MM>/part-000.parquet
```

這是 live update / gap repair / append merge 的 canonical write target。

規則：

- historical seed（`local_data/binance_um_klines/...`）**可以**仍是 year-only：
  `symbol=<SYMBOL>/year=<YYYY>/part-000.parquet`。
- current initialization from seed **會轉換**成 year/month：讀 seed parquet，依
  `open_time` 推導的 canonical `year`/`month` 重新寫出。不會把 seed 的 year-only
  layout 原樣 copy 進 current。轉換走 temp dir 後 atomic rename；失敗不留半成品、
  不覆蓋既有 current symbol、不改 seed。
- 若 current 內**同一 symbol 同時存在** year-only 與 year/month parquet，屬於
  **mixed layout**。DuckDB `hive_partitioning=true` 會因此報 Hive partition
  mismatch。

#### 檢查（dry-run，不改資料）

```bash
.venv/bin/python scripts/live_update.py \
  --interval 1m \
  --symbols BTCUSDT ETHUSDT \
  --audit-current-layout
```

輸出 JSON：`year_only_file_count`、`year_month_file_count`、`mixed_symbol_count`、
`mixed_symbols`、`status`（`ok` / `mixed_layout_detected`）。此模式只讀檔案，
**不寫** parquet / jsonl / state，**不**自動 migration。

#### Migration dry-run precheck（read-only）

```bash
.venv/bin/python scripts/live_update.py \
  --interval 1m \
  --symbols BTCUSDT ETHUSDT \
  --plan-current-layout-migration
```

每個 symbol 輸出 precheck，包含：

```text
symbol / interval
status        no_migration_needed / year_only_needs_migration / mixed_layout_needs_migration
year_only_file_count / year_month_file_count
year_only_files / year_month_files
expected_canonical_partition_count / expected_canonical_partitions
row_count / min_open_time / max_open_time / duplicate_open_time_count
recommended_action
```

`expected_canonical_partitions` 依每筆 `open_time` 推導的 canonical year/month
計算（不靠 year-only 原始路徑 year）。

差異：

- `--audit-current-layout` = layout audit（counts / mixed 偵測）。
- `--plan-current-layout-migration` = dry-run migration precheck（per-symbol
  status、expected partitions、row/duplicate 驗證資訊）。

兩者皆 **read-only**：只讀 parquet，**不寫**任何檔案、**不**移動 / 刪除 / 覆蓋
資料、**不**自動 migration、**不**打 Binance / exchangeInfo。未提供 `--symbols`
（或 `--symbols all`）時，掃描**本地 current dataset**（local discovery），不是
交易所全市場。

> mixed layout migration 必須**另行執行**，且需 row-count / duplicate /
> continuity 驗證後才可替換 symbol dir。本次只提供 audit 與 dry-run precheck，
> **不會**自動搬移既有 current 舊資料。
>
> ⚠️ precheck 會讀 parquet 的 `open_time` 欄位計算統計；1m 全市場（上千檔）可能
> 較慢。建議先小範圍（指定 symbols）跑。

---

## 5. Primary Key

```text
symbol + interval + open_time
```

同一個 primary key 只能保留一筆。

若重複：

```text
保留最後收到的版本。
```

---

## 6. Schema

建議欄位：

```text
interval
open_time
open_time_utc
open_time_taipei
date
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
schema_version
dataset_version
```

partition 欄位由路徑提供：

```text
symbol
year
```

若 existing historical seed parquet 已有既定 schema，current dataset 應盡量與 existing schema 對齊。

---

## 7. Schema Version

預設：

```text
schema_version = 1
dataset_version = current-v1
```

需寫入：

```text
state
closed_buffer record
current dataset metadata / parquet rows
```

若未來 schema 改版，需能透過 schema_version 判斷資料格式。

---

## 8. 時區規則

Binance timestamp 使用 millisecond。

需產生：

```text
open_time
open_time_utc
open_time_taipei
date
year
month
```

定義：

```text
open_time:
  Binance open time，millisecond timestamp。

open_time_utc:
  UTC datetime。

open_time_taipei:
  UTC+8 datetime。

date:
  open_time_taipei 的 YYYY-MM-DD。

year:
  open_time_taipei 的年份。

month:
  open_time_taipei 的月份。
```

---

## 9. KlineRecord

Agent 需建立 `KlineRecord` dataclass。

建議欄位：

```text
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
schema_version
dataset_version
```

需提供：

```text
physical_dict()
logical_dict()
partition_key()
record_key()
```

用途：

```text
physical_dict():
  寫入 parquet。

logical_dict():
  寫入 JSON log、latest、state。

partition_key():
  interval + symbol + year + month。

record_key():
  symbol + interval + open_time。
```

---

## 10. 來源標記

WebSocket 來源：

```text
source_archive = live_websocket:kline
archive_source = live_websocket
```

REST fallback 來源：

```text
source_archive = live_rest:/fapi/v1/klines
archive_source = live_rest
```

Webhook Binance kline 來源：

```text
source_archive = live_webhook:kline
archive_source = live_webhook
```

Webhook normalized 來源：

```text
source_archive = live_webhook:normalized
archive_source = live_webhook
```

Historical seed 初始化 current dataset 時，保留原始 source 欄位，不強制改成 live 來源。

---

## 11. Micro-batch Flush

不得每根 closed KBar 都立即 rewrite parquet。

流程：

```text
closed KBar
    ↓
closed_buffer
    ↓
per-partition write queue
    ↓
micro-batch flush
    ↓
current historical dataset
    ↓
state update
```

flush 條件：

```text
1. 每 partition 累積 rows >= flush_max_rows
2. 或距離上次 flush >= flush_seconds
3. 或程式 shutdown 前 forced flush
4. 或 --once 結束前 forced flush
```

預設 CLI：

```text
--flush-seconds 10
--flush-max-rows 1000
```

---

## 12. Single Partition Writer

partition key：

```text
interval + symbol + year + month
```

規則：

```text
同一 partition 同一時間只能有一個 writer。
所有 closed KBar 先進 partition queue。
由該 partition writer 負責 flush。
```

不得讓多個 WebSocket worker / REST fallback task 同時寫同一 parquet partition。

---

## 13. Merge 規則

micro-batch flush 時：

```text
1. 根據 partition key 找到目標 parquet。
2. 讀取既有 parquet。
3. 加入 queue 中所有新完整 KBar。
4. 依 symbol + interval + open_time 去重。
5. 同一 open_time 保留最後收到的版本。
6. 依 open_time 遞增排序。
7. 寫入 temporary parquet。
8. 使用 os.replace atomic replace 原檔案。
9. flush 成功後更新 state。
```

禁止：

```text
未收盤 KBar 不得寫入 current historical dataset。
closed_buffer 寫入成功不得直接更新 last_closed_open_time。
state.last_closed_open_time 只能在 current dataset flush 成功後更新。
```

---

## 14. Atomic Replace

寫 parquet 時：

```text
1. 寫入同目錄 temporary parquet。
2. 寫入完成並 flush。
3. 使用 os.replace 替換正式檔。
```

避免半寫入造成 parquet 損壞。

---

## 15. State 更新規則

以下事件可以更新：

```text
closed_buffer 寫入成功:
  可更新 last_buffered_open_time。

current dataset flush 成功:
  可更新 last_flushed_open_time。
  可更新 last_closed_open_time。
  可更新 merged_bar_count。
  可更新 last_target_path。
```

以下事件不得更新：

```text
只收到 WebSocket k.x = true 但尚未 flush。
只寫入 closed_buffer 但 parquet merge 失敗。
只進入 partition queue 但未 flush。
```

---

## 16. Rebuild 能力

closed_buffer 必須可用於重建 current historical dataset。

重建邏輯：

```text
historical seed parquet
        +
closed_buffer
        =
current historical dataset
```

未來可新增模式：

```bash
.venv/bin/python scripts/live_update.py \
  --interval all \
  --rebuild-current-from-closed-buffer
```

重建流程：

```text
1. 刪除或移出現有 current dataset。
2. 從 historical seed parquet 初始化 current dataset。
3. 依 interval 與日期順序讀取 closed_buffer。
4. 逐筆驗證 KBar。
5. 放入 partition queue 或直接 batch merge。
6. flush 到 current dataset。
7. 重建 state。
```

---

## 17. 查詢範例

```python
import duckdb

root = "local_data/binance_um_klines_current/interval=1m/parquet"

df = duckdb.sql(f"""
SELECT *
FROM read_parquet('{root}/**/*.parquet', hive_partitioning = true)
WHERE symbol = 'BTCUSDT'
ORDER BY open_time DESC
LIMIT 20
""").df()

print(df)
```
