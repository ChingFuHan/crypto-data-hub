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

> ⚠️ precheck 會讀 parquet 的 `open_time` 欄位計算統計；1m 全市場（上千檔）可能
> 較慢。建議先小範圍（指定 symbols）跑。

#### Batch planner（候選清單，read-only）

`--list-current-layout-migration-candidates` 列出下一批安全 migration 候選，
**只讀本地 current dataset**，不執行 migration、不寫資料、不接 Binance。

```bash
.venv/bin/python scripts/live_update.py \
  --interval 1m \
  --list-current-layout-migration-candidates \
  --limit 10 \
  --max-row-count 300000
```

預設：只列 `year_only_needs_migration`、排除 mixed、排除 already canonical。

排序（安全 / 便宜優先）：`duplicate_open_time_count == 0` 優先 → `row_count` 小
優先 → `expected_canonical_partition_count` 小優先 → symbol 字母 tie-break。

選項：`--limit N`、`--max-row-count N`、`--include-mixed`、
`--status <migration_status>`、`--output-symbols-only`。

直接接到 migration：

```bash
.venv/bin/python scripts/live_update.py --interval 1m \
  --list-current-layout-migration-candidates \
  --limit 10 --max-row-count 300000 --output-symbols-only
# -> AAOIUSDT ACUUSDT AMATUSDT ...

.venv/bin/python scripts/live_update.py --interval 1m \
  --symbols "AAOIUSDT ACUUSDT AMATUSDT" --migrate-current-layout --execute
```

> 建議每批用 `--limit 10` / `--max-row-count` 控制大小，分批 migrate、分批驗證。
> `BTCUSDT` / `ETHUSDT` 等 mixed layout 預設排除，等 year-only migration 穩定後，
> 再用 `--include-mixed` 或 `--status mixed_layout_needs_migration` 單獨處理。

#### Controlled batch planner（自動切批，plan / dry-run only）

`--plan-current-layout-migration-batches` 把候選 symbols **自動切成多批**，
免去手動貼 batch 006 / 007 / 008。第一版**只 plan / dry-run，不 execute**：

- 只讀本地 current dataset candidate pool，**不寫任何 parquet / stage / backup /
  jsonl / state / registry**。
- **不修改 `dataset_registry.json`**，**不接 Binance**（不打 exchangeInfo）。
- 即使輸出 `commands.execute` 字串，本 CLI **也不會自己執行 migration**；
  那只是給你手動複製。
- 第一版**沒有** `--execute-batches` / `--run-batches`，也不碰既有 migration
  execute flow、live daemon、`--once`、startup backfill。

```bash
.venv/bin/python scripts/live_update.py \
  --interval 1m \
  --plan-current-layout-migration-batches \
  --batch-size 10 \
  --max-row-count 300000 \
  --max-batches 2 \
  --quote-assets USDT \
  --exclude-delivery-contracts \
  --exclude-settled \
  --exclude-non-ascii \
  --exclude-symbols BTCUSDT ETHUSDT KAITOUSDC \
  --dry-run-batches
```

> **Primary universe = USDT quote perpetual。** primary universe 是 Binance
> USDⓈ-M Futures `PERPETUAL` `quote_asset = USDT`（含已下市 USDT 永續）。
> Binance UM **不等於只有 USDT pairs**；USDC / BUSD quote pairs、delivery、
> SETTLED、non-ASCII 都不屬於 primary universe。`--quote-assets USDT` 依 symbol
> suffix 偵測 quote asset（`USDT` / `USDC` / `BUSD`），只保留指定 quote 的候選；
> 非指定 quote 的 symbol 進 `excluded.quote_asset_mismatch`（reason
> `quote_asset_mismatch`，附 `detected_quote_asset`）。delivery 合約以
> `--exclude-delivery-contracts` 排除（先於 quote filter），避免 `BTCUSDT_230630`
> 被當成 USDT perpetual。仍建議用 `--exclude-symbols` 補上已知非 primary symbols（如
> `KAITOUSDC`）。`KAITOUSDC` 是 known quarantined symbol（USDC quote pair + corrupt
> source parquet）：不重跑 migration、不自動修復、不刪除。migration 前 source
> parquet readability precheck 失敗即停。見 `DATA_CONTRACT.md` →
> *Primary Universe Policy* 與根目錄 `INIT_VERIFY.md`。

選項：

- `--batch-size N`：每批 symbol 數（預設 10，正整數）。
- `--max-batches N`：最多輸出幾批（預設 1，正整數）。
- `--max-row-count N`：沿用 candidate planner 的 row count 上限（提供時須正整數）。
- `--candidate-scan-limit N`：限制掃描多少 ranked raw candidates；未提供時掃描完整
  local candidate pool。命中時 output 標示 `filters.candidate_scan_limit` 與
  `filters.hit_candidate_scan_limit = true`。
- `--exclude-delivery-contracts`：排除 `_YYMMDD` 交割合約（如 `BTCUSDT_230630`）。
- `--exclude-settled`：排除含 `SETTLED`（如 `CVXUSDTSETTLED`）。
- `--exclude-non-ascii`：排除非 ASCII symbol（如 `龙虾USDT`）。
- `--exclude-symbols A B` / `A,B` / `"A B"`：額外排除指定 symbols。
- `--quote-assets USDT` / `USDT,USDC` / `"USDT USDC"`：只保留指定 quote asset 的
  候選（suffix 偵測）。**只影響 batch planner candidate filtering**，不碰 live
  daemon / `--once` / startup backfill。output `filters.quote_assets` 列出生效清單。
- `--dry-run-batches`：對每批每顆呼叫 `--migrate-current-layout`（`execute=False`）
  dry-run，回傳結果；**仍不寫任何資料**。

> **Source parquet readability precheck。** `--migrate-current-layout`（dry-run 與
> `--execute`）migrate 前先檢查該 symbol 目錄底下所有 source parquet
> （size / head4 / tail4 / parquet magic / pyarrow metadata）。任一不可讀時 status
> 為 `source_parquet_unreadable`：dry-run **不回 `planned`**、execute **不建 stage /
> backup、不 promote、不改 source**；`--dry-run-batches` 的 `dry_run_results` 也會
> surface 此 status。output 含 `source_file_count` / `readable_file_count` /
> `unreadable_file_count` / `unreadable_files`（每筆 path / size / head4 / tail4 /
> `parquet_magic_ok` / `read_ok` / error）。`--audit-current-layout` 只檢查 layout，
> **不保證 parquet readability**。

預設行為：只收 `year_only_needs_migration`、排除 mixed layout / already canonical /
source_missing，並**預設排除 `BTCUSDT` / `ETHUSDT`**（兩者目前是 mixed，需最後單獨
處理）。delivery / SETTLED / non-ASCII 預設**不**排除，須各自開 flag。

candidate pool 規則：先抓**足夠大**的 ranked pool（沿用 candidate planner 的排序：
`duplicate_open_time_count == 0` → `row_count` 小 → `expected_canonical_partition_count`
小 → symbol 字母），**套用 filters 後再切 batch**，避免前段被 delivery / settled /
non-ascii / excluded symbols 佔滿而漏掉可用普通 symbols。

CLI 限制（獨立 mode，違反即報錯 fail fast、不讀寫資料）：

- 不得與 `--migrate-current-layout` / `--audit-current-layout` /
  `--plan-current-layout-migration` / `--list-current-layout-migration-candidates` /
  `--initialize-current-dataset` / `--once` / `--run-startup-backfill-once` 混用。
- 不接受 `--symbols`（避免誤以為能 scope batch planner）。
- `--interval all` 或 invalid interval 報錯；須單一 concrete interval。
- `--batch-size` / `--max-batches` 須正整數；`--max-row-count` /
  `--candidate-scan-limit` 若提供須正整數。

建議流程：先用上面那組「普通 symbols」參數規劃並 dry-run，確認 batches 後，再**手動**
複製每批的 `commands.execute` 逐批 `--migrate-current-layout --execute`、逐批驗證。
delivery / SETTLED / non-ASCII / `BTCUSDT` / `ETHUSDT` 都要分開處理；不要全市場 execute、
不要直接把 `--batch-size` 拉到 100（普通 symbols 先 10，穩定後再考慮 20；
`row_count > 300000` 的大 symbol 分小批或單顆處理）。

#### Single-symbol migration（real，dry-run by default）

`--migrate-current-layout` 真正把指定 symbol 從 year-only / mixed layout 轉成
canonical year/month。**預設 dry-run**（只輸出計畫，不寫資料），加 `--execute`
才會真的寫 / 替換。

```bash
# dry-run（不寫資料）
.venv/bin/python scripts/live_update.py \
  --interval 1m --symbols URNMUSDT --migrate-current-layout

# 實際執行
.venv/bin/python scripts/live_update.py \
  --interval 1m --symbols URNMUSDT --migrate-current-layout --execute
```

限制：

- 只支援**明確 symbols**；`--symbols all`、未提供 symbols、`--interval all` 皆報錯。
- 第一階段只處理「指定 interval + 指定 symbols」。

execute 流程：

```text
1. 跑 precheck。
2. 讀 current symbol dir 內所有 parquet rows。
3. 依 open_time 排序去重（同一 open_time 保留最後一筆，記 duplicate_replaced_count）。
4. 寫 stage dir（在 parquet root 之外）：
   interval=<I>/_layout_migration_stage/<ts>/symbol=<S>/year=<YYYY>/month=<MM>/part-000.parquet
5. 驗證 stage：row_count_after == unique_before、duplicate_after == 0、
   min/max open_time 不變、stage 只有 year/month（無 year-only）。
6. 備份原 dir（在 parquet root 之外）：
   interval=<I>/_layout_migration_backup/<ts>/symbol=<S>/
7. rename stage -> 正式 parquet/symbol=<S> dir。
8. 對新 dir 做 final precheck 確認 canonical。
```

驗證失敗時直接 abort，**原資料保持不變**（不備份、不替換）。result dict 含
`status`（`planned` / `migrated` / `verification_failed` / `no_migration_needed`
/ `source_missing`）、`stage_path`、`backup_path`、`row_count_before/after`、
`duplicate_*`、`min/max_open_time_*`、`written_partitions` 等。

#### Backup / stage 位置（不污染 parquet root）

stage 與 backup **不**放在 `interval=<I>/parquet/` 底下，而是放在 interval
dataset 底下的獨立目錄：

```text
local_data/binance_um_klines_current/interval=<I>/_layout_migration_stage/<ts>/symbol=<S>/
local_data/binance_um_klines_current/interval=<I>/_layout_migration_backup/<ts>/symbol=<S>/
```

`discover_current_dataset_symbols` / `audit_current_partition_layout` 只認
`parquet/symbol=<SYMBOL>` 形式的正式 symbol dir，會**忽略**：

```text
_layout_migration_stage / _layout_migration_backup
symbol=<S>.__stage_migrate_<ts>   （舊式，曾放在 parquet root）
symbol=<S>.__backup_migrate_<ts>  （舊式，曾放在 parquet root）
任何 symbol 名稱含 "." 的 dir
```

> 既有真實 local_data 若還有舊式 `parquet/symbol=<S>.__backup_migrate_<ts>`，本次
> **不自動搬移**，但 audit / discovery 會忽略它，不再當成正式 symbol。
>
> migration 會留下 `_layout_migration_backup/<ts>/` 備份（不自動刪除）。建議先用
> 小型 symbol（如 `URNMUSDT`）測試，再擴大。

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
