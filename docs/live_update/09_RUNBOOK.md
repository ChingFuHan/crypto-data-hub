# 09_RUNBOOK.md

# Live Update Runbook

本文件是 live update 的日常操作、檢查與故障排除指南。

---

## 1. 啟動所有週期

```bash
.venv/bin/python scripts/live_update.py --interval all
```

---

## 2. 啟動單一週期

```bash
.venv/bin/python scripts/live_update.py --interval 1m
```

---

## 3. 指定 symbols

小範圍 symbols（以下三種等價，normalize 成大寫並去重）：

```bash
--symbols BTCUSDT ETHUSDT
--symbols "BTCUSDT ETHUSDT"
--symbols BTCUSDT,ETHUSDT
```

```bash
.venv/bin/python scripts/live_update.py \
  --interval 1m \
  --symbols BTCUSDT ETHUSDT
```

全市場（Binance USD-M USDT 永續，透過 `/fapi/v1/exchangeInfo` resolve）：

```bash
--symbols all
```

> ⚠️ `--symbols all` 會增加 REST / WebSocket / IO 壓力。不建議一開始直接搭配
> `--interval all` 全市場跑。新機器應先小範圍驗收，再擴大。未提供 `--symbols`
> 的寫資料模式會明確失敗，不會默默全市場。

---

## 4. 使用 symbols file

```bash
.venv/bin/python scripts/live_update.py \
  --interval all \
  --symbols-file symbols.txt
```

---

## 5. 測試一次更新（one-shot cycle）

`--once` = run one complete live update cycle and exit：resolve symbols（必填）、
ensure current symbols from seed、跑一次 startup / REST gap repair（寫
closed_buffer、merge 進 current parquet、merge 成功後才更新 state），補到 latest
closed KBar 後結束。輸出 `once_update` JSON。

```bash
.venv/bin/python scripts/live_update.py \
  --interval 1m \
  --symbols BTCUSDT ETHUSDT \
  --once
```

與 `--run-startup-backfill-once` 共用同一核心流程；`--once` 是 user-facing
shorthand，`--run-startup-backfill-once` 是明確的 startup backfill one-shot 模式
（見第 20b 節）。未提供 `--symbols` 會明確 fail，不會默默全市場。seed 缺時回
bootstrap_required，不從 0 建歷史。

---

## 6. 限制 symbols 數量（全市場 smoke test）

先 resolve 全市場再截斷前 N 個（smoke test 用，不是新的 universe 定義）：

```bash
.venv/bin/python scripts/live_update.py \
  --interval 1m \
  --symbols all \
  --max-symbols 5
```

startup summary 的 `symbols_count` 反映截斷後的數量。

---

## 7. 只用 REST 模式

```bash
.venv/bin/python scripts/live_update.py \
  --interval all \
  --disable-websocket
```

用途：

```text
WebSocket 不穩時暫時使用
驗證 REST fallback
手動補洞
```

---

## 8. 只用 Webhook 模式

```bash
.venv/bin/python scripts/live_update.py \
  --interval all \
  --disable-websocket \
  --disable-rest-fallback
```

用途：

```text
外部 bridge 負責推送 KBar
本程式只接收 webhook
```

---

## 9. 健康檢查

```bash
curl http://127.0.0.1:8787/healthz
```

預期：

```json
{
  "status": "ok"
}
```

---

## 10. 查看 startup summary

啟動時確認：

```text
symbols_count
intervals_count
stream_count
connection_count
ws_batch_size
max_streams_per_connection
estimated_partition_count
startup_backfill_enabled
rest_fallback_enabled
websocket_enabled
webhook_enabled
```

若 stream_count 太大，需降低：

```text
--max-symbols
--symbols-file
--ws-batch-size
```

---

## 11. 查看 state

```bash
cat local_data/live_update/binance_um_klines/interval=1m/state/live_update_state.json
```

重點檢查：

```text
updated_at_utc
websocket.last_message_at_utc
websocket.reconnect_count
symbols.BTCUSDT.last_buffered_open_time
symbols.BTCUSDT.last_flushed_open_time
symbols.BTCUSDT.last_closed_open_time
symbols.BTCUSDT.last_closed_at_utc
symbols.BTCUSDT.merged_bar_count
```

---

## 12. 查看 latest

```bash
ls local_data/live_update/binance_um_klines/interval=1m/latest/
cat local_data/live_update/binance_um_klines/interval=1m/latest/symbol=BTCUSDT.json
```

用途：

```text
確認即時資料是否正在更新
確認 WebSocket 是否有收到資料
```

---

## 13. 查看 closed_buffer

```bash
find local_data/live_update/binance_um_klines/interval=1m/closed_buffer -type f | tail
```

查看最新：

```bash
tail -n 5 local_data/live_update/binance_um_klines/interval=1m/closed_buffer/date=YYYY-MM-DD/closed.jsonl
```

用途：

```text
確認完整 KBar 是否有寫入 replay log
```

---

## 14. 查看 current dataset

```bash
find local_data/binance_um_klines_current/interval=1m/parquet -name "*.parquet" | head
```

使用 DuckDB 查詢：

```python
import duckdb

root = "local_data/binance_um_klines_current/interval=1m/parquet"

df = duckdb.sql(f"""
SELECT symbol, open_time, open_time_taipei, open, high, low, close, volume
FROM read_parquet('{root}/**/*.parquet', hive_partitioning = true)
WHERE symbol = 'BTCUSDT'
ORDER BY open_time DESC
LIMIT 20
""").df()

print(df)
```

---

## 15. 查看 rejects

```bash
find local_data/live_update/binance_um_klines/interval=1m/rejects -type f | tail
```

若有 rejects：

```bash
tail -n 20 <rejects_file>
```

常見原因：

```text
close_time mismatch
open_time 未對齊 interval
OHLC 不合理
volume 負數
payload 欄位缺失
```

---

## 16. 停機後重啟

直接重新執行：

```bash
.venv/bin/python scripts/live_update.py --interval all
```

預期：

```text
啟動時根據 state.last_closed_open_time 自動計算缺口
使用 REST 補洞
補齊後啟動 WebSocket
```

---

## 17. 停機 1 天後預期補洞

若停機 1 天：

```text
1m  補 1440 根
3m  補 480 根
5m  補 288 根
15m 補 96 根
1h  補 24 根
4h  補 6 根
1d  視 latest_closed_open_time 補 1 根或 0 根
```

---

## 18. WebSocket 中斷處理

若 WebSocket 中斷：

```text
程式應嘗試 reconnect
reconnect 後執行 REST fallback gap check
補齊 state 與最新 closed KBar 之間的缺口
```

檢查：

```bash
cat local_data/live_update/binance_um_klines/interval=1m/state/live_update_state.json
```

查看：

```text
websocket.reconnect_count
websocket.last_reconnect_at_utc
websocket.last_message_at_utc
```

---

## 19. REST 被限流處理

若看到：

```text
HTTP 429
HTTP 418
```

處理：

```text
429:
  等待 backoff
  降低 request 頻率
  降低 max-symbols
  避免立即重啟狂打 API

418:
  停止 REST requests
  等待封鎖解除
  檢查 request rate
```

---

## 20. 檢查 continuity

```bash
.venv/bin/python scripts/live_update.py \
  --interval all \
  --symbols BTCUSDT ETHUSDT \
  --check-continuity
```

重點：

```text
duplicate_count = 0
missing_count = 0
lag_bars 合理
status = ok
```

---

## 20b. Partial current symbol missing 修復

情境：

```text
seed   local_data/binance_um_klines/interval=1m/parquet/symbol=ETHUSDT      存在
current local_data/binance_um_klines_current/interval=1m/parquet/symbol=ETHUSDT 不存在
```

這是 partial current dataset symbol missing，不是 historical bootstrap missing。

新機器流程：

```text
1. INIT.md 建 historical seed
2. live update 初始化指定 current symbol
3. 補最新 gap
```

只初始化指定 symbol 的 current dataset：

```bash
.venv/bin/python scripts/live_update.py \
  --interval 1m \
  --symbols ETHUSDT \
  --initialize-current-dataset
```

狀態：

```text
initialized_current_symbol_from_seed  seed 在、current 缺 -> copy 修復
already_available                     current 已存在 -> 不覆蓋、不重 copy
bootstrap_required                    seed 也缺 -> 需先建歷史 seed
```

再補最新 gap（會先確保 current symbol 存在，再依 max_open_time 補到 latest）：

```bash
.venv/bin/python scripts/live_update.py \
  --interval 1m \
  --symbols ETHUSDT \
  --run-startup-backfill-once
```

注意：

- 只處理明確指定的 symbols，不做整個 interval / 全市場隱式搬運。
- copy 先寫 temp dir 再 rename；失敗不留半成品。不覆蓋既有 current symbol、不刪
  資料、不改 seed、不寫 dataset_registry.json。
- 不建議一開始就全市場初始化；先小範圍驗收再擴大。

備註（layout）：initialize from seed 會把 seed parquet **轉換**成 current 的
canonical year/month layout（依 `open_time` 推導），不會把 seed 的 year-only
layout 原樣 copy 進 current。詳見第 20c 節。

---

## 20c. Current dataset layout audit（year/month governance）

current parquet canonical layout：

```text
symbol=<SYMBOL>/year=<YYYY>/month=<MM>/part-000.parquet
```

- historical seed 可以仍是 year-only：`symbol=<SYMBOL>/year=<YYYY>/part-000.parquet`。
- live update merge / gap repair / initialize-from-seed 一律寫 year/month。
- 若 current 同一 symbol 同時有 year-only 與 year/month parquet = mixed layout，
  DuckDB `hive_partitioning=true` 會報 Hive partition mismatch。

檢查（read-only，不改資料、不 migration）：

```bash
.venv/bin/python scripts/live_update.py \
  --interval 1m \
  --symbols BTCUSDT ETHUSDT \
  --audit-current-layout
```

或全市場 smoke 範圍：

```bash
.venv/bin/python scripts/live_update.py \
  --interval 1m \
  --symbols all --max-symbols 5 \
  --audit-current-layout
```

輸出 JSON（每 interval）：

```text
year_only_file_count
year_month_file_count
mixed_symbol_count
mixed_symbols
status        ok / mixed_layout_detected
```

### Migration dry-run precheck

```bash
.venv/bin/python scripts/live_update.py \
  --interval 1m \
  --symbols BTCUSDT ETHUSDT \
  --plan-current-layout-migration
```

每 symbol precheck 輸出：

```text
status        no_migration_needed / year_only_needs_migration / mixed_layout_needs_migration
year_only_file_count / year_month_file_count
year_only_files / year_month_files
expected_canonical_partition_count / expected_canonical_partitions   （依 open_time 推導）
row_count / min_open_time / max_open_time / duplicate_open_time_count
recommended_action
```

未提供 `--symbols`（或 `--symbols all`）時，掃描**本地 current dataset**
（local discovery）：

```bash
.venv/bin/python scripts/live_update.py \
  --interval 1m \
  --plan-current-layout-migration
```

注意：

- `--audit-current-layout` = layout audit；`--plan-current-layout-migration` =
  dry-run migration precheck。兩者皆只讀檔案，不寫 parquet / jsonl / state，不移動
  / 不刪除 / 不覆蓋資料，不自動 migration，**不打 Binance / exchangeInfo**。
- 此模式的 `all` 是本地 current dataset discovery，不是交易所全市場。
- precheck 讀 parquet `open_time` 欄位；1m 全市場（上千檔）可能較慢，建議先指定
  symbols 小範圍跑。

### Batch planner（候選清單，read-only）

列出下一批安全 migration 候選（只讀本地 current dataset，不 migrate、不寫資料、
不接 Binance）：

```bash
.venv/bin/python scripts/live_update.py \
  --interval 1m \
  --list-current-layout-migration-candidates \
  --limit 10 \
  --max-row-count 300000
```

預設只列 `year_only_needs_migration`，排除 mixed / canonical。排序：
`duplicate_open_time_count == 0` 優先 → `row_count` 小 →
`expected_canonical_partition_count` 小 → symbol。選項：`--limit`、
`--max-row-count`、`--include-mixed`、`--status`、`--output-symbols-only`。

接到 migration（先 dry-run 再 execute）：

```bash
SYMS=$(.venv/bin/python scripts/live_update.py --interval 1m \
  --list-current-layout-migration-candidates --limit 10 --max-row-count 300000 \
  --output-symbols-only)

.venv/bin/python scripts/live_update.py --interval 1m \
  --symbols "$SYMS" --migrate-current-layout            # dry-run
.venv/bin/python scripts/live_update.py --interval 1m \
  --symbols "$SYMS" --migrate-current-layout --execute  # 實際執行
```

建議每批 `--limit 10` / `--max-row-count` 控制大小，分批 migrate / 驗證。
`BTCUSDT` / `ETHUSDT` 等 mixed 預設排除，year-only migration 穩定後再用
`--include-mixed` 處理。

### Controlled batch planner（自動切批，plan / dry-run only）

不想再手動貼 batch 006 / 007 / 008 時，用
`--plan-current-layout-migration-batches` 自動把候選切成多批。第一版**只
plan / dry-run，不 execute**：**只讀**，不寫 parquet / stage / backup / jsonl /
state / registry，**不接 Binance**，不修改 `dataset_registry.json`，也不碰既有
migration execute flow / live daemon / `--once` / startup backfill。

普通 symbols 建議參數：

```bash
.venv/bin/python scripts/live_update.py \
  --interval 1m \
  --plan-current-layout-migration-batches \
  --batch-size 10 \
  --max-row-count 300000 \
  --max-batches 5 \
  --exclude-delivery-contracts \
  --exclude-settled \
  --exclude-non-ascii \
  --exclude-symbols BTCUSDT ETHUSDT \
  --dry-run-batches
```

行為：reuse candidate planner 取得足夠大的 ranked pool（排序同上），**套用 exclude
filters 後再切 batch**（避免前段 delivery / settled / non-ascii / excluded symbols
佔滿而漏掉普通 symbols）。預設只收 `year_only_needs_migration`，排除 mixed /
canonical / source_missing，並**預設排除 `BTCUSDT` / `ETHUSDT`**。`--dry-run-batches`
對每顆跑 `--migrate-current-layout` `execute=False` dry-run，**仍不寫資料**。
`--candidate-scan-limit N` 命中時 output 標示 `filters.hit_candidate_scan_limit = true`。

每批 output 含 `symbols` / `symbol_count` / `total_row_count` /
`total_expected_canonical_partition_count` / `max_*` 與 `commands.{dry_run,execute}`。
`commands.execute` **只是參考字串，本 CLI 不會自己 execute**；第一版**沒有**
`--execute-batches` / `--run-batches`。確認 batches 後，**手動**逐批複製
`commands.execute` 跑 `--migrate-current-layout --execute`、逐批驗證 continuity
（continuity 只在真實 execute 後處理，本 planner 不跑）。

獨立 mode（違反即 fail fast、不讀寫資料）：不得與 `--migrate-current-layout` /
`--audit-current-layout` / `--plan-current-layout-migration` /
`--list-current-layout-migration-candidates` / `--initialize-current-dataset` /
`--once` / `--run-startup-backfill-once` 混用，不接受 `--symbols`、`--interval all`、
invalid `--batch-size` / `--max-batches` / `--max-row-count` / `--candidate-scan-limit`。

注意：delivery / SETTLED / non-ASCII / `BTCUSDT` / `ETHUSDT` 都分開處理；不要全市場
execute；不要直接把 `--batch-size` 拉到 100（普通 symbols 先 10，穩定後再考慮 20）；
`row_count > 300000` 的大 symbol 分小批或單顆。

### Single-symbol migration（real，dry-run by default）

`--migrate-current-layout` 真正把指定 symbol 轉成 canonical year/month。預設
dry-run，加 `--execute` 才寫資料。只支援明確 symbols；`--symbols all`、未提供
symbols、`--interval all` 皆報錯。

```bash
# dry-run（不寫資料）
.venv/bin/python scripts/live_update.py \
  --interval 1m --symbols URNMUSDT --migrate-current-layout

# 實際執行
.venv/bin/python scripts/live_update.py \
  --interval 1m --symbols URNMUSDT --migrate-current-layout --execute
```

execute 流程：precheck → 讀全部 rows → 依 open_time 排序去重 → 寫 stage dir →
驗證（row_count_after == unique_before、duplicate_after == 0、min/max open_time
不變、stage 無 year-only）→ 備份原 dir → rename stage 為正式 dir → final precheck。
驗證失敗直接 abort，原資料不變。

stage / backup 位置（在 parquet root **之外**，不污染 discovery / audit）：

```text
local_data/binance_um_klines_current/interval=<I>/_layout_migration_stage/<ts>/symbol=<S>/
local_data/binance_um_klines_current/interval=<I>/_layout_migration_backup/<ts>/symbol=<S>/
```

注意：

- `discover_current_dataset_symbols` / `audit_current_partition_layout` 只認
  `parquet/symbol=<SYMBOL>`，會忽略 `_layout_migration_stage` /
  `_layout_migration_backup`，以及舊式 `parquet/symbol=<S>.__backup_migrate_<ts>` /
  `__stage_migrate_<ts>`（既有真實舊 backup 不自動搬移，但不會再被掃成 symbol）。
- migration 會留下 `_layout_migration_backup/<ts>/` 備份，不自動刪除（人工確認後再清）。
- 建議先用小型 symbol（如 `URNMUSDT`）測試，再擴大到更大 symbol。
- 第一階段只處理「指定 interval + 指定 symbols」；不支援全市場 / all 一次搬移。

---

## 21. current dataset 重建

未來可支援：

```bash
.venv/bin/python scripts/live_update.py \
  --interval all \
  --rebuild-current-from-closed-buffer
```

重建邏輯：

```text
historical seed parquet
        +
closed_buffer
        =
current historical dataset
```

---

## 22. Buffer retention / compression

若啟用：

```bash
.venv/bin/python scripts/live_update.py \
  --interval all \
  --compress-old-buffers
```

預期：

```text
壓縮舊的 event_buffer
壓縮舊的 websocket_buffer
壓縮舊的 webhook_buffer
壓縮舊的 rejects
不得預設刪除 closed_buffer
```

---

## 23. 建議每日檢查

```text
1. state 是否有更新。
2. latest 是否有更新。
3. closed_buffer 是否持續產生。
4. current dataset 是否有新 parquet 或更新。
5. last_closed_open_time 是否接近 latest_closed_open_time。
6. rejects 是否暴增。
7. WebSocket reconnect_count 是否異常。
8. continuity check 是否有 gap。
```

---

## 24. 常見問題

### state 沒更新

可能原因：

```text
WebSocket 沒收到 closed KBar
REST fallback 失敗
symbol 沒有交易
closed KBar 還在 partition queue 尚未 flush
parquet merge 失敗
```

### latest 有更新但 current dataset 沒更新

可能原因：

```text
收到的是未收盤 KBar
k.x = false
KBar 驗證失敗
partition queue 尚未達 flush 條件
parquet merge 失敗
```

### closed_buffer 有資料但 current dataset 沒資料

可能原因：

```text
partition writer 尚未 flush
flush 失敗
state 不應更新 last_closed_open_time
下次啟動需 replay closed_buffer 或 REST fallback 補洞
```

### 停機後資料缺洞

可能原因：

```text
startup backfill 被停用
state 損壞
current dataset 找不到最大 open_time
REST fallback 失敗
429 / 418 / 5xx 未妥善處理
```

### rejects 很多

可能原因：

```text
payload 格式不符合
timestamp 不對齊
欄位轉型失敗
OHLCV 不合理
```

---

## 25. 建議 commit

```bash
git add LIVE_UPDATE.md docs/live_update scripts/live_update.py
git commit -m "Add websocket-first live update current dataset runner"
```
