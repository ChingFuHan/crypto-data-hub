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

## 5. 測試一次更新

```bash
.venv/bin/python scripts/live_update.py \
  --interval 1m \
  --symbols BTCUSDT ETHUSDT \
  --once
```

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
