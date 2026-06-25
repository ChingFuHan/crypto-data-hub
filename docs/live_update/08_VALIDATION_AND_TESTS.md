# 08_VALIDATION_AND_TESTS.md

# Validation 與驗收測試

本文件定義資料驗證、程式驗收、連續性檢查與行為測試。

---

## 1. KBar 資料驗證

每筆 KBar 需驗證：

```text
symbol 不為空
interval 屬於支援清單
open_time 可轉為 int
close_time 可轉為 int
open/high/low/close 可轉為 float
volume/quote_volume 可轉為 float
trade_count 可轉為 int
open_time 對齊 interval
close_time = open_time + interval_ms - 1
open/high/low/close > 0
high >= max(open, close)
low <= min(open, close)
volume >= 0
quote_volume >= 0
trade_count >= 0
taker_buy_base_volume >= 0
taker_buy_quote_volume >= 0
taker_buy_base_volume <= volume
taker_buy_quote_volume <= quote_volume
```

---

## 2. 驗證失敗處理

```text
寫入 rejects
回傳 rejected 狀態
strict 模式下 raise exception
```

rejects 路徑：

```text
local_data/live_update/binance_um_klines/interval=<INTERVAL>/rejects/date=YYYY-MM-DD/rejects.jsonl
```

---

## 3. Python 依賴

需要：

```text
pyarrow
websockets
```

安裝：

```bash
.venv/bin/python -m pip install pyarrow websockets
```

若缺 `pyarrow`：

```text
pyarrow is required for parquet writing.
Install it with:
.venv/bin/python -m pip install pyarrow
```

若缺 `websockets`：

```text
websockets is required for WebSocket live update.
Install it with:
.venv/bin/python -m pip install websockets
```

若使用 `--disable-websocket`，可不要求 `websockets`。

---

## 4. 語法檢查

```bash
.venv/bin/python -m py_compile scripts/live_update.py
```

必須通過。

---

## 5. 單次 REST 更新驗收

執行：

```bash
.venv/bin/python scripts/live_update.py \
  --interval 1m \
  --symbols BTCUSDT ETHUSDT \
  --once
```

應產生或更新：

```text
local_data/live_update/binance_um_klines/interval=1m/
local_data/binance_um_klines_current/interval=1m/parquet/
```

檢查：

```bash
cat local_data/live_update/binance_um_klines/interval=1m/state/live_update_state.json
ls local_data/live_update/binance_um_klines/interval=1m/latest/
find local_data/binance_um_klines_current/interval=1m/parquet -name "*.parquet" | head
```

---

## 6. All Interval 驗收

執行：

```bash
.venv/bin/python scripts/live_update.py \
  --interval all \
  --symbols BTCUSDT ETHUSDT \
  --once
```

應產生或更新：

```text
local_data/live_update/binance_um_klines/interval=1m/
local_data/live_update/binance_um_klines/interval=3m/
local_data/live_update/binance_um_klines/interval=5m/
local_data/live_update/binance_um_klines/interval=15m/
local_data/live_update/binance_um_klines/interval=1h/
local_data/live_update/binance_um_klines/interval=4h/
local_data/live_update/binance_um_klines/interval=1d/

local_data/binance_um_klines_current/interval=1m/parquet/
local_data/binance_um_klines_current/interval=3m/parquet/
local_data/binance_um_klines_current/interval=5m/parquet/
local_data/binance_um_klines_current/interval=15m/parquet/
local_data/binance_um_klines_current/interval=1h/parquet/
local_data/binance_um_klines_current/interval=4h/parquet/
local_data/binance_um_klines_current/interval=1d/parquet/
```

---

## 7. WebSocket 驗收

執行：

```bash
.venv/bin/python scripts/live_update.py \
  --interval all \
  --symbols BTCUSDT ETHUSDT
```

預期：

```text
1. 啟動 startup backfill。
2. 啟動 webhook server。
3. 啟動 WebSocket manager。
4. 建立 combined kline streams。
5. 收到 combined wrapper payload 可正確解析。
6. 收到 raw kline payload 可正確解析。
7. 收到 k.x = false 時更新 websocket_buffer 與 latest。
8. 收到 k.x = true 時寫入 closed_buffer。
9. 收到 k.x = true 時 enqueue partition writer。
10. flush 成功後更新 current historical dataset。
11. flush 成功後更新 state.last_closed_open_time。
```

---

## 8. Webhook 驗收

啟動：

```bash
.venv/bin/python scripts/live_update.py \
  --interval all \
  --disable-websocket \
  --disable-rest-fallback
```

健康檢查：

```bash
curl http://127.0.0.1:8787/healthz
```

建立測試 payload：

```bash
cat > /tmp/sample_kline.json <<'JSON'
{
  "symbol": "BTCUSDT",
  "interval": "1m",
  "open_time": 1638747660000,
  "open": "50000.0",
  "high": "50100.0",
  "low": "49900.0",
  "close": "50050.0",
  "volume": "1000",
  "close_time": 1638747719999,
  "quote_volume": "50050000",
  "trade_count": 100,
  "taker_buy_base_volume": "500",
  "taker_buy_quote_volume": "25025000",
  "is_closed": true
}
JSON
```

送出：

```bash
curl -X POST http://127.0.0.1:8787/webhook/kline \
  -H "Content-Type: application/json" \
  -d @/tmp/sample_kline.json
```

預期：

```text
status = queued 或 merged
產生 webhook_buffer
產生 closed_buffer
更新 latest
forced flush 或 micro-batch flush 後更新 state
flush 後 merge current parquet
```

---

## 9. Startup Backfill 驗收

情境：

```text
interval = 1m
state.last_closed_open_time 落後目前最新完整 KBar 60 分鐘
```

預期：

```text
重啟後自動補回 60 根完整 KBar
```

情境：

```text
interval = 1m
state.last_closed_open_time 落後目前最新完整 KBar 1 天
```

預期：

```text
重啟後自動補回 1440 根完整 KBar
```

情境：

```text
interval = all
所有 interval 的 state 都落後目前最新完整 KBar 1 天
```

預期：

```text
1m  補回 1440 根
3m  補回 480 根
5m  補回 288 根
15m 補回 96 根
1h  補回 24 根
4h  補回 6 根
1d  視 latest_closed_open_time 補回 1 根或 0 根
```

---

## 10. REST Fallback 驗收

情境：

```text
WebSocket 中斷 10 分鐘
interval = 1m
```

預期：

```text
1. 程式偵測 WebSocket stale 或 reconnect。
2. 根據 state 計算 missing_bars。
3. 使用 REST 補回缺少的 1m KBar。
4. 寫入 event_buffer。
5. 寫入 closed_buffer。
6. enqueue partition writer。
7. flush 到 current historical dataset。
8. flush 成功後更新 state。
9. WebSocket 恢復後繼續接收最新資料。
```

---

## 11. REST Backoff 驗收

測試或模擬：

```text
HTTP 429
HTTP 418
HTTP 5xx
timeout
```

預期：

```text
429:
  exponential backoff
  不立即重試轟炸

418:
  停止 REST requests
  記錄 critical warning

5xx / timeout:
  retry with backoff
```

---

## 12. Continuity Check

需支援：

```bash
.venv/bin/python scripts/live_update.py \
  --interval all \
  --symbols BTCUSDT ETHUSDT \
  --check-continuity
```

檢查項目：

```text
每個 symbol + interval:
  open_time 是否依 interval 遞增
  是否有 duplicate open_time
  是否有 missing open_time
  close_time 是否等於 open_time + interval_ms - 1
  最新 open_time 是否接近 latest_closed_open_time
```

輸出：

```text
symbol
interval
rows
min_open_time
max_open_time
duplicate_count
missing_count
latest_closed_open_time
lag_bars
status
```

若有缺口：

```text
status = gap_detected
```

若正常：

```text
status = ok
```

---

## 13. Micro-batch Flush 驗收

測試：

```text
送入多筆同一 partition closed KBar。
```

預期：

```text
不應每根都 rewrite parquet。
需累積至 flush_max_rows 或 flush_seconds 後 flush。
forced shutdown 時需 flush pending rows。
flush 成功後才更新 last_closed_open_time。
```

---

## 14. 完成條件

```text
--interval all 會展開所有支援週期
WebSocket 是主要即時資料來源
combined stream parser 正常
REST 是 fallback / startup backfill / gap repair 來源
REST 429 / 418 / 5xx / timeout 有 backoff
Webhook payload 會進入即時資料區
未收盤 KBar 只更新 buffer / latest
完整 KBar 會寫入 closed_buffer
完整 KBar 會進入 partition queue
current dataset 透過 micro-batch flush 更新
state 只在 flush 成功後更新
live_update.py 啟動時會根據 state 自動補洞
停機 1 天後重啟會自動補回缺少的 KBar
WebSocket 斷線或 stale 時會用 REST fallback 補洞
研究 agent 預設只讀 current historical dataset
closed_buffer 可作為 replay source 重建 current dataset
支援 continuity check
支援 retention policy
```
