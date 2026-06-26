# 07_CLI_AND_MODES.md

# CLI 與執行模式規格

本文件定義 `scripts/live_update.py` 的 CLI 參數與執行模式。

---

## 1. 基本指令

啟動所有週期：

```bash
.venv/bin/python scripts/live_update.py --interval all
```

啟動單一週期：

```bash
.venv/bin/python scripts/live_update.py --interval 1m
```

---

## 2. 支援 Intervals

```text
all
1m
3m
5m
15m
1h
4h
1d
```

`all` 展開為：

```text
1m
3m
5m
15m
1h
4h
1d
```

不得把 `all` 傳給 Binance API。

---

## 3. CLI 參數

需支援：

```text
--repo-root
--interval
--symbols
--symbols-file
--max-symbols
--lookback-bars
--poll-seconds
--request-delay
--close-lag-ms
--http-timeout
--binance-rest-base-url
--binance-ws-base-url
--current-dataset-root
--seed-dataset-root
--webhook-host
--webhook-port
--disable-webhook
--disable-websocket
--disable-rest-fallback
--disable-startup-backfill
--once
--strict
--quiet-http
--ws-batch-size
--max-streams-per-connection
--max-total-streams
--startup-batch-size
--startup-batch-delay
--ws-stale-multiplier
--ws-reconnect-max-retries
--ws-reconnect-backoff-seconds
--ws-connection-rotate-hours
--rest-api-limit
--rest-max-retries
--rest-backoff-base-seconds
--rest-backoff-max-seconds
--gap-repair-seconds
--flush-seconds
--flush-max-rows
--buffer-retention-days
--closed-buffer-retention-days
--compress-old-buffers
--check-continuity
```

---

## 4. 預設值

```text
--repo-root .
--interval all
--lookback-bars 3
--poll-seconds 65
--request-delay 0.02
--close-lag-ms 2000
--http-timeout 15
--binance-rest-base-url https://fapi.binance.com
--binance-ws-base-url wss://fstream.binance.com
--webhook-host 127.0.0.1
--webhook-port 8787
--ws-batch-size 100
--max-streams-per-connection 1024
--max-total-streams 0
--startup-batch-size 5
--startup-batch-delay 1
--ws-stale-multiplier 3
--ws-reconnect-max-retries 10
--ws-reconnect-backoff-seconds 5
--ws-connection-rotate-hours 23
--rest-api-limit 1500
--rest-max-retries 5
--rest-backoff-base-seconds 1
--rest-backoff-max-seconds 60
--gap-repair-seconds 300
--flush-seconds 10
--flush-max-rows 1000
--buffer-retention-days 30
--closed-buffer-retention-days 0
```

`--closed-buffer-retention-days 0` 代表不自動刪除 closed_buffer。

---

## 5. Symbols 載入

優先順序：

```text
1. --symbols（含 all 展開）
2. --symbols-file
```

`--symbols` 接受以下等價寫法（normalize 成大寫並去重，保持順序）：

```bash
--symbols BTCUSDT ETHUSDT
--symbols "BTCUSDT ETHUSDT"
--symbols BTCUSDT,ETHUSDT
```

使用指定 symbols。

若指定：

```bash
--symbols-file symbols.txt
```

從檔案讀取 symbols，每行一個 symbol。

### `--symbols all`

`all` 是 CLI expansion sentinel，代表 Binance USD-M Futures 目前可交易的
USDT 永續合約。resolver 呼叫：

```text
GET https://fapi.binance.com/fapi/v1/exchangeInfo
```

篩選：

```text
status = TRADING
contractType = PERPETUAL
quoteAsset = USDT
```

排序依 symbol 字母排序。

限制：

- **不**使用 spot `/api/v3/exchangeInfo`。
- `all` 只在 CLI expansion 使用，resolve 成具體 symbols 後才進入
  per-symbol REST / WebSocket / backfill / continuity 流程。
- 絕不把 `all` 當 symbol 傳給 `/fapi/v1/klines`，不產生 `all@kline_*`
  WebSocket stream，也不寫進 state / parquet / buffer。

全市場 smoke test：

```bash
--symbols all --max-symbols 5
```

先 resolve 全市場，再截斷前 5 個（startup summary 的 `symbols_count`
反映截斷後數量）。這是 smoke test 用，不是新的 universe 定義。

### 未提供 `--symbols`

寫資料或可能產生大量工作量的模式（`--once`、
`--run-startup-backfill-once`、預設 live run）若未提供 `--symbols`，會明確
失敗，不會默默全市場：

```text
no symbols provided. Please provide --symbols BTCUSDT ETHUSDT or --symbols all ...
```

純 describe / layout 類模式維持既有行為，但不會把未提供 symbols 解讀成全市場
寫入。

若指定：

```bash
--max-symbols 20
```

resolve 後只取前 20 個。

> ⚠️ `--symbols all` 會增加 REST / WebSocket / IO 壓力，不建議一開始就搭配
> `--interval all` 全市場跑；新機器先小範圍驗收再擴大。

---

## 6. Symbol Universe 規則

```text
WebSocket 訂閱:
  只訂閱 currently TRADING symbols。

current dataset:
  保留 historical seed 與既有 current dataset 中的 symbols。
  不得因 symbol 下市就刪除資料。

newly listed symbol:
  若 current dataset 無資料，標記 bootstrap_required。
  可只抓最近 lookback_bars warm-up。
```

---

## 7. 預設模式

```bash
.venv/bin/python scripts/live_update.py --interval all
```

行為：

```text
1. 展開 all intervals。
2. 載入 symbols。
3. 印出 startup summary。
4. 初始化 current dataset。
5. startup backfill。
6. 啟動 webhook server。
7. 啟動 WebSocket manager。
8. 啟動 REST fallback manager。
9. 啟動 partition writers。
10. 持續更新 latest / closed_buffer / current dataset / state。
```

---

## 8. Startup Summary

啟動前需印出：

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

若 `stream_count > max_total_streams` 且 max_total_streams > 0：

```text
停止啟動並回報錯誤
```

---

## 9. 單一週期模式

```bash
.venv/bin/python scripts/live_update.py --interval 1m
```

只處理：

```text
1m
```

其他流程相同。

---

## 10. 指定 symbols

```bash
.venv/bin/python scripts/live_update.py \
  --interval all \
  --symbols BTCUSDT ETHUSDT SOLUSDT
```

只處理指定 symbols。

---

## 11. WebSocket-first 模式

```bash
.venv/bin/python scripts/live_update.py \
  --interval all \
  --disable-webhook
```

行為：

```text
啟動 WebSocket。
啟動 REST fallback。
不啟動 webhook。
```

---

## 12. REST-only / polling fallback 模式

```bash
.venv/bin/python scripts/live_update.py \
  --interval all \
  --disable-websocket
```

行為：

```text
不啟動 WebSocket。
啟動 REST startup backfill。
以 REST polling / fallback 模式更新資料。
```

此模式不是預設主模式。

---

## 13. Webhook-only 模式

```bash
.venv/bin/python scripts/live_update.py \
  --interval all \
  --disable-websocket \
  --disable-rest-fallback
```

行為：

```text
只啟動 webhook。
不啟動 WebSocket。
不使用 REST fallback。
```

---

## 14. `--once` 模式

```bash
.venv/bin/python scripts/live_update.py \
  --interval 1m \
  --symbols BTCUSDT ETHUSDT \
  --once
```

行為：

```text
1. 展開 intervals。
2. 載入 symbols。
3. 初始化 current dataset。
4. 執行 startup backfill。
5. 若 startup backfill 無缺口，抓最近 lookback_bars。
6. enqueue 已收盤 KBar。
7. forced flush all partition queues。
8. flush 成功後更新 state。
9. 執行 continuity check if enabled。
10. 結束程式。
```

用途：

```text
手動補洞
驗收
排程補洞
不啟動長駐 WebSocket
```

---

## 15. 停用 startup backfill

```bash
.venv/bin/python scripts/live_update.py \
  --interval all \
  --disable-startup-backfill
```

行為：

```text
不在啟動時補洞。
需明確 warning：資料可能不完整。
```

---

## 16. Continuity Check 模式

```bash
.venv/bin/python scripts/live_update.py \
  --interval all \
  --symbols BTCUSDT ETHUSDT \
  --check-continuity
```

行為：

```text
檢查 current dataset 是否連續。
不一定要啟動 WebSocket。
可作為驗收或日常檢查。
```

---

## 17. 啟動順序

預設啟動順序：

```text
1. parse CLI。
2. expand intervals。
3. load symbols。
4. print startup summary。
5. initialize current dataset for each interval。
6. startup backfill for each symbol + interval。
7. start partition writers。
8. start webhook server。
9. start WebSocket manager。
10. start REST fallback manager。
11. start retention manager。
12. handle shutdown signals。
```

---

## 18. 關閉處理

需支援：

```text
SIGINT
SIGTERM
```

收到後：

```text
設定 stop event
停止 WebSocket manager
停止 REST fallback manager
停止 webhook server
forced flush all partition queues
flush 成功後更新 state
shutdown webhook server
server_close
flush pending writes
印出 stopped message
```
