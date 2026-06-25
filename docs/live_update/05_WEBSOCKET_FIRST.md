# 05_WEBSOCKET_FIRST.md

# WebSocket-first 規格

本文件定義 WebSocket 主要即時來源、stream batching、combined stream、stale、reconnect 與 payload 處理。

---

## 1. WebSocket 角色

WebSocket 是 live update 的主要即時資料來源。

用途：

```text
接收最接近實盤的 KBar 更新
即時更新 latest
在 k.x = true 時產生 closed KBar
寫入 closed_buffer
進入 partition queue
透過 micro-batch flush 更新 current dataset
```

---

## 2. WebSocket Base URL

預設：

```text
wss://fstream.binance.com
```

CLI：

```text
--binance-ws-base-url
```

---

## 3. Routed Endpoint

Market stream 使用：

```text
wss://fstream.binance.com/market
```

combined stream final URL：

```text
wss://fstream.binance.com/market/stream?streams=<stream1>/<stream2>/...
```

raw stream final URL：

```text
wss://fstream.binance.com/market/ws/<streamName>
```

程式應優先使用 combined stream。

---

## 4. Stream 格式

```text
<symbol_lower>@kline_<interval>
```

範例：

```text
btcusdt@kline_1m
btcusdt@kline_3m
btcusdt@kline_5m
btcusdt@kline_15m
btcusdt@kline_1h
btcusdt@kline_4h
btcusdt@kline_1d
```

---

## 5. `--interval all`

若：

```bash
.venv/bin/python scripts/live_update.py --interval all
```

且 symbol 為：

```text
BTCUSDT
```

需建立：

```text
btcusdt@kline_1m
btcusdt@kline_3m
btcusdt@kline_5m
btcusdt@kline_15m
btcusdt@kline_1h
btcusdt@kline_4h
btcusdt@kline_1d
```

---

## 6. Combined Payload Wrapper

combined stream payload 格式：

```json
{
  "stream": "btcusdt@kline_1m",
  "data": {
    "e": "kline",
    "E": 1638747660000,
    "s": "BTCUSDT",
    "k": {}
  }
}
```

Parser 必須支援：

```text
combined wrapper payload
raw kline payload
```

不得只支援 raw payload。

---

## 7. Stream Batching

CLI：

```text
--ws-batch-size
```

預設：

```text
100
```

行為：

```text
1. 根據 symbols + intervals 產生 stream list。
2. 依 ws_batch_size 分批。
3. 每批建立一條 WebSocket connection。
4. 每條 connection 獨立管理 reconnect、last_message_at、stale 狀態。
```

當 symbols 很多時：

```text
stream_count = len(symbols) * len(intervals)
```

例如：

```text
300 symbols * 7 intervals = 2100 streams
```

不得把所有 streams 無限制塞進單一 connection。

---

## 8. WebSocket Hard Rules

需遵守：

```text
1. 單一 connection stream 數不得超過官方限制。
2. ws_batch_size 預設 100，但不得超過 max_streams_per_connection。
3. connection 可能定期斷線，需支援 reconnect 或 rotate。
4. 需處理 ping / pong。
5. 不得超過 WebSocket message rate limit。
```

CLI：

```text
--max-streams-per-connection
--max-total-streams
--startup-batch-size
--startup-batch-delay
--ws-connection-rotate-hours
```

預設：

```text
--max-streams-per-connection 1024
--max-total-streams 0
--startup-batch-size 5
--startup-batch-delay 1
--ws-connection-rotate-hours 23
```

`--max-total-streams 0` 代表不額外限制，但仍需遵守 per-connection limit。

---

## 9. Full Market 降載策略

當 `--interval all` 且 symbols 很多時，啟動前需印出 summary：

```text
symbols_count
intervals_count
stream_count
connection_count
ws_batch_size
max_streams_per_connection
estimated_partition_count
```

啟動時不得一次建立所有 connection。

需支援 staged startup：

```text
每次啟動 startup_batch_size 條 connections
等待 startup_batch_delay 秒
再啟動下一批
```

---

## 10. Payload 格式

WebSocket kline payload 範例：

```json
{
  "e": "kline",
  "E": 1638747660000,
  "s": "BTCUSDT",
  "k": {
    "t": 1638747660000,
    "T": 1638747719999,
    "s": "BTCUSDT",
    "i": "1m",
    "o": "50000.0",
    "c": "50050.0",
    "h": "50100.0",
    "l": "49900.0",
    "v": "1000",
    "n": 100,
    "x": false,
    "q": "50050000",
    "V": "500",
    "Q": "25025000",
    "B": "0"
  }
}
```

欄位對應：

```text
k.t -> open_time
k.T -> close_time
k.s -> symbol
k.i -> interval
k.o -> open
k.h -> high
k.l -> low
k.c -> close
k.v -> volume
k.q -> quote_volume
k.n -> trade_count
k.V -> taker_buy_base_volume
k.Q -> taker_buy_quote_volume
k.x -> is_closed
```

---

## 11. `k.x = false`

代表 KBar 尚未收盤。

處理：

```text
寫入 websocket_buffer
更新 latest
不得寫入 closed_buffer
不得進入 partition queue
不得 merge current dataset
```

---

## 12. `k.x = true`

代表 KBar 已完整收盤。

處理：

```text
寫入 websocket_buffer
更新 latest
驗證 KBar
寫入 closed_buffer
更新 last_buffered_open_time
送入 partition queue
等待 micro-batch flush
flush 成功後更新 last_closed_open_time
```

---

## 13. Stale 判斷

CLI：

```text
--ws-stale-multiplier
```

預設：

```text
3
```

每個 symbol + interval 計算：

```text
stale_threshold_ms = interval_ms * ws_stale_multiplier
```

若：

```text
now_ms - last_ws_message_time_ms > stale_threshold_ms
```

視為 stale。

stale 後：

```text
標記 symbol + interval stale
觸發 REST fallback gap check
不得假設資料完整
```

---

## 14. Reconnect

CLI：

```text
--ws-reconnect-max-retries
--ws-reconnect-backoff-seconds
```

預設：

```text
--ws-reconnect-max-retries 10
--ws-reconnect-backoff-seconds 5
```

規則：

```text
1. connection closed 後依 backoff 重連。
2. 每次重連前更新 reconnect_count。
3. 重連成功後更新 last_connected_at_utc。
4. 重連後對 connection 覆蓋的 symbol + interval 執行 REST fallback gap check。
5. 若超過 max retries，仍需保留 REST fallback 補洞能力。
```

---

## 15. Connection Rotate

WebSocket connection 不得假設永久有效。

需支援：

```text
定期 rotate connection
rotate 前建立新 connection
新 connection 收到資料後再關閉舊 connection
rotate 後執行 REST fallback gap check
```

預設：

```text
--ws-connection-rotate-hours 23
```

---

## 16. WebSocket State

state.websocket：

```json
{
  "enabled": true,
  "last_connected_at_utc": "2026-06-26T00:00:00Z",
  "last_message_at_utc": "2026-06-26T00:01:00Z",
  "last_reconnect_at_utc": null,
  "reconnect_count": 0
}
```

每個 symbol：

```json
{
  "last_ws_message_at_utc": "2026-06-26T00:01:00Z"
}
```

---

## 17. WebSocket Manager 實作要求

Agent 需設計：

```text
WebSocket manager
WebSocket connection worker
combined stream URL builder
combined payload parser
raw payload parser
stream batch builder
message parser
stale monitor
reconnect handler
connection rotate handler
REST fallback trigger
```

建議使用：

```text
asyncio
websockets
```

---

## 18. WebSocket 驗收

執行：

```bash
.venv/bin/python scripts/live_update.py \
  --interval all \
  --symbols BTCUSDT ETHUSDT
```

預期：

```text
啟動 startup backfill
啟動 WebSocket manager
建立 combined kline streams
收到 k.x = false 時更新 websocket_buffer 與 latest
收到 k.x = true 時寫入 closed_buffer
收到 k.x = true 時 enqueue partition writer
flush 成功後更新 current historical dataset
state 更新 last_closed_open_time
```
