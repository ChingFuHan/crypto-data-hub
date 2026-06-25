# 06_WEBHOOK.md

# Webhook 規格

本文件定義 webhook server、payload 格式與處理規則。

---

## 1. Webhook 角色

Webhook 是外部 bridge / agent / trigger system 的即時資料入口。

用途：

```text
接收外部轉送的 KBar payload
支援 Binance WebSocket kline payload
支援 normalized payload
更新 latest
寫入 webhook_buffer
closed KBar 寫入 closed_buffer
closed KBar 進入 partition queue
flush 成功後更新 current dataset 與 state
```

---

## 2. Server

預設：

```text
host: 127.0.0.1
port: 8787
```

CLI：

```text
--webhook-host
--webhook-port
--disable-webhook
```

---

## 3. Endpoints

```text
GET  /healthz
POST /webhook/kline
```

---

## 4. Healthz

回傳範例：

```json
{
  "status": "ok",
  "interval": "all",
  "active_intervals": ["1m", "3m", "5m", "15m", "1h", "4h", "1d"],
  "live_root": "local_data/live_update/binance_um_klines",
  "current_dataset_root": "local_data/binance_um_klines_current"
}
```

---

## 5. 支援 Payload

需支援：

```text
Binance WebSocket kline payload
Binance combined stream wrapper payload
Normalized kline payload
```

---

## 6. Binance WebSocket Kline Payload

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

## 7. Combined Stream Wrapper Payload

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

Webhook parser 必須能解析 wrapper 中的 `data`。

---

## 8. Normalized Payload

```json
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
```

---

## 9. 處理規則

未收盤：

```text
寫入 webhook_buffer
更新 latest
不得寫入 closed_buffer
不得進入 partition queue
不得 merge current dataset
```

已收盤：

```text
寫入 webhook_buffer
更新 latest
驗證 KBar
寫入 closed_buffer
更新 last_buffered_open_time
進入 partition queue
micro-batch flush
flush 成功後更新 state
```

若 payload 未提供 `is_closed`：

```text
close_time <= now_ms - close_lag_ms
```

判定是否完整。

---

## 10. 回應格式

成功但未收盤：

```json
{
  "status": "accepted",
  "is_closed": false
}
```

成功且已進入 queue：

```json
{
  "status": "queued",
  "is_closed": true
}
```

成功且同步 flush：

```json
{
  "status": "merged",
  "is_closed": true
}
```

驗證失敗：

```json
{
  "status": "rejected",
  "errors": []
}
```

---

## 11. Webhook-only 模式

執行：

```bash
.venv/bin/python scripts/live_update.py \
  --interval all \
  --disable-websocket \
  --disable-rest-fallback
```

行為：

```text
只啟動 webhook server。
不啟動 WebSocket。
不執行 REST fallback。
payload 仍可更新 latest / closed_buffer / current dataset。
```

---

## 12. 驗收

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
回應 status = queued 或 merged
產生 webhook_buffer
產生 closed_buffer
更新 latest
flush 後更新 state
flush 後 merge current historical parquet
```
