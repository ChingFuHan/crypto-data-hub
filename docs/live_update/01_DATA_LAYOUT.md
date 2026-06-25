# 01_DATA_LAYOUT.md

# Live Update 資料目錄設計

本文件定義 live update 的資料目錄、runtime buffer、state、latest、closed_buffer 與 retention。

---

## 1. 三個主要資料區

```text
local_data/binance_um_klines/
local_data/binance_um_klines_current/
local_data/live_update/
```

角色：

```text
local_data/binance_um_klines/
  第一次歷史資料建置產生的 historical seed parquet。

local_data/binance_um_klines_current/
  研究 agent 預設讀取的最新完整歷史資料。

local_data/live_update/
  live update runtime 資料。
```

---

## 2. Current Historical Dataset

路徑：

```text
local_data/binance_um_klines_current/interval=<INTERVAL>/parquet/
```

範例：

```text
local_data/binance_um_klines_current/interval=1m/parquet/
local_data/binance_um_klines_current/interval=3m/parquet/
local_data/binance_um_klines_current/interval=5m/parquet/
local_data/binance_um_klines_current/interval=15m/parquet/
local_data/binance_um_klines_current/interval=1h/parquet/
local_data/binance_um_klines_current/interval=4h/parquet/
local_data/binance_um_klines_current/interval=1d/parquet/
```

partition：

```text
symbol=<SYMBOL>/year=<YYYY>/month=<MM>/part-000.parquet
```

---

## 3. Live Runtime Root

路徑：

```text
local_data/live_update/binance_um_klines/interval=<INTERVAL>/
```

範例：

```text
local_data/live_update/binance_um_klines/interval=1m/
```

目錄：

```text
event_buffer/
websocket_buffer/
webhook_buffer/
closed_buffer/
latest/
rejects/
state/
logs/
```

完整結構：

```text
local_data/live_update/binance_um_klines/interval=1m/
  event_buffer/
    date=YYYY-MM-DD/
      events.jsonl

  websocket_buffer/
    date=YYYY-MM-DD/
      events.jsonl

  webhook_buffer/
    date=YYYY-MM-DD/
      events.jsonl

  closed_buffer/
    date=YYYY-MM-DD/
      closed.jsonl

  latest/
    symbol=BTCUSDT.json
    symbol=ETHUSDT.json

  rejects/
    date=YYYY-MM-DD/
      rejects.jsonl

  state/
    live_update_state.json

  logs/
    date=YYYY-MM-DD/
      runtime.log
      warnings.log
```

---

## 4. `event_buffer`

用途：

```text
保存 REST fallback、startup backfill、gap repair 收到的 KBar event。
```

路徑：

```text
local_data/live_update/binance_um_klines/interval=<INTERVAL>/event_buffer/date=YYYY-MM-DD/events.jsonl
```

格式：

```json
{
  "received_at_utc": "2026-06-26T00:00:00Z",
  "source": "rest_fallback",
  "record_key": {
    "symbol": "BTCUSDT",
    "interval": "1m",
    "open_time": 1782432000000
  },
  "validation_errors": [],
  "payload": {}
}
```

---

## 5. `websocket_buffer`

用途：

```text
保存 WebSocket 收到的原始 kline event。
可包含未收盤 KBar。
```

路徑：

```text
local_data/live_update/binance_um_klines/interval=<INTERVAL>/websocket_buffer/date=YYYY-MM-DD/events.jsonl
```

格式：

```json
{
  "received_at_utc": "2026-06-26T00:00:00Z",
  "source": "websocket",
  "stream": "btcusdt@kline_1m",
  "record_key": {
    "symbol": "BTCUSDT",
    "interval": "1m",
    "open_time": 1782432000000
  },
  "validation_errors": [],
  "payload": {}
}
```

---

## 6. `webhook_buffer`

用途：

```text
保存 webhook 收到的外部 KBar event。
可包含未收盤 KBar。
```

路徑：

```text
local_data/live_update/binance_um_klines/interval=<INTERVAL>/webhook_buffer/date=YYYY-MM-DD/events.jsonl
```

格式：

```json
{
  "received_at_utc": "2026-06-26T00:00:00Z",
  "source": "webhook",
  "record_key": {
    "symbol": "BTCUSDT",
    "interval": "1m",
    "open_time": 1782432000000
  },
  "validation_errors": [],
  "payload": {}
}
```

---

## 7. `latest`

用途：

```text
保存每個 symbol + interval 最新收到的一根 KBar 狀態。
可用於 trigger、alert、掃幣。
不是正式研究資料。
```

路徑：

```text
local_data/live_update/binance_um_klines/interval=<INTERVAL>/latest/symbol=<SYMBOL>.json
```

格式：

```json
{
  "updated_at_utc": "2026-06-26T00:00:00Z",
  "source": "websocket",
  "record": {
    "symbol": "BTCUSDT",
    "interval": "1m",
    "open_time": 1782432000000,
    "open": 65000.0,
    "high": 65100.0,
    "low": 64900.0,
    "close": 65050.0,
    "volume": 123.45,
    "close_time": 1782432059999,
    "is_closed": false
  },
  "validation_errors": []
}
```

---

## 8. `closed_buffer`

用途：

```text
保存已確認完整收盤的 KBar。
作為 replay / audit log。
可用於 current dataset 重建。
```

路徑：

```text
local_data/live_update/binance_um_klines/interval=<INTERVAL>/closed_buffer/date=YYYY-MM-DD/closed.jsonl
```

格式：

```json
{
  "closed_at_utc": "2026-06-26T00:00:00Z",
  "source": "websocket",
  "schema_version": 1,
  "record": {
    "symbol": "BTCUSDT",
    "interval": "1m",
    "open_time": 1782432000000,
    "open_time_utc": "2026-06-26T00:00:00",
    "open_time_taipei": "2026-06-26T08:00:00",
    "date": "2026-06-26",
    "year": 2026,
    "month": 6,
    "open": 65000.0,
    "high": 65100.0,
    "low": 64900.0,
    "close": 65050.0,
    "volume": 123.45,
    "close_time": 1782432059999,
    "quote_volume": 8000000.0,
    "trade_count": 1234,
    "taker_buy_base_volume": 60.0,
    "taker_buy_quote_volume": 3900000.0,
    "source_archive": "live_websocket:kline",
    "archive_source": "live_websocket",
    "archive_period": "2026-06-26"
  }
}
```

規則：

```text
完整 KBar 寫入 closed_buffer 後，必須進入 per-partition write queue。
closed_buffer 寫入成功不代表 current dataset 已完成。
closed_buffer 不是研究 agent 預設資料入口。
```

---

## 9. `rejects`

用途：

```text
保存解析失敗或驗證失敗的資料。
```

路徑：

```text
local_data/live_update/binance_um_klines/interval=<INTERVAL>/rejects/date=YYYY-MM-DD/rejects.jsonl
```

格式：

```json
{
  "rejected_at_utc": "2026-06-26T00:00:00Z",
  "source": "websocket",
  "error": "close_time mismatch",
  "record": {},
  "payload": {}
}
```

---

## 10. `state`

用途：

```text
保存每個 interval 的 live update 狀態。
支援 startup backfill、REST fallback、gap repair、resume。
```

路徑：

```text
local_data/live_update/binance_um_klines/interval=<INTERVAL>/state/live_update_state.json
```

格式：

```json
{
  "dataset": "market.binance.um.klines.live_update",
  "schema_version": 1,
  "dataset_version": "current-v1",
  "interval": "1m",
  "created_at_utc": "2026-06-26T00:00:00Z",
  "updated_at_utc": "2026-06-26T00:01:00Z",
  "current_dataset_root": "local_data/binance_um_klines_current/interval=1m/parquet",
  "websocket": {
    "enabled": true,
    "last_connected_at_utc": "2026-06-26T00:00:00Z",
    "last_message_at_utc": "2026-06-26T00:01:00Z",
    "last_reconnect_at_utc": null,
    "reconnect_count": 0
  },
  "symbols": {
    "BTCUSDT": {
      "last_buffered_open_time": 1782432000000,
      "last_flushed_open_time": 1782432000000,
      "last_closed_open_time": 1782432000000,
      "last_closed_at_utc": "2026-06-26T00:01:00Z",
      "last_ws_message_at_utc": "2026-06-26T00:01:00Z",
      "merged_bar_count": 1,
      "last_target_path": "local_data/binance_um_klines_current/interval=1m/parquet/symbol=BTCUSDT/year=2026/month=06/part-000.parquet"
    }
  }
}
```

定義：

```text
last_buffered_open_time:
  已寫入 closed_buffer 或 write queue 的最後 open_time。

last_flushed_open_time:
  已成功 flush 到 current dataset 的最後 open_time。

last_closed_open_time:
  已成功 merge / flush 到 current dataset 的最後 open_time。
  startup backfill 與 REST fallback 只能以此為主要續跑依據。
```

---

## 11. 寫入要求

JSONL append：

```text
event_buffer
websocket_buffer
webhook_buffer
closed_buffer
rejects
```

atomic JSON write：

```text
latest
state
```

micro-batch parquet merge：

```text
current historical dataset
```

所有寫入需先建立 parent directory。

---

## 12. Retention Policy

預設 retention：

```text
websocket_buffer:
  保留 7 天，可壓縮。

webhook_buffer:
  保留 30 天，可壓縮。

event_buffer:
  保留 30 天，可壓縮。

closed_buffer:
  長期保留，因為可 replay。
  第一版不得自動刪除 closed_buffer。

rejects:
  保留 90 天。

logs:
  保留 30 天。
```

CLI 可預留：

```text
--buffer-retention-days
--closed-buffer-retention-days
--compress-old-buffers
```

規則：

```text
closed_buffer 若啟用 retention，必須明確 warning：
刪除 closed_buffer 會降低 current dataset replay / rebuild 能力。
```
