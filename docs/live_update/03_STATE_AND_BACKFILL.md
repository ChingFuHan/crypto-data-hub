# 03_STATE_AND_BACKFILL.md

# State 與 Startup Backfill 規格

本文件定義 state、停機補洞、startup backfill 與 missing bars 計算。

---

## 1. State 角色

state 是 live update 的續跑依據。

每個 interval 有獨立 state：

```text
local_data/live_update/binance_um_klines/interval=<INTERVAL>/state/live_update_state.json
```

核心欄位：

```text
state.symbols[SYMBOL].last_closed_open_time
```

用途：

```text
startup backfill
REST fallback
gap repair
停機補洞
resume
```

---

## 2. State 格式

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

---

## 3. 欄位定義

```text
last_buffered_open_time:
  已寫入 closed_buffer 或 partition queue 的最後 open_time。

last_flushed_open_time:
  已成功 flush 到 current dataset 的最後 open_time。

last_closed_open_time:
  已成功 merge / flush 到 current dataset 的最後 open_time。
  startup backfill 與 REST fallback 的主要依據。
```

規則：

```text
closed_buffer 寫入成功不代表 current dataset 已完成。
partition queue 收到資料不代表 current dataset 已完成。
state.last_closed_open_time 只能在 parquet flush 成功後更新。
```

---

## 4. Startup Backfill 目的

`scripts/live_update.py` 啟動時，必須先執行 startup backfill，再進入正常 WebSocket 監聽。

目的：

```text
偵測程式停機、網路中斷、主機重啟、API 暫時失敗造成的 KBar 缺口，並自動補齊。
```

---

## 5. Startup Backfill 狀態來源

每個 symbol + interval 依序判斷：

```text
1. 優先讀取 state.symbols[SYMBOL].last_closed_open_time。
2. 若 state 不存在或該 symbol 無紀錄，從 current historical dataset 讀取最大 open_time。
3. 若 state 與 current dataset 都沒有資料，該 symbol 視為尚未初始化。
```

尚未初始化時：

```text
不得做無界 backfill。
```

處理方式：

```text
1. 若 current dataset 尚未建立，先嘗試從 historical seed parquet 初始化。
2. 若初始化後仍找不到該 symbol 最大 open_time，回報 bootstrap_required。
3. 對 bootstrap_required 的 symbol，只能抓最近 lookback_bars 作為 warm-up，不得從交易所無限制往前補。
```

---

## 6. Interval Milliseconds

```text
1m  = 60_000
3m  = 180_000
5m  = 300_000
15m = 900_000
1h  = 3_600_000
4h  = 14_400_000
1d  = 86_400_000
```

---

## 7. Latest Closed KBar 計算

程式需根據目前時間與 interval 計算最新已收盤 KBar。

公式：

```text
now_ms = current timestamp in milliseconds
safe_now_ms = now_ms - close_lag_ms
latest_closed_open_time = floor(safe_now_ms / interval_ms) * interval_ms - interval_ms
```

預設：

```text
close_lag_ms = 2000
```

---

## 8. Missing Bars 計算

已知：

```text
last_closed_open_time
latest_closed_open_time
interval_ms
```

缺口：

```text
missing_bars = max(
  0,
  (latest_closed_open_time - last_closed_open_time) // interval_ms
)
```

補洞起點：

```text
start_open_time = last_closed_open_time + interval_ms
```

補洞終點：

```text
end_open_time = latest_closed_open_time
```

---

## 9. 停機補洞範例

停機 1 天：

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

## 10. Startup Backfill 流程

```text
for interval in active_intervals:
    load state
    ensure current dataset

    for symbol in symbols:
        last_closed_open_time = state or max open_time from current dataset
        latest_closed_open_time = calculate from current time

        missing_bars = calculate gap

        if missing_bars > 0:
            use REST to backfill start_open_time to end_open_time
            validate each KBar
            write event_buffer
            write closed_buffer
            enqueue partition writer
            flush queue based on flush policy
            update state only after successful flush
```

---

## 11. Startup Backfill 與 lookback_bars 分工

`lookback_bars` 只用於：

```text
normal REST polling
--once fallback warm-up
bootstrap_required symbol warm-up
```

`lookback_bars` 不得取代 startup backfill。

startup backfill 必須：

```text
根據 state 與目前時間計算缺口
缺多少補多少
```

---

## 12. Backfill Chunking

REST API 單次回傳有 limit 上限。

必須 chunking：

```text
cursor = start_open_time

while cursor <= end_open_time:
    request startTime = cursor
    request endTime = end_open_time
    request limit = rest_api_limit

    validate returned KBars
    process closed KBars only

    cursor = last_returned_open_time + interval_ms
```

預設：

```text
rest_api_limit = 1500
```

若 REST 回傳空資料：

```text
停止該 symbol + interval backfill
寫入 warning log
避免無限迴圈
```

---

## 13. 中途失敗恢復

每成功 flush current dataset 後，更新 state。

若 backfill 中途失敗，下次啟動需從最後成功的：

```text
last_closed_open_time
```

繼續補。

如果 closed_buffer 已有資料但 current dataset 未 flush：

```text
下一次啟動需能從 closed_buffer replay
或使用 REST fallback 重新補齊
不得因 last_buffered_open_time 而跳過補洞
```

---

## 14. Startup Backfill 完成條件

```text
停機 1 分鐘，自動補 1 根 1m KBar。
停機 1 小時，自動補 60 根 1m KBar。
停機 1 天，自動補 1440 根 1m KBar。
停機 N 天，自動依 interval 與 state 計算缺口並分批補齊。
```

當 `--interval all` 時，所有支援週期都需各自完成。
