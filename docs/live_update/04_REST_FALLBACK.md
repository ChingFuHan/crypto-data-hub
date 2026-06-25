# 04_REST_FALLBACK.md

# REST Fallback 與 Gap Repair 規格

本文件定義 REST API 的用途、fallback 條件、補洞、rate limit/backoff 與 gap repair。

---

## 1. REST 的角色

REST 不作為主要即時來源。

REST 用於：

```text
startup backfill
WebSocket 斷線補洞
WebSocket stale 補洞
state 落後補洞
定期 gap repair
--disable-websocket 模式
--once 模式
```

---

## 2. REST API

使用 Binance USD-M Futures Kline API：

```text
GET https://fapi.binance.com/fapi/v1/klines
```

參數：

```text
symbol
interval
limit
startTime
endTime
```

CLI 預設：

```text
--binance-rest-base-url https://fapi.binance.com
--rest-api-limit 1500
--http-timeout 15
--request-delay 0.02
```

Klines 以 open time 唯一識別。

---

## 3. REST Row 欄位對應

```text
row[0]  -> open_time
row[1]  -> open
row[2]  -> high
row[3]  -> low
row[4]  -> close
row[5]  -> volume
row[6]  -> close_time
row[7]  -> quote_volume
row[8]  -> trade_count
row[9]  -> taker_buy_base_volume
row[10] -> taker_buy_quote_volume
```

---

## 4. 完整 KBar 判定

REST 回傳的 KBar 需判定是否已收盤。

```text
close_time <= now_ms - close_lag_ms
```

預設：

```text
close_lag_ms = 2000
```

未收盤：

```text
寫入 event_buffer
更新 latest
不得寫入 closed_buffer
不得 merge current dataset
```

已收盤：

```text
寫入 event_buffer
更新 latest
寫入 closed_buffer
enqueue partition writer
flush current dataset
flush 成功後更新 state
```

---

## 5. Fallback 觸發條件

以下情況需觸發 REST fallback：

```text
WebSocket connection closed
WebSocket reconnect 後
WebSocket reconnect 超過 retry 次數
某 symbol + interval 超過 stale threshold 未收到任何更新
state.last_closed_open_time 落後 latest_closed_open_time
程式重啟後發現缺口
定期 gap repair 發現缺口
```

---

## 6. Fallback 流程

```text
1. 讀取 state.last_closed_open_time。
2. 計算 latest_closed_open_time。
3. 計算 missing_bars。
4. 若 missing_bars > 0，使用 REST startTime / endTime 分批補齊。
5. 每根補回的完整 KBar 寫入 event_buffer。
6. 每根補回的完整 KBar 寫入 closed_buffer。
7. 每根補回的完整 KBar 進入 partition queue。
8. flush 到 current historical dataset。
9. flush 成功後更新 state。
10. 持續或恢復 WebSocket 監聽。
```

---

## 7. REST Chunking

```text
cursor = start_open_time

while cursor <= end_open_time:
    request startTime = cursor
    request endTime = end_open_time
    request limit = rest_api_limit

    if empty response:
        write warning
        break

    for each returned row:
        normalize row
        validate KBar
        if closed:
            write event_buffer
            write closed_buffer
            enqueue partition writer
        else:
            write event_buffer
            update latest

    flush affected partition queues when flush policy triggers
    cursor = last_returned_open_time + interval_ms
```

---

## 8. REST Rate Limit / Backoff

REST fallback 必須處理：

```text
HTTP 429
HTTP 418
HTTP 5xx
timeout
empty response
invalid symbol
delisted symbol
```

處理規則：

```text
HTTP 429:
  exponential backoff
  不得立即重試轟炸
  記錄 warning

HTTP 418:
  停止 REST requests
  記錄 critical warning
  不得繼續重試造成 IP ban 風險

HTTP 5xx:
  retry with exponential backoff

timeout:
  retry with exponential backoff

invalid symbol:
  標記 symbol_unavailable
  不得讓整體程式崩潰

delisted symbol:
  不訂閱 WebSocket
  保留 current dataset 既有歷史
```

建議 CLI：

```text
--rest-max-retries
--rest-backoff-base-seconds
--rest-backoff-max-seconds
```

預設：

```text
--rest-max-retries 5
--rest-backoff-base-seconds 1
--rest-backoff-max-seconds 60
```

---

## 9. Gap Repair

程式需支援定期 gap repair。

CLI：

```text
--gap-repair-seconds
```

預設：

```text
300
```

行為：

```text
每 gap_repair_seconds 秒，對 active symbols + intervals 執行 gap check。
若 state 落後 latest_closed_open_time，使用 REST 補洞。
```

---

## 10. `--disable-rest-fallback`

若使用：

```bash
.venv/bin/python scripts/live_update.py --disable-rest-fallback
```

行為：

```text
WebSocket 仍可啟動。
startup backfill 可依 --disable-startup-backfill 決定。
WebSocket stale / disconnect 不使用 REST 補洞。
需在 log 中明確警告資料可能出現缺口。
```

---

## 11. `--disable-websocket`

若使用：

```bash
.venv/bin/python scripts/live_update.py --disable-websocket
```

行為：

```text
不啟動 WebSocket。
REST 變成主要更新方式。
可用 poll-seconds 週期性抓最近 lookback_bars。
仍需 startup backfill。
```

此模式是 fallback 模式，不是預設主模式。

---

## 12. `--once`

`--once` 模式：

```text
1. 展開 intervals。
2. 載入 symbols。
3. 初始化 current dataset。
4. 執行 startup backfill。
5. 若無缺口，抓最近 lookback_bars。
6. enqueue 已收盤 KBar。
7. forced flush all partition queues。
8. flush 成功後更新 state。
9. 結束。
```

用途：

```text
手動補洞
驗收
排程補洞
不啟動長駐 WebSocket
```
