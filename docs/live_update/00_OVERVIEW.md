# 00_OVERVIEW.md

# Live Update 總覽

本文件定義 live update 的核心架構與不可變決策。

---

## 1. 核心目的

在第一次歷史資料建置完成後，後續不再依賴定期重跑官方 archive。

日常資料更新由 live update 接手。

```text
initial historical seed data
        +
live update closed KBar
        =
current historical dataset
```

---

## 2. 核心原則

```text
WebSocket-first:
  WebSocket 是主要即時資料來源。

REST-fallback:
  REST 用於 startup backfill、gap repair、WebSocket 斷線補洞。

Current dataset:
  研究 agent 預設只讀 current historical dataset。

Closed buffer:
  closed_buffer 是完整 KBar 的 replay / audit log。

State-driven:
  所有補洞都必須根據 state 與 current dataset 推導，不得只抓最近幾根。

Micro-batch flush:
  closed KBar 不得每根都無限制 rewrite parquet，需透過 per-partition queue 批次 flush。

Single partition writer:
  同一 interval + symbol + year + month partition 同時間只能有一個 writer。

Continuity check:
  需要能檢查 duplicate、missing、open_time interval alignment。
```

---

## 3. 不可更改的資料層

```text
local_data/binance_um_klines/
  第一次歷史資料建置產生的 historical seed parquet。

local_data/binance_um_klines_current/
  研究 agent 預設讀取的最新完整歷史資料。

local_data/live_update/
  runtime buffer、latest、closed_buffer、state、rejects。
```

---

## 4. 研究 agent 的唯一入口

研究 agent 預設只讀：

```text
local_data/binance_um_klines_current/interval=<INTERVAL>/parquet/
```

研究 agent 不應直接讀：

```text
local_data/binance_um_klines/
local_data/live_update/
```

除非任務明確要求資料稽核、debug、replay。

---

## 5. Live Update 資料流

```text
Binance WebSocket kline stream
        │
        ▼
latest KBar updates
        │
        ├── 未收盤 KBar
        │       └── websocket_buffer + latest
        │
        └── 已收盤 KBar
                └── closed_buffer
                        └── partition write queue
                                └── current historical dataset
```

```text
REST startup backfill / fallback / gap repair
        │
        ▼
closed KBar records
        │
        └── event_buffer
                └── closed_buffer
                        └── partition write queue
                                └── current historical dataset
```

```text
Webhook / external bridge
        │
        ▼
POST /webhook/kline
        │
        ├── 未收盤 KBar
        │       └── webhook_buffer + latest
        │
        └── 已收盤 KBar
                └── closed_buffer
                        └── partition write queue
                                └── current historical dataset
```

---

## 6. 支援週期

支援：

```text
1m
3m
5m
15m
1h
4h
1d
```

CLI 支援：

```text
--interval all
--interval 1m
--interval 3m
--interval 5m
--interval 15m
--interval 1h
--interval 4h
--interval 1d
```

`all` 只是 CLI 展開語意。  
程式不得將 `all` 傳給 Binance API。

---

## 7. `--interval all` 行為

當使用：

```bash
.venv/bin/python scripts/live_update.py --interval all
```

需展開為：

```text
1m
3m
5m
15m
1h
4h
1d
```

每個 interval 獨立管理：

```text
state
latest
event_buffer
websocket_buffer
webhook_buffer
closed_buffer
rejects
current historical dataset
startup backfill
REST fallback
partition writers
continuity checks
```

---

## 8. 來源優先級

```text
即時主來源:
  WebSocket

補洞來源:
  REST

外部事件來源:
  Webhook

正式研究資料:
  current historical dataset
```

---

## 9. 初始歷史資料與 live 資料的關係

第一次歷史資料建置產生 historical seed parquet。

live update 啟動時，若 current historical dataset 不存在，應從 seed parquet 初始化一份 current dataset。

之後所有新完整 KBar 都 merge 到 current dataset。

```text
historical seed parquet
        ↓
current historical dataset
        ↑
live update closed KBar
```

---

## 10. Archive 的角色

第一次建置後，日常更新不依賴 archive。

archive / historical seed 的角色：

```text
initial bootstrap
disaster recovery
current dataset rebuild base
```

live update 的角色：

```text
日常更新來源
實盤接近資料來源
最新完整 KBar 來源
```

---

## 11. Symbol Universe

live update 需區分：

```text
currently trading symbols:
  從 exchangeInfo 取得，可訂閱 WebSocket。

existing current dataset symbols:
  current historical dataset 已存在的 symbols，可能包含下市幣。

optional symbols file:
  使用者指定的 symbols 清單。
```

規則：

```text
WebSocket 只訂閱 currently TRADING symbols。
current dataset 可保留下市 symbols。
不得因 symbol 不在 exchangeInfo 中就刪除 current dataset 既有資料。
newly listed symbol 若 current dataset 無歷史，標記 bootstrap_required 或從 listing 後 warm-up。
```

---

## 12. 不建議在第一版加入的功能

第一版不要加入：

```text
Prometheus metrics
SQLite metadata DB
Kafka / Redpanda queue
DuckDB materialized view
分散式鎖
跨機器 worker
```

先完成單機穩定 live update、補洞、current dataset 更新與資料連續性檢查。
