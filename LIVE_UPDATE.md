# LIVE_UPDATE.md

# crypto-data-hub Live Update 入口文件

本文件是 `crypto-data-hub` live update 任務的唯一入口。

Agent 必須先閱讀本文件，再依序閱讀：

```text
docs/live_update/00_OVERVIEW.md
docs/live_update/01_DATA_LAYOUT.md
docs/live_update/02_CURRENT_DATASET.md
docs/live_update/03_STATE_AND_BACKFILL.md
docs/live_update/04_REST_FALLBACK.md
docs/live_update/05_WEBSOCKET_FIRST.md
docs/live_update/06_WEBHOOK.md
docs/live_update/07_CLI_AND_MODES.md
docs/live_update/08_VALIDATION_AND_TESTS.md
docs/live_update/09_RUNBOOK.md
```

不得跳讀。  
不得自行改資料層命名。  
不得把 `all` 當成 Binance API interval。  
不得把未收盤 KBar 寫入 current historical dataset。  
不得在規格衝突時自行猜測。

---

## 1. 任務目標

建立：

```text
scripts/live_update.py
```

完成後，使用者可以在 repo root 執行：

```bash
.venv/bin/python scripts/live_update.py --interval all
```

啟動 Binance USD-M Futures KBar live update。

---

## 2. 核心架構

Live update 採用：

```text
WebSocket-first
REST-fallback
state-driven startup backfill
current historical dataset
closed_buffer replay
micro-batch parquet flush
single partition writer
```

資料主線：

```text
initial historical seed data
        +
live update closed KBar
        =
current historical dataset
```

---

## 3. 資料來源角色

```text
WebSocket:
  主要即時來源，取得最接近實盤的 KBar 更新。

REST:
  fallback / startup backfill / gap repair 來源。

Webhook:
  外部 bridge / agent / trigger 系統的即時資料入口。

closed_buffer:
  完整 KBar 的 replay / audit log。

current historical dataset:
  研究 agent 唯一預設讀取入口。
```

---

## 4. 支援週期

支援：

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

當使用：

```bash
.venv/bin/python scripts/live_update.py --interval all
```

必須展開成：

```text
1m
3m
5m
15m
1h
4h
1d
```

`all` 是 CLI 展開語意。  
程式不得把 `all` 傳給 Binance REST API 或 WebSocket stream。

---

## 5. Source of Truth 規則

文件優先順序：

```text
1. LIVE_UPDATE.md
   任務入口、執行順序、不可違反規則。

2. docs/live_update/*.md
   各模組詳細規格。

3. 若總入口與分卷衝突：
   以 LIVE_UPDATE.md 的核心架構為準。
   以分卷中的該模組細節為準。

4. 若任兩份分卷衝突：
   停止實作並回報 conflict，不得自行猜測。
```

---

## 6. Agent 實作順序

Agent 必須依照以下 Phase 實作：

```text
Phase 1:
  建立資料結構、KlineRecord、基本路徑解析。

Phase 2:
  current historical dataset 初始化與 parquet merge。

Phase 3:
  state 管理與 startup backfill。

Phase 4:
  REST fallback / gap repair。

Phase 5:
  WebSocket manager、combined stream、stream batching、stale、reconnect。

Phase 6:
  webhook server。

Phase 7:
  CLI 整合與模式切換。

Phase 8:
  validation / tests / acceptance checks。
```

不得一開始就直接寫完整長駐程式。  
必須先完成可驗證的小階段。

---

## 7. 每個 Phase 的交付格式

Agent 每完成一個 Phase，必須輸出：

```text
1. changed files
2. implemented items
3. skipped items with reason
4. validation commands
5. validation result
6. next phase blockers
```

若某項未完成，必須明確列出，不得用模糊語句帶過。

---

## 8. 最終完成條件

完成後需滿足：

```text
--interval all 會展開所有支援週期
WebSocket 是主要即時資料來源
REST 是 fallback / startup backfill / gap repair 來源
Webhook payload 會進入即時資料區
未收盤 KBar 只更新 buffer / latest
完整 KBar 會寫入 closed_buffer
完整 KBar 會進入 per-partition write queue
current historical dataset 透過 micro-batch flush 更新
state 只能在 current dataset flush 成功後更新
live_update.py 啟動時會根據 state 自動補洞
停機 1 天後重啟會自動補回缺少的 KBar
WebSocket 斷線或 stale 時會用 REST fallback 補洞
REST 429 / 418 / 5xx / timeout 需 backoff 處理
研究 agent 預設只讀 current historical dataset
closed_buffer 可作為 replay source 重建 current dataset
支援資料連續性檢查
支援 buffer retention / compression
```

---

## 9. 建議 commit

```bash
git add LIVE_UPDATE.md docs/live_update scripts/live_update.py
git commit -m "Add websocket-first live update current dataset runner"
```
