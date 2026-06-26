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

## 0. 目前狀態（v0.14.0）

> **先讀此區。** 本節說明 live update 目前實際完成的範圍，避免把規格
> 誤讀成「production long-running daemon 已完成」。

- **Live Update Phase 1~8 MVP primitives complete。** 各 Phase 對應：
  - Phase 1 — primitives（`KlineRecord`、路徑解析、base 資料結構）
  - Phase 2 — current historical dataset 初始化 + Parquet merge
  - Phase 3 — state 管理與 startup backfill 規劃
  - Phase 4 — REST backfill / fallback / gap repair
  - Phase 5 — WebSocket manager、combined stream、batching、stale、reconnect
  - Phase 6 — webhook server
  - Phase 7 — CLI 整合與模式切換
  - Phase 8 — continuity check / validation checks / tests
- **CLI skeleton and validation checks complete。** `scripts/live_update.py`
  已存在並支援 `--interval all|1m|3m|5m|15m|1h|4h|1d`、`--symbols`、`--once`、
  `--check-continuity`、`--describe-layout`、`--describe-websocket-connections`、
  `--describe-webhook-server` 等模式。`all` 僅為 CLI 展開語意，絕不傳給
  Binance API。
- **`--once` = one-shot live update cycle。** `--once` 代表 run one complete live
  update cycle and exit：resolve symbols（必填）、ensure current symbols from
  seed、跑一次 startup / REST gap repair（寫 closed_buffer、merge 進 current
  parquet、merge 成功後才更新 state），補到 latest closed KBar 後結束，輸出
  `once_update` JSON。與 `--run-startup-backfill-once` 共用同一核心流程；`--once`
  是 user-facing shorthand，`--run-startup-backfill-once` 是明確的 startup
  backfill one-shot 模式。未提供 `--symbols` 會明確 fail；seed 缺回
  bootstrap_required，不從 0 建歷史。
- **`--symbols` parsing。** 支援以下等價小範圍寫法：
  `--symbols BTCUSDT ETHUSDT`、`--symbols "BTCUSDT ETHUSDT"`、
  `--symbols BTCUSDT,ETHUSDT`。symbols 會 normalize 成大寫並去重。
  全市場用 `--symbols all`（Binance USD-M USDT 永續，透過
  `/fapi/v1/exchangeInfo` resolve，篩選 `status=TRADING` /
  `contractType=PERPETUAL` / `quoteAsset=USDT`，**不**使用 spot
  `/api/v3/exchangeInfo`）。全市場 smoke test 用
  `--symbols all --max-symbols 5`（先 resolve 全市場再截斷前 5 個）。
  `all` 只在 CLI expansion 使用，絕不當作 symbol 傳給 `/fapi/v1/klines`，
  也不會出現在 WebSocket stream name 或寫進 state / parquet / buffer。
  寫資料的模式（`--once`、`--run-startup-backfill-once`、預設 run）若未提供
  `--symbols` 會明確失敗，不會默默全市場。
  ⚠️ `--symbols all` 會增加 REST / WebSocket / IO 壓力，不建議一開始就搭配
  `--interval all` 全市場跑；新機器先小範圍驗收再擴大。
- **Partial current symbol missing 修復。** interval marker / parquet 存在不代表
  每個 symbol 都在。若 seed symbol 存在但 current symbol 缺失，這是 partial
  current dataset symbol missing，**不是** historical bootstrap missing。可用
  `--initialize-current-dataset --symbols ETHUSDT` 只修復指定 symbol（copy seed
  parquet 到 current，temp dir 後 rename，不覆蓋既有、不刪資料、不改 seed）。
  `--run-startup-backfill-once` 會在 REST backfill 前先做此修復，再補到 latest
  closed KBar；seed 缺時仍回 bootstrap_required。狀態：
  `bootstrap_required` / `initialized_current_symbol_from_seed` /
  `already_available`。
- **Current dataset canonical layout = year/month。** current parquet canonical
  layout 為 `symbol=<S>/year=<YYYY>/month=<MM>/part-000.parquet`（live update /
  gap repair / merge 的 canonical target）。historical seed 可以仍是 year-only；
  current initialization from seed 會讀 seed parquet、依 `open_time` 推導
  canonical year/month 重新寫出（temp dir 後 atomic rename），不把 year-only
  原樣 copy 進 current。若 current 同一 symbol 同時有 year-only 與 year/month
  parquet 即 mixed layout（DuckDB hive partition mismatch）。用
  `--audit-current-layout`（read-only，輸出 JSON：`year_only_file_count` /
  `year_month_file_count` / `mixed_symbol_count` / `mixed_symbols` / `status`）
  檢查。本次只提供 audit / dry-run plan，**不**自動 migration 既有舊資料。
- **Production long-running orchestration hardening pending。** Phase 1~8 是
  MVP primitives 與可測試 CLI skeleton，**不是** production-ready 長駐
  daemon。orchestration、retention manager、長時間全市場 all-interval 部署
  的可靠性驗證尚未完成。
- **Full-market all-interval long-running deployment 應在小範圍驗收後再
  測。** 先用單一 interval（如 `1m`）+ 少量 symbols（如 `BTCUSDT ETHUSDT`）
  跑 `--describe-*` / `--check-continuity` / `--once` 驗收，通過後再考慮擴大
  範圍。
- **Historical materialization 仍是穩定主線。** Phase 6~12 的 Binance USD-M
  Futures Kline Parquet materialization 不受 live update 影響，仍是既有穩定
  主線；live update 是在其之上的增量更新層。
- **Registry 未變。** `market.binance.um.klines.current` 是否註冊為正式
  derived dataset、`market.binance.um.klines.live_update` 是否僅為 runtime
  operational namespace，為 pending governance decision，尚未寫入
  `dataset_registry.json`。

### 不可違反的底線

```text
不得 commit local_data、parquet、jsonl runtime artifacts。
不得把 --interval all 當成 Binance API interval 傳給 REST / WebSocket。
不得讓未收盤 KBar 進入 closed_buffer 或 current historical dataset。
WebSocket / REST / webhook 三條 live route 必須共用同一套 Kbar validation。
state.last_closed_open_time 只能在 current dataset flush 成功後更新。
```

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
