# INIT.md

# crypto-data-hub 新機器初始化入口

本檔案是本 repo 在新電腦、新 VM、災難復原、`local_data` rebuild，以及
Live Update 初始化環境中的啟動入口。它**不只是 historical seed rebuild
文件**：三層資料（historical seed / current dataset / live update runtime）的
初始化與復原都從這裡導引。

適用情境：

    剛 clone repo
    repo 已存在但需要確認是否可用
    local_data 不存在
    local_data 無法從舊機器搬移
    需要從 Binance public archive 重新建立 Binance USD-M Futures kline 資料
    需要初始化 current dataset / live update runtime
    需要驗證重建後的 local_data 是否完整可用（見 INIT_VERIFY.md）

本檔案只負責初始化導引。

> **初始化後必須執行 `INIT_VERIFY.md`。** 任一機器初始化 / rebuild /
> migration / live-update tooling 變更後，必須逐項通過 `INIT_VERIFY.md`
> 驗收清單。`INIT_VERIFY.md` 有 critical failure 時：不得繼續 migration、
> 不得 push、不得讓 research agent 使用該資料。

真正的任務規格以以下兩份檔案為準：

    planning/tasks/task_rebuild_all_klines.md
    planning/tasks/task_rebuild_all_klines_verify.md

不要在本檔案中重複或重新解釋兩份 task 的完整規則。

不要重跑歷史 Phase task：

    task_v0.07
    task_v0.08
    task_v0.09
    task_v0.10
    task_v0.11
    task_v0.12
    task_v0.13

新機器 rebuild 只使用：

    planning/tasks/task_rebuild_all_klines.md
    planning/tasks/task_rebuild_all_klines_verify.md

---

## 1. Repo rules

執行本檔案前，必須先閱讀並遵守：

    ROOT.md

若存在以下檔案，也應閱讀：

    AGENTS.md
    HANDOFF.md

規則優先順序：

    ROOT.md
    AGENTS.md / HANDOFF.md
    DATA_CONTRACT.md（primary universe policy）
    INIT.md
    INIT_VERIFY.md（初始化驗收清單）
    planning/tasks/task_rebuild_all_klines.md
    planning/tasks/task_rebuild_all_klines_verify.md

若規則衝突：

    停止
    回報衝突內容
    等待使用者決策

---

## 2. 基本定位

DataHub 架構：

    raw zip archive = immutable source layer
    parquet = materialized query layer
    DuckDB = standard query engine
    local_data = local generated data, gitignored

Repo 只保存：

    code
    docs
    tests
    registry
    planning/tasks

Repo 不保存：

    local_data/
    raw zip archive
    parquet output
    rebuild logs

local_data 必須保持 untracked。

---

## 2b. 三層資料架構（Historical Seed / Current / Live Update Runtime）

`local_data/` 內有三層資料，初始化與復原都要理解其角色：

```text
Layer 1: Historical Seed
  local_data/binance_um_klines/interval=<INTERVAL>/parquet/

Layer 2: Current Dataset
  local_data/binance_um_klines_current/interval=<INTERVAL>/parquet/

Layer 3: Live Update Runtime
  local_data/live_update/binance_um_klines/interval=<INTERVAL>/
```

角色：

- **Historical seed** 用於 bootstrap / rebuild / recovery，是 current dataset
  與 live update 的來源；由 `task_rebuild_all_klines.md` 從 Binance public
  archive 重建。
- **Current dataset** 是 research / live_update merge 後的**主要讀取層**，
  research agent 預設只讀這一層。canonical layout 為
  `symbol=<S>/year=<YYYY>/month=<MM>/part-000.parquet`。
- **Live update runtime** 包含 state / jsonl / closed_buffer /
  data_quality_notes / latest / rejects 等運行時資料。**不進 git**
  （git-ignored runtime data）。

其他規則：

- research agents 必須以 **read-only mount** 使用 Data HUB；不得寫入 current
  dataset、live update runtime 或 historical seed（除非任務明確要求 audit /
  debug / replay）。
- Live Update 必須遵守 **primary universe policy**（見下一節）。
- 三層資料皆在 `local_data/` 之下，**永不 commit**。

---

## 2c. Primary universe policy

本專案真正的 primary research / trading universe：

```text
venue       = Binance USDⓈ-M Futures
contract    = PERPETUAL
quote_asset = USDT
history     = include delisted USDT perpetual contracts（歷史研究用）
```

不屬於 primary universe（normal primary flow 排除）：

```text
quote_asset != USDT
USDC quote pairs（KAITOUSDC、BTCUSDC、SOLUSDC、DOGEUSDC）
BUSD quote pairs
delivery contracts（BTCUSDT_230630）
SETTLED symbols（CVXUSDTSETTLED）
non-ASCII symbols（龙虾USDT、币安人生USDT）
```

> Binance UM / USDⓈ-M Futures **不等於只有 USDT pairs**；它也含 USDC / BUSD
> quote pairs 與 delivery / settled / special symbols。**不要把所有 Binance UM
> symbols 當成 primary universe。** 完整定義見 `DATA_CONTRACT.md` →
> *Primary Universe Policy*。

非 primary 資料若已存在於 `local_data/`，採 inventory / quarantine，
**不得直接刪除、不得自動修復**。`KAITOUSDC` 是 known quarantined symbol
（USDC quote pair + corrupt source parquet），詳見 `INIT_VERIFY.md`。

### 標準 migration planner command

current dataset layout migration 規劃使用標準命令（read-only，dry-run；
`--quote-assets USDT` 已實作，僅影響 batch planner candidate filtering）：

```bash
.venv/bin/python scripts/live_update.py \
  --interval 1m \
  --plan-current-layout-migration-batches \
  --batch-size 10 \
  --max-row-count 300000 \
  --max-batches 2 \
  --quote-assets USDT \
  --exclude-delivery-contracts \
  --exclude-settled \
  --exclude-non-ascii \
  --exclude-symbols BTCUSDT ETHUSDT KAITOUSDC \
  --dry-run-batches
```

> `--quote-assets USDT` 已實作，只影響 current layout migration batch planner
> candidate filtering，不影響 live daemon / `--once` / startup backfill。
> 支援 `--quote-assets USDT`、`--quote-assets USDT,USDC`、
> `--quote-assets "USDT USDC"`；第一版依 symbol suffix 偵測 `USDT` / `USDC` /
> `BUSD`。delivery contracts 仍需用 `--exclude-delivery-contracts` 排除；quote
> mismatch 會出現在 `excluded.quote_asset_mismatch`，生效 filter 會出現在
> `filters.quote_assets`。`KAITOUSDC` 仍是 known quarantined symbol，不重跑、不修復、不刪除。
> migration 驗收見 `INIT_VERIFY.md` → *Migration planner verification*。

---

## 3. Clone repo

目標路徑：

    ~/work/crypto-data-hub

若 repo 不存在：

    mkdir -p ~/work
    cd ~/work
    git clone https://github.com/ChingFuHan/crypto-data-hub.git
    cd crypto-data-hub

若 repo 已存在：

    cd ~/work/crypto-data-hub
    git status --short

若工作樹乾淨：

    git pull --ff-only

若工作樹不乾淨：

    停止
    回報 git status --short
    等待使用者決策

檢查 repo 狀態：

    git log --oneline -5
    cat VERSION
    git status --short
    git ls-files local_data | wc -l

預期：

    VERSION >= v0.13.0
    git status 沒有非預期變更
    git ls-files local_data = 0

必要 task 檔案：

    planning/tasks/task_rebuild_all_klines.md
    planning/tasks/task_rebuild_all_klines_verify.md

若必要 task 檔案不存在：

    停止
    回報缺失檔案
    不自行建立替代 task

---

## 4. 長任務執行環境

完整 rebuild 是長時間任務，建議在 tmux 或 screen 中執行。

建議：

    tmux new -s datahub_rebuild

若 session 中斷：

    回到 repo
    檢查 local_data 狀態
    依 task policy 決定 resume / verify existing / rebuild to another path

不要在未確認狀態前直接重跑完整 rebuild。

---

## 5. Python 環境

建立並啟用 virtual environment：

    python3 -m venv .venv
    source .venv/bin/activate

更新 pip：

    python -m pip install --upgrade pip

安裝 runtime 依賴。依賴宣告在 repo 根目錄 `requirements.txt`
（目前流程只需要 `duckdb` 與 `pyarrow`；其餘皆使用 Python 標準函式庫）：

    python -m pip install -r requirements.txt

若日後 repo 改用 `pyproject.toml` 或 `setup.py` packaging：

    python -m pip install -e .

若連 `requirements.txt` 都不存在（非預期狀態）：

    停止
    回報 repo 根目錄檔案列表
    不自行建立 dependency file

初始化階段不要新增 dependency file（依賴宣告由 repo 維護者在正常開發流程更新，
不在新機器初始化時新增）。

---

## 6. Preflight checks

執行：

    df -h .
    df -i .

    python -m unittest discover tests

    python -m datahub.ingestion.binance_um_klines --help
    python -m datahub.materialization.binance_um_klines_parquet --help
    python -m datahub.validation --help

遇到以下情況時停止並回報：

    disk free < 120G
    inode 可用量不安全
    unit tests failed
    必要 CLI 無法執行

建議磁碟空間：

    free disk >= 300G
    free disk >= 400G 更安全

完整 rebuild 可能產生超過 140G 的 local_data，且可能執行數小時。

---

## 7. 執行 rebuild task

Preflight 通過後，讀取並執行：

    planning/tasks/task_rebuild_all_klines.md

必須完整遵守該 task。

預期 interval 順序：

    1d
    4h
    1h
    15m
    5m
    3m
    1m

每個 interval 的預期流程：

    raw ingestion
    raw validation
    parquet materialization
    parquet validation

在 rebuild 執行期間不要修改：

    code
    docs
    tests
    registry
    VERSION

不要 commit。
不要 push。

若 local_data 不存在：

    fresh rebuild

若 local_data 已存在且看似完整：

    停止
    回報 inventory
    等待使用者選擇：
      verify existing
      resume
      rebuild to another path

若 local_data 已存在但狀態不明：

    停止
    回報 inventory
    等待使用者決策

Inventory report 建議包含：

    du -sh local_data 2>/dev/null || true
    find local_data/binance_um_klines -maxdepth 2 -type d 2>/dev/null | sort | head -100
    find local_data/binance_um_klines -name manifest.json 2>/dev/null | sort
    find local_data/binance_um_klines -name materialization_manifest.json 2>/dev/null | sort

---

## 8. 執行 verify task

rebuild 完成後，讀取並執行：

    planning/tasks/task_rebuild_all_klines_verify.md

執行完整驗證：

    raw manifest checks
    raw validation
    parquet manifest checks
    parquet validation
    DuckDB smoke checks
    unit tests
    validation --all
    git safety
    disk report

注意 validation 語意：

    python -m datahub.validation --all 是 clone-safe global validation
    （registry / governance + 若本地存在則驗證 kline manifest）。
    它「不」代表已驗證所有 interval 的 local_data。
    完整 all-interval local_data 驗證必須依照
    planning/tasks/task_rebuild_all_klines_verify.md，逐 interval（raw + parquet）
    明確檢查。不要把 validation --all 的通過當成全 interval 已驗證。

Resume idempotency policy：

    只有在使用者明確授權時才執行 resume idempotency
    未授權時標記 resume_idempotency_status = SKIPPED_BY_USER_POLICY
    不因為使用者未授權 resume idempotency 而 REJECT

---

## 9. Final report

最後輸出兩份報告：

    rebuild report
    verify report

Final report 必須包含：

    Summary
    Environment
    Raw Rebuild
    Parquet Rebuild
    Raw Validation
    Parquet Validation
    DuckDB Smoke
    Global Validation
    Git Safety
    Disk
    Problems
    Final Decision

可接受結果：

    ACCEPT
    ACCEPT_WITH_WARNINGS

以下情況判定 REJECT：

    task_rebuild_all_klines.md 規定 REJECT
    task_rebuild_all_klines_verify.md 規定 REJECT

---

## 10. Hard limits

在 init / rebuild / verify 期間：

    不修改 code
    不修改 VERSION
    不修改 registry
    不新增 Phase
    不重跑歷史 Phase task
    不 commit
    不 push
    不 overwrite local_data，除非 task policy 允許
    不執行 resume idempotency，除非使用者明確授權

在 rebuild / verify 執行期間不要修改：

    code
    docs
    tests
    registry
    VERSION

local_data 必須保持 untracked：

    git ls-files local_data | wc -l

預期結果：

    0

---

## 11. 最簡使用方式

在新電腦的 agent 裡直接下：

    請先讀 repo 根目錄的 INIT.md。
    依照 INIT.md 完成新機器初始化。
    接著執行 planning/tasks/task_rebuild_all_klines.md。
    rebuild 完成後執行 planning/tasks/task_rebuild_all_klines_verify.md。
    不要修改 code/docs/VERSION/registry。
    不要 commit，不要 push。
    最後輸出 rebuild report + verify report。

---

## 12. 收尾檢查

任務結束後執行：

    git status --short
    git ls-files local_data | wc -l
    df -h .
    df -i .

預期：

    git status 沒有非預期 code/docs/test/registry 變更
    git ls-files local_data = 0
    disk free 仍在安全範圍
    rebuild report = ACCEPT 或 ACCEPT_WITH_WARNINGS
    verify report = ACCEPT 或 ACCEPT_WITH_WARNINGS
